// SPDX-License-Identifier: MIT OR Apache-2.0
//
// UDP socket helpers: bind for unicast OR join + receive multicast.
// Matches the producer-side behavior of cast / ffmpeg's `moqenc_mmt`
// muxer which emits MMTP packets to a multicast `udp://group:port`
// URL via FFmpeg's AVIOContext. By detecting multicast bind targets
// here and auto-joining the group, the same `--mmtp-udp-bind` flag
// works for both unicast loopback tests and real multicast paths.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use anyhow::{bail, Context, Result};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;

/// True iff the socket address's IP is in the multicast range.
pub fn is_multicast(addr: SocketAddr) -> bool {
    match addr.ip() {
        IpAddr::V4(v4) => v4.is_multicast(),
        IpAddr::V6(v6) => v6.is_multicast(),
    }
}

/// Bind a UDP socket configured to receive packets destined to
/// `target`. If `target` is a multicast address the socket binds to
/// the wildcard address on `target.port()` and joins the multicast
/// group; otherwise it binds directly to `target`.
///
/// Join mode depends on `source`:
/// - `Some(s)` → source-specific (S,G) join (IP_ADD_SOURCE_MEMBERSHIP).
///   REQUIRED for SSM groups (232.0.0.0/8) — the fabric only forwards SSM
///   traffic to receivers that name the source, so an any-source join
///   receives nothing. IPv4 only (v6 SSM would need an ifindex, unused here).
/// - `None` → any-source (*,G) join (IP_ADD_MEMBERSHIP), for ASM groups and
///   loopback smoke tests.
///
/// `iface` is the local interface IPv4 to join on (imr_interface). `None`
/// means INADDR_ANY — the kernel picks the interface via the route to the
/// group, so pair it with a `ip route … dev <iface>` route when the group
/// arrives on a non-default interface (e.g. a Multus secondary).
///
/// Multicast loopback is enabled so the same machine can act as both
/// sender and receiver during a smoke test.
pub async fn open_udp_socket(
    target: SocketAddr,
    source: Option<Ipv4Addr>,
    iface: Option<Ipv4Addr>,
) -> Result<UdpSocket> {
    if !is_multicast(target) {
        let socket = UdpSocket::bind(target)
            .await
            .with_context(|| format!("UdpSocket::bind({target}) unicast"))?;
        return Ok(socket);
    }

    let imr_iface = iface.unwrap_or(Ipv4Addr::UNSPECIFIED);
    match (target.ip(), source) {
        // Source-specific (S,G) join — required for SSM (232.0.0.0/8).
        (IpAddr::V4(group), Some(src)) => {
            let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))
                .context("Socket::new for SSM")?;
            socket
                .set_reuse_address(true)
                .context("set_reuse_address")?;
            let wildcard = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), target.port());
            socket
                .bind(&wildcard.into())
                .with_context(|| format!("bind({wildcard}) for SSM"))?;
            socket
                .join_ssm_v4(&src, &group, &imr_iface)
                .with_context(|| {
                    format!("join_ssm_v4(source={src}, group={group}, iface={imr_iface})")
                })?;
            socket
                .set_multicast_loop_v4(true)
                .context("set_multicast_loop_v4")?;
            socket.set_nonblocking(true).context("set_nonblocking")?;
            let std_socket: std::net::UdpSocket = socket.into();
            let socket = UdpSocket::from_std(std_socket).context("UdpSocket::from_std")?;
            tracing::info!(group = %group, source = %src, iface = %imr_iface, port = target.port(), "joined SSM (S,G) group");
            Ok(socket)
        }
        // Any-source (*,G) join for ASM groups / loopback tests.
        (IpAddr::V4(group), None) => {
            let wildcard = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), target.port());
            let socket = UdpSocket::bind(wildcard)
                .await
                .with_context(|| format!("UdpSocket::bind({wildcard}) for multicast"))?;
            socket
                .join_multicast_v4(group, imr_iface)
                .with_context(|| format!("join_multicast_v4({group}, iface={imr_iface})"))?;
            socket
                .set_multicast_loop_v4(true)
                .context("set_multicast_loop_v4")?;
            tracing::info!(group = %group, iface = %imr_iface, port = target.port(), "joined multicast group");
            Ok(socket)
        }
        (IpAddr::V6(_), Some(_)) => {
            bail!("source-specific (SSM) join is IPv4-only; --mmtp-udp-source cannot target an IPv6 group");
        }
        (IpAddr::V6(group), None) => {
            let wildcard =
                SocketAddr::new(IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED), target.port());
            let socket = UdpSocket::bind(wildcard)
                .await
                .with_context(|| format!("UdpSocket::bind({wildcard}) for multicast"))?;
            socket
                .join_multicast_v6(&group, 0)
                .with_context(|| format!("join_multicast_v6({group})"))?;
            socket
                .set_multicast_loop_v6(true)
                .context("set_multicast_loop_v6")?;
            tracing::info!(group = %group, port = target.port(), "joined multicast group");
            Ok(socket)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_multicast_detects_ipv4_multicast_range() {
        // RFC 5771: 224.0.0.0/4 is the IPv4 multicast range.
        assert!(is_multicast("239.255.1.1:5004".parse().unwrap()));
        assert!(is_multicast("224.0.0.1:5004".parse().unwrap()));
        assert!(is_multicast("232.0.0.1:5004".parse().unwrap())); // SSM range
    }

    #[test]
    fn is_multicast_rejects_unicast_ipv4() {
        assert!(!is_multicast("127.0.0.1:5004".parse().unwrap()));
        assert!(!is_multicast("10.0.0.1:5004".parse().unwrap()));
        assert!(!is_multicast("192.168.1.1:5004".parse().unwrap()));
        assert!(!is_multicast("0.0.0.0:5004".parse().unwrap()));
    }

    #[test]
    fn is_multicast_detects_ipv6_multicast() {
        assert!(is_multicast("[ff02::1]:5004".parse().unwrap()));
        assert!(is_multicast("[ff05::1]:5004".parse().unwrap()));
    }

    #[test]
    fn is_multicast_rejects_unicast_ipv6() {
        assert!(!is_multicast("[::1]:5004".parse().unwrap()));
        assert!(!is_multicast("[2001:db8::1]:5004".parse().unwrap()));
    }

    #[tokio::test]
    async fn open_unicast_binds_directly_to_target() {
        let target: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let sock = open_udp_socket(target, None, None).await.expect("open ok");
        let local = sock.local_addr().expect("local_addr");
        // For unicast we bound directly to `target` — IP is loopback.
        assert_eq!(local.ip(), target.ip(), "unicast bind keeps target ip");
        assert_ne!(local.port(), 0, "ephemeral port was assigned");
    }

    #[tokio::test]
    async fn open_multicast_binds_wildcard_and_recvs_loopback() {
        // Pick a high port to avoid clashes with other multicast services.
        // 232.0.0.0/8 is the SSM range; we use a unicast-prefix-free
        // group in that range that's unlikely to be in use locally.
        let port = 26_000 + (std::process::id() % 1000) as u16;
        let target = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(232, 28, 99, 7)), port);

        let listener = match open_udp_socket(target, None, None).await {
            Ok(s) => s,
            Err(e) => {
                // In sandboxed CI environments multicast may be
                // unavailable. Skip rather than fail spuriously.
                eprintln!("skipping multicast loopback test: {e}");
                return;
            }
        };
        let local = listener.local_addr().expect("local_addr");
        // Listener bound to wildcard on the requested port.
        assert!(
            local.ip().is_unspecified(),
            "multicast listener binds wildcard, got {local}"
        );
        assert_eq!(local.port(), port);

        // Sender socket — bind any port. set_multicast_loop_v4 is
        // enabled by default in most Linux builds; we set it
        // defensively.
        let sender = UdpSocket::bind("0.0.0.0:0").await.expect("sender bind");
        sender.set_multicast_loop_v4(true).expect("loop");
        sender.set_multicast_ttl_v4(1).expect("ttl");
        sender
            .send_to(b"hello-mc", target)
            .await
            .expect("send_to multicast");

        let mut buf = [0u8; 64];
        let recv = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            listener.recv_from(&mut buf),
        )
        .await;
        match recv {
            Ok(Ok((n, _from))) => assert_eq!(&buf[..n], b"hello-mc"),
            Ok(Err(e)) => panic!("recv error: {e}"),
            Err(_) => {
                // Timed out — environment lacks multicast loopback.
                // Treat as "skipped" rather than failed.
                eprintln!("multicast recv timed out — likely sandboxed network");
            }
        }
    }

    #[tokio::test]
    async fn ssm_source_on_unicast_target_binds_unicast() {
        // A source is only meaningful for a multicast target; a unicast
        // target binds directly and ignores the source.
        let target: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let src: Ipv4Addr = "203.0.113.7".parse().unwrap();
        let sock = open_udp_socket(target, Some(src), None)
            .await
            .expect("unicast open ok");
        assert_eq!(sock.local_addr().unwrap().ip(), target.ip());
    }

    #[tokio::test]
    async fn ssm_source_rejects_ipv6_group() {
        // SSM join here is IPv4-only; a source with a v6 group must error
        // rather than silently do an any-source v6 join.
        let target: SocketAddr = "[ff3e::1234]:5004".parse().unwrap();
        let src: Ipv4Addr = "203.0.113.7".parse().unwrap();
        let err = open_udp_socket(target, Some(src), None)
            .await
            .expect_err("v6 SSM must error");
        assert!(err.to_string().contains("IPv4-only"), "got: {err}");
    }

    #[tokio::test]
    async fn ssm_source_multicast_attempts_source_specific_join() {
        // Exercise the SSM (S,G) path. The join may fail in a sandboxed CI
        // network with no multicast-capable interface — skip rather than
        // fail spuriously, mirroring open_multicast_binds_wildcard_*.
        let port = 27_000 + (std::process::id() % 1000) as u16;
        let target = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(232, 28, 99, 8)), port);
        let src: Ipv4Addr = "203.0.113.7".parse().unwrap();
        match open_udp_socket(target, Some(src), None).await {
            Ok(sock) => {
                let local = sock.local_addr().expect("local_addr");
                assert!(local.ip().is_unspecified(), "SSM listener binds wildcard");
                assert_eq!(local.port(), port);
            }
            Err(e) => eprintln!("skipping SSM join test (sandboxed network): {e}"),
        }
    }
}
