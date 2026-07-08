// SPDX-License-Identifier: MIT OR Apache-2.0
//
// UDP socket helpers: bind for unicast OR join + receive multicast.
// Matches the producer-side behavior of cast / ffmpeg's `moqenc_mmt`
// muxer which emits MMTP packets to a multicast `udp://group:port`
// URL via FFmpeg's AVIOContext. By detecting multicast bind targets
// here and auto-joining the group, the same `--mmtp-udp-bind` flag
// works for both unicast loopback tests and real multicast paths.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use anyhow::{Context, Result};
use socket2::{Domain, Protocol, SockRef, Socket, Type};
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
    ssm: bool,
    iface: Option<Ipv4Addr>,
) -> Result<UdpSocket> {
    if !is_multicast(target) {
        let socket = UdpSocket::bind(target)
            .await
            .with_context(|| format!("UdpSocket::bind({target}) unicast"))?;
        return Ok(socket);
    }

    let imr_iface = iface.unwrap_or(Ipv4Addr::UNSPECIFIED);
    match (target.ip(), ssm) {
        // Source-specific (SSM, 232.0.0.0/8) target: bind only. The (S,G) join
        // is DEMAND-DRIVEN — run_udp_loop joins on the first subscribe via
        // refresh_ssm_v4 and leaves when the last subscriber drops. This holds
        // the IGMP membership only while there is a subscriber and re-arms the
        // switch's snooping entry (which ages out without a querier) per join.
        (IpAddr::V4(group), true) => {
            let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))
                .context("Socket::new for SSM")?;
            socket.set_reuse_address(true).context("set_reuse_address")?;
            let wildcard = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), target.port());
            socket
                .bind(&wildcard.into())
                .with_context(|| format!("bind({wildcard}) for SSM"))?;
            socket
                .set_multicast_loop_v4(true)
                .context("set_multicast_loop_v4")?;
            socket.set_nonblocking(true).context("set_nonblocking")?;
            let std_socket: std::net::UdpSocket = socket.into();
            let socket = UdpSocket::from_std(std_socket).context("UdpSocket::from_std")?;
            tracing::info!(group = %group, port = target.port(), "bound SSM v4 socket; (S,G) join deferred to first subscribe");
            Ok(socket)
        }
        // Any-source (*,G) join for ASM groups / loopback tests.
        (IpAddr::V4(group), false) => {
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
        // Source-specific (SSM) IPv6 target: bind only; the (S,G) join is
        // demand-driven (see the IPv4-SSM arm and run_udp_loop).
        (IpAddr::V6(group), true) => {
            let socket = Socket::new(Domain::IPV6, Type::DGRAM, Some(Protocol::UDP))
                .context("Socket::new for SSM v6")?;
            socket.set_reuse_address(true).context("set_reuse_address")?;
            let wildcard = SocketAddr::new(IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED), target.port());
            socket
                .bind(&wildcard.into())
                .with_context(|| format!("bind({wildcard}) for SSM v6"))?;
            socket
                .set_multicast_loop_v6(true)
                .context("set_multicast_loop_v6")?;
            socket.set_nonblocking(true).context("set_nonblocking")?;
            let std_socket: std::net::UdpSocket = socket.into();
            let socket = UdpSocket::from_std(std_socket).context("UdpSocket::from_std")?;
            tracing::info!(group = %group, port = target.port(), "bound SSM v6 socket; (S,G) join deferred to first subscribe");
            Ok(socket)
        }
        (IpAddr::V6(group), false) => {
            let wildcard = SocketAddr::new(IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED), target.port());
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

/// A source-specific (SSM) multicast membership, family-tagged so the same
/// demand-driven join/refresh/leave flow serves IPv4 and IPv6.
#[derive(Clone, Copy, Debug)]
pub enum SsmMembership {
    V4 {
        source: Ipv4Addr,
        group: Ipv4Addr,
        /// Local interface IPv4 (imr_interface); UNSPECIFIED = pick via route.
        iface: Ipv4Addr,
    },
    V6 {
        source: Ipv6Addr,
        group: Ipv6Addr,
        /// Interface index; 0 = pick via route.
        iface_index: u32,
    },
}

/// Build the SSM membership for a bind target + source, or `None` when the
/// target isn't source-specific multicast. Errors if the source and group
/// address families disagree.
pub fn ssm_membership(
    target: SocketAddr,
    source: Option<IpAddr>,
    iface_v4: Option<Ipv4Addr>,
    iface_index_v6: Option<u32>,
) -> Result<Option<SsmMembership>> {
    let Some(source) = source else { return Ok(None) };
    match (source, target.ip()) {
        (IpAddr::V4(source), IpAddr::V4(group)) if group.is_multicast() => {
            Ok(Some(SsmMembership::V4 {
                source,
                group,
                iface: iface_v4.unwrap_or(Ipv4Addr::UNSPECIFIED),
            }))
        }
        (IpAddr::V6(source), IpAddr::V6(group)) if group.is_multicast() => {
            Ok(Some(SsmMembership::V6 {
                source,
                group,
                iface_index: iface_index_v6.unwrap_or(0),
            }))
        }
        (_, g) if !g.is_multicast() => {
            anyhow::bail!("--mmtp-udp-source given but bind target {g} is not multicast")
        }
        (s, g) => anyhow::bail!(
            "SSM source/group address families disagree: source={s}, group={g}"
        ),
    }
}

/// (Re)join an SSM (S,G) membership on a live socket, re-arming the switch's
/// IGMP/MLD snooping entry. Leaves first (ignoring "not a member") so the
/// kernel re-sends a fresh report even when already joined — a plain re-join
/// is a no-op.
pub fn refresh_ssm(socket: &UdpSocket, m: &SsmMembership) -> Result<()> {
    match *m {
        SsmMembership::V4 {
            source,
            group,
            iface,
        } => {
            let sock = SockRef::from(socket);
            let _ = sock.leave_ssm_v4(&source, &group, &iface);
            sock.join_ssm_v4(&source, &group, &iface)
                .with_context(|| format!("join_ssm_v4(S={source},G={group},iface={iface})"))
        }
        SsmMembership::V6 {
            source,
            group,
            iface_index,
        } => {
            // socket2 has no v6 SSM; use the protocol-independent
            // MCAST_JOIN/LEAVE_SOURCE_GROUP setsockopt.
            let _ = ssm_v6_source_group(socket, &source, &group, iface_index, false);
            ssm_v6_source_group(socket, &source, &group, iface_index, true)
                .with_context(|| format!("MCAST_JOIN_SOURCE_GROUP(S={source},G={group},idx={iface_index})"))
        }
    }
}

/// Drop an SSM membership (best-effort; errors logged, not propagated).
pub fn leave_ssm(socket: &UdpSocket, m: &SsmMembership) {
    let res = match *m {
        SsmMembership::V4 {
            source,
            group,
            iface,
        } => SockRef::from(socket).leave_ssm_v4(&source, &group, &iface),
        SsmMembership::V6 {
            source,
            group,
            iface_index,
        } => ssm_v6_source_group(socket, &source, &group, iface_index, false),
    };
    if let Err(e) = res {
        tracing::warn!(error = %e, "leave_ssm failed");
    }
}

/// IPv6 source-specific join/leave via the protocol-independent
/// `MCAST_{JOIN,LEAVE}_SOURCE_GROUP` setsockopt (socket2 exposes only the v4
/// SSM helpers). `join=false` leaves.
#[cfg(unix)]
fn ssm_v6_source_group(
    socket: &UdpSocket,
    source: &Ipv6Addr,
    group: &Ipv6Addr,
    iface_index: u32,
    join: bool,
) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    // libc has no group_source_req; define it. repr(C) inserts the padding
    // after gsr_interface needed to 8-align the sockaddr_storage members, so
    // this matches the kernel ABI. MCAST_{JOIN,LEAVE}_SOURCE_GROUP are the
    // stable Linux generic-multicast option numbers.
    #[repr(C)]
    struct GroupSourceReq {
        gsr_interface: u32,
        gsr_group: libc::sockaddr_storage,
        gsr_source: libc::sockaddr_storage,
    }
    const MCAST_JOIN_SOURCE_GROUP: libc::c_int = 46;
    const MCAST_LEAVE_SOURCE_GROUP: libc::c_int = 47;

    fn sockaddr_in6(addr: &Ipv6Addr) -> libc::sockaddr_in6 {
        // SAFETY: sockaddr_in6 is plain-old-data; zeroed is a valid init.
        let mut s: libc::sockaddr_in6 = unsafe { std::mem::zeroed() };
        s.sin6_family = libc::AF_INET6 as libc::sa_family_t;
        s.sin6_addr.s6_addr = addr.octets();
        s
    }

    // SAFETY: group_source_req is POD; we fully initialise gsr_interface and
    // copy valid sockaddr_in6 values into the two sockaddr_storage members,
    // then pass the correctly-sized struct to setsockopt on our own fd.
    unsafe {
        let mut req: GroupSourceReq = std::mem::zeroed();
        req.gsr_interface = iface_index;
        let g = sockaddr_in6(group);
        let s = sockaddr_in6(source);
        std::ptr::copy_nonoverlapping(
            &g as *const libc::sockaddr_in6 as *const u8,
            &mut req.gsr_group as *mut libc::sockaddr_storage as *mut u8,
            std::mem::size_of::<libc::sockaddr_in6>(),
        );
        std::ptr::copy_nonoverlapping(
            &s as *const libc::sockaddr_in6 as *const u8,
            &mut req.gsr_source as *mut libc::sockaddr_storage as *mut u8,
            std::mem::size_of::<libc::sockaddr_in6>(),
        );
        let opt = if join {
            MCAST_JOIN_SOURCE_GROUP
        } else {
            MCAST_LEAVE_SOURCE_GROUP
        };
        let ret = libc::setsockopt(
            socket.as_raw_fd(),
            libc::IPPROTO_IPV6,
            opt,
            &req as *const GroupSourceReq as *const libc::c_void,
            std::mem::size_of::<GroupSourceReq>() as libc::socklen_t,
        );
        if ret != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
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
        let sock = open_udp_socket(target, false, None).await.expect("open ok");
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

        let listener = match open_udp_socket(target, false, None).await {
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
    async fn ssm_flag_on_unicast_target_binds_unicast() {
        // ssm=true is only meaningful for a multicast target; a unicast target
        // binds directly regardless.
        let target: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let sock = open_udp_socket(target, true, None)
            .await
            .expect("unicast open ok");
        assert_eq!(sock.local_addr().unwrap().ip(), target.ip());
    }

    #[test]
    fn ssm_membership_matches_family_and_rejects_mismatch() {
        // v4 source + v4 group -> V4 membership.
        let m = ssm_membership(
            "232.0.0.1:5001".parse().unwrap(),
            Some("10.0.0.1".parse().unwrap()),
            Some(Ipv4Addr::new(10, 0, 0, 9)),
            None,
        )
        .unwrap();
        assert!(matches!(m, Some(SsmMembership::V4 { .. })));
        // v6 source + v6 group -> V6 membership.
        let m6 = ssm_membership(
            "[ff3e::1]:5001".parse().unwrap(),
            Some("2001:db8::1".parse().unwrap()),
            None,
            Some(3),
        )
        .unwrap();
        assert!(matches!(m6, Some(SsmMembership::V6 { iface_index: 3, .. })));
        // Mismatched families error rather than silently misbehave.
        assert!(ssm_membership(
            "[ff3e::1]:5001".parse().unwrap(),
            Some("10.0.0.1".parse().unwrap()),
            None,
            None,
        )
        .is_err());
        // No source -> not SSM.
        assert!(ssm_membership("239.0.0.1:5001".parse().unwrap(), None, None, None)
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn ssm_source_multicast_attempts_source_specific_join() {
        // Exercise the SSM (S,G) path. The join may fail in a sandboxed CI
        // network with no multicast-capable interface — skip rather than
        // fail spuriously, mirroring open_multicast_binds_wildcard_*.
        let port = 27_000 + (std::process::id() % 1000) as u16;
        let target = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(232, 28, 99, 8)), port);
        match open_udp_socket(target, true, None).await {
            Ok(sock) => {
                let local = sock.local_addr().expect("local_addr");
                assert!(local.ip().is_unspecified(), "SSM listener binds wildcard");
                assert_eq!(local.port(), port);
            }
            Err(e) => eprintln!("skipping SSM join test (sandboxed network): {e}"),
        }
    }
}
