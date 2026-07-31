// SPDX-License-Identifier: MIT OR Apache-2.0
//
// Deterministic MMTP packet generator for the M.1 smoke test.
//
// Emits a stream of length-prefixed MMTP packets (matching the framing
// expected by `moq-pub-mmtp --mmtp-input stdin`) for two packet_ids:
//   1 = "video" (simulated)
//   2 = "audio" (simulated)
//
// Each MPU group has one Init packet at MPU sequence N. Each packet
// carries a tiny deterministic payload `b"<track>:<mpu_seq>"` so the
// expected per-track byte stream is predictable.
//
// Also writes the expected per-track byte stream (concatenation of
// the MMTP packets that landed on that track) to
// `<output-dir>/expected-{1,2}.bin`. The M.1 smoke compares these
// against `moq-sub-raw`'s per-track output files via sha256.
//
// Usage (UDP, lets the publisher start independently):
//   moq-pub-mmtp --mmtp-input udp --mmtp-udp-bind 127.0.0.1:5004 ... &
//   cargo run --release --example synth_mmtp -- \
//       --output-dir /tmp/m1-smoke --groups 8 --udp 127.0.0.1:5004
//
// Usage (stdin fallback):
//   cargo run --release --example synth_mmtp -- --output-dir /tmp/m1-smoke --groups 8 \
//     | moq-pub-mmtp --mmtp-input stdin --catalog-json catalog.json --name smoke URL

use std::io::Write;
use std::net::{SocketAddr, UdpSocket};
use std::path::PathBuf;

use bytes::BufMut;
use clap::Parser;
use mmt_core::header::{FragmentType, MfuDataUnit, MmtpHeader, MpuHeader, PacketType};

/// `synth_mmtp` CLI.
#[derive(Parser)]
struct Args {
    /// Directory to write expected per-track files to.
    #[arg(long, value_name = "DIR")]
    output_dir: PathBuf,

    /// Number of MPU groups to emit per track.
    #[arg(long, default_value = "8")]
    groups: u32,

    /// Sleep between successive packets in milliseconds. Each MPU
    /// becomes its own MoQ subgroup, and `SubgroupsReader` only
    /// surfaces the latest subgroup — pacing the emission lets the
    /// subscriber drain each subgroup before the next supersedes.
    #[arg(long = "packet-delay-ms", default_value = "50")]
    packet_delay_ms: u64,

    /// Optional UDP destination (one datagram per MMTP packet, no
    /// length prefix). When set, packets are sent here instead of
    /// to stdout — this lets the publisher run on
    /// `--mmtp-input udp` and start independently of this process.
    #[arg(long, value_name = "ADDR:PORT")]
    udp: Option<SocketAddr>,

    /// Number of MFU fragments to emit after each Init packet. Values of two or
    /// more exercise the FI=1/2/3 raw-passthrough path.
    #[arg(long, default_value = "0", value_name = "N")]
    fragment: u8,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    std::fs::create_dir_all(&args.output_dir)?;

    let mut expected_video = std::fs::File::create(args.output_dir.join("expected-1.bin"))?;
    let mut expected_audio = std::fs::File::create(args.output_dir.join("expected-2.bin"))?;
    let mut stdout = std::io::stdout().lock();
    let delay = std::time::Duration::from_millis(args.packet_delay_ms);

    let udp_sink = match args.udp {
        Some(target) => {
            let sock = UdpSocket::bind("0.0.0.0:0")?;
            if target.ip().is_multicast() {
                // Default loop is on on Linux; set defensively.
                sock.set_multicast_loop_v4(true)?;
                sock.set_multicast_ttl_v4(1)?;
            }
            Some((sock, target))
        }
        None => None,
    };

    for mpu_seq in 0..args.groups {
        for (packet_id, expected_sink) in [(1u16, &mut expected_video), (2u16, &mut expected_audio)]
        {
            let packets = if args.fragment == 0 {
                vec![build_init_packet(packet_id, mpu_seq)]
            } else {
                build_fragmented_mpu_sequence(packet_id, mpu_seq, args.fragment)
            };
            for packet in packets {
                match &udp_sink {
                    Some((sock, target)) => {
                        sock.send_to(&packet, target)?;
                    }
                    None => {
                        // Length-prefix framing (per moq-pub-mmtp::framing).
                        let prefix = (packet.len() as u32).to_be_bytes();
                        stdout.write_all(&prefix)?;
                        stdout.write_all(&packet)?;
                        stdout.flush()?;
                    }
                }
                // Expected per-track byte stream: raw packets only (no
                // length prefix or UDP framing — what lands on the wire
                // as MoQ object payloads per track).
                expected_sink.write_all(&packet)?;
                if args.packet_delay_ms > 0 {
                    std::thread::sleep(delay);
                }
            }
        }
    }
    Ok(())
}

/// Build one valid MPU Init packet for `packet_id` at `mpu_seq`.
fn build_init_packet(packet_id: u16, mpu_seq: u32) -> Vec<u8> {
    let mut hdr = MmtpHeader::new(packet_id, PacketType::Mpu);
    hdr.rap_flag = mpu_seq == 0; // First MPU of each track is the RAP.
    hdr.packet_sequence = mpu_seq;

    let mut buf = bytes::BytesMut::with_capacity(64);
    hdr.write_to(&mut buf).expect("write MmtpHeader");

    let mpu = MpuHeader::new(FragmentType::Init, mpu_seq);
    mpu.write_to(&mut buf).expect("write MpuHeader");

    // Deterministic payload — caller verifies byte-for-byte equality.
    let payload = format!("track={packet_id};mpu_seq={mpu_seq};payload");
    buf.put_slice(payload.as_bytes());
    buf.to_vec()
}

fn build_mfu_fragment_packet(
    packet_id: u16,
    mpu_seq: u32,
    fi: u8,
    fragment_counter: u8,
) -> Vec<u8> {
    let mut hdr = MmtpHeader::new(packet_id, PacketType::Mpu);
    hdr.packet_sequence = mpu_seq;
    hdr.timestamp = mpu_seq << 16;

    let payload = format!("track={packet_id};mpu_seq={mpu_seq};frag={fragment_counter};fi={fi}");
    let mut mpu = MpuHeader::new(FragmentType::Mfu, mpu_seq);
    mpu.fragmentation_indicator = fi;
    mpu.fragment_counter = fragment_counter;
    mpu.payload_length = (payload.len() + if fi <= 1 { MfuDataUnit::size() } else { 0 }) as u16;

    let mut buf = bytes::BytesMut::with_capacity(96);
    hdr.write_to(&mut buf).expect("write MmtpHeader");
    mpu.write_to(&mut buf).expect("write MpuHeader");
    if fi <= 1 {
        MfuDataUnit::new(mpu_seq, 1)
            .write_to(&mut buf)
            .expect("write MfuDataUnit");
    }
    buf.put_slice(payload.as_bytes());
    buf.to_vec()
}

fn build_fragmented_mpu_sequence(packet_id: u16, mpu_seq: u32, fragment_count: u8) -> Vec<Vec<u8>> {
    assert!(fragment_count >= 1, "fragment_count must be at least 1");
    let mut packets = Vec::with_capacity(fragment_count as usize + 1);
    packets.push(build_init_packet(packet_id, mpu_seq));
    for counter in 0..fragment_count {
        let fi = if fragment_count == 1 {
            0
        } else if counter == 0 {
            1
        } else if counter + 1 == fragment_count {
            3
        } else {
            2
        };
        packets.push(build_mfu_fragment_packet(packet_id, mpu_seq, fi, counter));
    }
    packets
}

#[cfg(test)]
mod tests {
    use super::*;
    use mmt_core::header::MMTP_HEADER_SIZE;

    #[test]
    fn build_fragmented_mpu_emits_init_plus_n_mfu_fragments() {
        let packets = build_fragmented_mpu_sequence(1, 10, 3);
        let expected = [
            (FragmentType::Init, 0, 0),
            (FragmentType::Mfu, 1, 0),
            (FragmentType::Mfu, 2, 1),
            (FragmentType::Mfu, 3, 2),
        ];
        assert_eq!(packets.len(), expected.len());

        for (packet, (fragment_type, fi, counter)) in packets.iter().zip(expected) {
            let mut mmtp_bytes: &[u8] = packet;
            let hdr = MmtpHeader::read_from(&mut mmtp_bytes).unwrap();
            let mut mpu_bytes: &[u8] = &packet[MMTP_HEADER_SIZE..];
            let (mpu, _) = MpuHeader::read_from(&mut mpu_bytes).unwrap();
            assert_eq!(hdr.packet_id, 1);
            assert_eq!(hdr.timestamp, if fi == 0 { 0 } else { 10 << 16 });
            assert_eq!(mpu.mpu_sequence, 10);
            assert_eq!(mpu.fragment_type, fragment_type);
            assert_eq!(mpu.fragmentation_indicator, fi);
            assert_eq!(mpu.fragment_counter, counter);
        }
    }
}
