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
use mmt_core::header::{FragmentType, MmtpHeader, MpuHeader, PacketType};

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
            let packet = build_init_packet(packet_id, mpu_seq);
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
