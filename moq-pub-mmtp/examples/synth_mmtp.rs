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

    /// Emit fragmented MFU packets for each MPU. The flag's value is
    /// the number of MFU fragments per MPU (in addition to the Init
    /// packet). Default 0 = current Init-only behavior; values >= 2
    /// exercise the raw-passthrough fragmentation path (BLO-8047 §B1):
    /// each MPU becomes Init + N MFU packets with `fragmentation_indicator`
    /// running 1 → 2... → 3, all sharing the same `mpu_sequence`. The
    /// publisher does not interpret FI; the receiver reassembles using
    /// `mmt-core::MfuReassembler`.
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
        for (packet_id, expected_sink) in [
            (1u16, &mut expected_video),
            (2u16, &mut expected_audio),
        ] {
            // --fragment N >= 1 emits Init + N MFU fragments per MPU
            // to exercise the raw-passthrough fragmentation path.
            // Default 0 keeps the original Init-only emission.
            let packets: Vec<Vec<u8>> = if args.fragment >= 1 {
                build_fragmented_mpu_sequence(packet_id, mpu_seq, args.fragment)
            } else {
                vec![build_init_packet(packet_id, mpu_seq)]
            };
            for packet in &packets {
                match &udp_sink {
                    Some((sock, target)) => {
                        sock.send_to(packet, target)?;
                    }
                    None => {
                        // Length-prefix framing (per moq-pub-mmtp::framing).
                        let prefix = (packet.len() as u32).to_be_bytes();
                        stdout.write_all(&prefix)?;
                        stdout.write_all(packet)?;
                        stdout.flush()?;
                    }
                }
                // Expected per-track byte stream: raw packets only (no
                // length prefix or UDP framing — what lands on the wire
                // as MoQ object payloads per track).
                expected_sink.write_all(packet)?;
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

/// Build one MMTP packet carrying a single MFU fragment.
///
/// `fi` is the MPU header's `fragmentation_indicator` field
/// (0=complete, 1=first, 2=middle, 3=last per ISO/IEC 23008-1
/// §9.2.3.3). `fragment_counter` increments within one MPU.
fn build_mfu_fragment_packet(
    packet_id: u16,
    mpu_seq: u32,
    fi: u8,
    fragment_counter: u8,
) -> Vec<u8> {
    let hdr = MmtpHeader::new(packet_id, PacketType::Mpu);
    let mut mpu = MpuHeader::new(FragmentType::Mfu, mpu_seq);
    mpu.fragmentation_indicator = fi;
    mpu.fragment_counter = fragment_counter;
    // Deterministic per-fragment payload — the M.1 smoke walks these
    // verbatim through the relay and sha256s each track's concatenated
    // output, so the bytes need to be reproducible and unique per
    // (packet_id, mpu_seq, fragment_counter).
    let payload = format!(
        "track={packet_id};mpu_seq={mpu_seq};frag={fragment_counter};fi={fi}"
    );
    mpu.payload_length = payload.len() as u16;
    let mut buf = bytes::BytesMut::with_capacity(64);
    hdr.write_to(&mut buf).expect("write MmtpHeader");
    mpu.write_to(&mut buf).expect("write MpuHeader");
    buf.put_slice(payload.as_bytes());
    buf.to_vec()
}

/// Build the full MMTP packet sequence for one MPU with `fragment_count`
/// MFU fragments. Returns `fragment_count + 1` packets:
///
///   index 0:    FragmentType::Init,  FI=0,  fragment_counter=0
///   index 1:    FragmentType::Mfu,   FI=1,  fragment_counter=0   (first MFU fragment)
///   index i:    FragmentType::Mfu,   FI=2,  fragment_counter=i-1 (middle, 1<i<N)
///   index N:    FragmentType::Mfu,   FI=3,  fragment_counter=N-1 (last)
///
/// For `fragment_count == 1` the single MFU packet uses FI=0 (complete,
/// not split) — degenerate but representable. The smoke uses
/// `fragment_count >= 2` to actually exercise the fragmentation path.
///
/// Panics if `fragment_count == 0` (a fragmented MPU must have at least
/// one MFU; callers wanting Init-only should use `build_init_packet`).
fn build_fragmented_mpu_sequence(
    packet_id: u16,
    mpu_seq: u32,
    fragment_count: u8,
) -> Vec<Vec<u8>> {
    assert!(
        fragment_count >= 1,
        "build_fragmented_mpu_sequence: fragment_count must be >= 1 \
         (use build_init_packet for Init-only)",
    );
    let mut packets = Vec::with_capacity(fragment_count as usize + 1);
    packets.push(build_init_packet(packet_id, mpu_seq));
    for i in 0..fragment_count {
        let fi = match (i, fragment_count) {
            (0, 1) => 0,                       // single fragment = complete
            (0, _) => 1,                       // first of multiple
            (k, n) if k + 1 == n => 3,         // last of multiple
            _ => 2,                            // middle
        };
        packets.push(build_mfu_fragment_packet(packet_id, mpu_seq, fi, i));
    }
    packets
}

#[cfg(test)]
mod tests {
    use super::*;
    use mmt_core::header::{MmtpHeader, MpuHeader, MMTP_HEADER_SIZE};

    #[test]
    fn build_fragmented_mpu_emits_init_plus_n_mfu_fragments() {
        // B1=C smoke prep (BLO-8047 §B1): synth_mmtp must be able to
        // emit a real fragmented MFU sequence so the M.1 smoke can
        // exercise the raw-passthrough path with FI != 0 packets.
        //
        // The sequence for one MPU with K MFU fragments is K+1 packets:
        //   packet 0: FragmentType::Init, FI=0, fragment_counter=0
        //   packet 1: FragmentType::Mfu,  FI=1, fragment_counter=0
        //   packet i: FragmentType::Mfu,  FI=2, fragment_counter=i-1   (1 < i < K)
        //   packet K: FragmentType::Mfu,  FI=3, fragment_counter=K-1
        //
        // For K=3 → 4 packets total, FI sequence [0, 1, 2, 3].
        let pkts = build_fragmented_mpu_sequence(
            /*packet_id=*/ 1,
            /*mpu_seq=*/ 10,
            /*fragment_count=*/ 3,
        );
        assert_eq!(pkts.len(), 4, "1 Init + 3 MFU fragments");

        // Parse every packet and walk the MMTP+MPU headers — this
        // verifies on-wire structure end-to-end, not just our internal
        // bookkeeping.
        let expected: [(FragmentType, u8, u8); 4] = [
            (FragmentType::Init, 0, 0),
            (FragmentType::Mfu, 1, 0),
            (FragmentType::Mfu, 2, 1),
            (FragmentType::Mfu, 3, 2),
        ];
        for (i, (want_ft, want_fi, want_counter)) in expected.iter().enumerate() {
            let mut cursor: &[u8] = &pkts[i];
            let hdr = MmtpHeader::read_from(&mut cursor)
                .unwrap_or_else(|e| panic!("packet {i}: MMTP header decode: {e:?}"));
            assert_eq!(hdr.packet_id, 1, "packet {i}: packet_id");
            assert_eq!(hdr.packet_type, PacketType::Mpu, "packet {i}: packet_type");

            let mut mpu_payload: &[u8] = &pkts[i][MMTP_HEADER_SIZE..];
            let (mpu, _len) = MpuHeader::read_from(&mut mpu_payload)
                .unwrap_or_else(|e| panic!("packet {i}: MPU header decode: {e:?}"));
            assert_eq!(mpu.mpu_sequence, 10, "packet {i}: mpu_sequence");
            assert_eq!(mpu.fragment_type, *want_ft, "packet {i}: fragment_type");
            assert_eq!(
                mpu.fragmentation_indicator, *want_fi,
                "packet {i}: fragmentation_indicator"
            );
            assert_eq!(
                mpu.fragment_counter, *want_counter,
                "packet {i}: fragment_counter"
            );
        }
    }
}
