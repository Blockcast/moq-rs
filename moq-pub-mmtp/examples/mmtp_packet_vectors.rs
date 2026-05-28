// SPDX-License-Identifier: MIT OR Apache-2.0
//
// Parity-vector generator for the Shaka MMTP packet parser (M.4 Track 1,
// ADR §T1.1).
//
// Builds raw MMTP packets with the *canonical* Rust encoders
// (`mmt_core::header::{MmtpHeader, MmtpHeaderExt, MpuHeader, SourceFecPayloadId}`)
// and serialises each (packet bytes -> expected parsed fields) pair to JSON.
// The Shaka parser (`shaka-player/lib/msf/mmtp_parser.js`) loads this fixture
// and asserts it recovers exactly these fields, so the JS bit-layout decode
// cannot drift from the Rust encoder without a test failure. This sidesteps
// the circular risk of hand-encoding the same byte layout in both the JS
// parser and its JS test.
//
// A MoQ object on the wire is one full raw MMTP packet (raw-passthrough
// container mode, M.1b §B1):
//   MMTP header (12 B) [+ SourceFecPayloadId (4 B) iff fec_type==1]
//     + MPU header (8 B, only for packet_type=Mpu) + media payload
//
// Regenerate (DO NOT hand-edit the JSON):
//   cargo run -p moq-pub-mmtp --example mmtp_packet_vectors -- \
//     ../shaka-player/test/test/assets/mmtp_packet_vectors.json

use bytes::{BufMut, BytesMut};
use mmt_core::header::{
    FragmentType, MmtpHeader, MmtpHeaderExt, MpuHeader, PacketType, SourceFecPayloadId,
};
use serde_json::{json, Value};

/// Lower-case hex, no separators.
fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(char::from_digit((b >> 4) as u32, 16).unwrap());
        s.push(char::from_digit((b & 0x0f) as u32, 16).unwrap());
    }
    s
}

fn packet_type_u8(t: PacketType) -> u8 {
    t as u8
}

fn fragment_type_u8(t: FragmentType) -> u8 {
    t as u8
}

/// Build an MPU-type packet (Init or Mfu) and its expected parsed fields.
#[allow(clippy::too_many_arguments)]
fn mpu_vector(
    name: &str,
    packet_id: u16,
    rap_flag: bool,
    packet_sequence: u32,
    fragment_type: FragmentType,
    fi: u8,
    fragment_counter: u8,
    mpu_sequence: u32,
    payload: &[u8],
) -> Value {
    let mut hdr = MmtpHeader::new(packet_id, PacketType::Mpu);
    hdr.rap_flag = rap_flag;
    hdr.packet_sequence = packet_sequence;

    let mut mpu = MpuHeader::new(fragment_type, mpu_sequence);
    mpu.fragmentation_indicator = fi;
    mpu.fragment_counter = fragment_counter;
    mpu.payload_length = payload.len() as u16;

    let mut buf = BytesMut::with_capacity(64);
    hdr.write_to(&mut buf).unwrap();
    mpu.write_to(&mut buf).unwrap();
    buf.put_slice(payload);
    let packet = buf.to_vec();

    json!({
        "name": name,
        "packet_hex": to_hex(&packet),
        "expected": {
            "version": 0,
            "fecType": 0,
            "rapFlag": rap_flag,
            "packetType": packet_type_u8(PacketType::Mpu),
            "packetId": packet_id,
            "timestamp": 0,
            "packetSequence": packet_sequence,
            "sourceFecPayloadId": Value::Null,
            "mpu": {
                "payloadLength": payload.len(),
                "fragmentType": fragment_type_u8(fragment_type),
                "timed": true,
                "fragmentationIndicator": fi,
                "aggregation": false,
                "fragmentCounter": fragment_counter,
                "mpuSequence": mpu_sequence,
            },
            "payload_hex": to_hex(payload),
        },
    })
}

/// Build an MPU Mfu packet carrying a SourceFecPayloadId (fec_type==1), which
/// sits between the 12-byte MMTP header and the 8-byte MPU header.
fn fec_vector(name: &str, packet_id: u16, ss_id: u32, mpu_sequence: u32, payload: &[u8]) -> Value {
    let ext = MmtpHeaderExt::with_fec(packet_id, PacketType::Mpu, SourceFecPayloadId::new(ss_id));

    let mut mpu = MpuHeader::new(FragmentType::Mfu, mpu_sequence);
    mpu.fragmentation_indicator = 1;
    mpu.fragment_counter = 0;
    mpu.payload_length = payload.len() as u16;

    let mut buf = BytesMut::with_capacity(64);
    ext.write_to(&mut buf).unwrap();
    mpu.write_to(&mut buf).unwrap();
    buf.put_slice(payload);
    let packet = buf.to_vec();

    json!({
        "name": name,
        "packet_hex": to_hex(&packet),
        "expected": {
            "version": 0,
            "fecType": 1,
            "rapFlag": false,
            "packetType": packet_type_u8(PacketType::Mpu),
            "packetId": packet_id,
            "timestamp": 0,
            "packetSequence": 0,
            "sourceFecPayloadId": ss_id,
            "mpu": {
                "payloadLength": payload.len(),
                "fragmentType": fragment_type_u8(FragmentType::Mfu),
                "timed": true,
                "fragmentationIndicator": 1,
                "aggregation": false,
                "fragmentCounter": 0,
                "mpuSequence": mpu_sequence,
            },
            "payload_hex": to_hex(payload),
        },
    })
}

/// Build a non-MPU packet (Repair): no MPU header, payload follows the MMTP
/// header directly. fec_type=2 (RepairMode0) carries no SourceFecPayloadId.
fn repair_vector(name: &str, packet_id: u16, payload: &[u8]) -> Value {
    let mut hdr = MmtpHeader::new(packet_id, PacketType::Repair);
    hdr.fec_type = 2;

    let mut buf = BytesMut::with_capacity(64);
    hdr.write_to(&mut buf).unwrap();
    buf.put_slice(payload);
    let packet = buf.to_vec();

    json!({
        "name": name,
        "packet_hex": to_hex(&packet),
        "expected": {
            "version": 0,
            "fecType": 2,
            "rapFlag": false,
            "packetType": packet_type_u8(PacketType::Repair),
            "packetId": packet_id,
            "timestamp": 0,
            "packetSequence": 0,
            "sourceFecPayloadId": Value::Null,
            "mpu": Value::Null,
            "payload_hex": to_hex(payload),
        },
    })
}

fn main() -> anyhow::Result<()> {
    let vectors = vec![
        // Mirrors synth_mmtp::build_init_packet(1, 0): RAP, FI=0, Init.
        mpu_vector(
            "init_video_mpu0",
            1,
            true,
            0,
            FragmentType::Init,
            0,
            0,
            0,
            b"track=1;mpu_seq=0;payload",
        ),
        // Mirrors synth_mmtp::build_mfu_fragment_packet(1, 0, fi=1, counter=0).
        mpu_vector(
            "mfu_fragment_first",
            1,
            false,
            0,
            FragmentType::Mfu,
            1,
            0,
            0,
            b"track=1;mpu_seq=0;frag=0;fi=1",
        ),
        // fec_type==1: 4-byte SourceFecPayloadId between MMTP and MPU headers.
        fec_vector("mfu_with_fec_source_id", 2, 0xCAFE_BABE, 7, b"fec-protected-mfu"),
        // Non-MPU packet: Repair, no MPU header.
        repair_vector("repair_packet", 1, b"\x00\x01\x02\x03repair-symbols"),
    ];

    let doc = json!({
        "_comment": concat!(
            "Generated by moq-pub-mmtp/examples/mmtp_packet_vectors.rs from the canonical ",
            "mmt_core header encoders. DO NOT hand-edit. Regenerate with: ",
            "cargo run -p moq-pub-mmtp --example mmtp_packet_vectors -- <path>",
        ),
        "source": "mmt_core::header::{MmtpHeader,MmtpHeaderExt,MpuHeader,SourceFecPayloadId}",
        "vectors": vectors,
    });

    let serialised = serde_json::to_string_pretty(&doc)?;
    match std::env::args().nth(1) {
        Some(path) => {
            std::fs::write(&path, format!("{serialised}\n"))?;
            eprintln!("wrote {} vectors to {path}", doc["vectors"].as_array().unwrap().len());
        }
        None => println!("{serialised}"),
    }
    Ok(())
}
