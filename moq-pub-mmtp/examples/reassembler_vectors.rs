// SPDX-License-Identifier: MIT OR Apache-2.0
//
// Parity-vector generator for the Shaka pure-JS MfuReassembler (M.4 Track 1,
// ADR §T1.3 / Q3).
//
// Feeds canonical MFU fragment sequences through the *canonical* Rust
// reassembler (`mmt_core::MfuReassembler`) and serialises each
// (input fragments → reassembled output) pair to JSON. The Shaka JS port
// (`shaka-player/lib/msf/mfu_reassembler.js`) loads this fixture and asserts
// byte-for-byte equality, so the JS implementation cannot drift from the Rust
// source-of-truth without a test failure.
//
// The vectors mirror the deterministic unit tests in
// `vendor/mmt-core/src/reassembler.rs::tests` (complete / two-in-order /
// out-of-order / three-fragment) plus one vector matching the real M.1b §B1
// smoke wire (Init is a separate object, so only the 3 *Mfu* fragments reach
// the reassembler).
//
// Only Mfu fragments are exercised here: the receiver's parser routes
// `FragmentType::Init` objects to the init-segment path, never to the
// reassembler, so Init bytes never appear as reassembler input.
//
// Regenerate (DO NOT hand-edit the JSON):
//   cargo run -p moq-pub-mmtp --example reassembler_vectors -- \
//     ../shaka-player/test/msf/mfu_reassembler_vectors.json
// or print to stdout:
//   cargo run -p moq-pub-mmtp --example reassembler_vectors

use bytes::Bytes;
use mmt_core::{MfuFragment, MfuReassembler, ReassembledMfu};
use serde_json::{json, Value};

/// One synthetic fragment in a vector's input sequence.
///
/// `rap` mirrors the `create_fragment` helper convention in
/// `reassembler.rs::tests`: the RAP flag is set on the first fragment of a
/// fragmented MFU (FI=1) and on a complete MFU (FI=0), and clear otherwise.
struct Frag {
    mpu_seq: u32,
    fi: u8,
    counter: u16,
    data: &'static [u8],
    rap: bool,
}

impl Frag {
    fn to_mfu_fragment(&self) -> MfuFragment {
        // timestamp = mpu_seq * 1000 mirrors the reassembler.rs test helper so
        // the JS port's timestamp-propagation (first fragment wins) is pinned.
        MfuFragment::new(
            self.mpu_seq,
            self.fi,
            self.counter,
            Bytes::copy_from_slice(self.data),
            self.mpu_seq.wrapping_mul(1000),
            self.rap,
        )
    }
}

/// Lower-case hex with no separators. Avoids pulling in the `hex` crate.
fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(char::from_digit((b >> 4) as u32, 16).unwrap());
        s.push(char::from_digit((b & 0x0f) as u32, 16).unwrap());
    }
    s
}

/// Feed `frags` (in order) through a fresh reassembler and return the single
/// `ReassembledMfu` that completes the sequence.
///
/// Panics if the sequence does not complete exactly one MFU — that would mean
/// the vector itself is malformed, which we want to catch loudly at generation
/// time rather than ship a bogus fixture.
fn reassemble(name: &str, frags: &[Frag]) -> ReassembledMfu {
    let mut reassembler = MfuReassembler::default();
    let mut completed: Option<ReassembledMfu> = None;
    for f in frags {
        if let Some(mfu) = reassembler.add_fragment(f.to_mfu_fragment()) {
            assert!(
                completed.is_none(),
                "vector '{name}' completed more than one MFU; expected exactly one",
            );
            completed = Some(mfu);
        }
    }
    completed.unwrap_or_else(|| panic!("vector '{name}' never completed an MFU"))
}

fn vector_json(name: &str, frags: &[Frag]) -> Value {
    let out = reassemble(name, frags);
    let fragments: Vec<Value> = frags
        .iter()
        .map(|f| {
            json!({
                "mpu_sequence": f.mpu_seq,
                "fi": f.fi,
                "fragment_counter": f.counter,
                "data_hex": to_hex(f.data),
                "rap_flag": f.rap,
                // timestamp mirrors to_mfu_fragment(): mpu_seq * 1000.
                "timestamp": f.mpu_seq.wrapping_mul(1000),
            })
        })
        .collect();
    json!({
        "name": name,
        "fragments": fragments,
        "expected": {
            "mpu_sequence": out.mpu_sequence_number,
            "data_hex": to_hex(&out.data),
            "fragment_count": out.fragment_count,
            "rap_flag": out.rap_flag,
            "timestamp": out.timestamp,
        },
    })
}

fn main() -> anyhow::Result<()> {
    // Vectors mirror reassembler.rs::tests deterministic cases by name.
    let vectors = vec![
        // test_complete_mfu_zero_copy
        vector_json(
            "complete_mfu_fi0",
            &[Frag { mpu_seq: 1, fi: 0, counter: 0, data: b"complete", rap: true }],
        ),
        // test_two_fragments_in_order
        vector_json(
            "two_fragments_in_order",
            &[
                Frag { mpu_seq: 2, fi: 1, counter: 0, data: b"AAA", rap: true },
                Frag { mpu_seq: 2, fi: 3, counter: 1, data: b"BBB", rap: false },
            ],
        ),
        // test_out_of_order_fragments (last arrives before first; sort by counter)
        vector_json(
            "out_of_order_fragments",
            &[
                Frag { mpu_seq: 3, fi: 3, counter: 1, data: b"ZZZ", rap: false },
                Frag { mpu_seq: 3, fi: 1, counter: 0, data: b"XXX", rap: true },
            ],
        ),
        // test_three_fragments
        vector_json(
            "three_fragments",
            &[
                Frag { mpu_seq: 4, fi: 1, counter: 0, data: b"A", rap: true },
                Frag { mpu_seq: 4, fi: 2, counter: 1, data: b"B", rap: false },
                Frag { mpu_seq: 4, fi: 3, counter: 2, data: b"C", rap: false },
            ],
        ),
        // M.1b §B1 smoke wire: Init is a separate object (not shown); the 3 Mfu
        // fragments for track=1, mpu_seq=0 reach the reassembler with the exact
        // synth_mmtp payload strings.
        vector_json(
            "b1_smoke_video_mpu0_3frag",
            &[
                Frag { mpu_seq: 0, fi: 1, counter: 0, data: b"track=1;mpu_seq=0;frag=0;fi=1", rap: true },
                Frag { mpu_seq: 0, fi: 2, counter: 1, data: b"track=1;mpu_seq=0;frag=1;fi=2", rap: false },
                Frag { mpu_seq: 0, fi: 3, counter: 2, data: b"track=1;mpu_seq=0;frag=2;fi=3", rap: false },
            ],
        ),
    ];

    let doc = json!({
        "_comment": concat!(
            "Generated by moq-pub-mmtp/examples/reassembler_vectors.rs from the canonical ",
            "mmt_core::MfuReassembler. DO NOT hand-edit. Regenerate with: ",
            "cargo run -p moq-pub-mmtp --example reassembler_vectors -- <path>",
        ),
        "source": "mmt_core::reassembler::MfuReassembler",
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
