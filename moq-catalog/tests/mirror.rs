//! Enforces the fixture provenance contract that `fixtures/README.md` states in
//! prose: `negative/` is a byte-identical mirror of libmmt's canonical
//! `catalog/negative/` corpus, and `negative-local/` holds only negatives libmmt
//! does not ship.
//!
//! `golden.rs` asks whether a fixture is *rejected*. That is a different
//! question, and it stays green through exactly the drift this file catches: the
//! merge at 1c9f26fb landed three local-only negatives in `negative/` with the
//! registry consistent, the arities right, and all three still rejecting. Only a
//! comparison against libmmt's actual listing separates a mirror from a
//! non-mirror, so that listing is vendored beside the fixtures.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

const MANIFEST: &str = include_str!("fixtures/libmmt-negative.manifest");

/// Vacuity guard. Both tests below are membership tests against the manifest, so
/// a manifest that failed to load would let `negative_local_fixtures_are_not_
/// libmmt_mirrors` pass while comparing against nothing.
///
/// Exact, not a floor: a floor leaves slack, and a truncation dropping only the
/// lines that happen not to be mirrored in `negative/` would clear it while
/// silently weakening the `negative-local/` direction -- the precise vacuity this
/// guard exists to prevent. The manifest is regenerated wholesale on every
/// deliberate sync, so the count is updated in the same edit that changes it.
const MANIFEST_ENTRIES: usize = 21;

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// libmmt's corpus as `file name -> git blob SHA`.
fn libmmt_corpus() -> BTreeMap<&'static str, &'static str> {
    let mut entries = BTreeMap::new();

    for line in MANIFEST.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let (sha, name) = line
            .split_once(char::is_whitespace)
            .unwrap_or_else(|| panic!("manifest line is not `<sha>  <name>`: {line}"));
        let name = name.trim();

        assert!(
            sha.len() == 40 && sha.chars().all(|c| c.is_ascii_hexdigit()),
            "manifest line does not start with a git blob SHA: {line}"
        );
        assert!(
            entries.insert(name, sha).is_none(),
            "manifest lists {name} twice"
        );
    }

    assert!(
        entries.len() == MANIFEST_ENTRIES,
        "manifest carries {} entries, expected exactly {MANIFEST_ENTRIES} -- \
         an empty or truncated manifest makes these checks vacuous. If libmmt \
         genuinely gained or lost a vector, regenerate the manifest and update \
         MANIFEST_ENTRIES in the same edit.",
        entries.len()
    );

    entries
}

/// The manifest entry carrying these bytes, whatever name libmmt files it under.
fn libmmt_name_for_sha<'a>(corpus: &BTreeMap<&'a str, &'a str>, sha: &str) -> Option<&'a str> {
    corpus
        .iter()
        .find(|(_, &vector_sha)| vector_sha == sha)
        .map(|(name, _)| *name)
}

/// `git hash-object` for a blob: SHA-1 over `blob <len>\0` then the content.
fn blob_sha(content: &[u8]) -> String {
    let mut hasher = sha1_smol::Sha1::new();
    hasher.update(format!("blob {}\0", content.len()).as_bytes());
    hasher.update(content);
    hasher.digest().to_string()
}

/// Every `.json` in a fixture directory as `(file name, git blob SHA)`.
///
/// Reads the directory rather than the `include_str!` tables in `golden.rs` on
/// purpose: a merge can add a file to `negative/` without registering it, and
/// that file is still a mirror violation.
fn hashed_fixtures(dir: &str) -> Vec<(String, String)> {
    let path = fixtures_dir().join(dir);
    let listing =
        fs::read_dir(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));

    let mut fixtures = Vec::new();
    for entry in listing {
        let entry = entry.expect("cannot stat fixture");
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.ends_with(".json") {
            continue;
        }
        let bytes = fs::read(entry.path()).expect("cannot read fixture");
        fixtures.push((name, blob_sha(&bytes)));
    }

    assert!(
        !fixtures.is_empty(),
        "no fixtures found in {} -- the check would pass vacuously",
        path.display()
    );

    fixtures.sort();
    fixtures
}

#[test]
fn negative_fixtures_are_byte_identical_mirrors_of_libmmt() {
    let corpus = libmmt_corpus();

    let drift: Vec<String> = hashed_fixtures("negative")
        .into_iter()
        .filter_map(|(name, sha)| match corpus.get(name.as_str()) {
            // Check the bytes before blaming the name. A renamed-but-identical
            // mirror is not a local negative, and sending it to negative-local/
            // just trips the other test with the opposite advice.
            None => Some(match libmmt_name_for_sha(&corpus, &sha) {
                Some(libmmt_name) => format!(
                    "{name}: byte-identical to libmmt's {libmmt_name} but renamed -- \
                     restore the libmmt name"
                ),
                None => format!(
                    "{name}: libmmt ships no vector by this name -- \
                     it belongs in negative-local/, not the mirror"
                ),
            }),
            Some(&expected) if expected != sha => Some(format!(
                "{name}: content differs from libmmt (here {sha}, libmmt {expected})"
            )),
            Some(_) => None,
        })
        .collect();

    assert!(
        drift.is_empty(),
        "negative/ is a byte-identical mirror of libmmt's catalog/negative/, but:\n  {}\n\
         Sync the fixture, or move it to negative-local/ and say why in fixtures/README.md.",
        drift.join("\n  ")
    );
}

#[test]
fn negative_local_fixtures_are_not_libmmt_mirrors() {
    // Keyed on content, not name: `endpoint-missing-protocol-and-source.json` is
    // deliberately a renamed near-copy of a libmmt vector, and must stay local
    // because its bytes differ. Only byte-identity makes it a mirror.
    let corpus = libmmt_corpus();
    let libmmt_content: BTreeSet<&str> = corpus.values().copied().collect();

    let promotable: Vec<String> = hashed_fixtures("negative-local")
        .into_iter()
        .filter(|(_, sha)| libmmt_content.contains(sha.as_str()))
        .map(|(name, sha)| {
            let libmmt_name = libmmt_name_for_sha(&corpus, &sha).unwrap_or("?");
            format!("{name}: byte-identical to libmmt's {libmmt_name}")
        })
        .collect();

    assert!(
        promotable.is_empty(),
        "negative-local/ holds only negatives libmmt does not ship, but:\n  {}\n\
         Promote these to negative/ so a libmmt sync keeps them current.",
        promotable.join("\n  ")
    );
}
