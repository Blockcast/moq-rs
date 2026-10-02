use moq_catalog::Root;
use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

const POSITIVE: [(&str, &str); 7] = [
    ("flat-av", include_str!("fixtures/positive/flat-av.json")),
    (
        "fec-multicast",
        include_str!("fixtures/positive/fec-multicast.json"),
    ),
    (
        "multicast-native-ssm-no-network-source",
        include_str!("fixtures/positive/multicast-native-ssm-no-network-source.json"),
    ),
    (
        "ticks-and-mixed-timescales",
        include_str!("fixtures/positive/ticks-and-mixed-timescales.json"),
    ),
    (
        "track-role",
        include_str!("fixtures/positive/track-role.json"),
    ),
    (
        "fec-layered-repair",
        include_str!("fixtures/positive/fec-layered-repair.json"),
    ),
    (
        "hang-catalog-to-string",
        include_str!("fixtures/positive/hang-catalog-to-string.json"),
    ),
];

/// Catalogs this crate must *parse* but must not accept as MSF.
///
/// `hang::CatalogRoot::to_string()` serializes every capture it holds, including
/// a `Container::Legacy` one. A legacy track is varint-framed, carries no MMTP
/// packetization, and has no MSF `packaging` value it could honestly claim, so
/// the emitter gives it `streamingFormat: "cmaf"` rather than the MSF envelope
/// (BLO-37925). libmmt's `catalog.schema.json` pins `streamingFormat` to the
/// constant `"mmtp"` and *defines* the MSF wire, so such a catalog is outside
/// that schema's scope rather than in violation of it -- which is why these
/// cannot live in `positive/`, whose contract is that both implementations
/// accept them.
///
/// moq-sub still has to consume one: `validate_catalog()` applies the MMTP-only
/// `Root::validate()` only when `streamingFormat == "mmtp"`, so the envelope is
/// what decides, and a subscriber that cannot even deserialize the catalog never
/// reaches that gate. That is not hypothetical -- before #93 this exact capture
/// failed on `deny_unknown_fields` at `keyframeIntervalMs`.
///
/// The third element is the substring the rejection must carry, for the same
/// reason `Reject::Validate` carries one: `Root::validate()` is a chain of early
/// returns, so asserting only that *something* refused the fixture lets any later
/// gate that starts firing keep the test green while the envelope discrimination
/// this fixture exists to pin loses its only coverage.
const NON_MSF: [(&str, &str, &str); 1] = [(
    "hang-legacy-catalog-to-string",
    include_str!("fixtures/non-msf/hang-legacy-catalog-to-string.json"),
    "streamingFormat must be mmtp",
)];

const NEGATIVE: [(&str, &str, Reject); 11] = [
    (
        "legacy-selection-params",
        include_str!("fixtures/negative/legacy-selection-params.json"),
        Reject::Parse,
    ),
    (
        "missing-mmtp-fields",
        include_str!("fixtures/negative/missing-mmtp-fields.json"),
        Reject::Validate("mmtpMode"),
    ),
    (
        "network-source-object",
        include_str!("fixtures/negative/network-source-object.json"),
        Reject::Parse,
    ),
    (
        "multicast-network-source-empty",
        include_str!("fixtures/negative/multicast-network-source-empty.json"),
        Reject::Validate("networkSource must not be empty when present"),
    ),
    (
        "repair-track-legacy-shape",
        include_str!("fixtures/negative/repair-track-legacy-shape.json"),
        Reject::Parse,
    ),
    (
        "multicast-endpoint-missing-protocol-and-source",
        include_str!("fixtures/negative/multicast-endpoint-missing-protocol-and-source.json"),
        // Double negative: libmmt pins `multicast.networkSource` to an array of
        // objects, so the string `"direct"` is schema-invalid on its own and
        // serde refuses it before the endpoint rule is ever reached. That rule
        // keeps its coverage in NEGATIVE_LOCAL below.
        Reject::Parse,
    ),
    (
        "raptorq-unaligned-symbol",
        include_str!("fixtures/negative/raptorq-unaligned-symbol.json"),
        Reject::Validate("symbolSize"),
    ),
    (
        "fec-repair-priority-out-of-band",
        include_str!("fixtures/negative/fec-repair-priority-out-of-band.json"),
        Reject::Validate("priority must be between 192 and 255"),
    ),
    (
        "fec-repair-layer-missing-symbols",
        include_str!("fixtures/negative/fec-repair-layer-missing-symbols.json"),
        Reject::Validate("repairLayer >= 1 requires depends, repairSymbols, and priority"),
    ),
    (
        "fec-repair-overlay-with-layer",
        include_str!("fixtures/negative/fec-repair-overlay-with-layer.json"),
        Reject::Validate("scope and repairLayer are mutually exclusive"),
    ),
    (
        "fec-repair-source-symbols-without-scope",
        include_str!("fixtures/negative/fec-repair-source-symbols-without-scope.json"),
        Reject::Validate("sourceSymbols requires scope"),
    ),
];

/// Negatives that are NOT libmmt mirrors. The first three exist because no
/// canonical vector reaches the rule each covers, so mirroring alone would leave
/// that rule untested — in each case confirmed by mutation-testing the rule and
/// watching nothing go red. The `legacy-*` three pin pre-MSF shapes libmmt never
/// emitted, so it has no vector for them.
const NEGATIVE_LOCAL: [(&str, &str, Reject); 6] = [
    (
        "endpoint-missing-protocol-and-source",
        include_str!("fixtures/negative-local/endpoint-missing-protocol-and-source.json"),
        Reject::Validate("endpoint requires protocol or sourceAddress"),
    ),
    (
        "fec-repair-overlay-missing-companions",
        include_str!("fixtures/negative-local/fec-repair-overlay-missing-companions.json"),
        Reject::Validate("scope requires depends, sourceSymbols, repairSymbols, and priority"),
    ),
    (
        "fec-repair-container-not-native",
        include_str!("fixtures/negative-local/fec-repair-container-not-native.json"),
        Reject::Validate("repairContainer must be native"),
    ),
    (
        "legacy-common-track-fields",
        include_str!("fixtures/negative-local/legacy-common-track-fields.json"),
        Reject::Parse,
    ),
    (
        "legacy-numeric-streaming-format",
        include_str!("fixtures/negative-local/legacy-numeric-streaming-format.json"),
        Reject::Parse,
    ),
    (
        "legacy-subgroup-history-groups",
        include_str!("fixtures/negative-local/legacy-subgroup-history-groups.json"),
        Reject::Parse,
    ),
];

fn assert_json_equivalent(actual: &serde_json::Value, expected: &serde_json::Value, path: &str) {
    match (actual, expected) {
        (serde_json::Value::Object(actual), serde_json::Value::Object(expected)) => {
            assert_eq!(
                actual.len(),
                expected.len(),
                "field count changed at {path}"
            );
            for (key, expected_value) in expected {
                let actual_value = actual
                    .get(key)
                    .unwrap_or_else(|| panic!("field {path}/{key} was dropped"));
                assert_json_equivalent(actual_value, expected_value, &format!("{path}/{key}"));
            }
        }
        (serde_json::Value::Array(actual), serde_json::Value::Array(expected)) => {
            assert_eq!(
                actual.len(),
                expected.len(),
                "array length changed at {path}"
            );
            for (index, (actual, expected)) in actual.iter().zip(expected).enumerate() {
                assert_json_equivalent(actual, expected, &format!("{path}/{index}"));
            }
        }
        (serde_json::Value::Number(actual), serde_json::Value::Number(expected)) => {
            assert_eq!(
                actual.as_f64(),
                expected.as_f64(),
                "number changed at {path}"
            );
        }
        _ => assert_eq!(actual, expected, "value changed at {path}"),
    }
}

#[test]
fn golden_positive_fixtures_validate_and_round_trip_without_loss() {
    for (name, json) in POSITIVE {
        let expected: serde_json::Value = serde_json::from_str(json).unwrap();
        let catalog: Root = serde_json::from_str(json)
            .unwrap_or_else(|error| panic!("{name} did not deserialize: {error}"));
        catalog
            .validate()
            .unwrap_or_else(|error| panic!("{name} did not validate: {error}"));
        let emitted = serde_json::to_value(catalog).unwrap();
        assert_json_equivalent(&emitted, &expected, name);
    }
}

#[test]
fn non_msf_fixtures_parse_and_round_trip_but_are_not_valid_msf() {
    for (name, json, reason) in NON_MSF {
        let expected: serde_json::Value = serde_json::from_str(json).unwrap();
        let catalog: Root = serde_json::from_str(json)
            .unwrap_or_else(|error| panic!("{name} did not deserialize: {error}"));

        // Assert the shape the rest of this test leans on, rather than trusting
        // the fixture was authored as intended: a fixture that quietly became an
        // MMTP catalog would make every assertion below pass for the wrong reason.
        assert_ne!(
            catalog.streaming_format, "mmtp",
            "{name} is registered as non-MSF but claims the MSF envelope"
        );

        // The MMTP-only validator must refuse it, and say which rule refused it.
        // This is the determination itself: such a catalog never claims to be an
        // MSF catalog, so accepting it here would mean `Root::validate()` had
        // stopped discriminating. Matching on the reason is what keeps a later
        // gate firing first from standing in for the one under test.
        let rendered = catalog
            .validate()
            .expect_err(&format!(
                "{name} must not validate as MSF: Root::validate has stopped discriminating on the envelope"
            ))
            .to_string();
        assert!(
            rendered.contains(reason),
            "{name} was rejected for the wrong reason: expected `{reason}`, got `{rendered}`"
        );

        // ...yet moq-sub must still accept it, because `validate_catalog()` gates
        // `Root::validate()` on the envelope. Mirrors moq-sub/src/media.rs.
        assert_eq!(
            catalog.version, 1,
            "{name} must carry a version moq-sub accepts"
        );

        let emitted = serde_json::to_value(catalog).unwrap();
        assert_json_equivalent(&emitted, &expected, name);
    }
}

#[test]
fn golden_negative_fixtures_are_rejected() {
    for (name, json, expected) in NEGATIVE.iter().chain(&NEGATIVE_LOCAL) {
        assert_rejected(name, json, expected);
    }
}

#[test]
fn every_fixture_file_is_registered() {
    let dirs: [(&str, Vec<&str>); 4] = [
        ("positive", POSITIVE.iter().map(|(name, _)| *name).collect()),
        (
            "non-msf",
            NON_MSF.iter().map(|(name, _, _)| *name).collect(),
        ),
        (
            "negative",
            NEGATIVE.iter().map(|(name, _, _)| *name).collect(),
        ),
        (
            "negative-local",
            NEGATIVE_LOCAL.iter().map(|(name, _, _)| *name).collect(),
        ),
    ];

    for (dir, names) in dirs {
        let registered: BTreeSet<&str> = names.into_iter().collect();
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(dir);

        let on_disk: BTreeSet<String> = fs::read_dir(&path)
            .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()))
            .map(|entry| {
                entry
                    .expect("cannot stat fixture")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .filter_map(|name| name.strip_suffix(".json").map(str::to_owned))
            .collect();

        assert!(
            !on_disk.is_empty(),
            "no fixtures found in {} -- the check would pass vacuously",
            path.display()
        );

        // Only the unregistered direction is checked: a table naming a file that
        // does not exist cannot reach this test, because `include_str!` fails to
        // compile first.
        let unregistered: Vec<&str> = on_disk
            .iter()
            .map(String::as_str)
            .filter(|name| !registered.contains(name))
            .collect();

        assert!(
            unregistered.is_empty(),
            "{dir}/ holds fixtures that no table in this file registers:\n  {}\n\
             They are compiled into no test, so they assert nothing while looking covered. \
             Add them to the table, or delete them.",
            unregistered.join("\n  ")
        );
    }
}

/// Which layer must reject a negative fixture.
///
/// The old harness collapsed both into `is_err()`, so it could not tell "the
/// rule under test refused this" from "serde refused the shape first". Four of
/// the eight negatives were silently in the second bucket, and one rule lost its
/// only coverage without any test going red (BLO-37534).
enum Reject {
    /// serde refuses the shape; `validate()` never runs.
    Parse,
    /// `validate()` refuses it, and says so — the substring must appear in the
    /// rendered error, so a fixture that starts failing for a different reason
    /// fails the test instead of quietly passing.
    Validate(&'static str),
}

fn assert_rejected(name: &str, json: &str, expected: &Reject) {
    match (serde_json::from_str::<Root>(json), expected) {
        (Err(_), Reject::Parse) => {}
        (Err(error), Reject::Validate(reason)) => panic!(
            "{name} was expected to reach validate() and fail on `{reason}`, \
             but serde rejected it first: {error}"
        ),
        (Ok(_), Reject::Parse) => panic!("{name} parsed, but was expected to fail serde"),
        (Ok(catalog), Reject::Validate(reason)) => match catalog.validate() {
            Ok(()) => panic!("{name} unexpectedly validated; expected `{reason}`"),
            Err(error) => {
                let rendered = error.to_string();
                assert!(
                    rendered.contains(reason),
                    "{name} was rejected for the wrong reason: \
                     expected `{reason}`, got `{rendered}`"
                );
            }
        },
    }
}
