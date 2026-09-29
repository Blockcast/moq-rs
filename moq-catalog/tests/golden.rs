use moq_catalog::Root;

const POSITIVE: [(&str, &str); 6] = [
    ("flat-av", include_str!("fixtures/positive/flat-av.json")),
    (
        "fec-multicast",
        include_str!("fixtures/positive/fec-multicast.json"),
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

const NEGATIVE: [(&str, &str, Reject); 10] = [
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
/// that rule untested -- in each case confirmed by mutation-testing the rule and
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
fn golden_negative_fixtures_are_rejected() {
    for (name, json, expected) in NEGATIVE.iter().chain(&NEGATIVE_LOCAL) {
        assert_rejected(name, json, expected);
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
