use moq_catalog::Root;

const POSITIVE: [(&str, &str); 3] = [
    ("flat-av", include_str!("fixtures/positive/flat-av.json")),
    (
        "fec-multicast",
        include_str!("fixtures/positive/fec-multicast.json"),
    ),
    (
        "ticks-and-mixed-timescales",
        include_str!("fixtures/positive/ticks-and-mixed-timescales.json"),
    ),
];

const NEGATIVE: [(&str, &str); 6] = [
    (
        "legacy-selection-params",
        include_str!("fixtures/negative/legacy-selection-params.json"),
    ),
    (
        "missing-mmtp-fields",
        include_str!("fixtures/negative/missing-mmtp-fields.json"),
    ),
    (
        "network-source-object",
        include_str!("fixtures/negative/network-source-object.json"),
    ),
    (
        "repair-track-legacy-shape",
        include_str!("fixtures/negative/repair-track-legacy-shape.json"),
    ),
    (
        "multicast-endpoint-missing-protocol-and-source",
        include_str!("fixtures/negative/multicast-endpoint-missing-protocol-and-source.json"),
    ),
    (
        "raptorq-unaligned-symbol",
        include_str!("fixtures/negative/raptorq-unaligned-symbol.json"),
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
    for (name, json) in NEGATIVE {
        let rejected = match serde_json::from_str::<Root>(json) {
            Ok(catalog) => catalog.validate().is_err(),
            Err(_) => true,
        };
        assert!(rejected, "{name} unexpectedly passed");
    }
}
