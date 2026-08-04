// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! Deterministic and opt-in live checks for the documented REST surface.

use std::collections::BTreeSet;
#[cfg(feature = "live-tests")]
use std::time::Duration;

const CLIENT_SOURCE: &str = include_str!("../src/client.rs");
const OPERATION_MANIFEST: &str = include_str!("fixtures/gateway_operations.txt");

fn documented_operations() -> Vec<&'static str> {
    OPERATION_MANIFEST
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect()
}

#[test]
fn every_documented_operation_is_named_in_the_client_source() {
    let operations = documented_operations();
    let unique = operations.iter().copied().collect::<BTreeSet<_>>();

    assert_eq!(operations.len(), 21, "update the expected operation count");
    assert_eq!(
        operations.len(),
        unique.len(),
        "operation manifest contains duplicates"
    );

    for operation in operations {
        let Some((method, path)) = operation.split_once(' ') else {
            panic!("invalid operation manifest entry: {operation}");
        };
        assert!(matches!(method, "GET" | "POST"));
        let route_literal = format!("\"{}\"", path.trim_start_matches('/'));
        assert!(
            CLIENT_SOURCE.contains(&route_literal),
            "client route is missing for {operation}"
        );
    }
}

#[cfg(feature = "live-tests")]
#[tokio::test]
#[ignore = "performs deliberate read-only network I/O against the provider Swagger document"]
async fn live_swagger_operation_set_matches_the_manifest() {
    use serde_json::Value;

    let client = reqwest::Client::builder()
        .no_proxy()
        .retry(reqwest::retry::never())
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap_or_else(|error| panic!("live Swagger client must build: {error}"));
    let document = client
        .get("https://api.topstepx.com/swagger/v1/swagger.json")
        .send()
        .await
        .unwrap_or_else(|error| panic!("live Swagger request must succeed: {error}"))
        .error_for_status()
        .unwrap_or_else(|error| panic!("live Swagger status must succeed: {error}"))
        .json::<Value>()
        .await
        .unwrap_or_else(|error| panic!("live Swagger document must decode: {error}"));
    let paths = document
        .get("paths")
        .and_then(Value::as_object)
        .unwrap_or_else(|| panic!("live Swagger document must contain a paths object"));
    let mut actual = BTreeSet::new();

    for (path, path_item) in paths {
        let methods = path_item
            .as_object()
            .unwrap_or_else(|| panic!("Swagger path item must be an object: {path}"));
        for method in methods.keys().filter(|method| {
            matches!(
                method.as_str(),
                "delete" | "get" | "head" | "options" | "patch" | "post" | "put" | "trace"
            )
        }) {
            actual.insert(format!("{} {path}", method.to_ascii_uppercase()));
        }
    }

    let expected = documented_operations()
        .into_iter()
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    assert_eq!(actual, expected, "the provider REST surface has drifted");
}
