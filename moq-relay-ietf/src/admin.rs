// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Plain-HTTP operator surface: certificate fingerprint, liveness, readiness,
//! an authenticated drain that fans GOAWAY out to every session, and Traffic
//! Monitor's `/_astats`.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    body::Bytes,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use url::Url;

use crate::{Drain, DrainState};

/// Header carrying the operator's drain secret.
pub const DRAIN_TOKEN_HEADER: &str = "x-blockcast-drain-token";

/// Largest Timeout a GOAWAY can carry (a vi64).
const MAX_TIMEOUT_MS: u64 = (1 << 62) - 1;

pub struct AdminConfig {
    /// Hex SHA-256 of the serving certificate.
    pub fingerprint: String,
    pub drain: Drain,
    /// Shared secret for `POST /_drain`; `None` disables the endpoint.
    pub drain_token: Option<String>,
    /// Traffic Router base URL, asked for the GOAWAY target when the drain
    /// request does not name one.
    pub tr_url: Option<Url>,
}

#[derive(Clone)]
struct AdminState {
    fingerprint: Arc<str>,
    drain: Drain,
    drain_token: Option<Arc<str>>,
    tr_url: Option<Url>,
    /// Why the relay reports itself unavailable without having drained.
    unavailable: Arc<Mutex<Option<&'static str>>>,
}

impl AdminState {
    fn not_available(&self) -> bool {
        self.drain.state().is_some() || self.unavailable.lock().unwrap().is_some()
    }

    fn mark_unavailable(&self, reason: &'static str) {
        *self.unavailable.lock().unwrap() = Some(reason);
    }
}

pub fn router(config: AdminConfig) -> Router {
    let state = AdminState {
        fingerprint: config.fingerprint.into(),
        drain: config.drain,
        drain_token: config.drain_token.filter(|t| !t.is_empty()).map(Into::into),
        tr_url: config.tr_url,
        unavailable: Default::default(),
    };
    Router::new()
        .route("/certificate.sha256", get(fingerprint))
        .route("/fingerprint", get(fingerprint))
        .route("/livez", get(|| async { "ok" }))
        .route("/readyz", get(readyz))
        .route("/_drain", post(drain))
        .route("/_astats", get(astats))
        .with_state(state)
}

async fn fingerprint(State(state): State<AdminState>) -> String {
    state.fingerprint.to_string()
}

async fn readyz(State(state): State<AdminState>) -> Response {
    if state.not_available() {
        (StatusCode::SERVICE_UNAVAILABLE, "draining").into_response()
    } else {
        "ready".into_response()
    }
}

fn error(status: StatusCode, error: &str) -> Response {
    (status, Json(json!({ "error": error }))).into_response()
}

/// Length-independent comparison so the token is not guessable by timing.
fn token_matches(actual: &[u8], expected: &[u8]) -> bool {
    actual.len() == expected.len()
        && actual
            .iter()
            .zip(expected)
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0
}

/// Drain parameters, from the query string or a JSON body (query wins).
struct DrainRequest {
    query: HashMap<String, String>,
    body: Value,
    host: Option<String>,
}

impl DrainRequest {
    fn get(&self, key: &str) -> Option<String> {
        self.query
            .get(key)
            .cloned()
            .or_else(|| match self.body.get(key)? {
                Value::String(s) => Some(s.clone()),
                Value::Number(n) => Some(n.to_string()),
                _ => None,
            })
            .filter(|v| !v.trim().is_empty())
    }

    fn current_host(&self) -> Option<String> {
        self.get("currentHost").or_else(|| self.host.clone())
    }
}

async fn drain(
    State(state): State<AdminState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(expected) = state.drain_token.as_deref() else {
        return error(StatusCode::SERVICE_UNAVAILABLE, "drain_endpoint_disabled");
    };
    let Some(actual) = headers.get(DRAIN_TOKEN_HEADER) else {
        return error(StatusCode::UNAUTHORIZED, "missing_drain_token");
    };
    if !token_matches(actual.as_bytes(), expected.as_bytes()) {
        return error(StatusCode::UNAUTHORIZED, "invalid_drain_token");
    }

    let body = if body.iter().all(u8::is_ascii_whitespace) {
        Value::Null
    } else {
        match serde_json::from_slice(&body) {
            Ok(body) => body,
            Err(_) => return error(StatusCode::BAD_REQUEST, "invalid_json_body"),
        }
    };
    let request = DrainRequest {
        query,
        body,
        host: headers
            .get("host")
            .and_then(|h| h.to_str().ok())
            .map(str::to_string),
    };

    let timeout_ms = match request.get("timeoutMs").map(|t| t.parse::<u64>()) {
        None => return error(StatusCode::BAD_REQUEST, "missing_timeoutMs"),
        Some(Ok(t)) if (1..=MAX_TIMEOUT_MS).contains(&t) => t,
        Some(_) => return error(StatusCode::BAD_REQUEST, "invalid_timeoutMs"),
    };

    let (uri, source) = match request.get("newSessionUri").or_else(|| request.get("uri")) {
        Some(uri) => (uri, "request"),
        None => match tr_target(&state, &request).await {
            Ok(uri) => (uri, "tr"),
            Err((status, reason)) => {
                // Without a target there is no GOAWAY to send, but the relay is
                // leaving: stop new traffic so Traffic Monitor routes around it.
                if status != StatusCode::BAD_REQUEST {
                    state.mark_unavailable(reason);
                }
                return error(status, reason);
            }
        },
    };
    if let Err(reason) = validate_uri(&uri) {
        return error(StatusCode::BAD_REQUEST, reason);
    }
    if targets_self(&uri, request.current_host().as_deref()) {
        state.mark_unavailable("tr_self_newSessionUri");
        return error(StatusCode::CONFLICT, "tr_self_newSessionUri");
    }

    let sessions = state.drain.start(DrainState {
        new_session_uri: uri,
        timeout_ms,
    });
    // A second drain keeps the first target; report the one actually sent.
    let sent = state.drain.state().expect("drain just started");
    tracing::info!(
        new_session_uri = %sent.new_session_uri,
        timeout_ms = sent.timeout_ms,
        source,
        sessions,
        "relay drain requested"
    );
    Json(json!({
        "draining": true,
        "newSessionUri": sent.new_session_uri,
        "targetSource": source,
        "timeoutMs": sent.timeout_ms,
        "sessions": sessions,
    }))
    .into_response()
}

fn validate_uri(uri: &str) -> Result<(), &'static str> {
    if uri.len() > moq_transport::coding::SessionUri::MAX_LEN {
        return Err("new_session_uri_too_long");
    }
    match Url::parse(uri) {
        Ok(url) if matches!(url.scheme(), "https" | "http" | "moqt") && url.has_host() => Ok(()),
        _ => Err("invalid_new_session_uri"),
    }
}

/// Whether `uri` points back at the host being drained.
fn targets_self(uri: &str, current_host: Option<&str>) -> bool {
    let (Some(current), Ok(target)) = (current_host, Url::parse(uri)) else {
        return false;
    };
    let Ok(current) = Url::parse(&format!("{}://{current}", target.scheme())) else {
        return false;
    };
    current.host_str().map(str::to_ascii_lowercase)
        == target.host_str().map(str::to_ascii_lowercase)
        && current.port_or_known_default() == target.port_or_known_default()
}

/// Ask Traffic Router which healthy relay the drained clients should use.
async fn tr_target(
    state: &AdminState,
    request: &DrainRequest,
) -> Result<String, (StatusCode, &'static str)> {
    let Some(base) = &state.tr_url else {
        return Err((StatusCode::BAD_REQUEST, "missing_newSessionUri"));
    };
    let bad = |reason| (StatusCode::BAD_REQUEST, reason);
    let ip = request
        .get("ip")
        .or_else(|| request.get("clientIp"))
        .ok_or(bad("missing_ip"))?;
    let ds = request
        .get("deliveryServiceId")
        .ok_or(bad("missing_deliveryServiceId"))?;
    let current_host = request.current_host().ok_or(bad("missing_currentHost"))?;

    let mut url = base.clone();
    let path = format!(
        "{}/crs/consistenthash/moq/goaway",
        url.path().trim_end_matches('/')
    );
    url.set_path(&path);
    {
        let mut q = url.query_pairs_mut();
        q.append_pair("ip", &ip)
            .append_pair("deliveryServiceId", &ds)
            .append_pair(
                "requestPath",
                &request.get("requestPath").unwrap_or_else(|| "/".into()),
            )
            .append_pair("currentHost", &current_host)
            .append_pair("m", "MoQ");
        if let Some(rq) = request.get("requestQuery") {
            q.append_pair("requestQuery", &rq);
        }
    }

    let failed = (StatusCode::BAD_GATEWAY, "tr_target_lookup_failed");
    let response = reqwest::Client::new()
        .get(url)
        .timeout(Duration::from_secs(2))
        .send()
        .await
        .map_err(|_| failed)?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "tr_no_healthy_moq_goaway_target",
        ));
    }
    if !response.status().is_success() {
        return Err(failed);
    }
    let body: Value = response
        .json()
        .await
        .map_err(|_| (StatusCode::BAD_GATEWAY, "invalid_tr_target_response"))?;
    body.get("newSessionUri")
        .and_then(Value::as_str)
        .filter(|uri| !uri.trim().is_empty())
        .map(str::to_string)
        .ok_or((StatusCode::BAD_GATEWAY, "missing_tr_newSessionUri"))
}

async fn astats(
    State(state): State<AdminState>,
    Query(query): Query<HashMap<String, String>>,
) -> Json<Value> {
    let drain = state.drain.state();
    let loadavg = std::fs::read_to_string("/proc/loadavg").unwrap_or_default();
    let net_dev = std::fs::read_to_string("/proc/net/dev").unwrap_or_default();
    let inf_name = query.get("inf.name").cloned().unwrap_or_default();
    let net_line = net_dev.lines().skip(2).find_map(|line| {
        let (name, _) = line.split_once(':')?;
        (!inf_name.is_empty() && name.trim() == inf_name).then(|| line.trim().to_string())
    });
    let not_available = state.not_available() || net_line.is_none();

    Json(json!({
        "ats": {
            "plugin.relay_stats.sessions": state.drain.session_count(),
            "plugin.relay_stats.draining": u8::from(drain.is_some()),
            "plugin.relay_stats.drain_new_session_uri":
                drain.as_ref().map(|d| d.new_session_uri.as_str()).unwrap_or(""),
            "plugin.relay_stats.drain_timeout_ms": drain.as_ref().map(|d| d.timeout_ms).unwrap_or(0),
            "plugin.relay_stats.drain_unavailable_reason":
                state.unavailable.lock().unwrap().unwrap_or(""),
        },
        "system": {
            "inf.name": inf_name,
            "inf.speed": 10000,
            "proc.net.dev": net_line.unwrap_or_default(),
            "proc.loadavg": loadavg.trim(),
            "configReloadRequests": 0,
            "lastReloadRequest": 0,
            "configReloads": 0,
            "lastReload": 0,
            "astatsLoad": 0,
            "notAvailable": not_available,
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOKEN: &str = "secret";

    async fn serve(router: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await });
        format!("http://{addr}")
    }

    /// A Traffic Router stub answering every GOAWAY lookup with `reply`,
    /// recording each request's query.
    async fn traffic_router(
        reply: (StatusCode, Value),
    ) -> (Url, Arc<Mutex<Vec<HashMap<String, String>>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = seen.clone();
        let router = Router::new().route(
            "/tr/crs/consistenthash/moq/goaway",
            get(move |Query(q): Query<HashMap<String, String>>| {
                log.lock().unwrap().push(q);
                let reply = reply.clone();
                async move { (reply.0, Json(reply.1)) }
            }),
        );
        (
            format!("{}/tr/", serve(router).await).parse().unwrap(),
            seen,
        )
    }

    async fn admin(token: Option<&str>, tr_url: Option<Url>) -> (String, Drain) {
        let drain = Drain::default();
        let base = serve(router(AdminConfig {
            fingerprint: "ab".repeat(32),
            drain: drain.clone(),
            drain_token: token.map(str::to_string),
            tr_url,
        }))
        .await;
        (base, drain)
    }

    async fn post_drain(base: &str, body: Value) -> (u16, Value) {
        let response = reqwest::Client::new()
            .post(format!("{base}/_drain"))
            .header(DRAIN_TOKEN_HEADER, TOKEN)
            .header("host", "relay-a.example.net:4443")
            .json(&body)
            .send()
            .await
            .unwrap();
        (response.status().as_u16(), response.json().await.unwrap())
    }

    async fn not_available(base: &str) -> bool {
        let astats: Value = reqwest::get(format!("{base}/_astats?inf.name=lo"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        astats["system"]["notAvailable"].as_bool().unwrap()
    }

    fn lookup() -> Value {
        json!({"ip": "203.0.113.7", "deliveryServiceId": "live", "requestPath": "/live/ns", "timeoutMs": 3000})
    }

    #[tokio::test]
    async fn drain_without_a_target_asks_traffic_router() {
        let next = "https://relay-b.example.net:4443/live/ns?m=MoQ";
        let (tr, seen) = traffic_router((StatusCode::OK, json!({"newSessionUri": next}))).await;
        let (base, drain) = admin(Some(TOKEN), Some(tr)).await;

        let (status, body) = post_drain(&base, lookup()).await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["newSessionUri"], next);
        assert_eq!(body["targetSource"], "tr");
        assert_eq!(drain.state().unwrap().new_session_uri, next);

        let q = seen.lock().unwrap().pop().unwrap();
        assert_eq!(q["ip"], "203.0.113.7");
        assert_eq!(q["deliveryServiceId"], "live");
        assert_eq!(q["requestPath"], "/live/ns");
        // Taken from the Host header when the request does not name it.
        assert_eq!(q["currentHost"], "relay-a.example.net:4443");
        assert_eq!(q["m"], "MoQ");
    }

    #[tokio::test]
    async fn traffic_router_failures_mark_unavailable_without_goaway() {
        for (reply, status, error) in [
            (
                (StatusCode::NOT_FOUND, json!({})),
                503,
                "tr_no_healthy_moq_goaway_target",
            ),
            (
                (StatusCode::OK, json!({"newSessionUri": ""})),
                502,
                "missing_tr_newSessionUri",
            ),
            (
                (StatusCode::INTERNAL_SERVER_ERROR, json!({})),
                502,
                "tr_target_lookup_failed",
            ),
            (
                (
                    StatusCode::OK,
                    json!({"newSessionUri": "https://relay-a.example.net:4443/x"}),
                ),
                409,
                "tr_self_newSessionUri",
            ),
        ] {
            let (tr, _) = traffic_router(reply).await;
            let (base, drain) = admin(Some(TOKEN), Some(tr)).await;
            assert!(!not_available(&base).await);

            let (got, body) = post_drain(&base, lookup()).await;
            assert_eq!((got, body["error"].as_str().unwrap()), (status, error));
            assert!(
                drain.state().is_none(),
                "{error}: no GOAWAY target, no GOAWAY"
            );
            assert!(not_available(&base).await, "{error}: must leave rotation");
        }
    }

    #[tokio::test]
    async fn drain_requests_are_validated() {
        let (base, drain) = admin(Some(TOKEN), None).await;
        let next = "https://relay-b.example.net/";
        for (body, error) in [
            (json!({"newSessionUri": next}), "missing_timeoutMs"),
            (
                json!({"newSessionUri": next, "timeoutMs": 0}),
                "invalid_timeoutMs",
            ),
            (
                json!({"newSessionUri": next, "timeoutMs": "soon"}),
                "invalid_timeoutMs",
            ),
            (
                json!({"newSessionUri": "ftp://relay-b/", "timeoutMs": 1}),
                "invalid_new_session_uri",
            ),
            (
                json!({"newSessionUri": "x".repeat(9000), "timeoutMs": 1}),
                "new_session_uri_too_long",
            ),
            (json!({"timeoutMs": 1}), "missing_newSessionUri"),
        ] {
            let (status, body) = post_drain(&base, body).await;
            assert_eq!((status, body["error"].as_str().unwrap()), (400, error));
        }
        assert!(drain.state().is_none());
        assert!(!not_available(&base).await);
    }

    #[tokio::test]
    async fn drain_is_disabled_without_a_token() {
        let (base, _) = admin(None, None).await;
        let (status, body) = post_drain(&base, json!({})).await;
        assert_eq!(
            (status, body["error"].as_str().unwrap()),
            (503, "drain_endpoint_disabled")
        );
    }

    #[test]
    fn self_target_compares_host_and_effective_port() {
        assert!(targets_self("https://a.example/x", Some("a.example:443")));
        assert!(targets_self(
            "https://A.example:4443/",
            Some("a.example:4443")
        ));
        assert!(!targets_self("https://a.example:4443/", Some("a.example")));
        assert!(!targets_self("https://b.example/", Some("a.example")));
        assert!(!targets_self("https://a.example/", None));
    }
}
