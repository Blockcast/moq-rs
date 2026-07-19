// SPDX-License-Identifier: MIT OR Apache-2.0
//
// On-demand CPU profiling endpoint. Compiled ONLY under `--features profiling`
// and activated ONLY when `MOQ_PUB_PROFILE_ADDR` is set at runtime. With the
// feature off (the default) this module is not compiled and pulls in no deps,
// so the shipped binary is byte-for-byte unchanged.
//
// Why pprof-rs: the pub is CPU-bound in userspace on a single task, so a
// signal-based CPU sampler (setitimer(ITIMER_PROF)+SIGPROF, entirely userspace)
// is the right instrument and — unlike `perf record` — needs no CAP_SYS_ADMIN,
// so it runs under the default (baseline) PodSecurity standard. The release
// binary already carries DWARF (`[profile.release] debug = true`), so frames
// symbolize without frame pointers.
//
// tiny_http runs the endpoint on its OWN std thread, sharing no state with the
// tokio runtime or the QUIC session, so it cannot perturb the hot path and
// still answers while the runtime is CPU-saturated. Bind loopback only
// (e.g. 127.0.0.1:6060) and reach it with `kubectl port-forward`.

use std::time::Duration;

use anyhow::Result;
use pprof::protos::Message;

/// Spawn the profiling HTTP endpoint iff `MOQ_PUB_PROFILE_ADDR` is set
/// (e.g. `127.0.0.1:6060`). No-op otherwise.
///
/// Endpoints (both accept an optional `?seconds=N`, default 30, clamped 1..=120):
///   GET /debug/pprof/flamegraph  -> image/svg+xml  (open in a browser)
///   GET /debug/pprof/profile     -> profile.proto  (go tool pprof / speedscope)
pub fn spawn_if_enabled() {
    let Ok(addr) = std::env::var("MOQ_PUB_PROFILE_ADDR") else {
        return;
    };
    let builder = std::thread::Builder::new().name("pprof-http".into());
    if let Err(e) = builder.spawn(move || serve(&addr)) {
        tracing::warn!(error = %e, "pprof-http: failed to spawn thread");
    }
}

fn serve(addr: &str) {
    let server = match tiny_http::Server::http(addr) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(%addr, error = %e, "pprof-http: bind failed; profiling disabled");
            return;
        }
    };
    tracing::warn!(%addr, "pprof profiling endpoint LIVE (MOQ_PUB_PROFILE_ADDR set)");

    for req in server.incoming_requests() {
        let url = req.url().to_string();
        let seconds = parse_seconds(&url);
        let want_flamegraph = url.contains("flamegraph");
        let response = match capture(seconds, want_flamegraph) {
            Ok((body, content_type)) => {
                // Static header name/value are known-valid; the parse cannot fail.
                let header =
                    tiny_http::Header::from_bytes(&b"Content-Type"[..], content_type.as_bytes())
                        .expect("static Content-Type header is valid");
                tiny_http::Response::from_data(body).with_header(header)
            }
            Err(e) => {
                tracing::warn!(error = %e, "pprof-http: capture failed");
                tiny_http::Response::from_string(format!("capture failed: {e}\n"))
                    .with_status_code(500)
            }
        };
        // A dropped client connection must not kill the endpoint thread.
        if let Err(e) = req.respond(response) {
            tracing::debug!(error = %e, "pprof-http: respond failed (client gone?)");
        }
    }
}

/// Sample the process for `seconds`, then render either an SVG flamegraph or a
/// pprof protobuf. Returns `(body, content_type)`.
fn capture(seconds: u64, flamegraph: bool) -> Result<(Vec<u8>, &'static str)> {
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(99)
        // Skip these libs to avoid libunwind/vdso unwinding hazards.
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .map_err(|e| anyhow::anyhow!("ProfilerGuardBuilder::build: {e}"))?;

    std::thread::sleep(Duration::from_secs(seconds));

    let report = guard
        .report()
        .build()
        .map_err(|e| anyhow::anyhow!("report build: {e}"))?;

    if flamegraph {
        let mut svg = Vec::new();
        report
            .flamegraph(&mut svg)
            .map_err(|e| anyhow::anyhow!("flamegraph render: {e}"))?;
        Ok((svg, "image/svg+xml"))
    } else {
        let profile = report
            .pprof()
            .map_err(|e| anyhow::anyhow!("pprof encode: {e}"))?;
        let mut buf = Vec::new();
        profile
            .write_to_writer(&mut buf)
            .map_err(|e| anyhow::anyhow!("pprof write: {e}"))?;
        Ok((buf, "application/octet-stream"))
    }
}

/// Parse `?seconds=N` from the URL query; default 30, clamp to `1..=120`.
fn parse_seconds(url: &str) -> u64 {
    url.split("seconds=")
        .nth(1)
        .and_then(|s| s.split('&').next())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(30)
        .clamp(1, 120)
}
