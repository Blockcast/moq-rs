// SPDX-License-Identifier: MIT OR Apache-2.0
//
// On-demand profiling endpoint. Compiled ONLY under `--features profiling`
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

#[cfg(feature = "heap-profiling")]
use std::ffi::CString;
#[cfg(feature = "heap-profiling")]
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;
use pprof::protos::Message;

/// Spawn the profiling HTTP endpoint iff `MOQ_PUB_PROFILE_ADDR` is set
/// (e.g. `127.0.0.1:6060`). No-op otherwise.
///
/// CPU endpoints accept an optional `?seconds=N`, default 30, clamped 1..=120:
///   GET /debug/pprof/flamegraph  -> image/svg+xml  (open in a browser)
///   GET /debug/pprof/profile     -> profile.proto  (go tool pprof / speedscope)
///
/// Feature `heap-profiling` adds:
///   GET /debug/pprof/heap        -> jemalloc heap_v2 profile
///   GET /debug/allocator         -> live/active/resident allocator byte totals
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
    #[cfg(feature = "heap-profiling")]
    if let Err(e) = activate_heap_profiling() {
        tracing::warn!(error = %e, "pprof-http: heap profiler unavailable; use the HEAP_PROFILING image build");
    }

    tracing::warn!(%addr, "pprof profiling endpoint LIVE (MOQ_PUB_PROFILE_ADDR set)");

    for req in server.incoming_requests() {
        let url = req.url().to_string();
        let response = match capture_route(&url) {
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

fn capture_route(url: &str) -> Result<(Vec<u8>, &'static str)> {
    #[cfg(feature = "heap-profiling")]
    if url.split('?').next() == Some("/debug/pprof/heap") {
        return capture_heap().map(|body| (body, "application/octet-stream"));
    }

    #[cfg(feature = "heap-profiling")]
    if url.split('?').next() == Some("/debug/allocator") {
        return allocator_stats().map(|body| (body.into_bytes(), "application/json"));
    }

    capture(parse_seconds(url), url.contains("flamegraph"))
}

#[cfg(feature = "heap-profiling")]
fn activate_heap_profiling() -> Result<()> {
    let configured = unsafe { tikv_jemalloc_ctl::raw::read::<bool>(b"opt.prof\0") }
        .map_err(|e| anyhow::anyhow!("read opt.prof: {e}"))?;
    if !configured {
        anyhow::bail!("jemalloc was started without prof:true");
    }
    unsafe { tikv_jemalloc_ctl::raw::write(b"prof.active\0", true) }
        .map_err(|e| anyhow::anyhow!("enable prof.active: {e}"))
}

#[cfg(feature = "heap-profiling")]
fn capture_heap() -> Result<Vec<u8>> {
    static CAPTURE_ID: AtomicU64 = AtomicU64::new(0);

    let id = CAPTURE_ID.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "moq-pub-mmtp-heap-{}-{id}.heap",
        std::process::id()
    ));
    let path_bytes = path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("heap profile path is not UTF-8"))?;
    let c_path = CString::new(path_bytes).map_err(|e| anyhow::anyhow!("heap profile path: {e}"))?;

    unsafe {
        tikv_jemalloc_ctl::raw::write(b"prof.dump\0", c_path.as_ptr())
            .map_err(|e| anyhow::anyhow!("prof.dump: {e}"))?;
    }
    let body = std::fs::read(&path)
        .map_err(|e| anyhow::anyhow!("read heap profile {}: {e}", path.display()))?;
    if let Err(e) = std::fs::remove_file(&path) {
        tracing::debug!(path = %path.display(), error = %e, "failed to remove temporary heap profile");
    }
    Ok(body)
}

#[cfg(feature = "heap-profiling")]
fn allocator_stats() -> Result<String> {
    use tikv_jemalloc_ctl::{epoch, stats};

    epoch::advance().map_err(|e| anyhow::anyhow!("advance allocator epoch: {e}"))?;
    let allocated = stats::allocated::read().map_err(|e| anyhow::anyhow!("allocated: {e}"))?;
    let active = stats::active::read().map_err(|e| anyhow::anyhow!("active: {e}"))?;
    let resident = stats::resident::read().map_err(|e| anyhow::anyhow!("resident: {e}"))?;
    let retained = stats::retained::read().map_err(|e| anyhow::anyhow!("retained: {e}"))?;
    let mapped = stats::mapped::read().map_err(|e| anyhow::anyhow!("mapped: {e}"))?;
    let metadata = stats::metadata::read().map_err(|e| anyhow::anyhow!("metadata: {e}"))?;

    Ok(serde_json::json!({
        "allocated_bytes": allocated,
        "active_bytes": active,
        "resident_bytes": resident,
        "reusable_active_bytes": active.saturating_sub(allocated),
        "retained_virtual_bytes": retained,
        "mapped_bytes": mapped,
        "metadata_bytes": metadata,
    })
    .to_string())
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

#[cfg(all(test, feature = "heap-profiling"))]
mod heap_tests {
    use super::*;

    #[test]
    fn heap_profile_and_allocator_totals_are_non_empty() {
        activate_heap_profiling().expect(
            "start tests with _RJEM_MALLOC_CONF=prof:true,prof_active:false,lg_prof_sample:0",
        );
        let retained = vec![0x5au8; 1024 * 1024];
        std::hint::black_box(&retained);

        let heap = capture_heap().expect("capture heap profile");
        assert!(
            heap.starts_with(b"heap_v2/"),
            "unexpected heap profile header"
        );
        assert!(heap.len() > 64, "heap profile was empty");

        let stats: serde_json::Value = serde_json::from_str(&allocator_stats().unwrap()).unwrap();
        assert!(stats["allocated_bytes"].as_u64().unwrap() > 0);
        assert!(stats["active_bytes"].as_u64().unwrap() > 0);
        assert!(stats["resident_bytes"].as_u64().unwrap() > 0);
    }
}
