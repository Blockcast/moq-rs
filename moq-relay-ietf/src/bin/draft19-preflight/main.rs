// SPDX-FileCopyrightText: 2026 Blockcast Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Phase 3 draft-19 preflight capture for the moq-rs server roles.
//!
//! Records, for one server role, the exact wire profile a real QUIC
//! negotiation selected and the exact GOAWAY bytes a draft-19 control stream
//! carried.
//!
//! Both halves are observations of the shipped binaries. This process is only
//! the draft-19 *client*: it dials a running `moq-relay-ietf --wire-profile
//! draft19` through [`moq_native_ietf::quic::Client::connect_with_profile`] and
//! [`Draft19Session::establish`], exactly as `moq-pub-mmtp`'s draft-19 branch
//! does, and then reads the control stream. The GOAWAY it decodes is the one
//! `serve_draft19_control_plane` emitted when the relay was signalled to drain,
//! so the artifact attests a production drain rather than a session-level
//! capture. `scripts/draft19-preflight-capture.sh` runs the relay, arms the
//! drain, and adds the relay's own GOAWAY log line to the emitted artifact as
//! `goaway.binary_evidence`, the same way it adds `sha256`: both are provenance
//! the script observes after this process exits, and both sit outside the
//! canonical digest.

use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::{Parser, ValueEnum};
use moq_native_ietf::{quic, tls};
use moq_transport::coding::Encode;
use moq_transport::profile::draft19::{Frame, GoAway, Setup, GOAWAY_TYPE, SETUP_TYPE};
use moq_transport::profile::WireProfile;
use moq_transport::session::{Draft19Session, Draft19SessionRole};
use serde::Serialize;
use url::Url;

/// The URI and timeout the relay is configured to advertise, and which this
/// capture requires the received GOAWAY to carry.
///
/// The URI matches the Phase 3 client rows so the server and client hex are
/// directly comparable. The timeout is deliberately non-zero and multi-byte
/// (250 encodes as the two-byte varint `80fa`), so a decoder that silently
/// dropped the Timeout field could not produce this capture.
const GOAWAY_URI: &str = "moqt://next.example";
const GOAWAY_TIMEOUT_MS: u64 = 250;

/// Both endpoints offer the relay's real additive set: draft-16 stays the
/// default and draft-19 is opt-in, exactly as `enabled_wire_profiles` builds it.
const OFFERED: [WireProfile; 2] = [WireProfile::Draft16, WireProfile::Draft19];

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Role {
    /// `moq-pub-mmtp --wire-profile draft19`: the draft-19 client side.
    Publisher,
    /// `moq-relay-ietf --wire-profile draft19 --node <self>`: the relay as it
    /// is launched to advertise itself as an origin. `--node` reaches no code
    /// the draft-19 accept path runs, so see `path_equivalent_to`.
    RelayRoot,
    /// `moq-relay-ietf --wire-profile draft19`: coordinator-consuming relay.
    RelayLeaf,
}

impl Role {
    const fn name(self) -> &'static str {
        match self {
            Self::Publisher => "publisher",
            Self::RelayRoot => "relay-root",
            Self::RelayLeaf => "relay-leaf",
        }
    }

    const fn binary(self) -> &'static str {
        match self {
            Self::Publisher => "moq-pub-mmtp",
            Self::RelayRoot | Self::RelayLeaf => "moq-relay-ietf",
        }
    }

    /// Which side of the exchange this artifact attests.
    const fn session_role(self) -> &'static str {
        match self {
            Self::Publisher => "client",
            Self::RelayRoot | Self::RelayLeaf => "server",
        }
    }

    /// GOAWAY always travels server to client: only a server may carry a New
    /// Session URI (`Draft19SessionRole::validate_received_goaway`).
    const fn goaway_direction(self) -> &'static str {
        match self {
            Self::Publisher => "relay_to_publisher",
            Self::RelayRoot | Self::RelayLeaf => "relay_to_subscriber",
        }
    }

    /// The role whose draft-19 negotiation this row duplicates, if any.
    ///
    /// The two relay rows observe one identical negotiation, not two. The only
    /// flag separating them is `--node`, and `RelayConfig.node` is written at
    /// `moq-relay-ietf/src/bin/moq-relay-ietf/main.rs:223` and never read
    /// anywhere in the crate; the value the capture passes also recomputes the
    /// byte-identical `relay_url` that the `None` fallback already produces.
    /// Recording that inside the digest is what stops a consumer counting them
    /// as two independent attestations.
    const fn path_equivalent_to(self) -> Option<&'static str> {
        match self {
            Self::Publisher => None,
            Self::RelayRoot => Some("relay-leaf"),
            Self::RelayLeaf => Some("relay-root"),
        }
    }
}

#[derive(Parser)]
#[command(about = "Capture a Phase 3 draft-19 preflight attestation for one moq-rs server role")]
struct Cli {
    /// Server role this attestation covers.
    #[arg(long, value_enum)]
    role: Role,

    /// Immutable source commit the capture ran against (full 40-hex).
    #[arg(long)]
    source_commit: String,

    /// Evidence that the shipped binary negotiated moqt-19: the log line
    /// `scripts/draft19-preflight-capture.sh` extracted from the real run.
    #[arg(long)]
    binary_evidence: String,

    /// The exact command line the shipped binary was invoked with.
    #[arg(long)]
    binary_invocation: String,

    /// RFC 3339 capture timestamp, supplied by the caller so the attestation
    /// records when the whole capture ran rather than when this process started.
    #[arg(long)]
    captured_at: String,

    /// Where to write the attestation JSON.
    #[arg(long)]
    output: PathBuf,

    /// The running `moq-relay-ietf --wire-profile draft19` to dial.
    #[arg(long)]
    relay_url: Url,

    /// Touched once the draft-19 session is established and admitted to the
    /// relay's drain. The caller waits for it before signalling the relay, so
    /// the drain cannot fire while SETUP is still in flight.
    #[arg(long)]
    ready_file: PathBuf,

    /// TLS material for the in-process QUIC pair.
    #[command(flatten)]
    tls: tls::Args,
}

/// The digest-covered subset: everything a third party can recompute offline
/// from this repository at `source_commit`. Capture timestamps and workflow
/// coordinates are provenance, not payload, so they stay outside the digest.
///
/// `attests` states the consequence of that split in the artifact itself,
/// because it is not the one a reader expects: every other field here is a
/// constant, an enum-derived string, or a deterministic function of those, so
/// the digest is an encoding fingerprint rather than a build fingerprint.
#[derive(Serialize)]
struct CanonicalPayload {
    role: String,
    binary: String,
    session_role: String,
    path_equivalent_to: Option<&'static str>,
    offered_versions: Vec<String>,
    selected_version: String,
    setup_type: String,
    goaway_direction: String,
    goaway_uri: String,
    goaway_timeout_ms: String,
    goaway_hex: String,
    attests: &'static str,
}

#[derive(Serialize)]
struct Source {
    repository: &'static str,
    source_commit: String,
}

#[derive(Serialize)]
struct Handshake {
    peer: &'static str,
    binary: String,
    binary_invocation: String,
    binary_evidence: String,
    offered_versions: Vec<String>,
    selected_version: String,
    setup_type: String,
}

#[derive(Serialize)]
struct Goaway {
    direction: String,
    stream: &'static str,
    hex: String,
    decoded_uri: String,
    decoded_timeout_ms: String,
}

#[derive(Serialize)]
struct CaptureMeta {
    captured_at: String,
    command: String,
    tool: &'static str,
    tool_version: String,
}

#[derive(Serialize)]
struct Attestation {
    schema_version: u8,
    role: String,
    source: Source,
    handshake: Handshake,
    goaway: Goaway,
    capture: CaptureMeta,
    canonical_payload: CanonicalPayload,
    canonical_encoding: &'static str,
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn endpoint(tls: &tls::Config, bind: &str) -> anyhow::Result<quic::Endpoint> {
    let config = quic::Config::new(bind.parse()?, None, tls.clone())?.with_wire_profiles(OFFERED);
    quic::Endpoint::new(config)
}

/// Dial the running relay and record the GOAWAY it sends when it drains.
///
/// Returns what the wire actually carried. This process never sends a GOAWAY:
/// the bytes below are the ones `serve_draft19_control_plane` wrote.
async fn capture(
    tls: &tls::Config,
    relay_url: &Url,
    ready_file: &Path,
) -> anyhow::Result<(WireProfile, GoAway, Vec<u8>)> {
    let client = endpoint(tls, "127.0.0.1:0")?.client;

    // The same two calls the `moq-pub-mmtp` draft-19 branch makes.
    let (conn, _id, _transport, selected) = client
        .connect_with_profile(relay_url, None, WireProfile::Draft19)
        .await
        .context("draft-19 connect to the relay failed")?;
    anyhow::ensure!(
        selected == WireProfile::Draft19,
        "expected the relay to select {}, got {selected}",
        WireProfile::Draft19,
    );

    let mut session = Draft19Session::establish(
        conn,
        Draft19SessionRole::Client,
        WireProfile::Draft19,
        Setup::default(),
    )
    .await
    .context("draft-19 session establish failed")?;

    // Only now is this connection admitted to the relay's drain, so only now
    // can the caller signal the relay without racing the SETUP exchange.
    std::fs::write(ready_file, b"ready")
        .with_context(|| format!("failed to write ready file {}", ready_file.display()))?;

    let frame: Frame = loop {
        let frame = session
            .receive_control()
            .await
            .context("control stream ended before the relay sent a GOAWAY")?;
        if frame.message_type == GOAWAY_TYPE {
            break frame;
        }
        tracing::debug!(
            message_type = frame.message_type,
            "ignoring non-GOAWAY control frame while waiting for the drain"
        );
    };
    let decoded = GoAway::from_frame(&frame).context("received frame is not a GOAWAY")?;

    // Fail closed rather than attest bytes that disagree with the relay's
    // configuration: that the two match is the whole claim of this row.
    anyhow::ensure!(
        decoded.new_session_uri.0 == GOAWAY_URI && decoded.timeout_ms == GOAWAY_TIMEOUT_MS,
        "relay sent GOAWAY uri={} timeout_ms={}, but was configured with uri={} timeout_ms={}",
        decoded.new_session_uri.0,
        decoded.timeout_ms,
        GOAWAY_URI,
        GOAWAY_TIMEOUT_MS,
    );

    // Re-encoding the frame the client decoded yields the bytes the control
    // stream carried, rather than the bytes this process hoped it sent.
    let mut wire = Vec::new();
    frame.encode(&mut wire)?;

    Ok((selected, decoded, wire))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let cli = Cli::parse();
    anyhow::ensure!(
        cli.source_commit.len() == 40 && cli.source_commit.bytes().all(|b| b.is_ascii_hexdigit()),
        "--source-commit must be a full 40-hex commit SHA"
    );
    anyhow::ensure!(
        cli.binary_evidence.contains(WireProfile::Draft19.name()),
        "--binary-evidence must quote a {} log line from the shipped binary",
        WireProfile::Draft19.name()
    );

    let (selected, goaway, wire) = capture(&cli.tls.load()?, &cli.relay_url, &cli.ready_file).await?;

    let offered: Vec<String> = OFFERED.iter().map(|p| p.name().to_string()).collect();
    let selected_version = selected.name().to_string();
    // Draft-19 SETUP is 0x2f00, not draft-16's 0x20. Rendered from the constant
    // the framing code itself encodes with, so the artifact cannot keep
    // asserting a type the wire profile has moved away from.
    let setup_type = format!("{SETUP_TYPE:#06x}");
    let goaway_hex = encode_hex(&wire);
    let timeout_ms = goaway.timeout_ms.to_string();

    let attestation = Attestation {
        schema_version: 1,
        role: cli.role.name().to_string(),
        source: Source {
            repository: "Blockcast/moq-rs",
            source_commit: cli.source_commit.clone(),
        },
        handshake: Handshake {
            peer: "moq-relay-ietf --wire-profile draft19, shipped binary, over real QUIC",
            binary: cli.role.binary().to_string(),
            binary_invocation: cli.binary_invocation.clone(),
            binary_evidence: cli.binary_evidence.clone(),
            offered_versions: offered.clone(),
            selected_version: selected_version.clone(),
            setup_type: setup_type.clone(),
        },
        goaway: Goaway {
            direction: cli.role.goaway_direction().to_string(),
            stream: "control",
            hex: goaway_hex.clone(),
            decoded_uri: goaway.new_session_uri.0.clone(),
            decoded_timeout_ms: timeout_ms.clone(),
        },
        capture: CaptureMeta {
            captured_at: cli.captured_at.clone(),
            command: format!("draft19-preflight --role {}", cli.role.name()),
            tool: "draft19-preflight",
            tool_version: env!("CARGO_PKG_VERSION").to_string(),
        },
        canonical_payload: CanonicalPayload {
            role: cli.role.name().to_string(),
            binary: cli.role.binary().to_string(),
            session_role: cli.role.session_role().to_string(),
            path_equivalent_to: cli.role.path_equivalent_to(),
            offered_versions: offered,
            selected_version,
            setup_type,
            goaway_direction: cli.role.goaway_direction().to_string(),
            goaway_uri: goaway.new_session_uri.0,
            goaway_timeout_ms: timeout_ms,
            goaway_hex,
            attests: "The draft-19 constants this repository encodes with at \
                      source_commit, and the GOAWAY bytes a shipped \
                      moq-relay-ietf emitted from serve_draft19_control_plane \
                      when it was signalled to drain, read off a real QUIC \
                      control stream by this process acting only as the \
                      draft-19 client. It does not cover the binaries' own \
                      negotiation: that is quoted in \
                      handshake.binary_evidence, and the relay's GOAWAY log \
                      line in goaway.binary_evidence. Both sit outside this \
                      digest because a log line carries a timestamp, a \
                      temporary directory and an ephemeral port. The bytes \
                      here are a deterministic function of the encoder and \
                      the configured URI and Timeout, so two commits share \
                      this digest unless the GOAWAY encoder or a profile \
                      constant changes. Authenticity of the shipped binary \
                      half rests on the workflow run and the artifact digest, \
                      not on this sha256. The byte encoding to recompute this \
                      digest over is named in canonical_encoding, one level \
                      up, which is outside the digest because it describes it.",
        },
        // Names the serialization, not just the charset: the artifact ships
        // this payload pretty-printed, so digesting the bytes as they appear
        // gives a different answer than the shipped sha256.
        canonical_encoding: "UTF-8 JSON, compact (no insignificant whitespace), \
                             keys in the order emitted here, minimal string \
                             escaping with non-ASCII emitted as raw UTF-8, one \
                             trailing LF. Recompute with: \
                             jq -c '.canonical_payload' <artifact> | sha256sum",
    };

    if let Some(parent) = cli.output.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(
        &cli.output,
        format!("{}\n", serde_json::to_string_pretty(&attestation)?),
    )?;
    println!("wrote {}", cli.output.display());
    Ok(())
}
