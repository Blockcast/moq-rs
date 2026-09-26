// SPDX-FileCopyrightText: 2026 Blockcast Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Phase 3 draft-19 preflight capture for the moq-rs server roles.
//!
//! Records, for one server role, the exact wire profile a real QUIC
//! negotiation selected and the exact GOAWAY bytes a draft-19 control stream
//! carried. It drives the same library entry points the shipped binaries call:
//! [`moq_native_ietf::quic::Client::connect_with_profile`] plus
//! [`Draft19Session::establish`] in the [`Draft19SessionRole::Client`] role for
//! `moq-pub-mmtp`, and the [`Draft19SessionRole::Server`] role for
//! `moq-relay-ietf`'s `serve_draft19_control_plane`.
//!
//! Scope, stated in the artifact and not softened here: neither shipped binary
//! emits a GOAWAY today, so the GOAWAY half is a session-level capture over a
//! real QUIC connection rather than an observation of a production drain.
//! `scripts/draft19-preflight-capture.sh` pairs this with a run of the real
//! binaries, which is what attests the negotiation half end to end.

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, ValueEnum};
use moq_native_ietf::{quic, tls};
use moq_transport::coding::{Encode, SessionUri};
use moq_transport::profile::draft19::{Frame, GoAway, Setup};
use moq_transport::profile::WireProfile;
use moq_transport::session::{Draft19Session, Draft19SessionRole};
use serde::Serialize;
use url::Url;

/// The URI and timeout the capture puts on the wire.
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
    /// `moq-relay-ietf --wire-profile draft19 --node <self>`: origin-advertising relay.
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

    /// TLS material for the in-process QUIC pair.
    #[command(flatten)]
    tls: tls::Args,
}

/// The digest-covered subset: everything a third party can recompute offline
/// from this repository at `source_commit`. Capture timestamps and workflow
/// coordinates are provenance, not payload, so they stay outside the digest.
#[derive(Serialize)]
struct CanonicalPayload {
    role: String,
    binary: String,
    session_role: String,
    offered_versions: Vec<String>,
    selected_version: String,
    setup_type: String,
    goaway_direction: String,
    goaway_uri: String,
    goaway_timeout_ms: String,
    goaway_hex: String,
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
    caveat: &'static str,
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

/// Runs the real negotiation and the real control-stream GOAWAY exchange, and
/// returns what the wire actually carried.
async fn capture(tls: &tls::Config) -> anyhow::Result<(WireProfile, GoAway, Vec<u8>)> {
    let mut server = endpoint(tls, "127.0.0.1:0")?
        .server
        .context("QUIC server endpoint requires a certificate")?;
    let addr: SocketAddr = server.local_addr()?;
    let client = endpoint(tls, "127.0.0.1:0")?.client;

    let accept = tokio::spawn(async move { server.accept().await });

    // Bind is 127.0.0.1, so the URL host must be too: resolving `localhost`
    // can hand back ::1 and the connection then never reaches this endpoint.
    let url = Url::parse(&format!("https://127.0.0.1:{}/", addr.port()))?;
    let (client_session, _id, _transport, client_selected) = client
        .connect_with_profile(&url, Some(addr), WireProfile::Draft19)
        .await
        .context("draft-19 client connect failed")?;

    let (server_session, info) = accept
        .await?
        .context("QUIC server accepted no connection")?;

    anyhow::ensure!(
        client_selected == WireProfile::Draft19 && info.selected_version == WireProfile::Draft19,
        "expected both peers to select {}, got client={client_selected} server={}",
        WireProfile::Draft19,
        info.selected_version,
    );

    // The same two calls `serve_draft19_control_plane` and the `moq-pub-mmtp`
    // draft-19 branch make.
    let server_task = tokio::spawn(Draft19Session::establish(
        server_session,
        Draft19SessionRole::Server,
        WireProfile::Draft19,
        Setup::default(),
    ));
    let mut client_session = Draft19Session::establish(
        client_session,
        Draft19SessionRole::Client,
        WireProfile::Draft19,
        Setup::default(),
    )
    .await
    .context("draft-19 client session establish failed")?;
    let mut server_session = server_task
        .await?
        .context("draft-19 server session establish failed")?;

    let sent = GoAway {
        new_session_uri: SessionUri(GOAWAY_URI.to_string()),
        timeout_ms: GOAWAY_TIMEOUT_MS,
    };
    server_session
        .send_control(&sent.clone().into_frame()?)
        .await
        .context("failed to send draft-19 GOAWAY")?;

    let frame: Frame = client_session
        .receive_control()
        .await
        .context("failed to receive draft-19 GOAWAY")?;
    let decoded = GoAway::from_frame(&frame).context("received frame is not a GOAWAY")?;
    anyhow::ensure!(
        decoded == sent,
        "GOAWAY did not survive the control stream intact"
    );

    // Re-encoding the frame the client decoded yields the bytes the control
    // stream carried, rather than the bytes this process hoped it sent.
    let mut wire = Vec::new();
    frame.encode(&mut wire)?;

    Ok((info.selected_version, decoded, wire))
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

    let (selected, goaway, wire) = capture(&cli.tls.load()?).await?;

    let offered: Vec<String> = OFFERED.iter().map(|p| p.name().to_string()).collect();
    let selected_version = selected.name().to_string();
    // Draft-19 SETUP is 0x2f00, not draft-16's 0x20. Pinning it here is what
    // makes "selected moqt-19" a framing claim and not just an ALPN string.
    let setup_type = "0x2f00".to_string();
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
            peer: "in-process-draft19-peer-over-real-quic",
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
            caveat: "No shipped moq-rs binary emits a draft-19 GOAWAY today: \
                     serve_draft19_control_plane only receives control messages and \
                     moq-pub-mmtp exits after SETUP. These bytes are a session-level \
                     capture over a real QUIC connection, not an observed relay drain.",
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
            offered_versions: offered,
            selected_version,
            setup_type,
            goaway_direction: cli.role.goaway_direction().to_string(),
            goaway_uri: goaway.new_session_uri.0,
            goaway_timeout_ms: timeout_ms,
            goaway_hex,
        },
        canonical_encoding: "UTF-8 JSON with ordered keys and one trailing LF",
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
