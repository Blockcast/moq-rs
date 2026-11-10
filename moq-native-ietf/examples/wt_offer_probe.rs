// SPDX-License-Identifier: MIT OR Apache-2.0
//
// Offer exact WebTransport subprotocol tokens and report which one the server
// selects (BLO-55523 leg c). `Client::connect_with_profile` only ever offers the
// ROOT-13 token `moqt-wt-16`, so a hosted relay that expects the bare draft-16
// `moqt-16` token cannot be measured with the fork's normal clients.
//
// Usage: wt_offer_probe <https-url> <token> [<token>...]
//
// Prints one line: `offered=<tokens> selected=<token|none>` on success, or
// `offered=<tokens> refused: <error>`. The URL path is never printed, because
// a hosted relay carries its connection token there.

use std::sync::Arc;

use anyhow::Context;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let url: url::Url = args
        .next()
        .context("usage: wt_offer_probe <https-url> <token> [<token>...]")?
        .parse()?;
    let offered: Vec<String> = args.collect();
    anyhow::ensure!(!offered.is_empty(), "offer at least one subprotocol token");
    let host = url.host_str().context("URL has no host")?.to_string();
    let port = url.port_or_known_default().context("URL has no port")?;
    let redacted = format!("{}://{host}:{port}/<redacted>", url.scheme());

    // TLS_DISABLE_VERIFY is for a local dry run against a self-signed relay only.
    let tls_args = moq_native_ietf::tls::Args {
        disable_verify: std::env::var_os("TLS_DISABLE_VERIFY").is_some(),
        ..Default::default()
    };
    let mut tls = tls_args.load()?.client;
    tls.alpn_protocols = vec![web_transport_quinn::ALPN.as_bytes().to_vec()];
    let tls: quinn::crypto::rustls::QuicClientConfig = tls.try_into()?;

    let addr = tokio::net::lookup_host((host.as_str(), port))
        .await?
        .next()
        .context("host did not resolve")?;
    let bind = if addr.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let endpoint = quinn::Endpoint::client(bind.parse()?)?;

    let offered_label = offered.join(",");
    let result = async {
        let connection = endpoint
            .connect_with(quinn::ClientConfig::new(Arc::new(tls)), addr, &host)?
            .await?;
        let mut request = web_transport_quinn::proto::ConnectRequest::new(url.clone());
        for token in &offered {
            request = request.with_protocol(token.as_str());
        }
        anyhow::Ok(web_transport_quinn::Session::connect(connection, request).await?)
    }
    .await;

    match result {
        Ok(session) => {
            let selected = session.response().protocol.clone();
            println!(
                "target={redacted} offered={offered_label} selected={}",
                selected.as_deref().unwrap_or("none")
            );
            Ok(())
        }
        Err(error) => {
            // The error text can echo the request URL; strip its path.
            let mut error = format!("{error:#}");
            if url.path().len() > 1 {
                error = error.replace(url.path(), "/<redacted>");
            }
            println!("target={redacted} offered={offered_label} refused: {error}");
            std::process::exit(1);
        }
    }
}
