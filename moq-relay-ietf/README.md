# moq-relay

A server that connects publishing clients to subscribing clients.
All subscriptions are deduplicated and cached, so that a single publisher can serve many subscribers.

## Usage

The publisher must choose a unique name for their broadcast, sent as the WebTransport path when connecting to the server.
Connection paths are normalized and validated: trailing slashes are trimmed, dot segments and percent-encoded characters are rejected, and empty segments are not allowed. Capitalization matters.

For example: `CONNECT https://relay.quic.video/BigBuckBunny`

The MoqTransport handshake includes a `role` parameter, which must be `publisher` or `subscriber`.
The specification allows a `both` role but you'll get an error.

You can have one publisher and any number of subscribers connected to the same path.
If the publisher disconnects, then all subscribers receive an error and will not get updates, even if a new publisher reuses the path.

## Authorization

Per-scope bearer-token authorization is configured via `ScopeConfig.auth`. When a scope has an auth policy, every accepted client session must present a valid token in the SETUP AUTHORIZATION TOKEN option (draft-ietf-moq-transport-18 §10.3.1.4), and the session is closed with UNAUTHORIZED otherwise. Scopes without a policy are unaffected. Known gap: relay-to-relay `--announce` forward links are relay-initiated and currently do not present a token; they emit a warning when connecting to an auth-enabled scope.

The enforcement model and fail-closed contract are documented in `src/auth/mod.rs`.
