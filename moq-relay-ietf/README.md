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

Per-scope bearer-token authorization is configured via `ScopeConfig.auth`. When a scope has an auth policy, every accepted client session must present a valid token in the SETUP AUTHORIZATION TOKEN option (draft-ietf-moq-transport-18 §10.3.1.4), and the session is closed with UNAUTHORIZED otherwise. Sessions the embedder tags as internal relay peers are exempt; it must authenticate those peers first. Scopes without a policy admit sessions as before, but in every scope a structurally invalid token option is still a protocol error (§10.2.2): an alias directive in SETUP closes with PROTOCOL_VIOLATION, and an undecodable Token with KEY_VALUE_FORMATTING_ERROR. Known gap: relay-to-relay `--announce` forward links are relay-initiated and currently do not present a token; they emit a warning when connecting to an auth-enabled scope.

Requests on an admitted public session on a scope with a policy are then checked against the token before the relay acts on them: PUBLISH_NAMESPACE, PUBLISH, SUBSCRIBE, SUBSCRIBE_NAMESPACE, SUBSCRIBE_TRACKS and TRACK_STATUS. A denied request is refused with REQUEST_ERROR UNAUTHORIZED and leaves the session running. SUBSCRIBE_NAMESPACE and SUBSCRIBE_TRACKS withhold namespaces and tracks the token does not cover. The enforcement model, the CAT action mapping, and the fail-closed contract are documented in `src/auth/mod.rs`.

The `auth-cat` crate feature, which provides Common Access Token (CAT, draft-ietf-moq-c4m) verification, is off by default. A relay built without it cannot verify any token, so every scope with a policy refuses all public sessions. Embedders that want token enforcement enable the feature and supply `ScopeAuthConfig` through `Coordinator::get_scope_config`.

**FETCH grant.** This relay does not serve FETCH yet. When relay FETCH serving lands, FETCH will require the CAT `moqt` claim to include a `Fetch(7)` scope covering the requested namespace and track; tokens that grant only `Subscribe(4)` will be denied. `CatAuthHook` already evaluates FETCH this way, and the request gate will land with FETCH serving.
