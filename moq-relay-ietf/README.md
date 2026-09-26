# moq-relay

A server that connects publishing clients to subscribing clients.
SUBSCRIBE requests are deduplicated and cached, so that a single publisher can serve many subscribers.
Standalone FETCH requests are never deduplicated. With `--fetch-retention-groups N --fetch-retention-bytes B` the relay retains the Objects of the N most recent groups of every track it receives, holding at most B bytes of them per track, and answers a FETCH from them when it holds every Object of the range; otherwise it creates a fresh upstream FETCH for the part it does not hold.

## Usage

The publisher must choose a unique name for their broadcast, sent as the WebTransport path when connecting to the server.
Connection paths are normalized and validated: trailing slashes are trimmed, dot segments and percent-encoded characters are rejected, and empty segments are not allowed. Capitalization matters.

For example: `CONNECT https://relay.quic.video/BigBuckBunny`

The MoqTransport handshake includes a `role` parameter, which must be `publisher` or `subscriber`.
The specification allows a `both` role but you'll get an error.

You can have one publisher and any number of subscribers connected to the same path.
If the publisher disconnects, then all subscribers receive an error and will not get updates, even if a new publisher reuses the path.
