<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->

# moq-pub-mmtp

`moq-pub-mmtp` publishes MMTP packets or opaque UDP datagrams over MoQ.

## Shred datagram PMTU contract

The raw-lossy Solana shred profile carries each 1,228-byte UDP payload as one
MoQ datagram. MoQ framing currently produces a datagram of at most 1,234 bytes.
The native QUIC endpoints therefore start with a 1,280-byte UDP payload MTU,
which requires an IP PMTU of at least 1,308 bytes over IPv4 or 1,328 bytes over
IPv6. A normal 1,500-byte Ethernet path satisfies this contract.

Quinn DPLPMTUD remains enabled to discover larger paths and detect black holes.
Deployments using VPN, AMT, GRE, or other encapsulation MUST verify the effective
PMTU end to end before enabling the shred profile. If the path falls below the
contract, the publisher keeps its bounded latest-wins retention policy and logs
counted `skipped_too_large` warnings rather than buffering or resetting the
subscription.
