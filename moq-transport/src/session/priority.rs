// SPDX-FileCopyrightText: 2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Mapping from MoQ Transport priorities onto QUIC stream send order.
//!
//! draft-ietf-moq-transport-16 §7.1 defines priority numbers where a *lower*
//! number means a *higher* priority (0 is the highest). §7.2 schedules by
//! Subscriber Priority first and uses Publisher Priority only to break ties.
//!
//! Quinn schedules the other way around: "Locally buffered data from streams
//! with higher priority will be transmitted before data from streams with lower
//! priority" (`quinn::SendStream::set_priority`). `web_transport::SendStream`
//! forwards its `i32` to quinn unchanged, so passing a MoQ priority number
//! through as-is schedules the least important data first.

/// Subscriber Priority used when the SUBSCRIBER_PRIORITY parameter is omitted
/// from SUBSCRIBE, PUBLISH_OK or FETCH (draft-ietf-moq-transport-16 §9.2.2.3).
pub const DEFAULT_SUBSCRIBER_PRIORITY: u8 = 128;

/// Send order to pass to `web_transport::SendStream::set_priority` for data
/// scheduled under the given MoQ Subscriber and Publisher Priorities.
///
/// The two 8-bit priorities are combined lexicographically, Subscriber Priority
/// in the high byte (§7.2 rule 1) and Publisher Priority in the low byte
/// (§7.2 rule 2), and the result is negated so that a numerically lower MoQ
/// priority becomes a numerically higher quinn priority.
pub(crate) fn send_order(subscriber_priority: u8, publisher_priority: u8) -> i32 {
    let moq_order = (i32::from(subscriber_priority) << 8) | i32::from(publisher_priority);
    -moq_order
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lower_publisher_priority_is_sent_first_across_full_range() {
        for subscriber in [0, DEFAULT_SUBSCRIBER_PRIORITY, u8::MAX] {
            for publisher in 0..u8::MAX {
                assert!(
                    send_order(subscriber, publisher) > send_order(subscriber, publisher + 1),
                    "publisher priority {publisher} must be sent before {} at subscriber priority {subscriber}",
                    publisher + 1
                );
            }
        }
    }

    #[test]
    fn lower_subscriber_priority_is_sent_first_across_full_range() {
        for publisher in [0, DEFAULT_SUBSCRIBER_PRIORITY, u8::MAX] {
            for subscriber in 0..u8::MAX {
                assert!(
                    send_order(subscriber, publisher) > send_order(subscriber + 1, publisher),
                    "subscriber priority {subscriber} must be sent before {} at publisher priority {publisher}",
                    subscriber + 1
                );
            }
        }
    }

    #[test]
    fn subscriber_priority_dominates_publisher_priority() {
        // §7.2 rule 1: a more urgent Subscriber Priority wins even against the
        // most urgent Publisher Priority of a less urgent request.
        for subscriber in 0..u8::MAX {
            assert!(send_order(subscriber, u8::MAX) > send_order(subscriber + 1, 0));
        }
    }

    #[test]
    fn source_media_is_sent_before_repair_at_equal_subscriber_priority() {
        // A source track at Publisher Priority 128 and its repair track at 240,
        // both subscribed with the default Subscriber Priority.
        let source = send_order(DEFAULT_SUBSCRIBER_PRIORITY, 128);
        let repair = send_order(DEFAULT_SUBSCRIBER_PRIORITY, 240);
        assert!(source > repair, "quinn sends larger priorities first");
    }

    #[test]
    fn range_endpoints() {
        assert_eq!(send_order(0, 0), 0);
        assert_eq!(send_order(u8::MAX, u8::MAX), -0xffff);
    }
}
