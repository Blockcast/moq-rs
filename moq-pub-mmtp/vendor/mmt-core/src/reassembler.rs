use bytes::{Bytes, BytesMut};
/**
 * MFU Fragment Reassembly for MMT - Zero-Copy Optimized
 *
 * Handles out-of-order delivery of fragmented MFUs according to
 * ISO/IEC 23008-1:2023 Section 9.2.3.3
 *
 * Performance optimizations:
 * - Zero-copy for complete MFUs (FI=0) - just moves ownership
 * - Pre-allocated reassembly buffer with capacity hints
 * - Avoids cloning Bytes where possible (uses slice views)
 * - Binary search for duplicate detection (O(log n) vs O(n))
 * - No allocations in hot data path
 *
 * Fragmentation Indicator (FI) values:
 * - 0: Complete MFU (single packet, no fragmentation)
 * - 1: First fragment
 * - 2: Middle fragment
 * - 3: Last fragment
 */
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Fragment information extracted from MPU header
#[derive(Debug)]
pub struct MfuFragment {
    /// MPU sequence number for grouping fragments
    pub mpu_sequence_number: u32,

    /// Fragmentation indicator (0-3)
    pub fragmentation_indicator: u8,

    /// Fragment counter within MFU
    pub fragment_counter: u16,

    /// Fragment payload (after MPU/MFU headers stripped)
    /// Uses Bytes for zero-copy slicing from network buffer
    pub data: Bytes,

    /// MMTP timestamp
    pub timestamp: u32,

    /// RAP (Random Access Point) flag from MMTP header
    /// Only meaningful for first fragment (FI=1) of a fragmented MFU
    pub rap_flag: bool,

    /// When fragment was received (for timeout tracking)
    #[allow(dead_code)]
    received_at: Instant,
}

impl MfuFragment {
    /// Create a new MFU fragment - takes ownership, no clone
    #[inline]
    pub fn new(
        mpu_sequence_number: u32,
        fragmentation_indicator: u8,
        fragment_counter: u16,
        data: Bytes,
        timestamp: u32,
        rap_flag: bool,
    ) -> Self {
        Self {
            mpu_sequence_number,
            fragmentation_indicator,
            fragment_counter,
            data,
            timestamp,
            rap_flag,
            received_at: Instant::now(),
        }
    }

    /// Create fragment with explicit receive time (for testing)
    #[inline]
    pub fn with_receive_time(
        mpu_sequence_number: u32,
        fragmentation_indicator: u8,
        fragment_counter: u16,
        data: Bytes,
        timestamp: u32,
        rap_flag: bool,
        received_at: Instant,
    ) -> Self {
        Self {
            mpu_sequence_number,
            fragmentation_indicator,
            fragment_counter,
            data,
            timestamp,
            rap_flag,
            received_at,
        }
    }
}

/// Reassembled MFU ready for decoder
#[derive(Debug)]
pub struct ReassembledMfu {
    /// MPU sequence number
    pub mpu_sequence_number: u32,

    /// Complete reassembled MFU data
    pub data: Bytes,

    /// Original timestamp from first fragment
    pub timestamp: u32,

    /// Number of fragments reassembled
    pub fragment_count: usize,

    /// RAP (Random Access Point) flag from first fragment
    /// Indicates whether this MFU contains a keyframe/IDR
    pub rap_flag: bool,
}

/// Statistics for monitoring reassembler health
#[derive(Debug, Clone, Default)]
pub struct ReassemblerStats {
    pub total_fragments: u64,
    pub reassembled_mfus: u64,
    pub timed_out_mfus: u64,
    pub out_of_order_fragments: u64,
    pub duplicate_fragments: u64,
}

/// Fragment reassembly buffer - pre-allocates for expected fragment count
struct FragmentBuffer {
    /// Buffered fragments for this MPU sequence
    /// Kept sorted by fragment_counter for O(log n) duplicate check
    fragments: Vec<MfuFragment>,

    /// When first fragment arrived
    started_at: Instant,

    /// Pre-allocated reassembly buffer (reused across reassemblies)
    reassembly_buf: BytesMut,

    /// Total data size accumulated (avoids re-summing)
    total_data_size: usize,

    /// RAP flag from first fragment (FI=1)
    /// Preserved for keyframe detection after reassembly
    first_rap_flag: Option<bool>,
}

impl FragmentBuffer {
    #[inline]
    fn new() -> Self {
        Self {
            fragments: Vec::with_capacity(4), // Typical: 2-4 fragments per MFU
            started_at: Instant::now(),
            reassembly_buf: BytesMut::with_capacity(16 * 1024), // 16KB initial
            total_data_size: 0,
            first_rap_flag: None,
        }
    }

    /// Binary search for fragment by counter - O(log n)
    #[inline]
    fn find_by_counter(&self, counter: u16) -> Result<usize, usize> {
        self.fragments
            .binary_search_by_key(&counter, |f| f.fragment_counter)
    }
}

/// MFU Fragment Reassembler - Zero-Copy Design
///
/// Buffers out-of-order fragments and reassembles complete MFUs
pub struct MfuReassembler {
    /// Active fragment buffers by MPU sequence number
    buffers: HashMap<u32, FragmentBuffer>,

    /// Maximum time to wait for incomplete MFU
    timeout: Duration,

    /// Maximum number of concurrent MFUs being assembled
    max_buffered_mfus: usize,

    /// Statistics
    stats: ReassemblerStats,

    /// Pool of reusable fragment buffers (reduces allocation)
    buffer_pool: Vec<FragmentBuffer>,
}

impl Default for MfuReassembler {
    fn default() -> Self {
        Self::new(Duration::from_secs(1), 100)
    }
}

impl MfuReassembler {
    /// Create a new MFU reassembler
    ///
    /// # Arguments
    /// * `timeout` - Maximum time to wait for incomplete MFU
    /// * `max_buffered_mfus` - Memory limit (max concurrent incomplete MFUs)
    pub fn new(timeout: Duration, max_buffered_mfus: usize) -> Self {
        Self {
            buffers: HashMap::with_capacity(max_buffered_mfus),
            timeout,
            max_buffered_mfus,
            stats: ReassemblerStats::default(),
            buffer_pool: Vec::with_capacity(16), // Pool up to 16 buffers
        }
    }

    /// Add a fragment to the reassembly buffer
    ///
    /// Returns `Some(ReassembledMfu)` if fragment completes an MFU,
    /// `None` if still waiting for more fragments
    ///
    /// # Performance
    /// - FI=0 (complete): Zero-copy, just moves ownership
    /// - Fragmented: O(log n) duplicate check, O(1) amortized insert
    #[inline]
    pub fn add_fragment(&mut self, fragment: MfuFragment) -> Option<ReassembledMfu> {
        self.stats.total_fragments += 1;

        let mpu_seq = fragment.mpu_sequence_number;
        let fi = fragment.fragmentation_indicator;

        // FI=0: Complete MFU in single packet - ZERO COPY
        // Just move the Bytes, don't clone
        if fi == 0 {
            self.stats.reassembled_mfus += 1;
            return Some(ReassembledMfu {
                mpu_sequence_number: mpu_seq,
                data: fragment.data, // Move, not clone
                timestamp: fragment.timestamp,
                fragment_count: 1,
                rap_flag: fragment.rap_flag,
            });
        }

        let data_len = fragment.data.len();
        let frag_rap_flag = fragment.rap_flag;

        // Get or create fragment buffer
        let buffer = self.buffers.entry(mpu_seq).or_insert_with(|| {
            // Try to reuse from pool, else create new
            self.buffer_pool.pop().unwrap_or_else(FragmentBuffer::new)
        });

        // Store RAP flag from first fragment (FI=1)
        // This is the only fragment that has the accurate RAP flag
        if fi == 1 {
            buffer.first_rap_flag = Some(frag_rap_flag);
        }

        // Binary search for duplicate - O(log n)
        match buffer.find_by_counter(fragment.fragment_counter) {
            Ok(idx) => {
                // Duplicate found - replace (subtract old size, add new)
                let old_size = buffer.fragments[idx].data.len();
                buffer.total_data_size = buffer.total_data_size - old_size + data_len;
                buffer.fragments[idx] = fragment;
                self.stats.duplicate_fragments += 1;
            }
            Err(idx) => {
                // Not found - insert at sorted position
                // Check for out-of-order (FI sequence)
                if !buffer.fragments.is_empty() {
                    let last_fi = buffer.fragments.last().unwrap().fragmentation_indicator;
                    if fi < last_fi {
                        self.stats.out_of_order_fragments += 1;
                    }
                }
                buffer.total_data_size += data_len;
                buffer.fragments.insert(idx, fragment);
            }
        }

        // Try to reassemble
        self.try_reassemble(mpu_seq)
    }

    /// Attempt to reassemble MFU from buffered fragments
    /// Uses pre-allocated buffer to avoid allocation
    fn try_reassemble(&mut self, mpu_seq: u32) -> Option<ReassembledMfu> {
        let buffer = self.buffers.get_mut(&mpu_seq)?;

        if buffer.fragments.is_empty() {
            return None;
        }

        // Fragments are already sorted by fragment_counter (maintained on insert)
        // Just verify the FI sequence is valid

        let first_fi = buffer.fragments.first()?.fragmentation_indicator;
        let last_fi = buffer.fragments.last()?.fragmentation_indicator;

        // Must have first fragment (FI=1)
        if first_fi != 1 {
            return None;
        }

        // Must have last fragment (FI=3)
        if last_fi != 3 {
            return None;
        }

        // Verify middle fragments are all FI=2
        for i in 1..buffer.fragments.len() - 1 {
            if buffer.fragments[i].fragmentation_indicator != 2 {
                log::warn!(
                    "Invalid fragment sequence for MPU {}: expected FI=2 at position {}, got {}",
                    mpu_seq,
                    i,
                    buffer.fragments[i].fragmentation_indicator
                );
                return None;
            }
        }

        // Reassemble using pre-allocated buffer
        // total_data_size was tracked incrementally - no need to sum
        let total_size = buffer.total_data_size;

        // Ensure capacity (will reuse existing allocation if sufficient)
        buffer.reassembly_buf.clear();
        buffer.reassembly_buf.reserve(total_size);

        // Copy fragments - this is unavoidable, but we minimize allocations
        for frag in &buffer.fragments {
            buffer.reassembly_buf.extend_from_slice(&frag.data);
        }

        let fragment_count = buffer.fragments.len();
        let timestamp = buffer.fragments.first()?.timestamp;
        // Use stored RAP flag from first fragment, default to false if not available
        let rap_flag = buffer.first_rap_flag.unwrap_or(false);

        // Freeze to Bytes (zero-copy conversion)
        let data = buffer.reassembly_buf.split().freeze();

        // Return buffer to pool for reuse (clear fragments but keep allocations)
        let mut returned_buffer = self.buffers.remove(&mpu_seq)?;
        returned_buffer.fragments.clear();
        returned_buffer.total_data_size = 0;
        returned_buffer.reassembly_buf.clear();
        returned_buffer.first_rap_flag = None;
        if self.buffer_pool.len() < 16 {
            self.buffer_pool.push(returned_buffer);
        }

        self.stats.reassembled_mfus += 1;

        log::debug!(
            "Reassembled MPU {} from {} fragments ({} bytes, rap={})",
            mpu_seq,
            fragment_count,
            total_size,
            rap_flag
        );

        Some(ReassembledMfu {
            mpu_sequence_number: mpu_seq,
            data,
            timestamp,
            fragment_count,
            rap_flag,
        })
    }

    /// Cleanup timed-out incomplete MFUs
    ///
    /// Should be called periodically (e.g., on each new fragment)
    pub fn cleanup_timeouts(&mut self) {
        let now = Instant::now();
        let timeout = self.timeout;

        // Pre-allocate with typical capacity (avoids allocation in common case)
        // Most cleanups will have 0-8 timeouts; capacity 16 covers edge cases
        let mut to_remove = Vec::with_capacity(16);

        for (mpu_seq, buffer) in &self.buffers {
            if now.duration_since(buffer.started_at) > timeout {
                to_remove.push(*mpu_seq);
            }
        }

        for mpu_seq in to_remove {
            if let Some(mut buffer) = self.buffers.remove(&mpu_seq) {
                log::warn!(
                    "Timeout for MPU {}: received {} fragments, discarding",
                    mpu_seq,
                    buffer.fragments.len()
                );
                self.stats.timed_out_mfus += 1;

                // Return to pool
                buffer.fragments.clear();
                buffer.total_data_size = 0;
                buffer.reassembly_buf.clear();
                if self.buffer_pool.len() < 16 {
                    self.buffer_pool.push(buffer);
                }
            }
        }

        // Enforce memory limit
        if self.buffers.len() > self.max_buffered_mfus {
            self.enforce_memory_limit();
        }
    }

    /// Enforce memory limit by dropping oldest incomplete MFUs
    fn enforce_memory_limit(&mut self) {
        // Find oldest buffer by started_at
        while self.buffers.len() > self.max_buffered_mfus {
            let oldest = self
                .buffers
                .iter()
                .min_by_key(|(_, buf)| buf.started_at)
                .map(|(seq, _)| *seq);

            if let Some(seq) = oldest {
                if let Some(mut buffer) = self.buffers.remove(&seq) {
                    log::warn!("Memory limit reached, dropping MPU {}", seq);
                    self.stats.timed_out_mfus += 1;

                    // Return to pool
                    buffer.fragments.clear();
                    buffer.total_data_size = 0;
                    if self.buffer_pool.len() < 16 {
                        self.buffer_pool.push(buffer);
                    }
                }
            } else {
                break;
            }
        }
    }

    /// Flush all buffered fragments (call on stream end)
    pub fn flush(&mut self) {
        // Return all buffers to pool
        for (_, mut buffer) in self.buffers.drain() {
            buffer.fragments.clear();
            buffer.total_data_size = 0;
            buffer.reassembly_buf.clear();
            if self.buffer_pool.len() < 16 {
                self.buffer_pool.push(buffer);
            }
        }
    }

    /// Get current statistics (returns reference, no allocation)
    #[inline]
    pub fn stats(&self) -> &ReassemblerStats {
        &self.stats
    }

    /// Get number of buffered MFUs
    #[inline]
    pub fn buffered_count(&self) -> usize {
        self.buffers.len()
    }

    /// Get total pending fragments
    #[inline]
    pub fn pending_fragments(&self) -> usize {
        self.buffers.values().map(|buf| buf.fragments.len()).sum()
    }

    /// Get pool size (for monitoring)
    #[inline]
    pub fn pool_size(&self) -> usize {
        self.buffer_pool.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_fragment(mpu_seq: u32, fi: u8, counter: u16, data: &[u8]) -> MfuFragment {
        // For tests, set rap_flag=true for first fragment (FI=1), false otherwise
        let rap_flag = fi == 1 || fi == 0;
        MfuFragment::new(
            mpu_seq,
            fi,
            counter,
            Bytes::copy_from_slice(data),
            mpu_seq * 1000,
            rap_flag,
        )
    }

    #[test]
    fn test_complete_mfu_zero_copy() {
        let mut reassembler = MfuReassembler::default();
        let data = Bytes::from_static(b"complete");
        let fragment = MfuFragment::new(1, 0, 0, data.clone(), 1000, true);

        let result = reassembler.add_fragment(fragment);
        assert!(result.is_some());

        let mfu = result.unwrap();
        assert_eq!(mfu.fragment_count, 1);
        assert_eq!(&mfu.data[..], b"complete");
    }

    #[test]
    fn test_two_fragments_in_order() {
        let mut reassembler = MfuReassembler::default();

        let first = create_fragment(2, 1, 0, b"AAA");
        let last = create_fragment(2, 3, 1, b"BBB");

        assert!(reassembler.add_fragment(first).is_none());

        let result = reassembler.add_fragment(last);
        assert!(result.is_some());

        let mfu = result.unwrap();
        assert_eq!(&mfu.data[..], b"AAABBB");
        assert_eq!(mfu.fragment_count, 2);
    }

    #[test]
    fn test_out_of_order_fragments() {
        let mut reassembler = MfuReassembler::default();

        let last = create_fragment(3, 3, 1, b"ZZZ");
        let first = create_fragment(3, 1, 0, b"XXX");

        assert!(reassembler.add_fragment(last).is_none());

        let result = reassembler.add_fragment(first);
        assert!(result.is_some());

        let mfu = result.unwrap();
        assert_eq!(&mfu.data[..], b"XXXZZZ");
        assert!(reassembler.stats().out_of_order_fragments > 0);
    }

    #[test]
    fn test_three_fragments() {
        let mut reassembler = MfuReassembler::default();

        let first = create_fragment(4, 1, 0, b"A");
        let middle = create_fragment(4, 2, 1, b"B");
        let last = create_fragment(4, 3, 2, b"C");

        assert!(reassembler.add_fragment(first).is_none());
        assert!(reassembler.add_fragment(middle).is_none());

        let result = reassembler.add_fragment(last);
        assert!(result.is_some());
        assert_eq!(&result.unwrap().data[..], b"ABC");
    }

    #[test]
    fn test_buffer_pool_reuse() {
        let mut reassembler = MfuReassembler::default();

        // Complete first MFU
        reassembler.add_fragment(create_fragment(1, 1, 0, b"A"));
        reassembler.add_fragment(create_fragment(1, 3, 1, b"B"));

        // Buffer should be returned to pool
        assert_eq!(reassembler.pool_size(), 1);

        // Complete second MFU - should reuse pooled buffer
        reassembler.add_fragment(create_fragment(2, 1, 0, b"C"));
        reassembler.add_fragment(create_fragment(2, 3, 1, b"D"));

        // Pool should still have 1 (reused, then returned)
        assert_eq!(reassembler.pool_size(), 1);
    }

    // =========================================================================
    // Packet Loss Simulation Tests
    // =========================================================================

    /// Simulate packet loss by dropping fragments with given probability
    fn simulate_loss<R: rand::Rng>(
        fragments: Vec<MfuFragment>,
        loss_rate: f64,
        rng: &mut R,
    ) -> Vec<MfuFragment> {
        fragments
            .into_iter()
            .filter(|_| rng.gen::<f64>() >= loss_rate)
            .collect()
    }

    /// Create a multi-fragment MFU with specified fragment count
    fn create_fragmented_mfu(
        mpu_seq: u32,
        num_fragments: usize,
        payload_size: usize,
    ) -> Vec<MfuFragment> {
        let mut fragments = Vec::with_capacity(num_fragments);

        for i in 0..num_fragments {
            let fi = if i == 0 {
                1 // First
            } else if i == num_fragments - 1 {
                3 // Last
            } else {
                2 // Middle
            };

            // RAP flag is only set on first fragment
            let rap_flag = fi == 1;

            let data = vec![((mpu_seq + i as u32) % 256) as u8; payload_size];
            fragments.push(MfuFragment::new(
                mpu_seq,
                fi,
                i as u16,
                Bytes::from(data),
                mpu_seq * 1000,
                rap_flag,
            ));
        }

        fragments
    }

    #[test]
    fn test_packet_loss_no_fec_partial_recovery() {
        // Simulates 10% packet loss WITHOUT FEC
        // Expected: Some MFUs will be incomplete (missing fragments)
        use rand::SeedableRng;
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        let mut reassembler = MfuReassembler::new(Duration::from_secs(1), 1000);
        let num_mfus = 100;
        let fragments_per_mfu = 5;
        let loss_rate = 0.10; // 10% packet loss

        let mut complete_mfus = 0;
        let mut incomplete_mfus = 0;

        for mpu_seq in 0..num_mfus {
            let fragments = create_fragmented_mfu(mpu_seq, fragments_per_mfu, 1000);
            let surviving = simulate_loss(fragments, loss_rate, &mut rng);

            let original_count = fragments_per_mfu;
            let surviving_count = surviving.len();

            // Add surviving fragments
            let mut reassembled = false;
            for frag in surviving {
                if let Some(_mfu) = reassembler.add_fragment(frag) {
                    reassembled = true;
                }
            }

            if reassembled && surviving_count == original_count {
                complete_mfus += 1;
            } else if surviving_count < original_count {
                incomplete_mfus += 1;
            }
        }

        println!("\n📊 NO FEC - 10% Loss Simulation:");
        println!("   Total MFUs:      {}", num_mfus);
        println!(
            "   Complete MFUs:   {} ({:.1}%)",
            complete_mfus,
            (complete_mfus as f64 / num_mfus as f64) * 100.0
        );
        println!(
            "   Incomplete MFUs: {} ({:.1}%)",
            incomplete_mfus,
            (incomplete_mfus as f64 / num_mfus as f64) * 100.0
        );
        println!("   Stats: {:?}", reassembler.stats());

        // Without FEC, expect significant incomplete MFUs
        // At 10% loss with 5 fragments per MFU, probability of complete = 0.9^5 ≈ 59%
        assert!(
            incomplete_mfus > 0,
            "Should have some incomplete MFUs without FEC"
        );
        assert!(complete_mfus > 0, "Should have some complete MFUs");
    }

    #[test]
    fn test_packet_loss_high_loss_rate() {
        // Simulates 30% packet loss - stress test
        use rand::SeedableRng;
        let mut rng = rand::rngs::StdRng::seed_from_u64(123);

        let mut reassembler = MfuReassembler::new(Duration::from_secs(1), 1000);
        let num_mfus = 100;
        let loss_rate = 0.30; // 30% loss

        let mut complete_mfus = 0;

        for mpu_seq in 0..num_mfus {
            // 3-fragment MFUs
            let fragments = create_fragmented_mfu(mpu_seq, 3, 500);
            let surviving = simulate_loss(fragments, loss_rate, &mut rng);

            for frag in surviving {
                if reassembler.add_fragment(frag).is_some() {
                    complete_mfus += 1;
                }
            }
        }

        println!("\n📊 NO FEC - 30% Loss Simulation:");
        println!("   Total MFUs:    {}", num_mfus);
        println!(
            "   Complete MFUs: {} ({:.1}%)",
            complete_mfus,
            (complete_mfus as f64 / num_mfus as f64) * 100.0
        );
        println!("   Expected:      ~34% (0.7^3 = 34.3%)");

        // At 30% loss with 3 fragments, probability = 0.7^3 ≈ 34%
        // Allow some variance
        let expected_complete = (0.7_f64).powi(3) * num_mfus as f64;
        assert!(
            (complete_mfus as f64 - expected_complete).abs() < 15.0,
            "Complete MFUs should be close to expected: got {}, expected ~{}",
            complete_mfus,
            expected_complete
        );
    }

    #[test]
    fn test_timeout_on_missing_fragments() {
        // Test that incomplete MFUs timeout correctly
        let mut reassembler = MfuReassembler::new(Duration::from_millis(50), 100);

        // Add only first fragment (missing last)
        let first = create_fragment(100, 1, 0, b"FIRST");
        assert!(reassembler.add_fragment(first).is_none());
        assert_eq!(reassembler.buffered_count(), 1);

        // Wait for timeout
        std::thread::sleep(Duration::from_millis(60));
        reassembler.cleanup_timeouts();

        // Should be cleaned up
        assert_eq!(reassembler.buffered_count(), 0);
        assert_eq!(reassembler.stats().timed_out_mfus, 1);
    }

    #[test]
    fn test_30fps_stream_with_5_percent_loss() {
        // Simulate 10 seconds of 30fps video with 5% loss
        // Each frame = 1 MFU with 4 fragments
        use rand::SeedableRng;
        let mut rng = rand::rngs::StdRng::seed_from_u64(999);

        let mut reassembler = MfuReassembler::new(Duration::from_millis(100), 500);

        let duration_sec = 10;
        let fps = 30;
        let total_frames = duration_sec * fps;
        let fragments_per_frame = 4;
        let loss_rate = 0.05; // 5% loss

        let mut decoded_frames = 0;
        let mut dropped_frames = 0;

        for frame_num in 0..total_frames {
            let fragments = create_fragmented_mfu(frame_num, fragments_per_frame, 2000);
            let original_len = fragments.len();
            let surviving = simulate_loss(fragments, loss_rate, &mut rng);
            let all_received = surviving.len() == original_len;

            for frag in surviving {
                if reassembler.add_fragment(frag).is_some() {
                    decoded_frames += 1;
                }
            }

            if !all_received {
                dropped_frames += 1;
            }
        }

        let decode_rate = (decoded_frames as f64 / total_frames as f64) * 100.0;

        println!("\n📊 30fps Stream - 5% Loss (No FEC):");
        println!("   Duration:       {} seconds", duration_sec);
        println!("   Total frames:   {}", total_frames);
        println!(
            "   Decoded frames: {} ({:.1}%)",
            decoded_frames, decode_rate
        );
        println!(
            "   Dropped frames: {} ({:.1}%)",
            dropped_frames,
            (dropped_frames as f64 / total_frames as f64) * 100.0
        );
        println!(
            "   Green fill:     ~{:.1}% of frames",
            (1.0 - decode_rate / 100.0) * 100.0
        );

        // Without FEC at 5% loss with 4 fragments: 0.95^4 ≈ 81.5% success
        // Minor green fill is expected (< 20% frames affected)
        assert!(decode_rate > 75.0, "Should decode most frames at 5% loss");
        assert!(
            decode_rate < 100.0,
            "Should have some frame loss without FEC"
        );
    }
}
