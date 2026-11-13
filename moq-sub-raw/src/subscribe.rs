// SPDX-License-Identifier: MIT OR Apache-2.0
//
// Subscribe-side primitives extracted as testable async functions so
// unit tests can drive them with in-process producer/consumer pairs
// (no real moq-transport session required). Tests live alongside the
// implementations; the real binary wires these into a session in
// main.rs.

use std::{
    io::Write,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::{anyhow, bail, Context, Result};
use moq_transport::{
    coding::{Encode, TrackNamespace},
    data::ExtensionHeaders,
    serve::{TrackReader, TrackReaderMode},
    session::NamespaceEvent,
};
use tokio::io::{AsyncWrite, AsyncWriteExt};

/// One line per received object, shared by every track of a run:
/// `track group subgroup object size sha256(payload) ext=<hex>`, tab-separated.
/// `subgroup` is `-` for datagrams; `ext` is the wire encoding of the object's
/// extension headers (`00` when there are none). Two subscribers' logs of one
/// track can be joined on (track, group, object) to tell whether a relay
/// changed any object's payload or extension headers in transit.
#[derive(Clone)]
pub struct ObjectLog(Arc<Mutex<Box<dyn Write + Send>>>);

impl ObjectLog {
    pub fn new(w: impl Write + Send + 'static) -> Self {
        Self(Arc::new(Mutex::new(Box::new(w))))
    }

    pub fn record(
        &self,
        track: &dyn std::fmt::Display,
        group: u64,
        subgroup: Option<u64>,
        object: u64,
        payload: &[u8],
        ext: &ExtensionHeaders,
    ) -> Result<()> {
        let mut enc = bytes::BytesMut::new();
        ext.encode(&mut enc)
            .map_err(|e| anyhow!("encode extension headers: {e:?}"))?;
        let digest = ring::digest::digest(&ring::digest::SHA256, payload);
        let sub = subgroup.map_or_else(|| "-".to_string(), |s| s.to_string());
        let mut w = self.0.lock().map_err(|_| anyhow!("object log poisoned"))?;
        writeln!(
            w,
            "{track}\t{group}\t{sub}\t{object}\t{}\t{}\text={}",
            payload.len(),
            hex::encode(digest),
            hex::encode(&enc)
        )?;
        w.flush()?;
        Ok(())
    }
}

/// Drain a track's payload bytes into `out`, in arrival order.
///
/// Handles both track modes the in-repo publishers emit:
///   - `Subgroups` — moq-pub-mmtp's MMTP mapping (each object = one MMTP
///     packet); subgroups → objects → chunks are concatenated verbatim.
///   - `Datagrams` — moq-pub-mmtp's `packaging=datagram` pass-through (each
///     datagram = one opaque payload, e.g. a Solana shred frame). The
///     transport ring is raw-lossy: superseded datagrams are skipped by
///     design and surface only in the reader's drop counter.
///
/// No separators between payloads — the caller owns any framing semantics
/// (for M.1, concatenation IS the raw packet stream).
///
/// Returns when the track's writer side closes (clean EOF) or the
/// underlying reader is dropped.
/// With `log`, every object is also recorded there (see [`ObjectLog`]).
pub async fn drain_track_to_writer<W: AsyncWrite + Unpin>(
    track: TrackReader,
    out: &mut W,
    log: Option<&ObjectLog>,
) -> Result<u64> {
    let mut bytes_written: u64 = 0;
    let mode = track.mode().await.context("track mode")?;
    match mode {
        TrackReaderMode::Subgroups(mut groups) => {
            while let Some(mut group) = groups.next().await.context("subgroups.next")? {
                while let Some(mut object) = group.next().await.context("subgroup object.next")? {
                    let mut payload = Vec::new();
                    while let Some(chunk) = object.read().await.context("object.read chunk")? {
                        out.write_all(&chunk).await.context("out.write_all chunk")?;
                        bytes_written += chunk.len() as u64;
                        if log.is_some() {
                            payload.extend_from_slice(&chunk);
                        }
                    }
                    if let Some(log) = log {
                        log.record(
                            &track.name,
                            group.group_id,
                            Some(group.subgroup_id),
                            object.info.object_id,
                            &payload,
                            &object.info.extension_headers,
                        )?;
                    }
                }
            }
        }
        TrackReaderMode::Datagrams(mut datagrams) => {
            while let Some(datagram) = datagrams.read().await.context("datagrams.read")? {
                out.write_all(&datagram.payload)
                    .await
                    .context("out.write_all datagram")?;
                bytes_written += datagram.payload.len() as u64;
                if let Some(log) = log {
                    log.record(
                        &track.name,
                        datagram.group_id,
                        None,
                        datagram.object_id,
                        &datagram.payload,
                        &datagram.extension_headers,
                    )?;
                }
            }
        }
        // Stream mode is not emitted by any in-repo publisher.
        _ => bail!(
            "track `{}` is not in subgroup or datagram mode (unsupported reader mode)",
            track.name
        ),
    }
    out.flush().await.context("flush output")?;
    Ok(bytes_written)
}

/// Whether a SUBSCRIBE_NAMESPACE event reports `namespace` itself as published.
///
/// `--await-namespace` uses the namespace as its own prefix, so the relay also
/// reports any longer namespace under it. Those do not route a SUBSCRIBE for
/// `namespace`, and a NAMESPACE_DONE never does.
pub fn namespace_published(event: &NamespaceEvent, namespace: &TrackNamespace) -> bool {
    matches!(event, NamespaceEvent::Added(published) if published == namespace)
}

/// Validate `--track` and `--output` arguments are paired 1:1 and
/// non-empty. Surfaces CLI misconfigurations BEFORE we open a session.
pub fn validate_track_output_pairs(tracks: &[String], outputs: &[PathBuf]) -> Result<()> {
    if tracks.is_empty() {
        bail!("at least one --track/--output pair required");
    }
    if tracks.len() != outputs.len() {
        bail!(
            "--track count {} does not match --output count {} (pair them positionally)",
            tracks.len(),
            outputs.len()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use moq_transport::{
        coding::TrackNamespace,
        serve::{Subgroup, Tracks},
    };
    use std::path::PathBuf;

    fn ns() -> TrackNamespace {
        TrackNamespace::from_utf8_path("test-broadcast")
    }

    // ---- validate_track_output_pairs ----

    #[test]
    fn validate_pairs_errors_when_no_args() {
        let err = validate_track_output_pairs(&[], &[]).unwrap_err();
        assert!(err.to_string().contains("at least one"), "got: {err}");
    }

    #[test]
    fn validate_pairs_errors_on_count_mismatch() {
        let tracks = vec!["v".to_string(), "a".to_string()];
        let outputs = vec![PathBuf::from("v.bin")];
        let err = validate_track_output_pairs(&tracks, &outputs).unwrap_err();
        assert!(err.to_string().contains("does not match"), "got: {err}");
    }

    #[test]
    fn validate_pairs_passes_on_matched_counts() {
        let tracks = vec!["v".to_string(), "a".to_string()];
        let outputs = vec![PathBuf::from("v.bin"), PathBuf::from("a.bin")];
        validate_track_output_pairs(&tracks, &outputs).expect("matched counts OK");
    }

    // ---- namespace_published ----

    #[test]
    fn namespace_published_matches_the_exact_namespace() {
        let event = NamespaceEvent::Added(ns());
        assert!(namespace_published(&event, &ns()));
    }

    #[test]
    fn namespace_published_ignores_a_longer_namespace() {
        let event = NamespaceEvent::Added(TrackNamespace::from_utf8_path("test-broadcast/other"));
        assert!(!namespace_published(&event, &ns()));
    }

    #[test]
    fn namespace_published_ignores_namespace_done() {
        let event = NamespaceEvent::Removed(ns());
        assert!(!namespace_published(&event, &ns()));
    }

    // ---- drain_track_to_writer ----

    /// Helper: build (producer, cached reader) pair WITHOUT dropping
    /// the producer first. Critical: subscribe via TracksReader BEFORE
    /// any writers drop — otherwise is_closed() returns true and the
    /// subscribe call falls through to the request queue (which has
    /// no one to answer in a unit test), hanging forever.
    fn make_track_pair(
        track_name: &str,
    ) -> (
        moq_transport::serve::TrackWriter,
        moq_transport::serve::TrackReader,
        // Keep the broadcast handles alive across the test.
        moq_transport::serve::TracksWriter,
        moq_transport::serve::TracksReader,
    ) {
        let (mut tw, _req, mut tr) = Tracks::new(ns()).produce();
        let track_writer = tw.create(track_name).expect("create track");
        let track_reader = tr.subscribe(ns(), track_name).expect("subscribe");
        (track_writer, track_reader, tw, tr)
    }

    #[tokio::test]
    async fn drain_concatenates_object_payloads_in_arrival_order() {
        // Producer side: open a single subgroup, write 3 objects.
        // Consumer side: drain the resulting TrackReader and confirm
        // the writer received the concatenated payload bytes in order.
        let (track_writer, track_reader, _tw, _tr) = make_track_pair("v");
        let mut subgroups = track_writer.subgroups().expect("subgroups mode");
        let mut subgroup = subgroups
            .create(Subgroup {
                group_id: 0,
                subgroup_id: 0,
                priority: 0,
            })
            .expect("create subgroup");
        subgroup
            .write(Bytes::from_static(b"alpha"))
            .expect("write 1");
        subgroup
            .write(Bytes::from_static(b"beta"))
            .expect("write 2");
        subgroup
            .write(Bytes::from_static(b"gamma"))
            .expect("write 3");
        drop(subgroup);
        drop(subgroups);

        let mut buf: Vec<u8> = Vec::new();
        let n = drain_track_to_writer(track_reader, &mut buf, None)
            .await
            .expect("drain ok");
        assert_eq!(n, b"alphabetagamma".len() as u64);
        assert_eq!(buf, b"alphabetagamma");
    }

    #[tokio::test]
    async fn drain_concatenates_across_multiple_groups() {
        // Producer/consumer run concurrently because moq-transport's
        // SubgroupsReader surfaces only the *latest* subgroup — slow
        // consumers miss intermediates. In production the wire RTT
        // gives the consumer a window between subgroups; here we use
        // an explicit yield via tokio::time::sleep.
        let (track_writer, track_reader, _tw, _tr) = make_track_pair("v");
        let mut subgroups = track_writer.subgroups().expect("subgroups mode");

        // Write g0 BEFORE spawning the consumer so it's first-latest.
        let mut g0 = subgroups
            .create(Subgroup {
                group_id: 0,
                subgroup_id: 0,
                priority: 0,
            })
            .expect("group 0");
        g0.write(Bytes::from_static(b"first-mpu-bytes")).unwrap();
        drop(g0);

        let producer = async move {
            // Give the consumer time to drain g0 fully before g1
            // supersedes it as the new latest_subgroup_reader.
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let mut g1 = subgroups
                .create(Subgroup {
                    group_id: 1,
                    subgroup_id: 0,
                    priority: 0,
                })
                .expect("group 1");
            g1.write(Bytes::from_static(b"second-mpu-bytes")).unwrap();
            drop(g1);
            drop(subgroups);
        };

        let mut buf: Vec<u8> = Vec::new();
        let consumer = drain_track_to_writer(track_reader, &mut buf, None);

        let (_, consumer_res) = tokio::join!(producer, consumer);
        consumer_res.expect("drain ok");

        assert_eq!(buf, b"first-mpu-bytessecond-mpu-bytes");
    }

    #[tokio::test]
    async fn drain_writes_zero_bytes_on_empty_track() {
        // Producer creates a subgroups track but writes nothing,
        // then drops. Drain returns Ok(0).
        let (track_writer, track_reader, _tw, _tr) = make_track_pair("v");
        let subgroups = track_writer.subgroups().expect("subgroups mode");
        drop(subgroups);

        let mut buf: Vec<u8> = Vec::new();
        let n = drain_track_to_writer(track_reader, &mut buf, None)
            .await
            .expect("drain ok");
        assert_eq!(n, 0);
        assert!(buf.is_empty());
    }

    #[tokio::test]
    async fn drain_concatenates_datagram_payloads_in_arrival_order() {
        // F1 (moq-rs PR #35): moq-pub-mmtp's packaging=datagram tracks are
        // native-datagram mode; the raw consumer must drain them instead of
        // bailing. The publisher-set history window keeps all three writes
        // retained so this drain is deterministic.
        let (mut track_writer, track_reader, _tw, _tr) = make_track_pair("shreds");
        track_writer
            .set_history_window(std::num::NonZeroU64::new(8).unwrap())
            .expect("set window");
        let mut datagrams = track_writer.datagrams().expect("datagrams mode");

        for (group_id, payload) in [(0u64, "alpha"), (1, "beta"), (2, "gamma")] {
            datagrams
                .write(moq_transport::serve::Datagram {
                    group_id,
                    object_id: 0,
                    priority: 0,
                    payload: Bytes::from_static(payload.as_bytes()),
                    extension_headers: Default::default(),
                })
                .expect("write datagram");
        }
        drop(datagrams);

        let mut buf: Vec<u8> = Vec::new();
        let n = drain_track_to_writer(track_reader, &mut buf, None)
            .await
            .expect("drain ok");
        assert_eq!(n, b"alphabetagamma".len() as u64);
        assert_eq!(buf, b"alphabetagamma");
    }

    /// A `Write` that hands its bytes back to the test.
    #[derive(Clone, Default)]
    struct Shared(Arc<Mutex<Vec<u8>>>);
    impl Write for Shared {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn object_log_records_ids_payload_digest_and_extension_headers() {
        let (track_writer, track_reader, _tw, _tr) = make_track_pair("v");
        let mut subgroups = track_writer.subgroups().expect("subgroups mode");
        let mut subgroup = subgroups
            .create(Subgroup {
                group_id: 7,
                subgroup_id: 2,
                priority: 0,
            })
            .expect("create subgroup");
        subgroup
            .write(Bytes::from_static(b"alpha"))
            .expect("write 1");
        let mut ext = ExtensionHeaders::new();
        ext.set_intvalue(0x0a, 5);
        let mut obj = subgroup.create(4, Some(ext)).expect("object 1");
        obj.write(Bytes::from_static(b"be")).expect("chunk 1");
        obj.write(Bytes::from_static(b"ta")).expect("chunk 2");
        drop(obj);
        drop(subgroup);
        drop(subgroups);

        let sink = Shared::default();
        let log = ObjectLog::new(sink.clone());
        let mut buf: Vec<u8> = Vec::new();
        drain_track_to_writer(track_reader, &mut buf, Some(&log))
            .await
            .expect("drain ok");
        assert_eq!(buf, b"alphabeta");
        let text = String::from_utf8(sink.0.lock().unwrap().clone()).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        let sha = |p: &[u8]| hex::encode(ring::digest::digest(&ring::digest::SHA256, p));
        assert_eq!(lines.len(), 2, "{text}");
        assert_eq!(
            lines[0],
            format!("v\t7\t2\t0\t5\t{}\text=00", sha(b"alpha"))
        );
        // chunks of one object are hashed as one payload; the extension is kept
        let f: Vec<&str> = lines[1].split('\t').collect();
        assert_eq!(&f[..6], ["v", "7", "2", "1", "4", &sha(b"beta")]);
        assert_ne!(f[6], "ext=00", "{text}");
    }
}
