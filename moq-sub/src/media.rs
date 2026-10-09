// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{io::Cursor, sync::Arc};

use anyhow::Context;
use moq_transport::serve::{
    SubgroupObjectReader, SubgroupReader, TrackReader, TrackReaderMode, Tracks, TracksReader,
    TracksWriter,
};
use moq_transport::session::Subscriber;
use mp4::ReadBox;
use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::Mutex,
    task::JoinSet,
};
use tracing::{debug, info, trace, warn};

pub struct Media<O> {
    subscriber: Subscriber,
    broadcast: TracksReader,
    tracks_writer: TracksWriter,
    output: Arc<Mutex<O>>,
    request_catalog: bool,
}

impl<O: AsyncWrite + Send + Unpin + 'static> Media<O> {
    pub async fn new(
        subscriber: Subscriber,
        tracks: Tracks,
        output: O,
        request_catalog: bool,
    ) -> anyhow::Result<Self> {
        let (tracks_writer, _tracks_request, tracks_reader) = tracks.produce();
        let broadcast = tracks_reader; // breadcrumb for navigating API name changes
        Ok(Self {
            subscriber,
            broadcast,
            tracks_writer,
            output: Arc::new(Mutex::new(output)),
            request_catalog,
        })
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        let catalog = if self.request_catalog {
            let buf = self.download_first_object("catalog", "catalog").await?;
            let s = std::str::from_utf8(&buf)?;
            let c: moq_catalog::Root = serde_json::from_str(s)?;
            info!("catalog: {c:#?}");
            validate_catalog(&c)?;
            Some(c)
        } else {
            None
        };
        let moov = {
            let init_track_name = init_track_name(catalog.as_ref())?;
            let buf = self.download_first_object(init_track_name, "init").await?;
            self.output.lock().await.write_all(&buf).await?;
            let mut reader = Cursor::new(&buf);

            let ftyp = read_atom(&mut reader).await?;
            anyhow::ensure!(&ftyp[4..8] == b"ftyp", "expected ftyp atom");

            let moov = read_atom(&mut reader).await?;
            anyhow::ensure!(&moov[4..8] == b"moov", "expected moov atom");
            let mut moov_reader = Cursor::new(&moov);
            let moov_header = mp4::BoxHeader::read(&mut moov_reader)?;

            mp4::MoovBox::read_box(&mut moov_reader, moov_header.size)?
        };

        let mut has_video = false;
        let mut has_audio = false;
        let mut tracks = vec![];
        let names = track_names(catalog.as_ref(), &moov)?;
        for (trak, name) in moov.traks.into_iter().zip(names) {
            info!("found track {name}");
            let mut active = false;
            if !has_video && trak.mdia.minf.stbl.stsd.avc1.is_some() {
                active = true;
                has_video = true;
                info!("using {name} for video");
            }
            if !has_audio && trak.mdia.minf.stbl.stsd.mp4a.is_some() {
                active = true;
                has_audio = true;
                info!("using {name} for audio");
            }
            if active {
                let track = self
                    .tracks_writer
                    .create(&name)
                    .context("failed to create track")?;

                let mut subscriber = self.subscriber.clone();
                tokio::task::spawn(async move {
                    subscriber.subscribe(track).await.unwrap_or_else(|err| {
                        warn!("failed to subscribe to track: {err:?}");
                    });
                });

                tracks.push(
                    self.broadcast
                        .subscribe(self.broadcast.namespace.clone(), &name)
                        .context("no track")?,
                );
            }
        }

        info!("playing {} tracks", tracks.len());
        let mut tasks = JoinSet::new();
        for track in tracks {
            let out = self.output.clone();
            tasks.spawn(async move {
                let name = track.name.clone();
                if let Err(err) = Self::recv_track(track, out).await {
                    warn!("failed to play track {name}: {err:?}");
                }
            });
        }
        while tasks.join_next().await.is_some() {}
        Ok(())
    }

    async fn download_first_object(
        &mut self,
        track_name: &str,
        alias: &'static str,
    ) -> anyhow::Result<Vec<u8>> {
        let track = self
            .tracks_writer
            .create(track_name)
            .context(format!("failed to create {alias} track"))?;

        let mut subscriber = self.subscriber.clone();
        tokio::task::spawn(async move {
            subscriber.subscribe(track).await.unwrap_or_else(|err| {
                warn!("failed to subscribe to {alias} track: {err:?}");
            });
        });

        let track = self
            .broadcast
            .subscribe(self.broadcast.namespace.clone(), track_name)
            .context(format!("no {alias} track"))?;
        let mut group = match track.mode().await? {
            TrackReaderMode::Subgroups(mut groups) => {
                groups.next().await?.context(format!("no {alias} group"))?
            }
            _ => anyhow::bail!("expected {alias} segment"),
        };

        let object = group
            .next()
            .await?
            .context(format!("no {alias} fragment"))?;
        let buf = Self::recv_object(object).await?;
        Ok(buf)
    }

    async fn recv_track(track: TrackReader, out: Arc<Mutex<O>>) -> anyhow::Result<()> {
        let name = track.name.clone();
        debug!("track {name}: start");
        if let TrackReaderMode::Subgroups(mut groups) = track.mode().await? {
            while let Some(group) = groups.next().await? {
                let out = out.clone();
                if let Err(err) = Self::recv_group(group, out).await {
                    warn!("failed to receive group: {err:?}");
                }
            }
        }
        debug!("track {name}: finish");
        Ok(())
    }

    async fn recv_group(mut group: SubgroupReader, out: Arc<Mutex<O>>) -> anyhow::Result<()> {
        trace!("group={} start", group.group_id);
        while let Some(object) = group.next().await? {
            trace!(
                "group={} fragment={} start",
                group.group_id,
                object.object_id
            );
            let out = out.clone();
            let buf = Self::recv_object(object).await?;

            out.lock().await.write_all(&buf).await?;
        }

        Ok(())
    }

    async fn recv_object(mut object: SubgroupObjectReader) -> anyhow::Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(object.size);
        while let Some(chunk) = object.read().await? {
            buf.extend_from_slice(&chunk);
        }
        Ok(buf)
    }
}

/// Resolve the track carrying the fMP4 init segment (`ftyp` + `moov`).
///
/// `initTrack` is optional in the catalog schema, and the MSF publisher leaves
/// it unset and inlines the init segment as base64 `initData` instead. moq-sub
/// only knows how to *fetch* one, so say so rather than unwrapping a `None`.
fn init_track_name(catalog: Option<&moq_catalog::Root>) -> anyhow::Result<&str> {
    let Some(catalog) = catalog else {
        return Ok("0.mp4");
    };
    let track = catalog
        .tracks
        .first()
        .context("catalog declares no tracks")?;
    track.init_track.as_deref().with_context(|| {
        format!(
            "track {:?} has no initTrack{}; moq-sub fetches the init segment from a \
             separate track",
            track.name,
            if track.init_data.is_some() {
                " (initData is present, but moq-sub does not read an inline one)"
            } else {
                ""
            }
        )
    })
}

/// Name the track carrying each moov trak, in moov order: the catalog track at
/// the same index, or `{track_id}.m4s` when no catalog was requested.
fn track_names(
    catalog: Option<&moq_catalog::Root>,
    moov: &mp4::MoovBox,
) -> anyhow::Result<Vec<String>> {
    moov.traks
        .iter()
        .enumerate()
        .map(|(idx, trak)| match catalog {
            None => Ok(format!("{}.m4s", trak.tkhd.track_id)),
            Some(catalog) => catalog
                .tracks
                .get(idx)
                .map(|track| track.name.clone())
                .with_context(|| {
                    format!(
                        "moov trak {idx} has no matching catalog track ({} traks vs {} \
                         catalog tracks)",
                        moov.traks.len(),
                        catalog.tracks.len()
                    )
                }),
        })
        .collect()
}

fn validate_catalog(catalog: &moq_catalog::Root) -> anyhow::Result<()> {
    anyhow::ensure!(catalog.version == 1, "unknown catalog version");
    if catalog.streaming_format == "mmtp" {
        catalog
            .validate()
            .map_err(|error| anyhow::anyhow!("invalid catalog: {error}"))?;
        // Everything below this gate is an fMP4/CMAF consumer: it reads a ftyp +
        // moov init segment and forwards whole objects. An MMTP broadcast carries
        // an MMTP packet header in front of every object, so without the refusal
        // we would read a packet header as a `ftyp` box and fail somewhere much
        // less obvious. moq-sub-raw does not depacketize MMTP either: it writes
        // the object payloads out verbatim, i.e. the raw MMTP packet stream.
        anyhow::bail!(
            "moq-sub cannot consume a streamingFormat=\"mmtp\" catalog: it would parse \
             MMTP packet headers as fMP4 boxes. Use moq-sub-raw to capture the raw MMTP \
             packet stream; moq-sub has no MFU depacketizer."
        );
    }
    Ok(())
}

// Read a full MP4 atom into a vector.
async fn read_atom<R: AsyncReadExt + Unpin>(reader: &mut R) -> anyhow::Result<Vec<u8>> {
    // Read the 8 bytes for the size + type
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf).await?;

    // Convert the first 4 bytes into the size.
    let size = u32::from_be_bytes(buf[0..4].try_into()?) as u64;

    let mut raw = buf.to_vec();

    let mut limit = match size {
        // Runs until the end of the file.
        0 => reader.take(u64::MAX),

        // The next 8 bytes are the extended size to be used instead.
        1 => {
            reader.read_exact(&mut buf).await?;
            let size_large = u64::from_be_bytes(buf);
            anyhow::ensure!(
                size_large >= 16,
                "impossible extended box size: {}",
                size_large
            );

            reader.take(size_large - 16)
        }

        2..=7 => {
            anyhow::bail!("impossible box size: {}", size)
        }

        size => reader.take(size - 8),
    };

    // Append to the vector and return it.
    let _read_bytes = limit.read_to_end(&mut raw).await?;

    Ok(raw)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmaf_catalog(version: u16) -> moq_catalog::Root {
        moq_catalog::Root {
            version,
            streaming_format: "cmaf".into(),
            streaming_format_version: "1".into(),
            supports_delta_updates: None,
            tracks: vec![moq_catalog::Track {
                name: "video".into(),
                packaging: Some(moq_catalog::TrackPackaging::Cmaf),
                ..Default::default()
            }],
            multicast: None,
            multicast_auth: None,
        }
    }

    #[test]
    fn rejects_unknown_version_for_non_mmtp_catalog() {
        assert!(validate_catalog(&cmaf_catalog(2)).is_err());
        assert!(validate_catalog(&cmaf_catalog(1)).is_ok());
    }

    /// moq-catalog's golden suite pins that this capture parses and does not
    /// validate as MSF, but it can only *mirror* the envelope gate by hand --
    /// `moq-sub` depends on `moq-catalog`, not the reverse, so if this function
    /// ever stops gating on `streamingFormat` that suite stays green while this
    /// consumer breaks on this exact catalog. Assert it where the gate lives.
    #[test]
    fn accepts_the_legacy_container_capture_hang_emits() {
        let json = include_str!(
            "../../moq-catalog/tests/fixtures/non-msf/hang-legacy-catalog-to-string.json"
        );
        let catalog: moq_catalog::Root = serde_json::from_str(json).unwrap();

        assert_ne!(
            catalog.streaming_format, "mmtp",
            "fixture drifted into the MSF envelope; this test would pass for the wrong reason"
        );
        assert!(
            catalog.validate().is_err(),
            "fixture is no longer a non-MSF capture; the gate below is untested"
        );

        validate_catalog(&catalog).expect("moq-sub must consume a legacy-container hang catalog");
    }

    /// The `nasa/iss/a` catalog as served by the production relay, captured
    /// 2026-10-09 (BLO-17758 probe). Kept verbatim: it is the shape that made
    /// `tracks[0].init_track.unwrap()` panic, and a hand-written stand-in would
    /// not have caught it -- every MMTP catalog this tree already ships as a
    /// fixture omits `initData` as well as `initTrack`, so none of them
    /// distinguishes "publisher inlines the init segment" from "there is no
    /// init segment at all".
    fn live_mmtp_catalog() -> moq_catalog::Root {
        let json = include_str!("../tests/fixtures/nasa-iss-a-mmtp-catalog.json");
        serde_json::from_str(json).expect("live capture must still deserialize")
    }

    /// Pins the three properties the two tests below actually depend on. Without
    /// this, a fixture that drifted to `cmaf`, or grew an `initTrack`, would let
    /// both of them pass for the wrong reason.
    #[test]
    fn live_capture_still_has_the_shape_this_guard_is_for() {
        let catalog = live_mmtp_catalog();
        assert_eq!(catalog.streaming_format, "mmtp");
        assert!(
            catalog.validate().is_ok(),
            "capture must be a *valid* MSF catalog: the point is that moq-sub \
             refuses it for being MMTP, not for being malformed"
        );
        let track = &catalog.tracks[0];
        assert!(track.init_track.is_none(), "fixture grew an initTrack");
        assert!(
            track.init_data.is_some(),
            "fixture lost its inline initData"
        );
    }

    /// AC 1: refuse, and say which tool to reach for instead. Everything past
    /// this gate parses fMP4 boxes, so an MMTP catalog that gets through fails
    /// later and far less legibly.
    #[test]
    fn refuses_an_mmtp_catalog_and_names_moq_sub_raw() {
        let error = validate_catalog(&live_mmtp_catalog())
            .expect_err("moq-sub has no MMTP depacketization; it must refuse")
            .to_string();
        assert!(error.contains("mmtp"), "must name the format: {error}");
        assert!(
            error.contains("moq-sub-raw"),
            "must name the subscriber that can do this: {error}"
        );
        assert!(
            error.contains("raw MMTP packet stream"),
            "must say what moq-sub-raw gives you: it captures MMTP packets, it \
             does not depacketize them: {error}"
        );
    }

    #[test]
    fn init_track_defaults_when_no_catalog_was_requested() {
        assert_eq!(init_track_name(None).unwrap(), "0.mp4");
    }

    #[test]
    fn init_track_comes_from_the_catalog_when_declared() {
        let mut catalog = cmaf_catalog(1);
        catalog.tracks[0].init_track = Some("video/init.mp4".into());
        assert_eq!(init_track_name(Some(&catalog)).unwrap(), "video/init.mp4");
    }

    /// AC 2: the same `initTrack: None` + `initData: Some(..)` shape on the CMAF
    /// envelope, which `validate_catalog` lets through, so this is the only
    /// guard standing between it and the old panic.
    #[test]
    fn init_track_errors_rather_than_panicking_on_an_inline_init_segment() {
        let mut catalog = cmaf_catalog(1);
        catalog.tracks[0].init_data = Some("AAAAGGZ0eXBpc281".into());
        assert!(catalog.tracks[0].init_track.is_none());

        let error = init_track_name(Some(&catalog))
            .expect_err("moq-sub cannot fetch an init segment it was never given a track for")
            .to_string();
        assert!(error.contains("initTrack"), "{error}");
        assert!(
            error.contains("initData"),
            "must say why the publisher thought it was fine: {error}"
        );
    }

    #[test]
    fn init_track_errors_on_a_catalog_with_no_tracks() {
        let mut catalog = cmaf_catalog(1);
        catalog.tracks.clear();
        assert!(init_track_name(Some(&catalog)).is_err());
    }

    fn moov(track_ids: &[u32]) -> mp4::MoovBox {
        let mut moov = mp4::MoovBox {
            traks: vec![Default::default(); track_ids.len()],
            ..Default::default()
        };
        for (trak, &track_id) in moov.traks.iter_mut().zip(track_ids) {
            trak.tkhd.track_id = track_id;
        }
        moov
    }

    #[test]
    fn track_names_come_from_the_catalog_or_default_to_the_trak_id() {
        assert_eq!(
            track_names(Some(&cmaf_catalog(1)), &moov(&[7])).unwrap(),
            ["video"]
        );
        assert_eq!(
            track_names(None, &moov(&[7, 9])).unwrap(),
            ["7.m4s", "9.m4s"]
        );
    }

    /// The count is the moov's, not how far the loop got: a 5-trak moov
    /// against a 2-track catalog must not be reported as "3 traks".
    #[test]
    fn track_names_report_the_true_moov_trak_count() {
        let mut catalog = cmaf_catalog(1);
        catalog.tracks.push(moq_catalog::Track {
            name: "audio".into(),
            ..Default::default()
        });

        let error = track_names(Some(&catalog), &moov(&[1, 2, 3, 4, 5]))
            .expect_err("trak 2 has no catalog track")
            .to_string();
        assert!(error.contains("moov trak 2 "), "{error}");
        assert!(error.contains("5 traks"), "{error}");
        assert!(error.contains("2 catalog tracks"), "{error}");
    }
}
