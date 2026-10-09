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
        let trak_count = moov.traks.len();
        for (idx, trak) in moov.traks.into_iter().enumerate() {
            let name = track_name(catalog.as_ref(), idx, trak_count, trak.tkhd.track_id)?;
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

/// Name of the catalog track carrying moov trak `idx` (of `trak_count`), or the
/// `{track_id}.m4s` default when no catalog was requested.
fn track_name(
    catalog: Option<&moq_catalog::Root>,
    idx: usize,
    trak_count: usize,
    track_id: u32,
) -> anyhow::Result<String> {
    let Some(catalog) = catalog else {
        return Ok(format!("{track_id}.m4s"));
    };
    let track = catalog.tracks.get(idx).with_context(|| {
        format!(
            "moov trak {idx} has no matching catalog track ({trak_count} traks vs {} catalog \
             tracks)",
            catalog.tracks.len()
        )
    })?;
    Ok(track.name.clone())
}

fn validate_catalog(catalog: &moq_catalog::Root) -> anyhow::Result<()> {
    anyhow::ensure!(catalog.version == 1, "unknown catalog version");
    if catalog.streaming_format == "mmtp" {
        // Everything below this gate is an fMP4/CMAF consumer: it reads a ftyp +
        // moov init segment and forwards whole objects. An MMTP broadcast carries
        // an MMTP packet header in front of every object, so without the refusal
        // we would read a packet header as a `ftyp` box and fail somewhere much
        // less obvious. Nothing in this tree depacketizes MFUs back into fMP4:
        // moq-sub-raw only captures the MMTP packets verbatim. The refusal comes
        // before any `validate()` so a slightly malformed mmtp catalog still
        // learns that moq-sub is the wrong tool, not just how to fix the catalog.
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
            error.contains("Use moq-sub-raw to capture the raw MMTP packet stream"),
            "must say what moq-sub-raw actually yields -- MMTP packets, not media: {error}"
        );
        assert!(
            error.contains("no MFU depacketizer"),
            "must not imply some subscriber turns MFUs back into fMP4: {error}"
        );
    }

    /// A malformed mmtp catalog must still be told moq-sub is the wrong tool;
    /// otherwise the user fixes the catalog only to be refused anyway.
    #[test]
    fn refuses_a_malformed_mmtp_catalog_with_the_same_redirect() {
        let mut catalog = live_mmtp_catalog();
        catalog.tracks[0].packaging = None;
        assert!(
            catalog.validate().is_err(),
            "fixture edit no longer makes the catalog invalid; this test is vacuous"
        );

        let error = validate_catalog(&catalog)
            .expect_err("moq-sub has no MMTP depacketization; it must refuse")
            .to_string();
        assert!(error.contains("moq-sub-raw"), "{error}");
    }

    #[test]
    fn track_name_defaults_when_no_catalog_was_requested() {
        assert_eq!(track_name(None, 0, 1, 7).unwrap(), "7.m4s");
    }

    #[test]
    fn track_name_reports_the_real_trak_count_when_the_catalog_runs_out() {
        let mut catalog = cmaf_catalog(1);
        catalog.tracks.push(moq_catalog::Track {
            name: "audio".into(),
            ..Default::default()
        });
        assert_eq!(track_name(Some(&catalog), 1, 5, 2).unwrap(), "audio");

        // A 5-trak moov against a 2-track catalog fails at trak 2, and must say
        // 5 -- not the 3 it had reached.
        let error = track_name(Some(&catalog), 2, 5, 3)
            .expect_err("no catalog track for trak 2")
            .to_string();
        assert!(error.contains("5 traks vs 2 catalog tracks"), "{error}");
        assert!(
            error.contains("trak 2"),
            "must say where it ran out: {error}"
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
}
