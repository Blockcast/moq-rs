//! Benchmarks for MMT header operations
//!
//! Target: <10ns per header write/read operation

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use mmt_core::{
    CodecConverter, FragmentType, MfuDataUnit, MmtpHeader, MpuHeader, PacketBuilder,
    PacketFragmenter, PacketType,
};

fn bench_mmtp_header_write(c: &mut Criterion) {
    let header = MmtpHeader {
        version: 0,
        payload_type_extension_flag: false,
        fec_type: 0,
        extension_flag: false,
        rap_flag: true,
        packet_type: PacketType::Mpu,
        packet_id: 0x1234,
        timestamp: 0xABCDEF01,
        packet_sequence: 0x12345678,
        extension: None,
    };
    let mut buf = vec![0u8; 128];

    c.bench_function("mmtp_header_write", |b| {
        b.iter(|| {
            let mut slice = &mut buf[..];
            header.write_to(&mut slice).unwrap();
            black_box(&buf);
        });
    });
}

fn bench_mmtp_header_read(c: &mut Criterion) {
    let header = MmtpHeader {
        version: 0,
        payload_type_extension_flag: false,
        fec_type: 0,
        extension_flag: false,
        rap_flag: true,
        packet_type: PacketType::Mpu,
        packet_id: 0x1234,
        timestamp: 0xABCDEF01,
        packet_sequence: 0x12345678,
        extension: None,
    };
    let mut buf = vec![0u8; 128];
    let mut slice = &mut buf[..];
    header.write_to(&mut slice).unwrap();

    c.bench_function("mmtp_header_read", |b| {
        b.iter(|| {
            let mut cursor = &buf[..];
            let parsed = MmtpHeader::read_from(&mut cursor).unwrap();
            black_box(parsed);
        });
    });
}

fn bench_mpu_header_write(c: &mut Criterion) {
    let header = MpuHeader::new(FragmentType::Fragment, 0x123456);
    let mut buf = vec![0u8; 64];

    c.bench_function("mpu_header_write", |b| {
        b.iter(|| {
            let mut slice = &mut buf[..];
            header.write_to(&mut slice).unwrap();
            black_box(&buf);
        });
    });
}

fn bench_mpu_header_read(c: &mut Criterion) {
    let header = MpuHeader::new(FragmentType::Fragment, 0x123456);
    let mut buf = vec![0u8; 64];
    let mut slice = &mut buf[..];
    header.write_to(&mut slice).unwrap();

    c.bench_function("mpu_header_read", |b| {
        b.iter(|| {
            let mut cursor = &buf[..];
            let parsed = MpuHeader::read_from(&mut cursor).unwrap();
            black_box(parsed);
        });
    });
}

fn bench_mfu_header_write(c: &mut Criterion) {
    let mfu = MfuDataUnit::new(0x12345678, 42);
    let mut buf = vec![0u8; 64];

    c.bench_function("mfu_header_write", |b| {
        b.iter(|| {
            let mut slice = &mut buf[..];
            mfu.write_to(&mut slice).unwrap();
            black_box(&buf);
        });
    });
}

fn bench_mfu_header_read(c: &mut Criterion) {
    let mfu = MfuDataUnit::new(0x12345678, 42);
    let mut buf = vec![0u8; 64];
    let mut slice = &mut buf[..];
    mfu.write_to(&mut slice).unwrap();

    c.bench_function("mfu_header_read", |b| {
        b.iter(|| {
            let mut cursor = &buf[..];
            let parsed = MfuDataUnit::read_from(&mut cursor).unwrap();
            black_box(parsed);
        });
    });
}

fn bench_annexb_to_hvcc(c: &mut Criterion) {
    // Simulate a typical video frame with multiple NAL units
    let mut annexb = Vec::new();
    for _ in 0..5 {
        annexb.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]); // Start code
        annexb.extend_from_slice(&[0x65; 1000]); // NAL data (1KB)
    }

    let mut group = c.benchmark_group("codec_conversion");
    group.throughput(Throughput::Bytes(annexb.len() as u64));

    group.bench_function("annexb_to_hvcc", |b| {
        b.iter(|| {
            let result = CodecConverter::annexb_to_hvcc(black_box(&annexb)).unwrap();
            black_box(result);
        });
    });

    group.finish();
}

fn bench_annexb_to_hvcc_inplace(c: &mut Criterion) {
    let mut annexb = Vec::new();
    for _ in 0..5 {
        annexb.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
        annexb.extend_from_slice(&[0x65; 1000]);
    }
    let mut output = vec![0u8; annexb.len() + 100];

    let mut group = c.benchmark_group("codec_conversion_inplace");
    group.throughput(Throughput::Bytes(annexb.len() as u64));

    group.bench_function("annexb_to_hvcc_inplace", |b| {
        b.iter(|| {
            let len =
                CodecConverter::annexb_to_hvcc_inplace(black_box(&annexb), black_box(&mut output))
                    .unwrap();
            black_box(len);
        });
    });

    group.finish();
}

fn bench_hvcc_to_annexb(c: &mut Criterion) {
    let mut hvcc = Vec::new();
    for _ in 0..5 {
        hvcc.extend_from_slice(&[0x00, 0x00, 0x03, 0xE8]); // Length = 1000
        hvcc.extend_from_slice(&[0x65; 1000]); // NAL data
    }

    let mut group = c.benchmark_group("codec_conversion");
    group.throughput(Throughput::Bytes(hvcc.len() as u64));

    group.bench_function("hvcc_to_annexb", |b| {
        b.iter(|| {
            let result = CodecConverter::hvcc_to_annexb(black_box(&hvcc)).unwrap();
            black_box(result);
        });
    });

    group.finish();
}

fn bench_packet_builder(c: &mut Criterion) {
    let payload = vec![0xAB; 1000];

    c.bench_function("packet_builder_mpu", |b| {
        let mut builder = PacketBuilder::new(1400);
        b.iter(|| {
            let packet = builder
                .build_mpu_packet(1, 0, FragmentType::Fragment, black_box(&payload), true)
                .unwrap();
            black_box(packet);
        });
    });
}

fn bench_packet_builder_mfu(c: &mut Criterion) {
    let sample_data = vec![0xAB; 500];

    c.bench_function("packet_builder_mfu", |b| {
        let mut builder = PacketBuilder::new(1400);
        b.iter(|| {
            let packet = builder
                .build_mfu_packet(1, 0, 1, 1, black_box(&sample_data), false)
                .unwrap();
            black_box(packet);
        });
    });
}

fn bench_fragmenter(c: &mut Criterion) {
    let large_payload = vec![0xAB; 10000]; // 10KB payload
    let fragmenter = PacketFragmenter::new(1400);

    c.bench_function("fragmenter_iteration", |b| {
        b.iter(|| {
            for fragment in fragmenter.fragment(black_box(&large_payload)) {
                black_box(fragment);
            }
        });
    });
}

fn bench_nal_type_detection(c: &mut Criterion) {
    let hevc_idr = [0x26, 0x01, 0x00, 0x00];
    let avc_idr = [0x65, 0x88, 0x00, 0x00];

    c.bench_function("hevc_nal_type", |b| {
        b.iter(|| {
            let t = CodecConverter::get_hevc_nal_type(black_box(&hevc_idr));
            black_box(t);
        });
    });

    c.bench_function("avc_nal_type", |b| {
        b.iter(|| {
            let t = CodecConverter::get_avc_nal_type(black_box(&avc_idr));
            black_box(t);
        });
    });

    c.bench_function("is_hevc_rap", |b| {
        b.iter(|| {
            let is_rap = CodecConverter::is_hevc_rap(black_box(&hevc_idr));
            black_box(is_rap);
        });
    });
}

fn bench_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput");
    group.throughput(Throughput::Elements(1_000_000));

    let header = MmtpHeader::new(1, PacketType::Mpu);
    let mut buf = vec![0u8; 128];

    group.bench_function("1M_headers_write", |b| {
        b.iter(|| {
            for _ in 0..1_000_000 {
                let mut slice = &mut buf[..];
                header.write_to(&mut slice).unwrap();
            }
            black_box(&buf);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_mmtp_header_write,
    bench_mmtp_header_read,
    bench_mpu_header_write,
    bench_mpu_header_read,
    bench_mfu_header_write,
    bench_mfu_header_read,
    bench_annexb_to_hvcc,
    bench_annexb_to_hvcc_inplace,
    bench_hvcc_to_annexb,
    bench_packet_builder,
    bench_packet_builder_mfu,
    bench_fragmenter,
    bench_nal_type_detection,
    bench_throughput,
);

criterion_main!(benches);
