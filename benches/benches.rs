use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use store::config::StoreConfig;
use store::BitCask;
use tempfile::{tempdir, TempDir};

const SIZES: [usize; 9] = [128, 256, 1024, 4096, 8192, 32768, 131072, 262144, 524288];

fn default_bitcask() -> (BitCask, TempDir) {
    let dir = tempdir().unwrap();
    let cfg = StoreConfig {
        log_dir: dir.path().to_path_buf(),
        max_log_file_size: 10_000_000,
    };
    // Return to ensure tempdir does not go out of scope.
    (BitCask::new(Arc::new(cfg)).unwrap(), dir)
}

fn benchmark_get(c: &mut Criterion) {
    let (bitcask, _dir) = default_bitcask();
    let mut group = c.benchmark_group("get");
    for size in SIZES.iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        let val = std::iter::repeat("@").take(*size).collect::<String>();
        bitcask.set("foo", &val).unwrap();
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| bitcask.get(black_box("foo")));
        });
    }
    group.finish();
}

fn benchmark_set(c: &mut Criterion) {
    let (bitcask, _dir) = default_bitcask();
    let mut group = c.benchmark_group("set");
    for size in SIZES.iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        let val = std::iter::repeat("@").take(*size).collect::<String>();
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            // TODO insane to set 2.2M times (would merge in normal circumstances)
            // must be a different way to bench this.
            b.iter(|| bitcask.set(black_box("foo"), black_box(&val)));
        });
    }
    group.finish();
}

criterion_group!(benches, benchmark_get, benchmark_set);
criterion_main!(benches);
