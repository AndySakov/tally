//! `cargo bench`. `set` is bounded by fsync, so it is paired with a
//! baseline of a bare append + `sync_all` of the same size: the gap between
//! the two is tally's own cost.

use std::fs::{self, OpenOptions};
use std::hint::black_box;
use std::io::Write;
use std::path::Path;
use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tally::{Key, Store, Value};

const VALUE_LEN: usize = 1024;

fn key_bytes(i: u64) -> String {
    format!("key-{i:010}")
}

fn key(i: u64) -> Key {
    Key::try_from(key_bytes(i).as_bytes()).unwrap()
}

/// Sample count for a group: `default`, or `TALLY_BENCH_SAMPLES` if set.
/// Groups set their own count, which overrides criterion's `--sample-size`.
fn samples(default: usize) -> usize {
    std::env::var("TALLY_BENCH_SAMPLES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn value(len: usize) -> Value {
    Value::try_from(&vec![0x5a; len][..]).unwrap()
}

/// Encodes one Set record. A second, independent implementation of the
/// format, so large fixtures can be written without one fsync per record.
fn encode_set(out: &mut Vec<u8>, key: &[u8], value: &[u8]) {
    let mut header = [0u8; 15];
    header[4] = 1;
    header[5..7].copy_from_slice(&(key.len() as u16).to_le_bytes());
    header[7..11].copy_from_slice(&(value.len() as u32).to_le_bytes());
    let payload_checksum = crc32c::crc32c_append(crc32c::crc32c(key), value);
    header[11..15].copy_from_slice(&payload_checksum.to_le_bytes());
    let header_checksum = crc32c::crc32c(&header[4..]);
    header[..4].copy_from_slice(&header_checksum.to_le_bytes());
    out.extend_from_slice(&header);
    out.extend_from_slice(key);
    out.extend_from_slice(value);
}

/// Writes `n` distinct Set records to `dir/tally.log`, then checks that a
/// store opened on it agrees, so a broken fixture fails loudly.
fn write_fixture(dir: &Path, n: u64, value_len: usize) {
    let value_bytes = vec![0x5a; value_len];
    let mut out = Vec::new();
    for i in 0..n {
        encode_set(&mut out, key_bytes(i).as_bytes(), &value_bytes);
    }
    fs::write(dir.join("tally.log"), out).unwrap();
    let store = Store::open(dir).unwrap();
    assert_eq!(store.get(&key(n - 1)).unwrap(), Some(value(value_len)));
}

fn set(c: &mut Criterion) {
    let mut group = c.benchmark_group("set");
    group
        .throughput(Throughput::Elements(1))
        .sample_size(samples(20))
        .measurement_time(Duration::from_secs(10));

    let tmp = tempfile::tempdir().unwrap();
    let mut store = Store::open(tmp.path()).unwrap();
    let mut i = 0;
    group.bench_function("1KiB value", |b| {
        b.iter_batched(
            || {
                i += 1;
                (key(i), value(VALUE_LEN))
            },
            |(k, v)| store.set(k, v).unwrap(),
            BatchSize::SmallInput,
        )
    });

    let mut record = Vec::new();
    encode_set(&mut record, key_bytes(0).as_bytes(), &[0x5a; VALUE_LEN]);
    let baseline = tempfile::tempdir().unwrap();
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(baseline.path().join("baseline.log"))
        .unwrap();
    group.bench_function("baseline: write + sync_all", |b| {
        b.iter(|| {
            file.write_all(&record).unwrap();
            file.sync_all().unwrap();
        })
    });
    group.finish();
}

fn get(c: &mut Criterion) {
    const N: u64 = 10_000;
    let mut group = c.benchmark_group("get");
    group.throughput(Throughput::Elements(1));

    let tmp = tempfile::tempdir().unwrap();
    write_fixture(tmp.path(), N, VALUE_LEN);
    let store = Store::open(tmp.path()).unwrap();
    let keys: Vec<Key> = (0..N).map(key).collect();

    // A stride coprime to N visits every key, not just neighbours.
    let mut i = 0usize;
    group.bench_function("hit, 1KiB value, warm cache", |b| {
        b.iter(|| {
            i = (i + 7919) % keys.len();
            black_box(store.get(&keys[i]).unwrap())
        })
    });

    let missing = Key::try_from(&b"absent"[..]).unwrap();
    group.bench_function("miss", |b| {
        b.iter(|| black_box(store.get(&missing).unwrap()))
    });
    group.finish();
}

fn open(c: &mut Criterion) {
    let mut group = c.benchmark_group("open");
    group.sample_size(samples(10));

    let empty = tempfile::tempdir().unwrap();
    group.bench_function("empty log (three fsyncs)", |b| {
        b.iter(|| Store::open(empty.path()).unwrap())
    });

    for n in [10_000u64, 100_000] {
        let tmp = tempfile::tempdir().unwrap();
        write_fixture(tmp.path(), n, 100);
        group.throughput(Throughput::Elements(n));
        group.bench_with_input(BenchmarkId::new("records, 100B values", n), &n, |b, _| {
            b.iter(|| Store::open(tmp.path()).unwrap())
        });
    }
    group.finish();
}

criterion_group!(benches, set, get, open);
criterion_main!(benches);
