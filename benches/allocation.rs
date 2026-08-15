//! Allocator hot paths: steady-state append throughput and the
//! speculate/rollback cycle the free lists exist to amortize — after warmup, a
//! cycle should recycle chunks (arena and index both) rather than allocate.

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use mesocarp::state_management::{CopyTimeline, Domain, Stamp};

fn append_throughput(c: &mut Criterion) {
    c.bench_function("record_1k_appends", |b| {
        b.iter_batched(
            || {
                let d = Domain::new(4096).unwrap();
                let tl = CopyTimeline::<u64>::new(512, &d).unwrap();
                (d, tl)
            },
            |(mut d, mut tl)| {
                for t in 1..=1_000u64 {
                    tl.record(&mut d, t, Stamp { time: t, seq: 0 }).unwrap();
                }
                (d, tl)
            },
            BatchSize::SmallInput,
        )
    });
}

fn speculate_rollback_cycle(c: &mut Criterion) {
    let mut d = Domain::new(4096).unwrap();
    let mut tl = CopyTimeline::<u64>::new(512, &d).unwrap();
    tl.record(&mut d, 0, Stamp { time: 0, seq: 0 }).unwrap();

    let mut t = 1u64;
    c.bench_function("speculate_256_rollback_rewind", |b| {
        b.iter(|| {
            let start = t;
            for _ in 0..256 {
                tl.record(&mut d, t, Stamp { time: t, seq: 0 }).unwrap();
                t += 1;
            }
            let mark = tl.partial_rollback(&d, start).unwrap().unwrap();
            unsafe { d.restore(mark).unwrap() };
        })
    });
}

criterion_group!(benches, append_throughput, speculate_rollback_cycle);
criterion_main!(benches);
