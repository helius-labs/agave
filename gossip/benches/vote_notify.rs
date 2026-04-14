use {
    criterion::{criterion_group, criterion_main, BenchmarkId, Criterion},
    solana_gossip::crds::VoteNotify,
    std::{
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc, Barrier, Condvar, Mutex,
        },
        thread,
        time::Duration,
    },
};

/// Measures wakeup latency: time from signal to receiver resuming execution.
/// Each approach uses a Barrier to synchronize sender/receiver, then a small
/// sleep to ensure the receiver is parked/sleeping before the signal fires.
fn bench_wakeup_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("vote_notify_wakeup");
    group.sample_size(200);
    group.measurement_time(Duration::from_secs(10));

    // --- condvar (our implementation) ---
    group.bench_function("condvar", |b| {
        let notify = Arc::new(VoteNotify::new());
        let barrier = Arc::new(Barrier::new(2));
        let latency_ns = Arc::new(AtomicU64::new(0));
        let running = Arc::new(AtomicBool::new(true));

        let notify_r = notify.clone();
        let barrier_r = barrier.clone();
        let latency_r = latency_ns.clone();
        let running_r = running.clone();

        let receiver = thread::spawn(move || {
            while running_r.load(Ordering::Relaxed) {
                barrier_r.wait();
                let start = std::time::Instant::now();
                notify_r.wait(Duration::from_secs(1));
                latency_r.store(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }
        });

        b.iter(|| {
            barrier.wait();
            thread::sleep(Duration::from_micros(50));
            notify.notify();
            // Wait for receiver to record latency
            thread::sleep(Duration::from_micros(200));
            latency_ns.load(Ordering::Relaxed)
        });

        running.store(false, Ordering::Relaxed);
        notify.notify(); // wake so it exits
        barrier.wait();
        receiver.join().unwrap();
    });

    // --- Mutex+Condvar (baseline) ---
    group.bench_function("mutex_condvar", |b| {
        let cv = Arc::new((Mutex::new(false), Condvar::new()));
        let barrier = Arc::new(Barrier::new(2));
        let latency_ns = Arc::new(AtomicU64::new(0));
        let running = Arc::new(AtomicBool::new(true));

        let cv_r = cv.clone();
        let barrier_r = barrier.clone();
        let latency_r = latency_ns.clone();
        let running_r = running.clone();

        let receiver = thread::spawn(move || {
            while running_r.load(Ordering::Relaxed) {
                barrier_r.wait();
                let start = std::time::Instant::now();
                let (lock, cvar) = &*cv_r;
                let mut has_new = lock.lock().unwrap();
                if !*has_new {
                    has_new = cvar.wait_timeout(has_new, Duration::from_secs(1)).unwrap().0;
                }
                *has_new = false;
                latency_r.store(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }
        });

        b.iter(|| {
            barrier.wait();
            thread::sleep(Duration::from_micros(50));
            let (lock, cvar) = &*cv;
            *lock.lock().unwrap() = true;
            cvar.notify_one();
            thread::sleep(Duration::from_micros(200));
            latency_ns.load(Ordering::Relaxed)
        });

        running.store(false, Ordering::Relaxed);
        {
            let (lock, cvar) = &*cv;
            *lock.lock().unwrap() = true;
            cvar.notify_one();
        }
        barrier.wait();
        receiver.join().unwrap();
    });

    // --- sleep(10ms) poll ---
    group.bench_function("sleep_10ms", |b| {
        let flag = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(2));
        let latency_ns = Arc::new(AtomicU64::new(0));
        let running = Arc::new(AtomicBool::new(true));

        let flag_r = flag.clone();
        let barrier_r = barrier.clone();
        let latency_r = latency_ns.clone();
        let running_r = running.clone();

        let receiver = thread::spawn(move || {
            while running_r.load(Ordering::Relaxed) {
                barrier_r.wait();
                let start = std::time::Instant::now();
                while !flag_r.swap(false, Ordering::Acquire) {
                    thread::sleep(Duration::from_millis(10));
                }
                latency_r.store(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }
        });

        b.iter(|| {
            barrier.wait();
            thread::sleep(Duration::from_micros(50));
            flag.store(true, Ordering::Release);
            thread::sleep(Duration::from_millis(15)); // wait for poll cycle
            latency_ns.load(Ordering::Relaxed)
        });

        running.store(false, Ordering::Relaxed);
        flag.store(true, Ordering::Release);
        barrier.wait();
        receiver.join().unwrap();
    });

    group.finish();
}

/// Measures notify() call overhead on the sender side under contention.
/// Multiple threads call notify() concurrently to simulate gossip insert threads.
fn bench_notify_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("vote_notify_contention");

    for num_contenders in [0, 4, 8] {
        // --- condvar (our implementation) ---
        group.bench_with_input(
            BenchmarkId::new("condvar", num_contenders),
            &num_contenders,
            |b, &n| {
                let notify = Arc::new(VoteNotify::new());

                let running = Arc::new(AtomicBool::new(true));
                let contenders: Vec<_> = (0..n)
                    .map(|_| {
                        let n = notify.clone();
                        let r = running.clone();
                        thread::spawn(move || {
                            while r.load(Ordering::Relaxed) {
                                n.notify();
                                std::hint::spin_loop();
                            }
                        })
                    })
                    .collect();

                b.iter(|| notify.notify());

                running.store(false, Ordering::Relaxed);
                for t in contenders {
                    t.join().unwrap();
                }
            },
        );

        // --- Mutex+Condvar ---
        group.bench_with_input(
            BenchmarkId::new("mutex_condvar", num_contenders),
            &num_contenders,
            |b, &n| {
                let cv = Arc::new((Mutex::new(false), Condvar::new()));

                let running = Arc::new(AtomicBool::new(true));
                let contenders: Vec<_> = (0..n)
                    .map(|_| {
                        let cv = cv.clone();
                        let r = running.clone();
                        thread::spawn(move || {
                            while r.load(Ordering::Relaxed) {
                                let (lock, cvar) = &*cv;
                                *lock.lock().unwrap() = true;
                                cvar.notify_one();
                                std::hint::spin_loop();
                            }
                        })
                    })
                    .collect();

                b.iter(|| {
                    let (lock, cvar) = &*cv;
                    *lock.lock().unwrap() = true;
                    cvar.notify_one();
                });

                running.store(false, Ordering::Relaxed);
                for t in contenders {
                    t.join().unwrap();
                }
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_wakeup_latency, bench_notify_contention);
criterion_main!(benches);
