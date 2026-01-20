use clap::{Parser, ValueEnum};
use dashmap::DashMap;
use rand::{thread_rng, Rng};
use rand_distr::{Distribution, Zipf};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

const VALUE_BYTES: usize = 1024; // 1 KB like YCSB
type Value = Box<[u8; VALUE_BYTES]>;

fn make_value(seed: usize) -> Value {
    let mut v = Box::new([0u8; VALUE_BYTES]);
    // touch a few bytes so it’s not “all the same” and pages get committed
    v[0] = (seed & 0xFF) as u8;
    v[VALUE_BYTES / 2] = ((seed >> 8) & 0xFF) as u8;
    v[VALUE_BYTES - 1] = ((seed >> 16) & 0xFF) as u8;
    v
}


#[derive(Copy, Clone, Debug, ValueEnum)]
enum WorkloadKind {
    A,
    B,
    C,
    D,
    E,
    F,
    All,
}

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Threads to use (1 = single-threaded)
    #[arg(long, default_value_t = 4)]
    threads: usize,

    /// Which workload to run: a|b|c|all
    #[arg(long, value_enum, default_value_t = WorkloadKind::All)]
    workload: WorkloadKind,

    /// Number of records to pre-load (dataset size)
    #[arg(long, default_value_t = 60_000_000)]
    recordcount: usize,

    /// Number of operations to run
    #[arg(long, default_value_t = 200_000_000)]
    operationcount: usize,

    /// Use zipfian request distribution (YCSB default). If false -> uniform.
    #[arg(long, default_value_t = true)]
    zipfian: bool,

    /// Zipfian exponent (1.03 is a common YCSB-ish value)
    #[arg(long, default_value_t = 1.03)]
    zipf_s: f64,
}

#[derive(Copy, Clone)]
struct YcsbWorkload {
    name: &'static str,
    readprop: f64,
    updateprop: f64,
    insertprop: f64,
    scanprop: f64,
}

fn selected_workloads(kind: WorkloadKind) -> Vec<YcsbWorkload> {
    let a = YcsbWorkload {
        name: "workloada",
        readprop: 0.5,
        updateprop: 0.5,
        insertprop: 0.0,
        scanprop: 0.0,
    };
    let b = YcsbWorkload {
        name: "workloadb",
        readprop: 0.95,
        updateprop: 0.05,
        insertprop: 0.0,
        scanprop: 0.0,
    };
    let c = YcsbWorkload {
        name: "workloadc",
        readprop: 1.0,
        updateprop: 0.0,
        insertprop: 0.0,
        scanprop: 0.0,
    };
    let d = YcsbWorkload {
        name: "workloadd",
        readprop: 0.75,
        updateprop: 0.20,
        insertprop: 0.05,
        scanprop: 0.0,
    };
    let e = YcsbWorkload {
        name: "workloade",
        readprop: 0.55,
        updateprop: 0.0,
        insertprop: 0.0,
        scanprop: 0.45,
    };
    let f = YcsbWorkload {
        name: "workloadf",
        readprop: 0.25,
        updateprop: 0.25,
        insertprop: 0.25,
        scanprop: 0.25,
    };

    match kind {
        WorkloadKind::A => vec![a],
        WorkloadKind::B => vec![b],
        WorkloadKind::C => vec![c],
        WorkloadKind::D => vec![d],
        WorkloadKind::E => vec![e],
        WorkloadKind::F => vec![f],
        WorkloadKind::All => vec![a, b, c, d, e, f],
    }
}

fn main() {
    let args = Args::parse();
    assert!(args.threads >= 1, "--threads must be >= 1");

    let workloads = selected_workloads(args.workload);

    let warmup_frac = 0.05; // 5% of operations as warmup

    for wl in workloads {
        println!(
            "Running {} | threads={} | records={} | ops={} | dist={}",
            wl.name,
            args.threads,
            args.recordcount,
            args.operationcount,
            if args.zipfian { "zipfian" } else { "uniform" }
        );

        let map = Arc::new(DashMap::new());

        // Load phase
        for key in 0..args.recordcount {
            let mut v = Box::new([0u8; VALUE_BYTES]);
            v[0] = (key & 0xFF) as u8; // touch it so it’s not optimized away
            map.insert(key, make_value(key));

        }


        // -------- Warmup phase (not measured) --------
        let warmup_ops = (args.operationcount as f64 * warmup_frac) as usize;
        let warmup_threads = args.threads;

        let base = warmup_ops / warmup_threads;
        let rem = warmup_ops % warmup_threads;

        let mut warmup_handles = Vec::with_capacity(warmup_threads);
        for tid in 0..warmup_threads {
            let map = Arc::clone(&map);
            let ops = base + if tid < rem { 1 } else { 0 };
            let recordcount = args.recordcount;
            let zipfian = args.zipfian;
            let zipf_s = args.zipf_s;

            warmup_handles.push(thread::spawn(move || {
                let mut rng = thread_rng();

                let zipf = if zipfian {
                    Some(Zipf::new(recordcount as u64, zipf_s).unwrap())
                } else {
                    None
                };

                for _ in 0..ops {
                    let op_choice: f64 = rng.gen();
                    let key = if let Some(z) = &zipf {
                        (z.sample(&mut rng) as usize) % recordcount
                    } else {
                        rng.gen_range(0..recordcount)
                    };

                    if op_choice < wl.readprop {
                        let _ = map.get(&key);
                    } else if op_choice < wl.readprop + wl.updateprop {
                        map.insert(key, make_value(key));

                    } else if op_choice < wl.readprop + wl.updateprop + wl.insertprop {
                        map.insert(key, make_value(key));

                    }
                }
            }));
        }

        for h in warmup_handles {
            h.join().unwrap();
        }

        // Optional: small pause to let the system settle
        std::thread::sleep(std::time::Duration::from_millis(200));

        let start = Instant::now();

        // Split operations across threads; distribute remainder to first threads
        let base = args.operationcount / args.threads;
        let rem = args.operationcount % args.threads;

        let mut handles = Vec::with_capacity(args.threads);
        for tid in 0..args.threads {
            let map = Arc::clone(&map);
            let ops = base + if tid < rem { 1 } else { 0 };
            let recordcount = args.recordcount;
            let zipfian = args.zipfian;
            let zipf_s = args.zipf_s;

            handles.push(thread::spawn(move || {
                let mut rng = thread_rng();

                let zipf = if zipfian {
                    Some(Zipf::new(recordcount as u64, zipf_s).unwrap())
                } else {
                    None
                };

                for _ in 0..ops {
                    let op_choice: f64 = rng.gen(); // ok on your Rust, or use rng.gen::<f64>()
                    let key = if let Some(z) = &zipf {
                        (z.sample(&mut rng) as usize) % recordcount
                    } else {
                        rng.gen_range(0..recordcount)
                    };

                    if op_choice < wl.readprop {
                        let _ = map.get(&key);
                    } else if op_choice < wl.readprop + wl.updateprop {
                        map.insert(key, make_value(key));

                    } else if op_choice < wl.readprop + wl.updateprop + wl.insertprop {
                        map.insert(key, make_value(key));

                    } else {
                        // scan not implemented
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let elapsed = start.elapsed();
        let secs = elapsed.as_secs_f64();
        let throughput = (args.operationcount as f64) / secs;

        println!(
            "Completed {} in {:.3?} | throughput = {:.2} Mops/s\n",
            wl.name,
            elapsed,
            throughput / 1e6
        );
    }
}
