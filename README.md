### Build & Run Instructions

First, build the project in release mode:

```bash
cargo build --release
```

Then run the benchmark:
```bash
./target/release/dashmap_ycsb --threads 16 --workload a
```

For more details about dashmap, see the official repository:

https://github.com/xacrimon/dashmap
