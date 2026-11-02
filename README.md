# lattice-volume-broker

Rust port of 0xLattice VolumeBroker, pinned at `9173f75c7c88a60b56512c71f3917224792af686`. Original MIT license retained. Provides `MemoryBroker` and SQLite `DiskBroker` through `VolumeBroker` and `RetainedRootBroker` traits.

Both backends atomically validate/publish complete content-addressed volumes, reject changed root memberships, fetch member data through complete owning volumes, cascade local/near/far reads, and maintain byte-exact named retained-root scopes. Retaining a root does not retain nested volumes. Store does not retain; sweep removes unretained volumes and orphan content. Targeted release reclaims only the named volumes and newly orphaned content.

Disk storage preserves Swift schema v2, validates exact canonical table/index definitions, rejects unexpected protected triggers/indexes/foreign keys and inconsistent foreign keys, refuses automatic migration, and uses WAL with FULL synchronous commits. A separate read-only connection serves committed snapshots during writes. Reads revalidate content hashes; re-storing valid complete volume bytes repairs corrupt content without replacing membership.

```rust,no_run
use lattice_volume_broker::{DiskBroker,VolumeBroker,RetainedRootBroker,Roots};
let broker = DiskBroker::open("volumes.sqlite")?;
// broker.store(&serialized_volume)?;
// broker.advance_retained_roots("current", &Roots::from([root]))?;
# Ok::<(), lattice_volume_broker::Error>(())
```

The backend methods are **blocking**. `AsyncBroker::new(Arc::new(broker))` offloads every operation to Tokio’s blocking executor and implements Cashew’s async batch ContentSource, Fetcher, and VolumeStorer. Cancellation does not interrupt an already-running transaction. `MemoryBroker` and `DiskBroker` implement `lattice_cashew::ContentSource`; serialized volumes and CID validation come from sibling `../cashew-rs`. There are no application-chain dependencies.

Run `cargo test`, `cargo clippy --all-targets -- -D warnings`, and `cargo fmt --check`. Tests cover both backends' retention/atomicity, byte-exact scope names, batch reads beyond SQLite chunk size, tier cascades, concurrent merging, incomplete-volume serve gates, corruption repair, fail-closed schema checks, committed snapshot reads, and abrupt subprocess exit with dirty uncommitted changes.

On macOS, `tools/sqlite-interop.sh` compiles the pinned upstream Swift SQLite connection implementation, creates a Swift database, verifies/updates it from Rust, then verifies the Rust-modified database from Swift. It checks schema and persisted-content interoperability, not the entire Swift storage test suite. See [PORTING.md](PORTING.md) for coverage and remaining integration work.
