# Port coverage

Upstream: https://github.com/0xLattice/VolumeBroker at 9173f75c7c88a60b56512c71f3917224792af686.

| Swift source / behavior | Rust implementation | Evidence |
| --- | --- | --- |
| VolumeBroker tier cascade / batch missing set | VolumeBroker defaults | mixed near/far members and empty blobs |
| MemoryBroker atomic state | MemoryBroker / RwLock | lifecycle, batch conflict rollback, retention failure rollback |
| CASVolumeStore | DiskBroker store/load | complete membership, >500 entry batch, corrupted content repair |
| RetainedRootIndex | RetainedRootBroker implementations | replace/merge/release/sweep, shared ownership, nested volume independence, targeted release |
| SQLiteConnection schema v2 | DiskBroker open | spoof/missing schema, protected trigger/index/external FK rejection, old-version refusal |
| SQLiteConnection WAL durability | write connection + read-only connection | reopen, abrupt process exit, uncommitted rollback, independent read snapshot |
| SerializedVolume validation | lattice-cashew reexport | invalid member rejects whole batch; canonical CID validation delegated |
| Swift/Rust persistence interoperability | tools/sqlite-interop.sh | actual pinned Swift SQLiteConnection creates and reopens Rust-modified DB |
| RWLock | standard RwLock / Mutex | concurrent merges and shared owners |

All public storage/retention operations are implemented. `checkpoint` returns a Result rather than ignoring SQLite errors. Rust reads use one mutex per connection; reads can proceed alongside writes, but multiple read queries serialize. Cashew sync ContentSource is implemented for each concrete broker. AsyncBroker offloads calls to Tokio’s blocking executor, implements async ContentSource/Fetcher/VolumeStorer, and preserves serialized volume boundaries. Typed plain/encrypted Header resolution and retained lifecycle are covered by real Cashew integration tests. Tier graphs must be acyclic, as in Swift.

Remaining platform verification: host end-to-end volume lifecycle under live node sync, operating-system/filesystem power-loss testing, non-macOS CI, load/performance benchmarks. Abrupt process exit validates WAL recovery and rollback, not physical disk power-loss behavior. Hash algorithm support is the explicit supported set of lattice-cashew; unsupported algorithms fail closed. This document does not assert that every Swift test has been mechanically translated.

## Recovery model boundary

Reviewed `0xLattice/lattice-formal` revision `a9f4c2879b6565ae11c17d35efe14475c5830978`, `specs/Recovery.tla` and README's “Import and recovery” section. That model covers a node's immutable fact journal and graph reducer, **not** the VolumeBroker CAS database. VolumeBroker tests do not prove graph recovery, revision monotonicity, fork-choice reprojection, or eventual publication.

| Model assumption/property | Applicable storage evidence | Boundary |
| --- | --- | --- |
| One durable batch is atomic | `sql_failure_mid_batch_rolls_back_all_content_membership_and_retention` injects an abort after CAS/metadata changes and checks every table is empty; failed retained release restores roots and volumes | Node must independently transact all facts in its journal |
| `VisibleImpliesDurable` | Transactions commit before store returns; separate read-only WAL connection sees only committed snapshots | Node must withhold publish/send/mining effects until its required durable writes succeed |
| `FailureLeavesGraphUnchanged` | Storage failure leaves CAS/membership/retention unchanged, tested during SQL mutations | No graph is owned here; node must keep graph changes externally invisible on failure |
| `ReplayIdempotent` | Repeated identical volume batches and retention merges are idempotent | Reducer replay and prerequisite ordering are node responsibilities |
| `ConflictRejected` | Existing root memberships cannot change; invalid bytes/batch conflicts leave earlier volumes intact | Immutable block/grind metadata conflict checks belong in the fact journal |
| Crash loses volatile state, keeps durable prefix | Abrupt subprocess exit with both committed WAL and uncommitted dirty changes; reopen preserves committed volume and retained scope and discards uncommitted updates | Process-exit recovery is not an electrical power-loss test |

The model requires a durable revision floor. A count of durable fact batches is **not** a count of volumes or retained roots and must not be inferred from this broker. Child-level revision bumps without facts remain a model limitation documented upstream. CAS publication and named-root retention are separate transactions by the original API; a host must coordinate sweep against its own store/retain transition. This crate does not silently invent a cross-database transaction or node mutation lease.
