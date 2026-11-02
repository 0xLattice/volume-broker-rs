use lattice_cashew::cid_for_bytes;
use lattice_volume_broker::*;
use std::{collections::BTreeMap, sync::Arc};
fn volume(label: &str, shared: &[u8]) -> SerializedVolume {
    let data = label.as_bytes().to_vec();
    let root = cid_for_bytes(0x55, &data);
    SerializedVolume::new(
        root.clone(),
        BTreeMap::from([(root, data), (cid_for_bytes(0x55, shared), shared.to_vec())]),
    )
}
fn roots(v: &[&SerializedVolume]) -> Roots {
    v.iter().map(|v| v.root.clone()).collect()
}
fn exercise(b: &dyn RetainedRootBroker) {
    let a = volume("a", b"shared");
    let c = volume("c", b"shared");
    b.store_volumes_local(&[a.clone(), c.clone()]).unwrap();
    assert!(b.has_volume(&a.root));
    assert_eq!(b.fetch(&cid_for_bytes(0x55, b"shared")).unwrap(), b"shared");
    assert_eq!(b.fetch_volume(&a.root).unwrap().entries, a.entries);
    b.advance_retained_roots("one", &roots(&[&a])).unwrap();
    b.merge_retained_roots("one", &roots(&[&c])).unwrap();
    b.advance_retained_roots("two", &roots(&[&a])).unwrap();
    assert_eq!(b.retained_roots("one").unwrap(), roots(&[&a, &c]));
    assert_eq!(b.release_retained_roots("one", &roots(&[&a])).unwrap(), 0);
    assert_eq!(b.release_retained_roots("two", &roots(&[&a])).unwrap(), 1);
    assert!(!b.has_volume(&a.root));
    assert_eq!(b.fetch(&cid_for_bytes(0x55, b"shared")).unwrap(), b"shared");
    assert_eq!(b.sweep().unwrap(), 0);
    b.advance_retained_roots("one", &Roots::new()).unwrap();
    assert_eq!(b.sweep().unwrap(), 1);
    assert!(b.fetch(&cid_for_bytes(0x55, b"shared")).is_err());
    assert!(b.advance_retained_roots("one", &roots(&[&a])).is_err());
}
#[test]
fn memory_lifecycle() {
    exercise(&MemoryBroker::default());
}
#[test]
fn disk_lifecycle() {
    let tmp = tempfile::tempdir().unwrap();
    exercise(&DiskBroker::open(tmp.path().join("db")).unwrap());
}
fn atomic(b: &dyn RetainedRootBroker) {
    let a = volume("a", b"shared");
    b.store(&a).unwrap();
    let c = volume("c", b"shared");
    let mut conflict = a.clone();
    conflict.entries.remove(&cid_for_bytes(0x55, b"shared"));
    assert!(matches!(
        b.store_volumes_local(&[c.clone(), conflict]),
        Err(Error::ConflictingVolume(_))
    ));
    assert!(!b.has_volume(&c.root));
    let mut invalid = c.clone();
    invalid.entries.insert(c.root.clone(), vec![0]);
    assert!(
        b.store_volumes_local(&[volume("new", b"new"), invalid])
            .is_err()
    );
    assert!(!b.has_volume(&volume("new", b"new").root));
    b.advance_retained_roots("scope", &roots(&[&a])).unwrap();
    assert!(b.advance_retained_roots("scope", &roots(&[&c])).is_err());
    assert_eq!(b.retained_roots("scope").unwrap(), roots(&[&a]));
    assert!(b.merge_retained_roots("scope", &roots(&[&a, &c])).is_err());
    assert_eq!(b.retained_roots("scope").unwrap(), roots(&[&a]));
}
#[test]
fn memory_atomicity() {
    atomic(&MemoryBroker::default());
}
#[test]
fn disk_atomicity() {
    let tmp = tempfile::tempdir().unwrap();
    atomic(&DiskBroker::open(tmp.path().join("db")).unwrap());
}
fn names(b: &dyn RetainedRootBroker) {
    let a = volume("a", b"shared");
    b.store(&a).unwrap();
    for name in ["é", "e\u{301}", "pre%_", "pre%_x", "prefix"] {
        b.merge_retained_roots(name, &roots(&[&a])).unwrap();
    }
    assert_eq!(b.retained_scopes("pre%_").unwrap(), vec!["pre%_", "pre%_x"]);
    assert_eq!(b.retained_scopes("é").unwrap(), vec!["é"]);
    assert_eq!(b.retained_scopes("e\u{301}").unwrap(), vec!["e\u{301}"]);
    assert!(
        b.advance_retained_roots("bad\0name", &roots(&[&a]))
            .is_err()
    );
    assert!(b.advance_retained_roots("", &roots(&[&a])).is_err());
    assert!(
        b.release_retained_roots("scope", &Roots::from(["\0".into()]))
            .is_err()
    );
    assert!(b.retained_scopes("\0").unwrap().is_empty());
}
#[test]
fn byte_exact_scopes() {
    names(&MemoryBroker::default());
    let t = tempfile::tempdir().unwrap();
    names(&DiskBroker::open(t.path().join("db")).unwrap());
}
#[test]
fn cascading_batch_and_empty_blobs() {
    let near = Arc::new(MemoryBroker::default());
    let far = Arc::new(MemoryBroker::default());
    let a = volume("a", b"");
    let b = volume("b", b"shared");
    near.store(&a).unwrap();
    far.store(&b).unwrap();
    let local = MemoryBroker::new(Some(near), Some(far));
    let query = a.entries.keys().chain(b.entries.keys()).cloned().collect();
    let found = local.fetch_data(&query);
    assert_eq!(found.len(), 4);
    assert_eq!(found[&cid_for_bytes(0x55, b"")], Vec::<u8>::new());
}
#[test]
fn disk_reopen_and_corruption_repair() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("db");
    let v = volume("root", b"shared");
    {
        let b = DiskBroker::open(&path).unwrap();
        b.store(&v).unwrap();
        b.advance_retained_roots("scope", &roots(&[&v])).unwrap();
    }
    let b = DiskBroker::open(&path).unwrap();
    assert_eq!(b.retained_roots("scope").unwrap(), roots(&[&v]));
    let raw = rusqlite::Connection::open(&path).unwrap();
    raw.execute(
        "UPDATE cas_data SET data=?1 WHERE cid=?2",
        rusqlite::params![b"corrupt".to_vec(), v.root],
    )
    .unwrap();
    assert!(b.fetch_volume_local(&v.root).is_none());
    assert!(b.fetch(&v.root).is_err());
    b.store(&v).unwrap();
    assert_eq!(b.fetch(&v.root).unwrap(), b"root");
}
#[test]
fn incomplete_volume_never_served() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("db");
    let b = DiskBroker::open(&path).unwrap();
    let v = volume("root", b"shared");
    b.store(&v).unwrap();
    let raw = rusqlite::Connection::open(&path).unwrap();
    raw.execute(
        "UPDATE volume_metadata SET entry_count=3 WHERE root=?1",
        [&v.root],
    )
    .unwrap();
    assert!(!b.has_volume(&v.root));
    assert!(b.fetch_volume_local(&v.root).is_none());
    assert!(b.fetch(&cid_for_bytes(0x55, b"shared")).is_err());
    assert!(b.advance_retained_roots("scope", &roots(&[&v])).is_err());
    assert!(b.store(&v).is_err());
}
#[test]
fn schema_fails_closed() {
    for sql in [
        "PRAGMA user_version=1",
        "CREATE TABLE legacy(x)",
        "CREATE TABLE cas_data(cid TEXT PRIMARY KEY,data BLOB); PRAGMA user_version=2",
    ] {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("db");
        rusqlite::Connection::open(&p)
            .unwrap()
            .execute_batch(sql)
            .unwrap();
        assert!(DiskBroker::open(&p).is_err());
    }
    for sql in [
        "CREATE TRIGGER evil AFTER INSERT ON cas_data BEGIN DELETE FROM cas_data; END",
        "CREATE INDEX extra ON cas_data(data)",
        "CREATE TABLE evil(parent TEXT REFERENCES cas_data(cid))",
        "DROP INDEX idx_ve_cid",
    ] {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("db");
        drop(DiskBroker::open(&p).unwrap());
        rusqlite::Connection::open(&p)
            .unwrap()
            .execute_batch(sql)
            .unwrap();
        assert!(matches!(DiskBroker::open(&p), Err(Error::InvalidSchema)));
    }
}
#[test]
fn more_than_sql_parameter_chunk() {
    let tmp = tempfile::tempdir().unwrap();
    let b = DiskBroker::open(tmp.path().join("db")).unwrap();
    let mut entries = Entries::new();
    for i in 0..1201 {
        let data = format!("entry{i}").into_bytes();
        entries.insert(cid_for_bytes(0x55, &data), data);
    }
    let root = entries.keys().next().unwrap().clone();
    b.store(&SerializedVolume::new(root, entries.clone()))
        .unwrap();
    assert_eq!(
        b.fetch_data_local(&entries.keys().cloned().collect()),
        entries
    );
}
#[test]
fn concurrent_merges_and_shared_owners() {
    let tmp = tempfile::tempdir().unwrap();
    let b = Arc::new(DiskBroker::open(tmp.path().join("db")).unwrap());
    let workers: Vec<_> = (0..8)
        .map(|i| {
            let b = b.clone();
            std::thread::spawn(move || {
                let v = volume(&format!("root{i}"), b"shared");
                b.store(&v).unwrap();
                b.merge_retained_roots("scope", &roots(&[&v])).unwrap();
            })
        })
        .collect();
    for w in workers {
        w.join().unwrap();
    }
    assert_eq!(b.retained_roots("scope").unwrap().len(), 8);
    assert_eq!(b.sweep().unwrap(), 0);
}
#[test]
fn crash_writer() {
    let Ok(path) = std::env::var("LATTICE_BROKER_CRASH_DB") else {
        return;
    };
    let broker = DiskBroker::open(&path).unwrap();
    let volume = volume("durable", b"shared");
    broker.store(&volume).unwrap();
    broker
        .advance_retained_roots("durable", &roots(&[&volume]))
        .unwrap();
    let raw = rusqlite::Connection::open(&path).unwrap();
    raw.execute_batch(
        "BEGIN IMMEDIATE; UPDATE cas_data SET data=X'00'; DELETE FROM retained_roots;",
    )
    .unwrap();
    // Skip destructors with a live dirty transaction and committed WAL frames.
    std::process::exit(0);
}
#[test]
fn crash_recovery_preserves_commits_and_rolls_back_uncommitted_changes() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("db");
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("crash_writer")
        .env("LATTICE_BROKER_CRASH_DB", &path)
        .status()
        .unwrap();
    assert!(status.success());
    let broker = DiskBroker::open(&path).unwrap();
    let v = volume("durable", b"shared");
    assert_eq!(
        broker.fetch_volume_local(&v.root).unwrap().entries,
        v.entries
    );
    assert_eq!(broker.retained_roots("durable").unwrap(), roots(&[&v]));
    assert_eq!(broker.sweep().unwrap(), 0);
}
#[test]
fn readers_see_committed_snapshot_while_writer_holds_transaction() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("db");
    let broker = DiskBroker::open(&path).unwrap();
    let v = volume("durable", b"shared");
    broker.store(&v).unwrap();
    let raw = rusqlite::Connection::open(&path).unwrap();
    raw.execute_batch("BEGIN IMMEDIATE; UPDATE cas_data SET data=X'00';")
        .unwrap();
    assert_eq!(
        broker.fetch_volume_local(&v.root).unwrap().entries,
        v.entries
    );
    raw.execute_batch("ROLLBACK").unwrap();
}
#[test]
fn retaining_parent_does_not_retain_nested_volume() {
    fn exercise(b: &dyn RetainedRootBroker) {
        let nested = volume("nested", b"leaf");
        let mut parent = volume("parent", b"nested");
        parent
            .entries
            .insert(nested.root.clone(), b"nested".to_vec());
        b.store_volumes_local(&[parent.clone(), nested.clone()])
            .unwrap();
        b.advance_retained_roots("parent", &roots(&[&parent]))
            .unwrap();
        assert_eq!(b.sweep().unwrap(), 1);
        assert!(b.has_volume(&parent.root));
        assert!(!b.has_volume(&nested.root));
        assert_eq!(b.fetch(&nested.root).unwrap(), b"nested");
        assert!(b.fetch(&cid_for_bytes(0x55, b"leaf")).is_err());
    }
    exercise(&MemoryBroker::default());
    let dir = tempfile::tempdir().unwrap();
    exercise(&DiskBroker::open(dir.path().join("db")).unwrap());
}
#[test]
fn targeted_release_does_not_sweep_unrelated_unretained_content() {
    fn exercise(b: &dyn RetainedRootBroker) {
        let a = volume("a", b"shared");
        let unrelated = volume("unrelated", b"shared");
        b.store_volumes_local(&[a.clone(), unrelated.clone()])
            .unwrap();
        assert_eq!(
            b.release_retained_roots("absent", &roots(&[&a])).unwrap(),
            1
        );
        assert!(b.has_volume(&unrelated.root));
    }
    exercise(&MemoryBroker::default());
    let dir = tempfile::tempdir().unwrap();
    exercise(&DiskBroker::open(dir.path().join("db")).unwrap());
}
#[test]
fn sql_failure_mid_batch_rolls_back_all_content_membership_and_retention() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("db");
    let broker = DiskBroker::open(&path).unwrap();
    let raw = rusqlite::Connection::open(&path).unwrap();
    // Inject a fault after schema validation, after metadata/CAS inserts but inside membership insertion.
    raw.execute_batch("CREATE TRIGGER inject_failure BEFORE INSERT ON volume_entries WHEN (SELECT count(*) FROM volume_entries)>0 BEGIN SELECT RAISE(ABORT,'injected storage failure'); END").unwrap();
    let a = volume("a", b"shared");
    let b = volume("b", b"shared");
    assert!(broker.store_volumes_local(&[a.clone(), b.clone()]).is_err());
    for table in [
        "cas_data",
        "volume_metadata",
        "volume_entries",
        "retained_roots",
    ] {
        let count: i64 = raw
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }
    assert!(!broker.has_volume(&a.root));
    assert!(!broker.has_volume(&b.root));
    raw.execute_batch("DROP TRIGGER inject_failure").unwrap();
    broker.store_volumes_local(&[a.clone(), b.clone()]).unwrap();
    broker
        .advance_retained_roots("scope", &roots(&[&a, &b]))
        .unwrap();
    raw.execute_batch("CREATE TRIGGER inject_failure BEFORE DELETE ON volume_metadata BEGIN SELECT RAISE(ABORT,'injected release failure'); END").unwrap();
    assert!(
        broker
            .release_retained_roots("scope", &roots(&[&a, &b]))
            .is_err()
    );
    assert_eq!(broker.retained_roots("scope").unwrap(), roots(&[&a, &b]));
    assert!(broker.has_volume(&a.root));
    assert!(broker.has_volume(&b.root));
}
#[test]
fn replay_identical_batches_is_idempotent() {
    fn exercise(b: &dyn RetainedRootBroker) {
        let a = volume("a", b"shared");
        let c = volume("c", b"shared");
        for _ in 0..5 {
            b.store_volumes_local(&[a.clone(), c.clone(), a.clone()])
                .unwrap();
            b.merge_retained_roots("scope", &roots(&[&a, &c])).unwrap();
        }
        assert_eq!(b.retained_roots("scope").unwrap(), roots(&[&a, &c]));
        assert_eq!(
            b.release_retained_roots("scope", &roots(&[&a, &c]))
                .unwrap(),
            2
        );
        assert_eq!(b.sweep().unwrap(), 0);
    }
    exercise(&MemoryBroker::default());
    let dir = tempfile::tempdir().unwrap();
    exercise(&DiskBroker::open(dir.path().join("db")).unwrap());
}

#[test]
fn wal_snapshot_survives_release_and_excludes_later_writes() {
    let directory = tempfile::tempdir().unwrap();
    let broker = Arc::new(DiskBroker::open(directory.path().join("snapshot.db")).unwrap());
    let before = volume("before", b"old shared");
    let after = volume("after", b"new shared");
    broker
        .store_volumes_local(std::slice::from_ref(&before))
        .unwrap();
    broker
        .merge_retained_roots("held", &roots(&[&before]))
        .unwrap();
    // Establish the snapshot before any fetch, not lazily at the first fetch.
    let snapshot = broker.read_snapshot().unwrap();
    let writer = broker.clone();
    let old = before.clone();
    let new = after.clone();
    std::thread::spawn(move || {
        writer
            .release_retained_roots("held", &roots(&[&old]))
            .unwrap();
        writer.store_volumes_local(&[new]).unwrap();
    })
    .join()
    .unwrap();
    assert!(!broker.has_volume(&before.root));
    assert!(broker.has_volume(&after.root));
    assert_eq!(
        snapshot
            .fetch_data(&before.entries.keys().cloned().collect())
            .unwrap(),
        before.entries
    );
    assert!(snapshot.fetch_data(&roots(&[&after])).unwrap().is_empty());
    let fresh = broker.read_snapshot().unwrap();
    assert!(fresh.fetch_data(&roots(&[&before])).unwrap().is_empty());
    drop(fresh);
    drop(snapshot);
    // With leases released the WAL can be fully checkpointed again.
    broker.checkpoint().unwrap();
}
