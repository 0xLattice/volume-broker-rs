use super::*;
use lattice_cashew::validate_entry;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params, params_from_iter};
use std::{path::Path, sync::Mutex, time::Duration};
const COMPLETE: &str = "vm.entry_count = (SELECT COUNT(*) FROM volume_entries manifest_members WHERE manifest_members.root = vm.root) AND EXISTS (SELECT 1 FROM volume_entries root_member WHERE root_member.root = vm.root AND root_member.cid = vm.root)";
const SCHEMA: [(&str, &str, &str); 7] = [
    (
        "table",
        "cas_data",
        "CREATE TABLE cas_data (cid TEXT PRIMARY KEY, data BLOB NOT NULL)",
    ),
    (
        "table",
        "volume_metadata",
        "CREATE TABLE volume_metadata (root TEXT PRIMARY KEY, entry_count INTEGER NOT NULL CHECK (typeof(entry_count) = 'integer' AND entry_count > 0))",
    ),
    (
        "table",
        "volume_entries",
        "CREATE TABLE volume_entries (root TEXT NOT NULL REFERENCES volume_metadata(root) ON DELETE CASCADE, cid TEXT NOT NULL REFERENCES cas_data(cid), PRIMARY KEY (root, cid))",
    ),
    (
        "table",
        "retained_roots",
        "CREATE TABLE retained_roots (scope TEXT NOT NULL, root TEXT NOT NULL, PRIMARY KEY (scope, root))",
    ),
    (
        "index",
        "idx_ve_cid",
        "CREATE INDEX idx_ve_cid ON volume_entries(cid)",
    ),
    (
        "index",
        "idx_ve_root",
        "CREATE INDEX idx_ve_root ON volume_entries(root)",
    ),
    (
        "index",
        "idx_retained_roots_root",
        "CREATE INDEX idx_retained_roots_root ON retained_roots(root)",
    ),
];
/// SQLite schema v2 is interoperable with the pinned Swift VolumeBroker.
/// Writes are serialized; a separate read-only WAL connection serves committed snapshots.
/// Transactions and WAL FULL provide atomic durability.
pub struct DiskBroker {
    near: Option<Arc<dyn VolumeBroker>>,
    far: Option<Arc<dyn VolumeBroker>>,
    connection: Mutex<Connection>,
    reader: Mutex<Connection>,
}
impl DiskBroker {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        Self::with_tiers(path, None, None)
    }
    pub fn with_tiers(
        path: impl AsRef<Path>,
        near: Option<Arc<dyn VolumeBroker>>,
        far: Option<Arc<dyn VolumeBroker>>,
    ) -> Result<Self, Error> {
        let path = path.as_ref();
        let mut db = Connection::open(path)?;
        db.busy_timeout(Duration::from_secs(30))?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let version: i64 = tx.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        if version != 2 {
            let count: i64 = tx.query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
                [],
                |r| r.get(0),
            )?;
            if version != 0 || count != 0 {
                return Err(Error::MigrationRequired {
                    found: version,
                    required: 2,
                });
            }
            for (_, _, sql) in SCHEMA {
                tx.execute_batch(sql)?;
            }
            tx.execute_batch("PRAGMA user_version=2")?;
        }
        validate_schema(&tx)?;
        tx.commit()?;
        db.execute_batch("PRAGMA foreign_keys=ON; PRAGMA synchronous=FULL;")?;
        let journal: String = db.query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))?;
        if !journal.eq_ignore_ascii_case("wal") {
            return Err(Error::InconsistentState("WAL unavailable"));
        }
        let foreign_keys: i64 = db.query_row("PRAGMA foreign_keys", [], |r| r.get(0))?;
        let sync: i64 = db.query_row("PRAGMA synchronous", [], |r| r.get(0))?;
        if foreign_keys != 1 || sync != 2 {
            return Err(Error::InconsistentState(
                "durability or foreign key enforcement unavailable",
            ));
        }
        db.busy_timeout(Duration::from_secs(5))?;
        db.execute_batch("PRAGMA cache_size=-65536; PRAGMA mmap_size=268435456; PRAGMA temp_store=MEMORY; BEGIN IMMEDIATE; COMMIT;")?;
        let reader = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        reader.busy_timeout(Duration::from_secs(5))?;
        reader.execute_batch("PRAGMA foreign_keys=ON; PRAGMA cache_size=-16384;")?;
        Ok(Self {
            near,
            far,
            connection: Mutex::new(db),
            reader: Mutex::new(reader),
        })
    }
    /// Fold committed WAL pages into the database; stores are already durable without it.
    pub fn checkpoint(&self) -> Result<(), Error> {
        self.connection
            .lock()
            .unwrap()
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
        Ok(())
    }
    fn retain(&self, scope: &str, roots: &Roots, replace: bool) -> Result<(), Error> {
        validate_retention(scope, roots)?;
        let mut db = self.connection.lock().unwrap();
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for root in roots {
            if !complete(&tx, root)? {
                return Err(Error::MissingRetainedRoot(root.clone()));
            }
        }
        if replace {
            tx.execute("DELETE FROM retained_roots WHERE scope=?1", [scope])?;
        }
        for root in roots {
            tx.execute(
                "INSERT OR IGNORE INTO retained_roots(scope,root) VALUES(?1,?2)",
                params![scope, root],
            )?;
        }
        tx.commit()?;
        Ok(())
    }
}
fn canonical_sql(sql: &str) -> String {
    let chars: Vec<_> = sql.trim().chars().collect();
    let mut result = String::new();
    let mut quote = None;
    let mut space = false;
    let mut i = 0;
    let word = |c: char| c.is_alphanumeric() || c == '_' || c == '$';
    let separator =
        |a: char, b: char| (word(a) && word(b)) || "'\"`".contains(a) || "'\"`".contains(b);
    while i < chars.len() {
        let c = chars[i];
        if let Some(q) = quote {
            result.push(c);
            if c == q {
                if chars.get(i + 1) == Some(&q) {
                    i += 1;
                    result.push(q);
                } else {
                    quote = None
                }
            }
        } else if "'\"`".contains(c) {
            if space && result.chars().last().is_some_and(|p| separator(p, c)) {
                result.push(' ')
            }
            space = false;
            quote = Some(c);
            result.push(c);
        } else if c.is_whitespace() {
            space = true;
        } else {
            if space && result.chars().last().is_some_and(|p| separator(p, c)) {
                result.push(' ')
            }
            space = false;
            result.push(c);
        }
        i += 1;
    }
    result
}
fn validate_schema(db: &Connection) -> Result<(), Error> {
    for (kind, name, sql) in SCHEMA {
        let stored: Option<String> = db
            .query_row(
                "SELECT sql FROM sqlite_schema WHERE type=?1 AND name=?2",
                params![kind, name],
                |r| r.get(0),
            )
            .optional()?;
        if stored.as_deref().map(canonical_sql) != Some(canonical_sql(sql)) {
            return Err(Error::InvalidSchema);
        }
    }
    let protected = "'cas_data','volume_metadata','volume_entries','retained_roots'";
    for query in [
        format!(
            "SELECT COUNT(*) FROM sqlite_schema WHERE type='trigger' AND tbl_name IN ({protected})"
        ),
        format!(
            "SELECT COUNT(*) FROM sqlite_schema WHERE type='index' AND tbl_name IN ({protected}) AND sql IS NOT NULL AND name NOT IN ('idx_ve_cid','idx_ve_root','idx_retained_roots_root')"
        ),
        format!(
            "SELECT COUNT(*) FROM sqlite_schema child JOIN pragma_foreign_key_list(child.name) fk WHERE child.type='table' AND child.name NOT IN ({protected}) AND fk.\"table\" COLLATE NOCASE IN ({protected})"
        ),
        "SELECT COUNT(*) FROM pragma_foreign_key_check".into(),
    ] {
        let count: i64 = db.query_row(&query, [], |r| r.get(0))?;
        if count != 0 {
            return Err(Error::InvalidSchema);
        }
    }
    Ok(())
}
fn complete(db: &Connection, root: &str) -> Result<bool, Error> {
    Ok(db
        .query_row(
            &format!("SELECT 1 FROM volume_metadata vm WHERE vm.root=?1 AND {COMPLETE}"),
            [root],
            |r| r.get::<_, i64>(0),
        )
        .optional()?
        .is_some())
}
fn members(db: &Connection, root: &str) -> Result<Roots, Error> {
    Ok(db
        .prepare("SELECT cid FROM volume_entries WHERE root=?1")?
        .query_map([root], |r| r.get(0))?
        .collect::<Result<Roots, _>>()?)
}
fn load_volume(db: &Connection, root: &str) -> Result<Option<SerializedVolume>, Error> {
    let sql = format!(
        "SELECT ve.cid,cd.data FROM volume_metadata vm JOIN volume_entries ve ON ve.root=vm.root JOIN cas_data cd ON cd.cid=ve.cid WHERE vm.root=?1 AND {COMPLETE}"
    );
    let entries: Entries = db
        .prepare(&sql)?
        .query_map([root], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<Result<_, _>>()?;
    if entries.is_empty() || entries.iter().any(|(c, d)| validate_entry(c, d).is_err()) {
        return Ok(None);
    }
    Ok(Some(SerializedVolume::new(root, entries)))
}
impl VolumeBroker for DiskBroker {
    fn near(&self) -> Option<&Arc<dyn VolumeBroker>> {
        self.near.as_ref()
    }
    fn far(&self) -> Option<&Arc<dyn VolumeBroker>> {
        self.far.as_ref()
    }
    fn has_volume(&self, root: &str) -> bool {
        complete(&self.reader.lock().unwrap(), root).unwrap_or(false)
    }
    fn fetch_volume_local(&self, root: &str) -> Option<SerializedVolume> {
        load_volume(&self.reader.lock().unwrap(), root)
            .ok()
            .flatten()
    }
    fn fetch_data_local(&self, cids: &Roots) -> Entries {
        fn load(db: &Connection, cids: &Roots) -> Result<Entries, Error> {
            let mut found = Entries::new();
            let ordered: Vec<_> = cids.iter().collect();
            for chunk in ordered.chunks(500) {
                let placeholders = vec!["?"; chunk.len()].join(",");
                let sql = format!(
                    "SELECT cd.cid,cd.data FROM cas_data cd WHERE cd.cid IN ({placeholders}) AND EXISTS (SELECT 1 FROM volume_entries owner JOIN volume_metadata vm ON vm.root=owner.root WHERE owner.cid=cd.cid AND {COMPLETE})"
                );
                let mut stmt = db.prepare(&sql)?;
                let rows = stmt.query_map(params_from_iter(chunk.iter()), |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?))
                })?;
                for row in rows {
                    let (cid, data) = row?;
                    if validate_entry(&cid, &data).is_ok() {
                        found.insert(cid, data);
                    }
                }
            }
            Ok(found)
        }
        load(&self.reader.lock().unwrap(), cids).unwrap_or_default()
    }
    fn store_volumes_local(&self, volumes: &[SerializedVolume]) -> Result<(), Error> {
        let (memberships, content) = validate_batch(volumes)?;
        if memberships.is_empty() {
            return Ok(());
        }
        let mut db = self.connection.lock().unwrap();
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for (root, expected) in &memberships {
            let count: Option<i64> = tx
                .query_row(
                    "SELECT entry_count FROM volume_metadata WHERE root=?1",
                    [root],
                    |r| r.get(0),
                )
                .optional()?;
            let existing = members(&tx, root)?;
            if count.is_some_and(|n| n != expected.len() as i64 || existing != *expected)
                || count.is_none() && !existing.is_empty()
            {
                return Err(Error::ConflictingVolume(root.clone()));
            }
        }
        for (cid, data) in &content {
            let old: Option<Vec<u8>> = tx
                .query_row("SELECT data FROM cas_data WHERE cid=?1", [cid], |r| {
                    r.get(0)
                })
                .optional()?;
            if old
                .as_ref()
                .is_some_and(|o| o != data && validate_entry(cid, o).is_ok())
            {
                return Err(Error::ConflictingContent(cid.clone()));
            }
        }
        for (root, entries) in &memberships {
            tx.execute(
                "INSERT OR IGNORE INTO volume_metadata(root,entry_count) VALUES(?1,?2)",
                params![root, entries.len() as i64],
            )?;
        }
        for (cid, data) in &content {
            tx.execute("INSERT INTO cas_data(cid,data) VALUES(?1,?2) ON CONFLICT(cid) DO UPDATE SET data=excluded.data WHERE cas_data.data != excluded.data",params![cid,data])?;
        }
        for (root, entries) in &memberships {
            for cid in entries {
                tx.execute(
                    "INSERT OR IGNORE INTO volume_entries(root,cid) VALUES(?1,?2)",
                    params![root, cid],
                )?;
            }
            if !complete(&tx, root)? {
                return Err(Error::ConflictingVolume(root.clone()));
            }
        }
        tx.commit()?;
        Ok(())
    }
}
impl RetainedRootBroker for DiskBroker {
    fn advance_retained_roots(&self, scope: &str, roots: &Roots) -> Result<(), Error> {
        self.retain(scope, roots, true)
    }
    fn merge_retained_roots(&self, scope: &str, roots: &Roots) -> Result<(), Error> {
        self.retain(scope, roots, false)
    }
    fn retained_roots(&self, scope: &str) -> Result<Roots, Error> {
        if scope.is_empty() || scope.contains('\0') {
            return Ok(Roots::new());
        }
        let db = self.reader.lock().unwrap();
        Ok(db
            .prepare("SELECT root FROM retained_roots WHERE scope=?1 ORDER BY root")?
            .query_map([scope], |r| r.get(0))?
            .collect::<Result<_, _>>()?)
    }
    fn retained_scopes(&self, prefix: &str) -> Result<Vec<String>, Error> {
        if prefix.contains('\0') {
            return Ok(vec![]);
        }
        let db = self.reader.lock().unwrap();
        let sql = "WITH RECURSIVE found(scope) AS (SELECT min(scope) FROM retained_roots WHERE scope >= ?1 UNION ALL SELECT (SELECT min(scope) FROM retained_roots WHERE scope > found.scope) FROM found WHERE scope IS NOT NULL AND substr(CAST(scope AS BLOB),1,length(CAST(?1 AS BLOB)))=CAST(?1 AS BLOB)) SELECT scope FROM found WHERE scope IS NOT NULL AND substr(CAST(scope AS BLOB),1,length(CAST(?1 AS BLOB)))=CAST(?1 AS BLOB)";
        Ok(db
            .prepare(sql)?
            .query_map([prefix], |r| r.get(0))?
            .collect::<Result<_, _>>()?)
    }
    fn release_retained_roots(&self, scope: &str, roots: &Roots) -> Result<usize, Error> {
        validate_retention(scope, roots)?;
        let mut db = self.connection.lock().unwrap();
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for r in roots {
            tx.execute(
                "DELETE FROM retained_roots WHERE scope=?1 AND root=?2",
                params![scope, r],
            )?;
        }
        let mut removed = 0;
        for r in roots {
            let cids = members(&tx, r)?;
            if tx.execute("DELETE FROM volume_metadata WHERE root=?1 AND NOT EXISTS (SELECT 1 FROM retained_roots WHERE root=?1)",[r])?>0{removed+=1;for cid in cids{tx.execute("DELETE FROM cas_data WHERE cid=?1 AND NOT EXISTS (SELECT 1 FROM volume_entries WHERE cid=?1)",[cid])?;}}
        }
        tx.commit()?;
        Ok(removed)
    }
    fn sweep(&self) -> Result<usize, Error> {
        let mut db = self.connection.lock().unwrap();
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let removed = tx.execute(
            "DELETE FROM volume_metadata WHERE root NOT IN (SELECT root FROM retained_roots)",
            [],
        )?;
        tx.execute("DELETE FROM cas_data WHERE NOT EXISTS (SELECT 1 FROM volume_entries ve WHERE ve.cid=cas_data.cid)",[])?;
        tx.commit()?;
        Ok(removed)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn canonical_sql_preserves_quoted_content() {
        assert_eq!(
            canonical_sql("  CREATE  TABLE x (a TEXT CHECK (a = 'a b'))  "),
            "CREATE TABLE x(a TEXT CHECK(a= 'a b'))"
        );
        assert_ne!(
            canonical_sql("CHECK(a='a b')"),
            canonical_sql("CHECK(a='ab')")
        );
    }
}
