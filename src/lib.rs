//! Atomic content-addressed volume storage and named root retention.
//! Methods are blocking; async runtimes should call disk operations in a blocking pool.
pub use lattice_cashew::SerializedVolume;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
mod asynchronous;
mod disk;
mod memory;
pub use asynchronous::AsyncBroker;
pub use disk::{DiskBroker, DiskReadSnapshot};
pub use memory::MemoryBroker;
pub type Entries = BTreeMap<String, Vec<u8>>;
pub type Roots = BTreeSet<String>;
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("blocking worker failed: {0}")]
    Worker(String),
    #[error("SQLite error: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("invalid serialized volume: {0}")]
    InvalidVolume(#[from] lattice_cashew::Error),
    #[error("schema version {found} requires migration to {required}")]
    MigrationRequired { found: i64, required: i64 },
    #[error("invalid schema at version 2")]
    InvalidSchema,
    #[error("not found")]
    NotFound,
    #[error("invalid retained roots: {0}")]
    InvalidRetainedRoots(&'static str),
    #[error("missing retained root {0}")]
    MissingRetainedRoot(String),
    #[error("conflicting content {0}")]
    ConflictingContent(String),
    #[error("conflicting volume {0}")]
    ConflictingVolume(String),
    #[error("inconsistent state: {0}")]
    InconsistentState(&'static str),
}
pub trait VolumeBroker: Send + Sync {
    fn near(&self) -> Option<&Arc<dyn VolumeBroker>> {
        None
    }
    fn far(&self) -> Option<&Arc<dyn VolumeBroker>> {
        None
    }
    fn has_volume(&self, root: &str) -> bool;
    fn fetch_volume_local(&self, root: &str) -> Option<SerializedVolume>;
    fn fetch_data_local(&self, cids: &Roots) -> Entries;
    fn store_volumes_local(&self, volumes: &[SerializedVolume]) -> Result<(), Error>;
    fn store(&self, volume: &SerializedVolume) -> Result<(), Error> {
        self.store_volumes_local(std::slice::from_ref(volume))
    }
    fn fetch_volume(&self, root: &str) -> Option<SerializedVolume> {
        self.fetch_volume_local(root)
            .or_else(|| self.near().and_then(|n| n.fetch_volume(root)))
            .or_else(|| self.far().and_then(|f| f.fetch_volume(root)))
    }
    fn fetch_data(&self, cids: &Roots) -> Entries {
        let mut found = self.fetch_data_local(cids);
        for tier in [self.near(), self.far()].into_iter().flatten() {
            let missing = cids
                .iter()
                .filter(|k| !found.contains_key(*k))
                .cloned()
                .collect::<Roots>();
            if !missing.is_empty() {
                for (k, v) in tier.fetch_data(&missing) {
                    found.entry(k).or_insert(v);
                }
            }
        }
        found
    }
    fn fetch(&self, cid: &str) -> Result<Vec<u8>, Error> {
        self.fetch_data(&Roots::from([cid.into()]))
            .remove(cid)
            .ok_or(Error::NotFound)
    }
    fn fetch_many(&self, cids: &Roots) -> Entries {
        self.fetch_data(&cids.iter().filter(|c| !c.is_empty()).cloned().collect())
    }
}
pub trait RetainedRootBroker: VolumeBroker {
    fn advance_retained_roots(&self, scope: &str, roots: &Roots) -> Result<(), Error>;
    fn merge_retained_roots(&self, scope: &str, roots: &Roots) -> Result<(), Error>;
    fn retained_roots(&self, scope: &str) -> Result<Roots, Error>;
    fn retained_scopes(&self, prefix: &str) -> Result<Vec<String>, Error>;
    fn release_retained_roots(&self, scope: &str, roots: &Roots) -> Result<usize, Error>;
    fn sweep(&self) -> Result<usize, Error>;
}
fn validate_retention(scope: &str, roots: &Roots) -> Result<(), Error> {
    if scope.is_empty() || scope.contains('\0') {
        return Err(Error::InvalidRetainedRoots(
            "scope must be nonempty without NUL",
        ));
    }
    if roots.iter().any(|r| r.is_empty() || r.contains('\0')) {
        return Err(Error::InvalidRetainedRoots(
            "roots must be nonempty without NUL",
        ));
    }
    Ok(())
}
fn validate_batch(
    volumes: &[SerializedVolume],
) -> Result<(BTreeMap<String, Roots>, Entries), Error> {
    let mut memberships = BTreeMap::<String, Roots>::new();
    let mut content = Entries::new();
    for v in volumes {
        v.validate()?;
        let keys: Roots = v.entries.keys().cloned().collect();
        if memberships
            .insert(v.root.clone(), keys.clone())
            .is_some_and(|old| old != keys)
        {
            return Err(Error::ConflictingVolume(v.root.clone()));
        }
        for (cid, data) in &v.entries {
            if content
                .insert(cid.clone(), data.clone())
                .is_some_and(|old| old != *data)
            {
                return Err(Error::ConflictingContent(cid.clone()));
            }
        }
    }
    Ok((memberships, content))
}
impl lattice_cashew::ContentSource for MemoryBroker {
    fn fetch(&self, cid: &str) -> Result<Vec<u8>, lattice_cashew::Error> {
        VolumeBroker::fetch(self, cid)
            .map_err(|_| lattice_cashew::Error::MissingContent(cid.into()))
    }
}
impl lattice_cashew::ContentSource for DiskBroker {
    fn fetch(&self, cid: &str) -> Result<Vec<u8>, lattice_cashew::Error> {
        VolumeBroker::fetch(self, cid)
            .map_err(|_| lattice_cashew::Error::MissingContent(cid.into()))
    }
}
