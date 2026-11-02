use super::*;
use std::sync::RwLock;
#[derive(Default)]
struct State {
    content: Entries,
    members: BTreeMap<String, Roots>,
    owners: BTreeMap<String, Roots>,
    retained: BTreeMap<String, Roots>,
}
impl State {
    fn complete(&self, root: &str) -> bool {
        self.members.get(root).is_some_and(|m| {
            m.contains(root)
                && m.iter().all(|c| {
                    self.content.contains_key(c)
                        && self.owners.get(c).is_some_and(|o| o.contains(root))
                })
        })
    }
    fn remove(&mut self, root: &str) -> bool {
        if let Some(members) = self.members.remove(root) {
            for c in members {
                if let Some(owners) = self.owners.get_mut(&c) {
                    owners.remove(root);
                    if owners.is_empty() {
                        self.owners.remove(&c);
                        self.content.remove(&c);
                    }
                }
            }
            true
        } else {
            false
        }
    }
    fn retained(&self, root: &str) -> bool {
        self.retained.values().any(|r| r.contains(root))
    }
    fn validate_roots(&self, roots: &Roots) -> Result<(), Error> {
        for r in roots {
            if !self.complete(r) {
                return Err(Error::MissingRetainedRoot(r.clone()));
            }
        }
        Ok(())
    }
}
#[derive(Default)]
pub struct MemoryBroker {
    near: Option<Arc<dyn VolumeBroker>>,
    far: Option<Arc<dyn VolumeBroker>>,
    state: RwLock<State>,
}
impl MemoryBroker {
    pub fn new(near: Option<Arc<dyn VolumeBroker>>, far: Option<Arc<dyn VolumeBroker>>) -> Self {
        Self {
            near,
            far,
            state: RwLock::default(),
        }
    }
}
impl VolumeBroker for MemoryBroker {
    fn near(&self) -> Option<&Arc<dyn VolumeBroker>> {
        self.near.as_ref()
    }
    fn far(&self) -> Option<&Arc<dyn VolumeBroker>> {
        self.far.as_ref()
    }
    fn has_volume(&self, root: &str) -> bool {
        self.state.read().unwrap().complete(root)
    }
    fn fetch_volume_local(&self, root: &str) -> Option<SerializedVolume> {
        let s = self.state.read().unwrap();
        if !s.complete(root) {
            return None;
        }
        Some(SerializedVolume::new(
            root,
            s.members[root]
                .iter()
                .map(|c| (c.clone(), s.content[c].clone()))
                .collect(),
        ))
    }
    fn fetch_data_local(&self, cids: &Roots) -> Entries {
        let s = self.state.read().unwrap();
        cids.iter()
            .filter_map(|c| {
                s.owners
                    .get(c)
                    .filter(|o| !o.is_empty())
                    .and_then(|_| s.content.get(c))
                    .map(|d| (c.clone(), d.clone()))
            })
            .collect()
    }
    fn store_volumes_local(&self, volumes: &[SerializedVolume]) -> Result<(), Error> {
        let (members, content) = validate_batch(volumes)?;
        let mut s = self.state.write().unwrap();
        for (r, m) in &members {
            if s.members.get(r).is_some_and(|old| old != m) {
                return Err(Error::ConflictingVolume(r.clone()));
            }
        }
        for (c, d) in &content {
            if s.content.get(c).is_some_and(|old| old != d) {
                return Err(Error::ConflictingContent(c.clone()));
            }
        }
        for (r, m) in members {
            for c in &m {
                s.owners.entry(c.clone()).or_default().insert(r.clone());
            }
            s.members.insert(r, m);
        }
        s.content.extend(content);
        Ok(())
    }
}
impl RetainedRootBroker for MemoryBroker {
    fn advance_retained_roots(&self, scope: &str, roots: &Roots) -> Result<(), Error> {
        validate_retention(scope, roots)?;
        let mut s = self.state.write().unwrap();
        s.validate_roots(roots)?;
        if roots.is_empty() {
            s.retained.remove(scope);
        } else {
            s.retained.insert(scope.into(), roots.clone());
        }
        Ok(())
    }
    fn merge_retained_roots(&self, scope: &str, roots: &Roots) -> Result<(), Error> {
        validate_retention(scope, roots)?;
        let mut s = self.state.write().unwrap();
        s.validate_roots(roots)?;
        if !roots.is_empty() {
            s.retained
                .entry(scope.into())
                .or_default()
                .extend(roots.iter().cloned());
        }
        Ok(())
    }
    fn retained_roots(&self, scope: &str) -> Result<Roots, Error> {
        Ok(self
            .state
            .read()
            .unwrap()
            .retained
            .get(scope)
            .cloned()
            .unwrap_or_default())
    }
    fn retained_scopes(&self, prefix: &str) -> Result<Vec<String>, Error> {
        Ok(self
            .state
            .read()
            .unwrap()
            .retained
            .keys()
            .filter(|s| s.starts_with(prefix))
            .cloned()
            .collect())
    }
    fn release_retained_roots(&self, scope: &str, roots: &Roots) -> Result<usize, Error> {
        validate_retention(scope, roots)?;
        let mut s = self.state.write().unwrap();
        if let Some(held) = s.retained.get_mut(scope) {
            held.retain(|r| !roots.contains(r));
            if held.is_empty() {
                s.retained.remove(scope);
            }
        }
        let dead: Vec<_> = roots.iter().filter(|r| !s.retained(r)).cloned().collect();
        Ok(dead.iter().filter(|r| s.remove(r)).count())
    }
    fn sweep(&self) -> Result<usize, Error> {
        let mut s = self.state.write().unwrap();
        let dead: Vec<_> = s
            .members
            .keys()
            .filter(|r| !s.retained(r))
            .cloned()
            .collect();
        for r in &dead {
            s.remove(r);
        }
        Ok(dead.len())
    }
}
