use super::*;
/// Async facade that runs each broker operation on Tokio's blocking executor.
/// Cancellation of a caller does not cancel an already-running atomic transaction.
pub struct AsyncBroker<B: ?Sized> {
    inner: Arc<B>,
}
impl<B: ?Sized> Clone for AsyncBroker<B> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
impl<B: VolumeBroker + ?Sized + 'static> AsyncBroker<B> {
    pub fn new(inner: Arc<B>) -> Self {
        Self { inner }
    }
    pub fn inner(&self) -> &Arc<B> {
        &self.inner
    }
    async fn run<R: Send + 'static>(
        &self,
        action: impl FnOnce(&B) -> R + Send + 'static,
    ) -> Result<R, Error> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || action(&inner))
            .await
            .map_err(|e| Error::Worker(e.to_string()))
    }
    pub async fn has_volume(&self, root: String) -> Result<bool, Error> {
        self.run(move |b| b.has_volume(&root)).await
    }
    pub async fn fetch_volume(&self, root: String) -> Result<Option<SerializedVolume>, Error> {
        self.run(move |b| b.fetch_volume(&root)).await
    }
    pub async fn fetch_data(&self, cids: Roots) -> Result<Entries, Error> {
        self.run(move |b| b.fetch_data(&cids)).await
    }
    pub async fn fetch_one(&self, cid: String) -> Result<Vec<u8>, Error> {
        self.run(move |b| VolumeBroker::fetch(b, &cid)).await?
    }
    pub async fn store_volumes(&self, volumes: Vec<SerializedVolume>) -> Result<(), Error> {
        self.run(move |b| b.store_volumes_local(&volumes)).await?
    }
}
impl<B: RetainedRootBroker + ?Sized + 'static> AsyncBroker<B> {
    pub async fn advance_retained_roots(&self, scope: String, roots: Roots) -> Result<(), Error> {
        self.run(move |b| b.advance_retained_roots(&scope, &roots))
            .await?
    }
    pub async fn merge_retained_roots(&self, scope: String, roots: Roots) -> Result<(), Error> {
        self.run(move |b| b.merge_retained_roots(&scope, &roots))
            .await?
    }
    pub async fn retained_roots(&self, scope: String) -> Result<Roots, Error> {
        self.run(move |b| b.retained_roots(&scope)).await?
    }
    pub async fn retained_scopes(&self, prefix: String) -> Result<Vec<String>, Error> {
        self.run(move |b| b.retained_scopes(&prefix)).await?
    }
    pub async fn release_retained_roots(
        &self,
        scope: String,
        roots: Roots,
    ) -> Result<usize, Error> {
        self.run(move |b| b.release_retained_roots(&scope, &roots))
            .await?
    }
    pub async fn sweep(&self) -> Result<usize, Error> {
        self.run(|b| b.sweep()).await?
    }
}
#[async_trait::async_trait]
impl<B: VolumeBroker + ?Sized + 'static> lattice_cashew::source::ContentSource for AsyncBroker<B> {
    async fn fetch(&self, cids: Roots) -> Entries {
        self.fetch_data(cids).await.unwrap_or_default()
    }
}
#[async_trait::async_trait]
impl<B: VolumeBroker + ?Sized + 'static> lattice_cashew::source::Fetcher for AsyncBroker<B> {
    async fn fetch_one(&self, cid: &str) -> Result<Vec<u8>, lattice_cashew::Error> {
        AsyncBroker::fetch_one(self, cid.to_owned())
            .await
            .map_err(|e| match e {
                Error::NotFound => lattice_cashew::Error::MissingContent(cid.into()),
                other => lattice_cashew::Error::Source(other.to_string()),
            })
    }
}
#[async_trait::async_trait]
impl<B: VolumeBroker + ?Sized + 'static> lattice_cashew::source::VolumeStorer for AsyncBroker<B> {
    async fn store_volume(&self, volume: SerializedVolume) -> Result<(), lattice_cashew::Error> {
        self.store_volumes(vec![volume])
            .await
            .map_err(|e| lattice_cashew::Error::Storage(e.to_string()))
    }
}
