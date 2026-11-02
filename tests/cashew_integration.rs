use lattice_cashew::Header;
use lattice_volume_broker::*;
use std::sync::Arc;
#[test]
fn typed_headers_resolve_through_real_disk_and_memory_tiers() {
    let temp = tempfile::tempdir().unwrap();
    let disk = Arc::new(DiskBroker::open(temp.path().join("db")).unwrap());
    let key = [7; 32];
    let header = Header::encrypted_with_nonce("typed payload".to_owned(), &key, &[8; 12]).unwrap();
    let volume = SerializedVolume::new(
        header.raw_cid.clone(),
        Entries::from([(header.raw_cid.clone(), header.bytes(Some(&key)).unwrap())]),
    );
    disk.store(&volume).unwrap();
    disk.advance_retained_roots("typed", &Roots::from([volume.root.clone()]))
        .unwrap();
    let tier = MemoryBroker::new(Some(disk), None);
    let stripped = header.removing_node();
    assert!(stripped.resolve(&tier, Some(&[9; 32])).is_err());
    assert_eq!(
        stripped
            .resolve(&tier, Some(&key))
            .unwrap()
            .node
            .as_deref()
            .unwrap(),
        "typed payload"
    );
}
#[tokio::test(flavor = "current_thread")]
async fn asynchronous_broker_integrates_with_verified_cashew_source() {
    let temp = tempfile::tempdir().unwrap();
    let disk = Arc::new(DiskBroker::open(temp.path().join("db")).unwrap());
    let broker = AsyncBroker::new(disk);
    let header = Header::new("async payload".to_owned()).unwrap();
    let cid = header.raw_cid.clone();
    broker
        .store_volumes(vec![SerializedVolume::new(
            cid.clone(),
            Entries::from([(cid.clone(), header.bytes(None).unwrap())]),
        )])
        .await
        .unwrap();
    broker
        .advance_retained_roots("async".into(), Roots::from([cid.clone()]))
        .await
        .unwrap();
    let resolved = header
        .removing_node()
        .resolve_async(&broker, None)
        .await
        .unwrap();
    assert_eq!(resolved.node.as_deref().unwrap(), "async payload");
    assert_eq!(
        broker
            .release_retained_roots("async".into(), Roots::from([cid.clone()]))
            .await
            .unwrap(),
        1
    );
    assert!(!broker.has_volume(cid).await.unwrap());
}
#[derive(serde::Serialize, serde::Deserialize)]
struct Folder {
    ordinary: Header<String>,
    nested: lattice_cashew::Volume<String>,
}
impl lattice_cashew::DagNode for Folder {
    fn links(&self) -> Vec<&dyn lattice_cashew::DagHeader> {
        vec![&self.ordinary, &self.nested]
    }
}
#[tokio::test(flavor = "current_thread")]
async fn recursive_typed_volumes_keep_their_own_retention_boundaries() {
    use lattice_cashew::{Volume, volume::serialize_recursive};
    let ordinary = Header::new("ordinary".to_owned()).unwrap();
    let nested = Volume::new("nested".to_owned()).unwrap();
    let nested_header = nested.0.removing_node();
    let outer = Volume::new(Folder {
        ordinary: ordinary.clone(),
        nested,
    })
    .unwrap();
    let root = outer.0.raw_cid.clone();
    let batch = serialize_recursive(&outer, &()).unwrap();
    assert_eq!(batch.len(), 2);
    let dir = tempfile::tempdir().unwrap();
    let disk = Arc::new(DiskBroker::open(dir.path().join("db")).unwrap());
    let broker = AsyncBroker::new(disk.clone());
    broker.store_volumes(batch).await.unwrap();
    broker
        .advance_retained_roots("outer".into(), Roots::from([root.clone()]))
        .await
        .unwrap();
    assert_eq!(broker.sweep().await.unwrap(), 1);
    let folder = outer
        .0
        .removing_node()
        .resolve_async(&broker, None)
        .await
        .unwrap();
    let held = folder.node.as_deref().unwrap();
    assert_eq!(
        held.ordinary
            .resolve_async(&broker, None)
            .await
            .unwrap()
            .node
            .as_deref()
            .unwrap(),
        "ordinary"
    );
    assert!(nested_header.resolve_async(&broker, None).await.is_err());
    assert!(broker.has_volume(root).await.unwrap());
}
