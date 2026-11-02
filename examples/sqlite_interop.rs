use lattice_cashew::cid_for_bytes;
use lattice_volume_broker::*;
fn main() {
    let path = std::env::args().nth(1).expect("database path");
    let broker = DiskBroker::open(path).unwrap();
    let hello = cid_for_bytes(0x55, b"hello");
    assert_eq!(broker.fetch(&hello).unwrap(), b"hello");
    assert_eq!(
        broker.retained_roots("swift").unwrap(),
        Roots::from([hello])
    );
    let world = cid_for_bytes(0x55, b"world");
    broker
        .store(&SerializedVolume::new(
            world.clone(),
            Entries::from([(world.clone(), b"world".to_vec())]),
        ))
        .unwrap();
    broker
        .advance_retained_roots("rust", &Roots::from([world]))
        .unwrap();
    broker.checkpoint().unwrap();
    println!("Rust opened Swift schema v2, verified retained content, and committed a new volume");
}
