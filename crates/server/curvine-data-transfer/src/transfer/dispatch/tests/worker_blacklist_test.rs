use super::super::WorkerBlacklist;
use std::sync::{mpsc, Arc, Barrier};
use std::time::Duration;

#[test]
fn expired_blacklist_entry_does_not_deadlock() {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let blacklist = WorkerBlacklist::new();
        assert!(!blacklist.is_blocked(1, 100));
        blacklist.block(1, 100);
        assert!(blacklist.is_blocked(1, 100 + WorkerBlacklist::TTL_MS - 1));
        assert!(!blacklist.is_blocked(1, 100 + WorkerBlacklist::TTL_MS));
        assert!(!blacklist.is_blocked(1, 100 + WorkerBlacklist::TTL_MS));
        tx.send(()).unwrap();
    });
    rx.recv_timeout(Duration::from_secs(5))
        .expect("expired blacklist lookup did not complete");
}

#[test]
fn concurrent_expiry_and_reblock_preserve_new_block() {
    let blacklist = Arc::new(WorkerBlacklist::new());
    for _ in 0..1000 {
        blacklist.block(1, 0);
        let barrier = Arc::new(Barrier::new(2));
        std::thread::scope(|scope| {
            let blacklist = &blacklist;
            let barrier2 = barrier.clone();
            scope.spawn(move || {
                barrier2.wait();
                blacklist.is_blocked(1, WorkerBlacklist::TTL_MS);
            });
            barrier.wait();
            blacklist.block(1, WorkerBlacklist::TTL_MS);
        });
        assert!(blacklist.is_blocked(1, WorkerBlacklist::TTL_MS));
    }
}
