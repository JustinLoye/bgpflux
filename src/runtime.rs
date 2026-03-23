use std::collections::HashSet;
use std::sync::{LazyLock, Mutex};

use std::sync::OnceLock;
use tokio::runtime::Runtime;
use tokio::sync::Semaphore;

// Static collectors string
// Faster for accessing in the BgpElem -> BgpStreamElem conversion in the hot loop
static INTERNED_COLLECTORS: LazyLock<Mutex<HashSet<&'static str>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

pub(crate) fn intern_collector(s: String) -> &'static str {
    let mut cache = INTERNED_COLLECTORS.lock().unwrap();
    if let Some(&existing) = cache.get(s.as_str()) {
        existing
    } else {
        let leaked: &'static str = Box::leak(s.into_boxed_str());
        cache.insert(leaked);
        leaked
    }
}

// Global async runtime
pub(crate) fn global_runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
    })
}

pub(crate) fn download_semaphore() -> &'static Semaphore {
    static SEM: OnceLock<Semaphore> = OnceLock::new();
    // Limit hardcoded to 10 because it's of one of the archive project limit
    SEM.get_or_init(|| Semaphore::new(10))
}
