/// Condition variable for blocking threads while waiting for events.
pub mod condvar;
/// RAII guard types (Guard, ReadGuard, WriteGuard) that release locks on drop.
pub mod guard;
/// Multi-producer, single-consumer channels for message passing.
pub mod mpsc;
/// Mutual exclusion primitive with multiple locking strategies.
pub mod mutex;
/// Reader-writer lock allowing multiple readers or one writer.
pub mod rwlock;
/// Spinlock for short-lived critical sections.
pub mod spinlock;

pub use guard::Guard;
pub use mutex::{Mutex, NotAvailable};
pub use spinlock::Spinlock;

#[cfg(not(target_arch = "wasm32"))]
pub use std::time::Instant;
#[cfg(target_arch = "wasm32")]
pub use web_time::Instant;

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(inline_js = "
export function _web_thread_supportsAtomicsWait() {
    if (typeof SharedArrayBuffer === 'undefined') return false;
    if (typeof Atomics === 'undefined' || typeof Atomics.wait !== 'function') return false;
    try {
        const sab = new SharedArrayBuffer(4);
        const ia = new Int32Array(sab);
        const result = Atomics.wait(ia, 0, 0, 0);
        return result === 'timed-out' || result === 'not-equal';
    } catch (_) {
        return false;
    }
}
")]
extern "C" {
    fn _web_thread_supportsAtomicsWait() -> bool;
}

#[cfg(target_arch = "wasm32")]
pub(crate) fn atomics_wait_supported() -> bool {
    _web_thread_supportsAtomicsWait()
}
