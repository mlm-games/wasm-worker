use crate::sync::guard::{ReadGuard, WriteGuard};
use crate::sync::spinlock::Spinlock;
use std::cell::UnsafeCell;
use std::sync::atomic::AtomicU8;

pub(crate) const UNLOCKED: u8 = 0;
pub(crate) const LOCKED_WRITE: u8 = 0b10000000;

/// A reader-writer lock that works across native and WebAssembly.
///
/// Allows multiple concurrent readers or one exclusive writer.
/// Adapts locking strategy to the platform (blocks on native/wasm workers,
/// spins on wasm main thread).
#[derive(Debug, Default)]
pub struct RwLock<T> {
    pub(crate) inner: UnsafeCell<T>,
    pub(crate) data_lock: AtomicU8,
    pub(crate) waiting_sync_read_threads: Spinlock<Vec<crate::Thread>>,
    pub(crate) waiting_sync_write_threads: Spinlock<Vec<crate::Thread>>,
    pub(crate) waiting_async_read_threads: Spinlock<Vec<r#continue::Sender<()>>>,
    pub(crate) waiting_async_write_threads: Spinlock<Vec<r#continue::Sender<()>>>,
}

impl<T> RwLock<T> {
    /// Creates a new read-write lock with the given value.
    pub const fn new(value: T) -> RwLock<T> {
        RwLock {
            inner: UnsafeCell::new(value),
            data_lock: AtomicU8::new(UNLOCKED),
            waiting_sync_read_threads: Spinlock::new(vec![]),
            waiting_async_read_threads: Spinlock::new(vec![]),
            waiting_sync_write_threads: Spinlock::new(vec![]),
            waiting_async_write_threads: Spinlock::new(vec![]),
        }
    }

    pub(crate) fn did_unlock_write(&self) {
        let threads = self.waiting_sync_read_threads.with_mut(std::mem::take);
        for thread in threads {
            thread.unpark();
        }
        let senders = self.waiting_async_read_threads.with_mut(std::mem::take);
        for sender in senders {
            sender.send(());
        }
        let threads = self.waiting_sync_write_threads.with_mut(std::mem::take);
        for thread in threads {
            thread.unpark();
        }
        let senders = self.waiting_async_write_threads.with_mut(std::mem::take);
        for sender in senders {
            sender.send(());
        }
    }

    pub(crate) fn did_unlock_read(&self) {
        let threads = self.waiting_sync_write_threads.with_mut(std::mem::take);
        for thread in threads {
            thread.unpark();
        }
        let senders = self.waiting_async_write_threads.with_mut(std::mem::take);
        for sender in senders {
            sender.send(());
        }
    }

    fn try_lock_write_internal(&self) -> bool {
        self.data_lock
            .compare_exchange(
                UNLOCKED,
                LOCKED_WRITE,
                std::sync::atomic::Ordering::Acquire,
                std::sync::atomic::Ordering::Relaxed,
            )
            .is_ok()
    }

    fn try_lock_read_internal(&self) -> bool {
        loop {
            let current = self.data_lock.load(std::sync::atomic::Ordering::Relaxed);
            if current == LOCKED_WRITE {
                return false;
            }
            if self
                .data_lock
                .compare_exchange_weak(
                    current,
                    current + 1,
                    std::sync::atomic::Ordering::Acquire,
                    std::sync::atomic::Ordering::Relaxed,
                )
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Attempts to acquire a read lock without blocking.
    pub fn try_lock_read(&self) -> Result<ReadGuard<'_, T>, crate::sync::NotAvailable> {
        if self.try_lock_read_internal() {
            Ok(ReadGuard { rwlock: self })
        } else {
            Err(crate::sync::NotAvailable)
        }
    }

    /// Attempts to acquire a write lock without blocking.
    pub fn try_lock_write(&self) -> Result<WriteGuard<'_, T>, crate::sync::NotAvailable> {
        if self.try_lock_write_internal() {
            Ok(WriteGuard { rwlock: self })
        } else {
            Err(crate::sync::NotAvailable)
        }
    }

    /// Acquires a read lock by spinning until available.
    pub fn lock_spin_read(&self) -> ReadGuard<'_, T> {
        while !self.try_lock_read_internal() {
            std::hint::spin_loop();
        }
        ReadGuard { rwlock: self }
    }

    /// Acquires a write lock by spinning until available.
    pub fn lock_spin_write(&self) -> WriteGuard<'_, T> {
        while !self.try_lock_write_internal() {
            std::hint::spin_loop();
        }
        WriteGuard { rwlock: self }
    }

    /// Acquires a read lock by blocking via thread parking.
    pub fn lock_block_read(&self) -> ReadGuard<'_, T> {
        loop {
            if self.try_lock_read_internal() {
                return ReadGuard { rwlock: self };
            }
            self.waiting_sync_read_threads.with_mut(|threads| {
                threads.push(crate::current());
            });
            crate::park();
        }
    }

    /// Acquires a write lock by blocking via thread parking.
    pub fn lock_block_write(&self) -> WriteGuard<'_, T> {
        loop {
            if self.try_lock_write_internal() {
                return WriteGuard { rwlock: self };
            }
            self.waiting_sync_write_threads.with_mut(|threads| {
                threads.push(crate::current());
            });
            crate::park();
        }
    }

    /// Asynchronously acquires a read lock.
    pub async fn lock_async_read(&self) -> ReadGuard<'_, T> {
        loop {
            if self.try_lock_read_internal() {
                return ReadGuard { rwlock: self };
            }
            let receiver = self.waiting_async_read_threads.with_mut(|senders| {
                let (sender, receiver) = r#continue::continuation();
                senders.push(sender);
                receiver
            });
            receiver.await;
        }
    }

    /// Asynchronously acquires a write lock.
    pub async fn lock_async_write(&self) -> WriteGuard<'_, T> {
        loop {
            if self.try_lock_write_internal() {
                return WriteGuard { rwlock: self };
            }
            let receiver = self.waiting_async_write_threads.with_mut(|senders| {
                let (sender, receiver) = r#continue::continuation();
                senders.push(sender);
                receiver
            });
            receiver.await;
        }
    }

    /// Acquires a read lock using the best strategy for the platform.
    pub fn lock_sync_read(&self) -> ReadGuard<'_, T> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.lock_block_read()
        }
        #[cfg(target_arch = "wasm32")]
        {
            if crate::sync::atomics_wait_supported() {
                self.lock_block_read()
            } else {
                self.lock_spin_read()
            }
        }
    }

    /// Acquires a write lock using the best strategy for the platform.
    pub fn lock_sync_write(&self) -> WriteGuard<'_, T> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.lock_block_write()
        }
        #[cfg(target_arch = "wasm32")]
        {
            if crate::sync::atomics_wait_supported() {
                self.lock_block_write()
            } else {
                self.lock_spin_write()
            }
        }
    }

    /// Acquires a read lock, calls `f`, then releases.
    pub fn with_sync<R, F: FnOnce(&T) -> R>(&self, f: F) -> R {
        let guard = self.lock_sync_read();
        f(&guard)
    }

    /// Acquires a write lock, calls `f`, then releases.
    pub fn with_mut_sync<R, F: FnOnce(&mut T) -> R>(&self, f: F) -> R {
        let mut guard = self.lock_sync_write();
        f(&mut guard)
    }

    /// Asynchronously acquires a read lock, calls `f`, then releases.
    pub async fn with_async<R, F: FnOnce(&T) -> R>(&self, f: F) -> R {
        let guard = self.lock_async_read().await;
        f(&guard)
    }

    /// Asynchronously acquires a write lock, calls `f`, then releases.
    pub async fn with_mut_async<R, F: FnOnce(&mut T) -> R>(&self, f: F) -> R {
        let mut guard = self.lock_async_write().await;
        f(&mut guard)
    }
}

unsafe impl<T: Send> Send for RwLock<T> {}
unsafe impl<T: Send> Sync for RwLock<T> {}

impl<T: std::fmt::Display> std::fmt::Display for RwLock<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.try_lock_read() {
            Ok(guard) => std::fmt::Display::fmt(&*guard, f),
            Err(_) => write!(f, "RwLock {{ <locked> }}"),
        }
    }
}

impl<T> From<T> for RwLock<T> {
    fn from(value: T) -> Self {
        RwLock::new(value)
    }
}
