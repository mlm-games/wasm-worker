#![allow(missing_docs)]

use crate::sync::guard::Guard;
use crate::sync::spinlock::Spinlock;
use std::cell::UnsafeCell;
use std::future::Future;
use std::marker::Unpin;
use std::sync::atomic::AtomicBool;

#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;
#[cfg(target_arch = "wasm32")]
use web_time::Instant;

#[derive(Debug, Copy, Clone)]
pub struct NotAvailable;

#[derive(Debug)]
pub struct Mutex<T> {
    pub(crate) inner: UnsafeCell<T>,
    pub(crate) data_lock: AtomicBool,
    pub(crate) waiting_sync_threads: Spinlock<Vec<crate::Thread>>,
    pub(crate) waiting_async_threads: Spinlock<Vec<r#continue::Sender<()>>>,
}

impl<T> Mutex<T> {
    pub const fn new(value: T) -> Self {
        Mutex {
            inner: UnsafeCell::new(value),
            data_lock: AtomicBool::new(false),
            waiting_sync_threads: Spinlock::new(vec![]),
            waiting_async_threads: Spinlock::new(vec![]),
        }
    }

    pub fn try_lock(&self) -> Result<Guard<'_, T>, NotAvailable> {
        if self
            .data_lock
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::Acquire,
                std::sync::atomic::Ordering::Relaxed,
            )
            .is_ok()
        {
            let data = unsafe { &mut *self.inner.get() };
            Ok(Guard { mutex: self, data })
        } else {
            Err(NotAvailable)
        }
    }

    pub fn lock_spin(&self) -> Guard<'_, T> {
        while self.data_lock.swap(true, std::sync::atomic::Ordering::Acquire) {
            std::hint::spin_loop();
        }
        let data = unsafe { &mut *self.inner.get() };
        Guard { mutex: self, data }
    }

    pub fn lock_spin_timeout(&self, deadline: Instant) -> Option<Guard<'_, T>> {
        while self.data_lock.swap(true, std::sync::atomic::Ordering::Acquire) {
            if Instant::now() >= deadline {
                return None;
            }
            std::hint::spin_loop();
        }
        let data = unsafe { &mut *self.inner.get() };
        Some(Guard { mutex: self, data })
    }

    pub fn lock_block(&self) -> Guard<'_, T> {
        loop {
            let r = self.waiting_sync_threads.with_mut(|threads| {
                match self.try_lock() {
                    Ok(guard) => Ok(guard),
                    Err(_) => {
                        threads.push(crate::current());
                        Err(NotAvailable)
                    }
                }
            });
            match r {
                Ok(guard) => return guard,
                Err(NotAvailable) => crate::park(),
            }
        }
    }

    pub fn lock_block_timeout(&self, deadline: Instant) -> Option<Guard<'_, T>> {
        loop {
            let now = Instant::now();
            if now >= deadline {
                if let Ok(guard) = self.try_lock() {
                    return Some(guard);
                }
                return None;
            }

            let r = self
                .waiting_sync_threads
                .with_mut(|threads| match self.try_lock() {
                    Ok(guard) => Ok(guard),
                    Err(_) => {
                        threads.push(crate::current());
                        Err(NotAvailable)
                    }
                });

            match r {
                Ok(guard) => return Some(guard),
                Err(NotAvailable) => {
                    let remaining = deadline - Instant::now();
                    crate::park_timeout(remaining);
                }
            }
        }
    }

    pub async fn lock_async(&self) -> Guard<'_, T> {
        loop {
            let a = self.waiting_async_threads.with_mut(|senders| {
                match self.try_lock() {
                    Ok(guard) => Ok(guard),
                    Err(NotAvailable) => {
                        let (sender, receiver) = r#continue::continuation();
                        senders.push(sender);
                        Err(receiver)
                    }
                }
            });
            match a {
                Ok(guard) => return guard,
                Err(receiver) => {
                    receiver.await;
                }
            }
        }
    }

    pub async fn lock_async_timeout(&self, deadline: Instant) -> Option<Guard<'_, T>> {
        loop {
            let now = Instant::now();
            if now >= deadline {
                if let Ok(guard) = self.try_lock() {
                    return Some(guard);
                }
                return None;
            }

            let a = self.waiting_async_threads.with_mut(|senders| {
                match self.try_lock() {
                    Ok(guard) => Ok(guard),
                    Err(NotAvailable) => {
                        let (sender, receiver) = r#continue::continuation();
                        senders.push(sender);
                        Err(receiver)
                    }
                }
            });

            match a {
                Ok(guard) => return Some(guard),
                Err(receiver) => {
                    let (timeout_sender, timeout_receiver) = r#continue::continuation();
                    let deadline_clone = deadline;
                    crate::Builder::new()
                        .name("lock_async_timeout".to_string())
                        .spawn(move || {
                            let now = Instant::now();
                            if deadline_clone > now {
                                let duration = deadline_clone - now;
                                crate::sleep(duration);
                            }
                            timeout_sender.send(());
                        })
                        .expect("Failed to spawn timeout thread");

                    struct Race<F1, F2> {
                        notify: Option<F1>,
                        timeout: Option<F2>,
                    }

                    impl<F1: Future + Unpin, F2: Future + Unpin> Future for Race<F1, F2> {
                        type Output = bool;

                        fn poll(
                            self: std::pin::Pin<&mut Self>,
                            cx: &mut std::task::Context<'_>,
                        ) -> std::task::Poll<Self::Output> {
                            let this = unsafe { self.get_unchecked_mut() };
                            if let Some(ref mut notify) = this.notify {
                                if std::pin::Pin::new(notify).poll(cx).is_ready() {
                                    this.notify = None;
                                    return std::task::Poll::Ready(false);
                                }
                            }
                            if let Some(ref mut timeout) = this.timeout {
                                if std::pin::Pin::new(timeout).poll(cx).is_ready() {
                                    this.timeout = None;
                                    return std::task::Poll::Ready(true);
                                }
                            }
                            std::task::Poll::Pending
                        }
                    }

                    let timed_out = Race {
                        notify: Some(receiver),
                        timeout: Some(timeout_receiver),
                    }
                    .await;

                    if timed_out {
                        return None;
                    }
                }
            }
        }
    }

    pub(crate) fn did_unlock(&self) {
        let threads = self.waiting_sync_threads.with_mut(std::mem::take);
        for thread in threads {
            thread.unpark();
        }
        let senders = self.waiting_async_threads.with_mut(std::mem::take);
        for sender in senders {
            sender.send(());
        }
    }

    pub fn lock_sync(&self) -> Guard<'_, T> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.lock_block()
        }
        #[cfg(target_arch = "wasm32")]
        {
            if crate::sync::atomics_wait_supported() {
                self.lock_block()
            } else {
                self.lock_spin()
            }
        }
    }

    pub fn lock_sync_timeout(&self, deadline: Instant) -> Option<Guard<'_, T>> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.lock_block_timeout(deadline)
        }
        #[cfg(target_arch = "wasm32")]
        {
            if crate::sync::atomics_wait_supported() {
                self.lock_block_timeout(deadline)
            } else {
                self.lock_spin_timeout(deadline)
            }
        }
    }

    pub fn with_sync<R, F: FnOnce(&T) -> R>(&self, f: F) -> R {
        let guard = self.lock_sync();
        f(&guard)
    }

    pub fn with_mut_sync<R, F: FnOnce(&mut T) -> R>(&self, f: F) -> R {
        let mut guard = self.lock_sync();
        f(&mut guard)
    }

    pub async fn with_async<R, F: FnOnce(&T) -> R>(&self, f: F) -> R {
        let guard = self.lock_async().await;
        f(&guard)
    }

    pub async fn with_mut_async<R, F: FnOnce(&mut T) -> R>(&self, f: F) -> R {
        let mut guard = self.lock_async().await;
        f(&mut guard)
    }
}

unsafe impl<T: Send> Send for Mutex<T> {}
unsafe impl<T: Send> Sync for Mutex<T> {}

impl<T: Default> Default for Mutex<T> {
    fn default() -> Self {
        Mutex::new(T::default())
    }
}

impl<T: std::fmt::Display> std::fmt::Display for Mutex<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.try_lock() {
            Ok(guard) => std::fmt::Display::fmt(&*guard, f),
            Err(_) => write!(f, "Mutex {{ <locked> }}"),
        }
    }
}

impl<T> From<T> for Mutex<T> {
    fn from(value: T) -> Self {
        Mutex::new(value)
    }
}
