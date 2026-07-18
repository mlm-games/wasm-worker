use crate::sync::guard::Guard;
use crate::sync::spinlock::Spinlock;
use std::future::Future;
use std::marker::Unpin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;
#[cfg(target_arch = "wasm32")]
use web_time::Instant;

#[derive(Debug)]
struct AsyncWaiter {
    id: u64,
    sender: r#continue::Sender<()>,
}

static ASYNC_WAITER_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub struct Condvar {
    waiting_sync_threads: Spinlock<Vec<crate::Thread>>,
    waiting_async_threads: Spinlock<Vec<AsyncWaiter>>,
    waiting_spin_threads: Spinlock<Vec<Arc<AtomicBool>>>,
}

impl Condvar {
    pub const fn new() -> Self {
        Condvar {
            waiting_sync_threads: Spinlock::new(vec![]),
            waiting_async_threads: Spinlock::new(vec![]),
            waiting_spin_threads: Spinlock::new(vec![]),
        }
    }

    pub fn notify_one(&self) {
        let thread = self.waiting_spin_threads.with_mut(|threads| threads.pop());
        if let Some(thread) = thread {
            thread.store(true, Ordering::Release);
            return;
        }
        let thread = self.waiting_sync_threads.with_mut(|threads| threads.pop());
        if let Some(thread) = thread {
            thread.unpark();
            return;
        }
        let waiter = self.waiting_async_threads.with_mut(|waiters| waiters.pop());
        if let Some(waiter) = waiter {
            waiter.sender.send(());
        }
    }

    pub fn notify_all(&self) {
        let threads = self.waiting_spin_threads.with_mut(std::mem::take);
        for thread in threads {
            thread.store(true, Ordering::Release);
        }
        let threads = self.waiting_sync_threads.with_mut(std::mem::take);
        for thread in threads {
            thread.unpark();
        }
        let waiters = self.waiting_async_threads.with_mut(std::mem::take);
        for waiter in waiters {
            waiter.sender.send(());
        }
    }

    pub fn wait_sync<'a, T>(&self, guard: Guard<'a, T>) -> Guard<'a, T> {
        #[cfg(not(target_arch = "wasm32"))]
        { self.wait_block(guard) }
        #[cfg(target_arch = "wasm32")]
        {
            if crate::sync::atomics_wait_supported() {
                self.wait_block(guard)
            } else {
                self.wait_spin(guard)
            }
        }
    }

    pub fn wait_sync_while<'a, T, F>(
        &self,
        mut guard: Guard<'a, T>,
        mut condition: F,
    ) -> Guard<'a, T>
    where
        F: FnMut(&mut T) -> bool,
    {
        #[cfg(not(target_arch = "wasm32"))]
        {
            while condition(&mut guard) {
                guard = self.wait_block(guard);
            }
            guard
        }
        #[cfg(target_arch = "wasm32")]
        {
            if crate::sync::atomics_wait_supported() {
                while condition(&mut guard) {
                    guard = self.wait_block(guard);
                }
                guard
            } else {
                while condition(&mut guard) {
                    guard = self.wait_spin(guard);
                }
                guard
            }
        }
    }

    pub fn wait_sync_timeout<'a, T>(
        &self,
        guard: Guard<'a, T>,
        deadline: Instant,
    ) -> (Guard<'a, T>, WaitTimeoutResult) {
        #[cfg(not(target_arch = "wasm32"))]
        { self.wait_block_timeout(guard, deadline) }
        #[cfg(target_arch = "wasm32")]
        {
            if crate::sync::atomics_wait_supported() {
                self.wait_block_timeout(guard, deadline)
            } else {
                self.wait_spin_timeout(guard, deadline)
            }
        }
    }

    pub fn wait_sync_timeout_while<'a, T, F>(
        &self,
        mut guard: Guard<'a, T>,
        deadline: Instant,
        mut condition: F,
    ) -> (Guard<'a, T>, WaitTimeoutResult)
    where
        F: FnMut(&mut T) -> bool,
    {
        #[cfg(not(target_arch = "wasm32"))]
        {
            while condition(&mut guard) {
                let result;
                (guard, result) = self.wait_block_timeout(guard, deadline);
                if result.timed_out() {
                    return (guard, result);
                }
            }
            (guard, WaitTimeoutResult(false))
        }
        #[cfg(target_arch = "wasm32")]
        {
            if crate::sync::atomics_wait_supported() {
                while condition(&mut guard) {
                    let result;
                    (guard, result) = self.wait_block_timeout(guard, deadline);
                    if result.timed_out() {
                        return (guard, result);
                    }
                }
                (guard, WaitTimeoutResult(false))
            } else {
                while condition(&mut guard) {
                    let result;
                    (guard, result) = self.wait_spin_timeout(guard, deadline);
                    if result.timed_out() {
                        return (guard, result);
                    }
                }
                (guard, WaitTimeoutResult(false))
            }
        }
    }

    pub fn wait_block<'a, T>(&self, guard: Guard<'a, T>) -> Guard<'a, T> {
        let mutex = guard.mutex;
        self.waiting_sync_threads.with_mut(|threads| {
            threads.push(crate::current());
        });
        drop(guard);
        crate::park();
        mutex.lock_sync()
    }

    pub fn wait_block_while<'a, T, F>(
        &self,
        mut guard: Guard<'a, T>,
        mut condition: F,
    ) -> Guard<'a, T>
    where
        F: FnMut(&mut T) -> bool,
    {
        while condition(&mut guard) {
            guard = self.wait_block(guard);
        }
        guard
    }

    pub fn wait_block_timeout<'a, T>(
        &self,
        guard: Guard<'a, T>,
        deadline: Instant,
    ) -> (Guard<'a, T>, WaitTimeoutResult) {
        let mutex = guard.mutex;
        self.waiting_sync_threads.with_mut(|threads| {
            threads.push(crate::current());
        });
        drop(guard);

        loop {
            let now = Instant::now();
            if now >= deadline {
                let notified = self.waiting_sync_threads.with_mut(|threads| {
                    let current = crate::current();
                    if let Some(pos) = threads.iter().position(|x| x.id() == current.id()) {
                        threads.remove(pos);
                        false
                    } else {
                        true
                    }
                });
                return if notified {
                    (mutex.lock_sync(), WaitTimeoutResult(false))
                } else {
                    (mutex.lock_sync(), WaitTimeoutResult(true))
                };
            }

            let timeout = deadline - now;
            crate::park_timeout(timeout);

            let notified = self.waiting_sync_threads.with_mut(|threads| {
                let current = crate::current();
                if threads.iter().any(|x| x.id() == current.id()) {
                    false
                } else {
                    true
                }
            });

            if notified {
                return (mutex.lock_sync(), WaitTimeoutResult(false));
            }
        }
    }

    pub fn wait_block_timeout_while<'a, T, F>(
        &self,
        mut guard: Guard<'a, T>,
        deadline: Instant,
        mut condition: F,
    ) -> (Guard<'a, T>, WaitTimeoutResult)
    where
        F: FnMut(&mut T) -> bool,
    {
        while condition(&mut guard) {
            let result;
            (guard, result) = self.wait_block_timeout(guard, deadline);
            if result.timed_out() {
                return (guard, result);
            }
        }
        (guard, WaitTimeoutResult(false))
    }

    pub fn wait_spin<'a, T>(&self, guard: Guard<'a, T>) -> Guard<'a, T> {
        let wake = Arc::new(AtomicBool::new(false));
        let mutex = guard.mutex;
        self.waiting_spin_threads.with_mut(|e| e.push(wake.clone()));
        drop(guard);
        while !wake.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
        mutex.lock_sync()
    }

    pub fn wait_spin_while<'a, T, F>(
        &self,
        mut guard: Guard<'a, T>,
        mut condition: F,
    ) -> Guard<'a, T>
    where
        F: FnMut(&mut T) -> bool,
    {
        while condition(&mut guard) {
            guard = self.wait_spin(guard);
        }
        guard
    }

    pub fn wait_spin_timeout<'a, T>(
        &self,
        guard: Guard<'a, T>,
        deadline: Instant,
    ) -> (Guard<'a, T>, WaitTimeoutResult) {
        let wake = Arc::new(AtomicBool::new(false));
        let mutex = guard.mutex;
        self.waiting_spin_threads.with_mut(|e| e.push(wake.clone()));
        drop(guard);

        loop {
            if wake.load(Ordering::Acquire) {
                return (mutex.lock_sync(), WaitTimeoutResult(false));
            }
            if Instant::now() >= deadline {
                let notified = self.waiting_spin_threads.with_mut(|threads| {
                    if let Some(pos) = threads.iter().position(|x| Arc::ptr_eq(x, &wake)) {
                        threads.remove(pos);
                        false
                    } else {
                        true
                    }
                });
                if notified {
                    while !wake.load(Ordering::Acquire) {
                        std::hint::spin_loop();
                    }
                    return (mutex.lock_sync(), WaitTimeoutResult(false));
                } else {
                    return (mutex.lock_sync(), WaitTimeoutResult(true));
                }
            }
            std::hint::spin_loop();
        }
    }

    pub fn wait_spin_timeout_while<'a, T, F>(
        &self,
        mut guard: Guard<'a, T>,
        deadline: Instant,
        mut condition: F,
    ) -> (Guard<'a, T>, WaitTimeoutResult)
    where
        F: FnMut(&mut T) -> bool,
    {
        while condition(&mut guard) {
            let result;
            (guard, result) = self.wait_spin_timeout(guard, deadline);
            if result.timed_out() {
                return (guard, result);
            }
        }
        (guard, WaitTimeoutResult(false))
    }

    pub async fn wait_async<'a, T>(&self, guard: Guard<'a, T>) -> Guard<'a, T> {
        let mutex = guard.mutex;
        let receiver = self.waiting_async_threads.with_mut(|waiters| {
            let (sender, receiver) = r#continue::continuation();
            let id = ASYNC_WAITER_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
            waiters.push(AsyncWaiter { id, sender });
            receiver
        });
        drop(guard);
        receiver.await;
        mutex.lock_async().await
    }

    pub async fn wait_async_while<'a, T, F>(
        &self,
        mut guard: Guard<'a, T>,
        mut condition: F,
    ) -> Guard<'a, T>
    where
        F: FnMut(&mut T) -> bool,
    {
        while condition(&mut guard) {
            guard = self.wait_async(guard).await;
        }
        guard
    }

    pub async fn wait_async_timeout<'a, T>(
        &self,
        guard: Guard<'a, T>,
        deadline: Instant,
    ) -> (Guard<'a, T>, WaitTimeoutResult) {
        let mutex = guard.mutex;
        let waiter_id = ASYNC_WAITER_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        let (notify_sender, notify_receiver) = r#continue::continuation();
        let (timeout_sender, timeout_receiver) = r#continue::continuation();

        self.waiting_async_threads.with_mut(|waiters| {
            waiters.push(AsyncWaiter {
                id: waiter_id,
                sender: notify_sender,
            });
        });

        crate::spawn(move || {
            let now = Instant::now();
            if deadline > now {
                let duration = deadline - now;
                crate::sleep(duration);
            }
            timeout_sender.send(());
        });

        drop(guard);

        struct Race<F1, F2> {
            notify: Option<F1>,
            timeout: Option<F2>,
        }

        impl<F1: Future + Unpin, F2: Future + Unpin>
            Future for Race<F1, F2>
        {
            type Output = bool;

            fn poll(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Self::Output> {
                if let Some(ref mut notify) = self.notify {
                    if std::pin::Pin::new(notify).poll(cx).is_ready() {
                        self.notify = None;
                        return std::task::Poll::Ready(false);
                    }
                }
                if let Some(ref mut timeout) = self.timeout {
                    if std::pin::Pin::new(timeout).poll(cx).is_ready() {
                        self.timeout = None;
                        return std::task::Poll::Ready(true);
                    }
                }
                std::task::Poll::Pending
            }
        }

        let timed_out = Race {
            notify: Some(notify_receiver),
            timeout: Some(timeout_receiver),
        }
        .await;

        if timed_out {
            self.waiting_async_threads.with_mut(|waiters| {
                if let Some(pos) = waiters.iter().position(|w| w.id == waiter_id) {
                    let waiter = waiters.remove(pos);
                    waiter.sender.send(());
                }
            });
        }

        let guard = mutex.lock_async().await;
        (guard, WaitTimeoutResult(timed_out))
    }

    pub async fn wait_async_timeout_while<'a, T, F>(
        &self,
        mut guard: Guard<'a, T>,
        deadline: Instant,
        mut condition: F,
    ) -> (Guard<'a, T>, WaitTimeoutResult)
    where
        F: FnMut(&mut T) -> bool,
    {
        while condition(&mut guard) {
            let result;
            (guard, result) = self.wait_async_timeout(guard, deadline).await;
            if result.timed_out() {
                return (guard, result);
            }
        }
        (guard, WaitTimeoutResult(false))
    }
}

#[derive(Debug, PartialEq, Eq, Copy, Clone, Hash, Default)]
pub struct WaitTimeoutResult(bool);

impl WaitTimeoutResult {
    pub fn timed_out(&self) -> bool {
        self.0
    }
}

impl Default for Condvar {
    fn default() -> Self {
        Condvar::new()
    }
}

unsafe impl Send for Condvar {}
unsafe impl Sync for Condvar {}
