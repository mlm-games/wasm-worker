use crate::sync::condvar::Condvar;
use crate::sync::Mutex;
use std::cell::Cell;
use std::collections::VecDeque;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct Shared<T> {
    queue: Mutex<VecDeque<T>>,
    condvar: Condvar,
    sender_count: AtomicUsize,
    receiver_active: AtomicBool,
}

/// The sending half of an mpsc channel.
///
/// Can be cloned to create multiple producers.
pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let old_count = self.shared.sender_count.fetch_sub(1, Ordering::SeqCst);
        if old_count == 1 {
            self.shared.condvar.notify_all();
        }
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.shared.sender_count.fetch_add(1, Ordering::SeqCst);
        Sender {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender").finish()
    }
}

/// The receiving half of an mpsc channel.
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
    _marker: PhantomData<Cell<()>>,
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.shared.receiver_active.store(false, Ordering::SeqCst);
        self.shared.condvar.notify_all();
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver").finish()
    }
}

/// Error returned by `try_recv`.
#[derive(PartialEq, Eq, Clone, Copy, Debug, Hash)]
#[non_exhaustive]
pub enum TryRecvError {
    /// The channel is empty but not disconnected.
    Empty,
    /// The channel is disconnected.
    Disconnected,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryRecvError::Empty => "receiving on an empty channel".fmt(f),
            TryRecvError::Disconnected => "receiving on a closed channel".fmt(f),
        }
    }
}

impl std::error::Error for TryRecvError {}

/// Error returned by `recv_timeout` methods.
#[derive(PartialEq, Eq, Clone, Copy, Debug, Hash)]
#[non_exhaustive]
pub enum RecvTimeoutError {
    /// Operation timed out.
    Timeout,
    /// Channel is disconnected.
    Disconnected,
}

impl fmt::Display for RecvTimeoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecvTimeoutError::Timeout => "timed out waiting on channel".fmt(f),
            RecvTimeoutError::Disconnected => "channel is empty and disconnected".fmt(f),
        }
    }
}

impl std::error::Error for RecvTimeoutError {}

/// Error returned by `recv` when the channel is disconnected.
#[derive(PartialEq, Eq, Clone, Copy, Debug, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum RecvError {
    /// The channel has been disconnected.
    Disconnected,
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        "receiving on a closed channel".fmt(f)
    }
}

impl std::error::Error for RecvError {}

/// Error returned by `send` when the receiver is disconnected.
#[derive(PartialEq, Eq, Clone, Copy)]
pub struct SendError<T>(pub T);

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendError").finish_non_exhaustive()
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        "sending on a closed channel".fmt(f)
    }
}

impl<T> std::error::Error for SendError<T> {}

/// Creates a new mpsc channel, returning the sender and receiver halves.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        queue: Mutex::new(VecDeque::new()),
        condvar: Condvar::new(),
        sender_count: AtomicUsize::new(1),
        receiver_active: AtomicBool::new(true),
    });
    (
        Sender {
            shared: Arc::clone(&shared),
        },
        Receiver {
            shared,
            _marker: PhantomData,
        },
    )
}

impl<T> Sender<T> {
    /// Sends a value, spinning if the lock is contended.
    pub fn send_spin(&self, t: T) -> Result<(), SendError<T>> {
        if !self.shared.receiver_active.load(Ordering::SeqCst) {
            return Err(SendError(t));
        }
        let mut queue = self.shared.queue.lock_spin();
        if !self.shared.receiver_active.load(Ordering::SeqCst) {
            return Err(SendError(t));
        }
        queue.push_back(t);
        drop(queue);
        self.shared.condvar.notify_one();
        Ok(())
    }

    /// Sends a value, blocking if the lock is contended.
    pub fn send_block(&self, t: T) -> Result<(), SendError<T>> {
        if !self.shared.receiver_active.load(Ordering::SeqCst) {
            return Err(SendError(t));
        }
        let mut queue = self.shared.queue.lock_block();
        if !self.shared.receiver_active.load(Ordering::SeqCst) {
            return Err(SendError(t));
        }
        queue.push_back(t);
        drop(queue);
        self.shared.condvar.notify_one();
        Ok(())
    }

    /// Sends a value using the best strategy for the platform.
    pub fn send_sync(&self, t: T) -> Result<(), SendError<T>> {
        if !self.shared.receiver_active.load(Ordering::SeqCst) {
            return Err(SendError(t));
        }
        let mut queue = self.shared.queue.lock_sync();
        if !self.shared.receiver_active.load(Ordering::SeqCst) {
            return Err(SendError(t));
        }
        queue.push_back(t);
        drop(queue);
        self.shared.condvar.notify_one();
        Ok(())
    }

    /// Sends a value asynchronously.
    pub async fn send_async(&self, t: T) -> Result<(), SendError<T>> {
        if !self.shared.receiver_active.load(Ordering::SeqCst) {
            return Err(SendError(t));
        }
        let mut queue = self.shared.queue.lock_async().await;
        if !self.shared.receiver_active.load(Ordering::SeqCst) {
            return Err(SendError(t));
        }
        queue.push_back(t);
        drop(queue);
        self.shared.condvar.notify_one();
        Ok(())
    }
}

impl<T> Receiver<T> {
    /// Attempts to receive a value without blocking.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let mut queue = match self.shared.queue.try_lock() {
            Ok(guard) => guard,
            Err(_) => return Err(TryRecvError::Empty),
        };
        match queue.pop_front() {
            Some(t) => Ok(t),
            None => {
                if self.shared.sender_count.load(Ordering::SeqCst) == 0 {
                    Err(TryRecvError::Disconnected)
                } else {
                    Err(TryRecvError::Empty)
                }
            }
        }
    }

    /// Receives a value, spinning if empty.
    pub fn recv_spin(&self) -> Result<T, RecvError> {
        let mut queue = self.shared.queue.lock_spin();
        loop {
            if let Some(t) = queue.pop_front() {
                return Ok(t);
            }
            if self.shared.sender_count.load(Ordering::SeqCst) == 0 {
                return Err(RecvError::Disconnected);
            }
            queue = self.shared.condvar.wait_spin(queue);
        }
    }

    /// Receives a value, spinning if empty, with a deadline.
    pub fn recv_spin_timeout(&self, deadline: crate::sync::Instant) -> Result<T, RecvTimeoutError> {
        let mut queue = match self.shared.queue.lock_spin_timeout(deadline) {
            Some(guard) => guard,
            None => return Err(RecvTimeoutError::Timeout),
        };
        loop {
            if let Some(t) = queue.pop_front() {
                return Ok(t);
            }
            if self.shared.sender_count.load(Ordering::SeqCst) == 0 {
                return Err(RecvTimeoutError::Disconnected);
            }
            let result;
            (queue, result) = self.shared.condvar.wait_spin_timeout(queue, deadline);
            if result.timed_out() {
                return Err(RecvTimeoutError::Timeout);
            }
        }
    }

    /// Receives a value, blocking if empty.
    pub fn recv_block(&self) -> Result<T, RecvError> {
        let mut queue = self.shared.queue.lock_block();
        loop {
            if let Some(t) = queue.pop_front() {
                return Ok(t);
            }
            if self.shared.sender_count.load(Ordering::SeqCst) == 0 {
                return Err(RecvError::Disconnected);
            }
            queue = self.shared.condvar.wait_block(queue);
        }
    }

    /// Receives a value, blocking if empty, with a deadline.
    pub fn recv_block_timeout(&self, deadline: crate::sync::Instant) -> Result<T, RecvTimeoutError> {
        let mut queue = match self.shared.queue.lock_block_timeout(deadline) {
            Some(guard) => guard,
            None => return Err(RecvTimeoutError::Timeout),
        };
        loop {
            if let Some(t) = queue.pop_front() {
                return Ok(t);
            }
            if self.shared.sender_count.load(Ordering::SeqCst) == 0 {
                return Err(RecvTimeoutError::Disconnected);
            }
            let result;
            (queue, result) = self.shared.condvar.wait_block_timeout(queue, deadline);
            if result.timed_out() {
                return Err(RecvTimeoutError::Timeout);
            }
        }
    }

    /// Receives a value using the best strategy for the platform.
    pub fn recv_sync(&self) -> Result<T, RecvError> {
        let mut queue = self.shared.queue.lock_sync();
        loop {
            if let Some(t) = queue.pop_front() {
                return Ok(t);
            }
            if self.shared.sender_count.load(Ordering::SeqCst) == 0 {
                return Err(RecvError::Disconnected);
            }
            queue = self.shared.condvar.wait_sync(queue);
        }
    }

    /// Receives a value using the best platform strategy with a deadline.
    pub fn recv_sync_timeout(&self, deadline: crate::sync::Instant) -> Result<T, RecvTimeoutError> {
        let mut queue = match self.shared.queue.lock_sync_timeout(deadline) {
            Some(guard) => guard,
            None => return Err(RecvTimeoutError::Timeout),
        };
        loop {
            if let Some(t) = queue.pop_front() {
                return Ok(t);
            }
            if self.shared.sender_count.load(Ordering::SeqCst) == 0 {
                return Err(RecvTimeoutError::Disconnected);
            }
            let result;
            (queue, result) = self.shared.condvar.wait_sync_timeout(queue, deadline);
            if result.timed_out() {
                return Err(RecvTimeoutError::Timeout);
            }
        }
    }

    /// Receives a value asynchronously.
    pub async fn recv_async(&self) -> Result<T, RecvError> {
        let mut queue = self.shared.queue.lock_async().await;
        loop {
            if let Some(t) = queue.pop_front() {
                return Ok(t);
            }
            if self.shared.sender_count.load(Ordering::SeqCst) == 0 {
                return Err(RecvError::Disconnected);
            }
            queue = self.shared.condvar.wait_async(queue).await;
        }
    }

    /// Receives a value asynchronously with a deadline.
    pub async fn recv_async_timeout(
        &self,
        deadline: crate::sync::Instant,
    ) -> Result<T, RecvTimeoutError> {
        let mut queue = match self.shared.queue.lock_async_timeout(deadline).await {
            Some(guard) => guard,
            None => return Err(RecvTimeoutError::Timeout),
        };
        loop {
            if let Some(t) = queue.pop_front() {
                return Ok(t);
            }
            if self.shared.sender_count.load(Ordering::SeqCst) == 0 {
                return Err(RecvTimeoutError::Disconnected);
            }
            let result;
            (queue, result) = self
                .shared
                .condvar
                .wait_async_timeout(queue, deadline)
                .await;
            if result.timed_out() {
                return Err(RecvTimeoutError::Timeout);
            }
        }
    }
}

impl<T> Iterator for IntoIter<T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        self.rx.recv_sync().ok()
    }
}

/// An iterator over messages from the receiver.
pub struct IntoIter<T> {
    rx: Receiver<T>,
}

impl<T> fmt::Debug for IntoIter<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IntoIter").finish_non_exhaustive()
    }
}

impl<T> IntoIterator for Receiver<T> {
    type Item = T;
    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> IntoIter<T> {
        IntoIter { rx: self }
    }
}

unsafe impl<T: Send> Send for Sender<T> {}
unsafe impl<T: Send> Sync for Sender<T> {}
unsafe impl<T: Send> Send for Receiver<T> {}
