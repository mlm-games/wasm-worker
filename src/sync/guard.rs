use crate::sync::Mutex;
use crate::sync::rwlock::{RwLock, LOCKED_WRITE, UNLOCKED};

/// RAII guard that releases a `Mutex` lock on drop.
pub struct Guard<'a, T> {
    pub(crate) mutex: &'a Mutex<T>,
    pub(crate) data: &'a mut T,
}

impl<T> std::ops::Deref for Guard<'_, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.data
    }
}

impl<T> std::ops::DerefMut for Guard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data
    }
}

impl<T> Drop for Guard<'_, T> {
    fn drop(&mut self) {
        self.mutex
            .data_lock
            .store(false, std::sync::atomic::Ordering::Release);
        self.mutex.did_unlock();
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for Guard<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Guard")
            .field("data", &**self)
            .finish_non_exhaustive()
    }
}

impl<T: std::fmt::Display> std::fmt::Display for Guard<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&**self, f)
    }
}

impl<T> AsRef<T> for Guard<'_, T> {
    fn as_ref(&self) -> &T {
        self
    }
}

impl<T> AsMut<T> for Guard<'_, T> {
    fn as_mut(&mut self) -> &mut T {
        self
    }
}

/// RAII guard that releases a `RwLock` read lock on drop.
pub struct ReadGuard<'a, T> {
    pub(crate) rwlock: &'a RwLock<T>,
}

/// RAII guard that releases a `RwLock` write lock on drop.
pub struct WriteGuard<'a, T> {
    pub(crate) rwlock: &'a RwLock<T>,
}

impl<T> AsRef<T> for ReadGuard<'_, T> {
    fn as_ref(&self) -> &T {
        self
    }
}

impl<T> AsRef<T> for WriteGuard<'_, T> {
    fn as_ref(&self) -> &T {
        self
    }
}

impl<T> AsMut<T> for WriteGuard<'_, T> {
    fn as_mut(&mut self) -> &mut T {
        &mut *self
    }
}

impl<T> std::ops::Deref for WriteGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        unsafe { &*self.rwlock.inner.get() }
    }
}

impl<T> std::ops::DerefMut for WriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.rwlock.inner.get() }
    }
}

impl<T> std::ops::Deref for ReadGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.rwlock.inner.get() }
    }
}

impl<T> Drop for ReadGuard<'_, T> {
    fn drop(&mut self) {
        let r = self
            .rwlock
            .data_lock
            .fetch_sub(1, std::sync::atomic::Ordering::Release);
        assert!(r > 0);
        self.rwlock.did_unlock_read();
    }
}

impl<T> Drop for WriteGuard<'_, T> {
    fn drop(&mut self) {
        let old = self
            .rwlock
            .data_lock
            .swap(UNLOCKED, std::sync::atomic::Ordering::Release);
        assert!(old == LOCKED_WRITE);
        self.rwlock.did_unlock_write();
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for ReadGuard<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadGuard")
            .field("data", &**self)
            .finish_non_exhaustive()
    }
}

impl<T: std::fmt::Display> std::fmt::Display for ReadGuard<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&**self, f)
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for WriteGuard<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteGuard")
            .field("data", &**self)
            .finish_non_exhaustive()
    }
}

impl<T: std::fmt::Display> std::fmt::Display for WriteGuard<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&**self, f)
    }
}
