use std::cell::UnsafeCell;
use std::fmt;

/// A spinlock for short-lived critical sections.
pub struct Spinlock<T> {
    data: UnsafeCell<T>,
    locked: std::sync::atomic::AtomicBool,
}

impl<T> Spinlock<T> {
    /// Creates a new spinlock with the given initial value.
    pub const fn new(data: T) -> Self {
        Self {
            data: UnsafeCell::new(data),
            locked: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Executes a closure with exclusive mutable access.
    pub fn with_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        while self.locked.swap(true, std::sync::atomic::Ordering::Acquire) {
            std::hint::spin_loop();
        }
        let result = unsafe { f(&mut *self.data.get()) };
        self.locked.store(false, std::sync::atomic::Ordering::Release);
        result
    }
}

impl<T> fmt::Debug for Spinlock<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Spinlock").finish_non_exhaustive()
    }
}

unsafe impl<T: Send> Send for Spinlock<T> {}
unsafe impl<T: Send> Sync for Spinlock<T> {}

impl<T: Default> Default for Spinlock<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> From<T> for Spinlock<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}
