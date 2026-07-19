// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{
    fmt,
    future::Future,
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, Weak},
    task,
};

struct StateInner<T> {
    value: T,
    wakers: Vec<task::Waker>,
    epoch: usize,
    dropped: Option<()>,
}

impl<T> StateInner<T> {
    pub fn new(value: T) -> Self {
        Self {
            value,
            wakers: Vec::new(),
            epoch: 0,
            dropped: Some(()),
        }
    }

    pub fn register(&mut self, waker: &task::Waker) {
        // Fast path: under the publisher's nested `tokio::select!`, a still-Pending
        // `StateChanged` is re-polled on every wake of the task — even when this
        // state did not change — which made the original `retain` rewrite + waker
        // clone (an Arc atomic bump) + `Vec::push` the dominant per-object cost
        // (~half of the shred publisher's CPU). If an equivalent waker is already
        // registered there is nothing to do: skip the scan, the clone, and the
        // push. `notify` drains every waker on each epoch, so a new or changed
        // waker is still appended below and stale entries never outlive one epoch.
        // (Same idempotent-register idiom as web-transport-quinn's accept_uni.)
        if self.wakers.iter().any(|existing| existing.will_wake(waker)) {
            return;
        }
        self.wakers.push(waker.clone());
    }

    pub fn notify(&mut self) {
        self.epoch += 1;
        for waker in self.wakers.drain(..) {
            waker.wake();
        }
    }
}

impl<T: Default> Default for StateInner<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: fmt::Debug> fmt::Debug for StateInner<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.value.fmt(f)
    }
}

pub struct State<T> {
    state: Arc<Mutex<StateInner<T>>>,
    drop: Arc<StateDrop<T>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StateError {
    Poisoned,
}

impl<T> State<T> {
    pub fn new(initial: T) -> Self {
        let state = Arc::new(Mutex::new(StateInner::new(initial)));

        Self {
            state: state.clone(),
            drop: Arc::new(StateDrop { state }),
        }
    }

    pub fn lock(&self) -> StateRef<'_, T> {
        StateRef {
            state: self.state.clone(),
            drop: self.drop.clone(),
            lock: self.state.lock().unwrap(),
        }
    }

    pub fn try_lock(&self) -> Result<StateRef<'_, T>, StateError> {
        let lock = self.state.lock().map_err(|_| StateError::Poisoned)?;
        Ok(StateRef {
            state: self.state.clone(),
            drop: self.drop.clone(),
            lock,
        })
    }

    pub fn lock_mut(&self) -> Option<StateMut<'_, T>> {
        let lock = self.state.lock().unwrap();
        lock.dropped?;
        Some(StateMut {
            lock,
            _drop: self.drop.clone(),
        })
    }

    pub fn try_lock_mut(&self) -> Result<Option<StateMut<'_, T>>, StateError> {
        let lock = self.state.lock().map_err(|_| StateError::Poisoned)?;
        if lock.dropped.is_none() {
            return Ok(None);
        }

        Ok(Some(StateMut {
            lock,
            _drop: self.drop.clone(),
        }))
    }

    pub fn downgrade(&self) -> StateWeak<T> {
        StateWeak {
            state: Arc::downgrade(&self.state),
            drop: Arc::downgrade(&self.drop),
        }
    }

    pub fn split(self) -> (Self, Self) {
        let state = self.state.clone();
        (
            self, // important that we don't make a new drop here
            Self {
                state: state.clone(),
                drop: Arc::new(StateDrop { state }),
            },
        )
    }
}

impl<T> Clone for State<T> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            drop: self.drop.clone(),
        }
    }
}

impl<T: Default> Default for State<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: fmt::Debug> fmt::Debug for State<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.state.try_lock() {
            Ok(lock) => lock.value.fmt(f),
            Err(_) => write!(f, "<locked>"),
        }
    }
}

pub struct StateRef<'a, T> {
    state: Arc<Mutex<StateInner<T>>>,
    lock: MutexGuard<'a, StateInner<T>>,
    drop: Arc<StateDrop<T>>,
}

impl<'a, T> StateRef<'a, T> {
    // Release the lock and wait for a notification when next updated.
    pub fn modified(self) -> Option<StateChanged<T>> {
        self.lock.dropped?;

        Some(StateChanged {
            state: self.state,
            epoch: self.lock.epoch,
        })
    }

    // Upgrade to a mutable references that automatically calls notify on drop.
    pub fn into_mut(self) -> Option<StateMut<'a, T>> {
        self.lock.dropped?;
        Some(StateMut {
            lock: self.lock,
            _drop: self.drop,
        })
    }
}

impl<T> Deref for StateRef<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.lock.value
    }
}

impl<T: fmt::Debug> fmt::Debug for StateRef<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.lock.fmt(f)
    }
}

pub struct StateMut<'a, T> {
    lock: MutexGuard<'a, StateInner<T>>,
    _drop: Arc<StateDrop<T>>,
}

impl<T> Deref for StateMut<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.lock.value
    }
}

impl<T> DerefMut for StateMut<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.lock.value
    }
}

impl<T> Drop for StateMut<'_, T> {
    fn drop(&mut self) {
        self.lock.notify();
    }
}

impl<T: fmt::Debug> fmt::Debug for StateMut<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.lock.fmt(f)
    }
}

pub struct StateChanged<T> {
    state: Arc<Mutex<StateInner<T>>>,
    epoch: usize,
}

impl<T> Future for StateChanged<T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        // TODO is there an API we can make that doesn't drop this lock?
        let mut state = self.state.lock().unwrap();

        if state.epoch > self.epoch {
            task::Poll::Ready(())
        } else {
            state.register(cx.waker());
            task::Poll::Pending
        }
    }
}

pub struct StateWeak<T> {
    state: Weak<Mutex<StateInner<T>>>,
    drop: Weak<StateDrop<T>>,
}

impl<T> StateWeak<T> {
    pub fn upgrade(&self) -> Option<State<T>> {
        if let (Some(state), Some(drop)) = (self.state.upgrade(), self.drop.upgrade()) {
            Some(State { state, drop })
        } else {
            None
        }
    }
}

impl<T> Clone for StateWeak<T> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            drop: self.drop.clone(),
        }
    }
}

struct StateDrop<T> {
    state: Arc<Mutex<StateInner<T>>>,
}

impl<T> Drop for StateDrop<T> {
    fn drop(&mut self) {
        let Ok(mut state) = self.state.lock() else {
            tracing::error!("watch state lock poisoned while dropping state");
            return;
        };
        state.dropped = None;
        state.notify();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    // A waker that counts wakes. Two Wakers built from the SAME Arc report
    // will_wake() == true (identical RawWaker data pointers); Wakers from
    // different Arcs report false. That lets each test drive both will_wake
    // branches of StateInner::register deterministically, with no async runtime
    // (moq-transport enables tokio's `macros` feature only — no `rt`).
    struct CountingWaker(AtomicUsize);

    impl CountingWaker {
        fn new() -> Arc<Self> {
            Arc::new(CountingWaker(AtomicUsize::new(0)))
        }
        fn count(self: &Arc<Self>) -> usize {
            self.0.load(Ordering::SeqCst)
        }
    }

    impl Wake for CountingWaker {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    // StateChanged is Unpin (Arc + usize), so Pin::new is sound.
    fn poll_once<T>(fut: &mut StateChanged<T>, waker: &Waker) -> Poll<()> {
        let mut cx = Context::from_waker(waker);
        Pin::new(fut).poll(&mut cx)
    }

    // Guards the exact behavior task #139 changes: an already-registered waker
    // that re-polls (the idempotent fast path) must stay registered and be
    // woken on the next state change. A fast path that dropped the registration
    // would show count == 0 after notify().
    #[test]
    fn idempotent_repoll_still_wakes() {
        let state = State::new(0u32);
        let mut fut = state.lock().modified().expect("live");

        let w = CountingWaker::new();
        let waker = Waker::from(w.clone());

        assert_eq!(poll_once(&mut fut, &waker), Poll::Pending); // registers
        assert_eq!(poll_once(&mut fut, &waker), Poll::Pending); // fast path
        assert_eq!(w.count(), 0, "woke before any state change");

        {
            let mut m = state.lock_mut().expect("live");
            *m = 1; // StateMut::drop -> notify()
        }
        assert_eq!(w.count(), 1, "registered waker not woken after change");

        assert_eq!(poll_once(&mut fut, &waker), Poll::Ready(()));
    }

    // Guards multi-waiter fan-out: two distinct tasks awaiting the same state
    // must BOTH be woken. A single-waiter swap (e.g. a blanket AtomicWaker)
    // would drop one. Also checks that re-polling A on the fast path does not
    // evict B's registration.
    #[test]
    fn multi_waiter_none_dropped() {
        let state = State::new(0u32);
        let mut fa = state.lock().modified().unwrap();
        let mut fb = state.lock().modified().unwrap();

        let wa = CountingWaker::new();
        let wb = CountingWaker::new();

        assert_eq!(poll_once(&mut fa, &Waker::from(wa.clone())), Poll::Pending);
        assert_eq!(poll_once(&mut fb, &Waker::from(wb.clone())), Poll::Pending);
        // Re-poll A (fast path) — must not disturb B's registration.
        assert_eq!(poll_once(&mut fa, &Waker::from(wa.clone())), Poll::Pending);

        {
            let mut m = state.lock_mut().unwrap();
            *m = 1;
        }
        assert_eq!(wa.count(), 1, "waiter A lost its wakeup");
        assert_eq!(wb.count(), 1, "waiter B lost its wakeup");
    }

    // Guards the will_wake == false branch: when a task re-polls with a NEW
    // waker identity (as a runtime may hand out after relocating a task), the
    // new waker must end up registered so the CURRENT task is woken. A fast
    // path that early-returned on any non-empty vec would lose this wakeup.
    #[test]
    fn changed_waker_identity_still_wakes_latest() {
        let state = State::new(0u32);
        let mut fut = state.lock().modified().unwrap();

        let w1 = CountingWaker::new();
        let w2 = CountingWaker::new(); // distinct Arc => will_wake(w1, w2) == false

        assert_eq!(poll_once(&mut fut, &Waker::from(w1.clone())), Poll::Pending);
        assert_eq!(poll_once(&mut fut, &Waker::from(w2.clone())), Poll::Pending);

        {
            let mut m = state.lock_mut().unwrap();
            *m = 1;
        }
        assert!(
            w2.count() >= 1,
            "latest waker not woken (lost wakeup on waker-identity change)"
        );
    }

    // Guards the O(n) growth regression: repeated re-polls by ONE task must
    // keep exactly one registration, not one-per-poll.
    #[test]
    fn repeated_repoll_does_not_grow_wakers() {
        let state = State::new(0u32);
        let mut fut = state.lock().modified().unwrap();
        let w = CountingWaker::new();
        let waker = Waker::from(w.clone());

        for _ in 0..64 {
            assert_eq!(poll_once(&mut fut, &waker), Poll::Pending);
        }
        let n = state.state.lock().unwrap().wakers.len();
        assert_eq!(n, 1, "waker vec grew under repeated re-poll");
    }

    // Drop semantics: dropping the last State handle wakes any outstanding
    // waiter, and the waiter then completes.
    #[test]
    fn drop_wakes_waiters() {
        let state = State::new(0u32);
        let mut fut = state.lock().modified().unwrap();
        let w = CountingWaker::new();
        let waker = Waker::from(w.clone());

        assert_eq!(poll_once(&mut fut, &waker), Poll::Pending);

        drop(state); // StateDrop::drop -> dropped = None + notify()
        assert_eq!(w.count(), 1, "waiter not woken on state drop");

        assert_eq!(poll_once(&mut fut, &waker), Poll::Ready(()));
    }
}
