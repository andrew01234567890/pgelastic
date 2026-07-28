//! The FIFO wait queue.
//!
//! Two properties carry the whole design.
//!
//! **Waiters deregister on `Drop`.** The natural way to bound a wait is
//! `tokio::time::timeout(d, waiter)`, which on expiry drops the future and
//! nothing else. A waiter that only unregisters on a code path it may never
//! reach leaves a dead entry at the head of the queue, and every subsequent
//! release is handed to a client that is no longer listening. The pool wedges
//! with idle backends and waiting clients. So deregistration lives in `Drop`,
//! which no cancellation can skip.
//!
//! **A released link goes straight to the head of the queue.** Pushing it onto
//! an idle list and waking everyone re-races the waiters, which is both unfair
//! and a thundering herd; sending it down the head waiter's `oneshot` is neither.

use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use thiserror::Error;
use tokio::sync::oneshot;

/// Where a waiter is inserted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub enum Priority {
    /// Cancel requests, which are useless if they arrive after the query they
    /// were meant to cancel has finished.
    Cancel,
    #[default]
    Normal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum WaitError {
    #[error("the pool is closed")]
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct Ticket(u64);

struct Slot<T> {
    ticket: Ticket,
    priority: Priority,
    enqueued_at: Instant,
    sender: oneshot::Sender<T>,
}

impl<T> fmt::Debug for Slot<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Slot")
            .field("ticket", &self.ticket)
            .field("priority", &self.priority)
            .field("enqueued_at", &self.enqueued_at)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct Inner<T> {
    slots: VecDeque<Slot<T>>,
    next_ticket: u64,
    closed: bool,
}

/// A strictly ordered queue of clients waiting for a backend link.
///
/// One queue belongs to one pool, and a pool key names exactly one tenant, so
/// ordering within a queue is ordering within a tenant. Cancel requests are
/// inserted ahead of normal waiters and in arrival order among themselves.
#[derive(Debug)]
pub struct WaitQueue<T> {
    inner: Arc<Mutex<Inner<T>>>,
}

impl<T> Default for WaitQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> WaitQueue<T> {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                slots: VecDeque::new(),
                next_ticket: 0,
                closed: false,
            })),
        }
    }

    pub fn len(&self) -> usize {
        self.inner.lock().slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().slots.is_empty()
    }

    /// How long the oldest waiter has been queued.
    pub fn max_wait(&self, now: Instant) -> Duration {
        let inner = self.inner.lock();
        inner
            .slots
            .iter()
            .map(|slot| now.saturating_duration_since(slot.enqueued_at))
            .max()
            .unwrap_or_default()
    }

    /// Joins the queue.
    ///
    /// The returned [`Waiter`] must be polled to receive a link, and removes
    /// itself from the queue when dropped whether it was polled or not.
    pub fn enqueue(&self, priority: Priority, now: Instant) -> Result<Waiter<T>, WaitError> {
        let mut inner = self.inner.lock();
        if inner.closed {
            return Err(WaitError::Closed);
        }

        let ticket = Ticket(inner.next_ticket);
        inner.next_ticket += 1;
        let (sender, receiver) = oneshot::channel();
        let slot = Slot {
            ticket,
            priority,
            enqueued_at: now,
            sender,
        };

        let position = match priority {
            Priority::Cancel => inner
                .slots
                .iter()
                .position(|existing| existing.priority > Priority::Cancel)
                .unwrap_or(inner.slots.len()),
            Priority::Normal => inner.slots.len(),
        };
        inner.slots.insert(position, slot);

        Ok(Waiter {
            inner: Arc::clone(&self.inner),
            ticket,
            receiver,
            settled: false,
        })
    }

    /// Hands `value` to the waiter at the head of the queue.
    ///
    /// Returns the value back when there is nobody to give it to, which is the
    /// caller's signal to put the link on the idle list instead. Waiters that
    /// vanished between being chosen and being sent to are skipped, so a value
    /// is delivered at most once and never lost.
    pub fn hand_off(&self, value: T) -> Result<(), T> {
        let mut value = value;
        loop {
            let sender = {
                let mut inner = self.inner.lock();
                match inner.slots.pop_front() {
                    Some(slot) => slot.sender,
                    None => return Err(value),
                }
            };
            match sender.send(value) {
                Ok(()) => return Ok(()),
                Err(returned) => value = returned,
            }
        }
    }

    /// Fails every waiter and refuses new ones.
    pub fn close(&self) {
        let mut inner = self.inner.lock();
        inner.closed = true;
        inner.slots.clear();
    }

    pub fn is_closed(&self) -> bool {
        self.inner.lock().closed
    }
}

/// A client's place in the queue.
///
/// Dropping it leaves no trace, however it is dropped.
pub struct Waiter<T> {
    inner: Arc<Mutex<Inner<T>>>,
    ticket: Ticket,
    receiver: oneshot::Receiver<T>,
    settled: bool,
}

impl<T> fmt::Debug for Waiter<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Waiter")
            .field("ticket", &self.ticket)
            .field("settled", &self.settled)
            .finish_non_exhaustive()
    }
}

impl<T> Future for Waiter<T> {
    type Output = Result<T, WaitError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.receiver).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(value)) => {
                this.settled = true;
                Poll::Ready(Ok(value))
            }
            Poll::Ready(Err(_)) => {
                this.settled = true;
                Poll::Ready(Err(WaitError::Closed))
            }
        }
    }
}

impl<T> Drop for Waiter<T> {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        let mut inner = self.inner.lock();
        if let Some(position) = inner
            .slots
            .iter()
            .position(|slot| slot.ticket == self.ticket)
        {
            inner.slots.remove(position);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::task::Waker;

    use super::*;

    fn poll_once<T>(waiter: &mut Waiter<T>) -> Poll<Result<T, WaitError>> {
        let mut context = Context::from_waker(Waker::noop());
        Pin::new(waiter).poll(&mut context)
    }

    #[tokio::test]
    async fn a_released_link_goes_to_the_head_of_the_queue() {
        let queue = WaitQueue::new();
        let now = Instant::now();
        let first = queue.enqueue(Priority::Normal, now).unwrap();
        let second = queue.enqueue(Priority::Normal, now).unwrap();

        queue.hand_off(7u32).unwrap();

        assert_eq!(first.await.unwrap(), 7);
        assert_eq!(queue.len(), 1);
        drop(second);
    }

    #[tokio::test]
    async fn waiters_are_served_strictly_in_order() {
        let queue = WaitQueue::new();
        let now = Instant::now();
        let waiters: Vec<_> = (0..4)
            .map(|_| queue.enqueue(Priority::Normal, now).unwrap())
            .collect();

        for value in 0..4u32 {
            queue.hand_off(value).unwrap();
        }

        for (expected, waiter) in waiters.into_iter().enumerate() {
            assert_eq!(waiter.await.unwrap(), u32::try_from(expected).unwrap());
        }
    }

    #[tokio::test]
    async fn a_cancel_request_overtakes_normal_waiters() {
        let queue = WaitQueue::new();
        let now = Instant::now();
        let normal = queue.enqueue(Priority::Normal, now).unwrap();
        let cancel = queue.enqueue(Priority::Cancel, now).unwrap();

        queue.hand_off(1u32).unwrap();
        queue.hand_off(2u32).unwrap();

        assert_eq!(cancel.await.unwrap(), 1);
        assert_eq!(normal.await.unwrap(), 2);
    }

    #[tokio::test]
    async fn cancel_requests_keep_their_own_order() {
        let queue = WaitQueue::new();
        let now = Instant::now();
        let first = queue.enqueue(Priority::Cancel, now).unwrap();
        let second = queue.enqueue(Priority::Cancel, now).unwrap();
        let third = queue.enqueue(Priority::Cancel, now).unwrap();

        for value in 0..3u32 {
            queue.hand_off(value).unwrap();
        }

        assert_eq!(first.await.unwrap(), 0);
        assert_eq!(second.await.unwrap(), 1);
        assert_eq!(third.await.unwrap(), 2);
    }

    #[test]
    fn a_dropped_waiter_leaves_no_entry_behind() {
        let queue: WaitQueue<u32> = WaitQueue::new();
        let now = Instant::now();
        let waiter = queue.enqueue(Priority::Normal, now).unwrap();
        assert_eq!(queue.len(), 1);
        drop(waiter);
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn a_waiter_dropped_after_being_polled_still_deregisters() {
        let queue: WaitQueue<u32> = WaitQueue::new();
        let mut waiter = queue.enqueue(Priority::Normal, Instant::now()).unwrap();
        assert!(poll_once(&mut waiter).is_pending());
        drop(waiter);
        assert_eq!(queue.len(), 0);
    }

    #[tokio::test]
    async fn a_timed_out_waiter_does_not_wedge_the_queue() {
        let queue: WaitQueue<u32> = WaitQueue::new();
        let now = Instant::now();

        let abandoned = queue.enqueue(Priority::Normal, now).unwrap();
        let timeout = tokio::time::timeout(Duration::from_millis(10), abandoned);
        assert!(timeout.await.is_err());
        assert_eq!(queue.len(), 0);

        let live = queue.enqueue(Priority::Normal, now).unwrap();
        queue.hand_off(9u32).unwrap();
        assert_eq!(live.await.unwrap(), 9);
    }

    #[test]
    fn handing_off_to_an_empty_queue_returns_the_link() {
        let queue = WaitQueue::new();
        assert_eq!(queue.hand_off(3u32), Err(3));
    }

    #[tokio::test]
    async fn a_link_is_never_delivered_to_a_waiter_that_vanished_mid_handoff() {
        let queue = WaitQueue::new();
        let now = Instant::now();

        let mut departed = queue.enqueue(Priority::Normal, now).unwrap();
        let present = queue.enqueue(Priority::Normal, now).unwrap();

        // Leaves the slot registered while making its receiver unreachable:
        // exactly the window between a waiter being chosen and being sent to.
        departed.receiver.close();
        std::mem::forget(departed);

        assert!(queue.hand_off(5u32).is_ok());
        assert_eq!(present.await.unwrap(), 5);
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn the_longest_wait_is_reported_for_the_oldest_waiter() {
        let queue: WaitQueue<u32> = WaitQueue::new();
        let start = Instant::now();
        let _old = queue.enqueue(Priority::Normal, start).unwrap();
        let _new = queue
            .enqueue(Priority::Normal, start + Duration::from_secs(5))
            .unwrap();
        assert_eq!(
            queue.max_wait(start + Duration::from_secs(6)),
            Duration::from_secs(6)
        );
    }

    #[tokio::test]
    async fn closing_the_queue_fails_every_waiter() {
        let queue: WaitQueue<u32> = WaitQueue::new();
        let waiter = queue.enqueue(Priority::Normal, Instant::now()).unwrap();
        queue.close();
        assert_eq!(waiter.await, Err(WaitError::Closed));
        assert_eq!(
            queue.enqueue(Priority::Normal, Instant::now()).err(),
            Some(WaitError::Closed)
        );
        assert!(queue.is_closed());
    }
}
