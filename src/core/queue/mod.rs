use std::sync::{Arc, Mutex};
use std::collections::VecDeque;

/// core queue structure: handles only enqueue/dequeue logic
pub struct Queue<T>{
    items: VecDeque<T>,
}

impl <T> Queue <T> {
    /// Create a new, empty queue
    pub(crate) fn new() -> Self {
        Self{ items:VecDeque::new() }
    }

    /// Enqueue an item
    pub(crate) fn enqueue(&mut self, item: T) {
        self.items.push_back(item);
        // --post operation assertion
        assert!(self.items.len() > 0, "Queue must have at least one item after enqueue");
    }

    /// Dequeue an item
    pub(crate) fn dequeue(&mut self) -> Option<T> {
        let len_before = self.items.len();
        let result = self.items.pop_front();
        // -- post op assertion: queue size decreases if dequeue succeeded
        match result {
            Some(_) => assert_eq!(self.items.len(), len_before - 1, "Queue length should decrease by 1"),
            None => assert_eq!(self.items.len(), len_before, "Queue length unchanged when empty"),
        }
        result
    }

    /// Get the current queue length
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

}

/// Thread-safe wrapper around the queue
pub type SafeQueue<T> = Arc<Mutex<Queue<T>>>;