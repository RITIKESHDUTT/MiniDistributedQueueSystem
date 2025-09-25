use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::HashMap;
use serde::{Serialize, Deserialize};

static EVENT_COUNTER: AtomicU64 = AtomicU64::new(1); // global counter for unique event IDs

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum EventOp {
    Enqueue,
    Dequeue,
}

#[derive( Clone, Debug, Serialize, Deserialize)]
pub struct Event<T> {
    pub global_id: u64,           // unique event ID
    pub origin_node: String,
    pub op: EventOp,
    pub item: Option<T>,
    pub clock: HashMap<String, u64>,
}

impl<T> Event<T> {

    fn next_id() -> u64 {
        EVENT_COUNTER.fetch_add(1, Ordering::SeqCst)
    }

    pub fn new_enqueue(origin_node: String, item: T, clock:  HashMap<String, u64>) -> Self {
        Self {
            global_id: Self::next_id(),
            origin_node,
            op: EventOp::Enqueue,
            item: Some(item),
            clock,
        }
    }

    pub fn new_dequeue(origin_node: String, item: Option<T>, clock:  HashMap<String, u64>) -> Self {
        Self {
            global_id: Self::next_id(),
            origin_node,
            op: EventOp::Dequeue,
            item,
            clock,
        }
    }
    /// Get the timestamp for this event's originating node
    fn origin_timestamp(&self) -> u64 {
        self.clock.get(&self.origin_node).copied().unwrap_or(0)
    }

    /// Calculate a total ordering value for priority queue sorting
    /// This is a simplified approach - in practice, you might want more sophisticated ordering
    fn total_order_value(&self) -> u64 {
        // Sum all clock values, weighted by node_id hash for determinism
        let mut total = 0u64;
        for (node, &time) in &self.clock {
            let node_hash = node.chars().map(|c| c as u64).sum::<u64>();
            total = total.saturating_add(time.saturating_mul(1000).saturating_add(node_hash % 1000));
        }
        total
    }
}

impl<T> PartialEq for Event<T> {
    fn eq(&self, other: &Self) -> bool {
        self.clock == other.clock && self.origin_node == other.origin_node
    }
}

impl<T> Eq for Event<T> {}

impl<T> PartialOrd for Event<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Event<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Primary ordering: total order value (sum of vector clock)
        match self.total_order_value().cmp(&other.total_order_value()) {
            std::cmp::Ordering::Equal => {
                // Secondary: origin timestamp
                match self.origin_timestamp().cmp(&other.origin_timestamp()) {
                    std::cmp::Ordering::Equal => {
                        // Tie-breaker: node_id for deterministic ordering
                        self.origin_node.cmp(&other.origin_node)
                    }
                    other_order => other_order,
                }
            }
            other_order => other_order,
        }
    }
}