use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::HashMap;

/// Vector Clock
#[derive(Debug, Clone)]
pub struct VectorClock {
    /// Each node ID maps to an atomic counter
    clock: Arc<Mutex<HashMap<String, Arc<AtomicU64>>>>,
    node_id: String,
}

impl VectorClock {
    /// Create a new clock starting at 0
    pub(crate) fn new(node_id: &str, nodes: &[&str]) -> Self {
        let mut map = HashMap::new();
        for &id in nodes {
            map.insert(id.to_string(), Arc::new(AtomicU64::new(0)));
        }
        // Ensure the current node is included
        if !map.contains_key(node_id) {
            map.insert(node_id.to_string(), Arc::new(AtomicU64::new(0)));
        }
        Self {
            clock: Arc::new(Mutex::new(map)),
            node_id: node_id.to_string()
        }
    }
    // Create a new clock with just the current node (for single-process testing)
    pub fn new_single(node_id: &str) -> Self {
        let mut map = HashMap::new();
        map.insert(node_id.to_string(), Arc::new(AtomicU64::new(0)));
        Self {
            clock: Arc::new(Mutex::new(map)),
            node_id: node_id.to_string()
        }
    }

    /// Get current clock
    pub(crate) fn now(&self) -> u64 {
        let map = self.clock.lock().unwrap();
        map[&self.node_id].load(Ordering::SeqCst)
    }

    /// Get the full vector clock as a HashMap snapshot
    pub fn snapshot(&self) -> HashMap<String, u64> {
        let map = self.clock.lock().unwrap();
        map.iter()
            .map(|(k, v)| (k.clone(), v.load(Ordering::SeqCst)))
            .collect()
    }

    /// Increment clock for a local event
    pub(crate) fn tick(&self) -> u64 {
        let map = self.clock.lock().unwrap();
        let counter = map.get(&self.node_id).unwrap();
        counter.fetch_add(1, Ordering::SeqCst) + 1
    }

    // Update this clock with a remote vector clock (taking max of each component)
    pub fn update(&self, remote: &HashMap<String, u64>) {
        let map = self.clock.lock().unwrap();
        // First, increment our own clock
        if let Some(local) = map.get(&self.node_id) {
            local.fetch_add(1, Ordering::SeqCst);
        }

        // Then update with remote values (take max)
        for (id, remote_val) in remote {
            if let Some(local) = map.get(id) {
                let mut current = local.load(Ordering::SeqCst);
                while current < *remote_val {
                    match local.compare_exchange_weak(
                        current,
                        *remote_val,
                        Ordering::SeqCst,
                        Ordering::Acquire
                    ) {
                        Ok(_) => break,
                        Err(new_current) => current = new_current,
                    }
                }
            } else {
                // If we don't know about this node, we could add it
                // For now, we'll just ignore unknown nodes
            }
        }
    }
    /// Add a new node to the vector clock
    pub fn add_node(&self, node_id: &str) {
        let mut map = self.clock.lock().unwrap();
        if !map.contains_key(node_id) {
            map.insert(node_id.to_string(), Arc::new(AtomicU64::new(0)));
        }
    }

    /// Check if this vector clock happened before another (partial ordering)
    pub fn happened_before(&self, other: &HashMap<String, u64>) -> bool {
        let my_snapshot = self.snapshot();

        let mut strictly_less = false;
        for (node, &my_val) in &my_snapshot {
            let other_val = other.get(node).copied().unwrap_or(0);
            if my_val > other_val {
                return false; // Not happened-before if any component is greater
            } else if my_val < other_val {
                strictly_less = true;
            }
        }

        // Check for nodes that exist in other but not in my_snapshot
        for (node, &other_val) in other {
            if !my_snapshot.contains_key(node) && other_val > 0 {
                strictly_less = true;
            }
        }

        strictly_less
    }

    pub fn tick_snapshot(&self) -> HashMap<String, u64> {
        self.tick(); // increment local counter
        self.snapshot() // return the snapshot
    }
}

/// Thread-safe shared clock
pub type SafeVectorClock = Arc<VectorClock>;