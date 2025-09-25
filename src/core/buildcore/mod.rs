use std::cmp::Reverse;
pub use crate::core::{
    queue::{Queue, SafeQueue},
    clock::{VectorClock, SafeVectorClock},
    log::{LogEntry, Logger, SafeLogger, State},
    event::{Event, EventOp}
};
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

/// Unified Queue System Builder
pub struct DistributedQueueSystem<T> {
    node_id:String,
    queue: SafeQueue<T>,
    logger: SafeLogger<T>,
    clock: SafeVectorClock,
    applied_events: Mutex<HashMap<String, HashSet<u64>>>, // Track applied events per node to prevent duplicates
    event_buffer: Mutex<BinaryHeap<Reverse<Event<T>>>>, // Event buffer for ordering (events that arrived out of order)
}

impl<T: Clone + Send + 'static> DistributedQueueSystem<T> {
    /// Create a new QueueSystem
    pub fn new(node_id:String) -> Self {
        Self {
            queue: Arc::new(Mutex::new(Queue::new())),
            logger: Arc::new(Mutex::new(Logger::new(node_id.clone()))),
            clock: Arc::new(VectorClock::new_single(&node_id)),
            applied_events: Mutex::new(HashMap::new()),
            event_buffer: Mutex::new(BinaryHeap::new()),
            node_id,
        }
    }

    /// Create a new QueueSystem with known nodes
    pub fn new_with_nodes(node_id:String, nodes: &[&str]) -> Self {
        Self{
            node_id: node_id.clone(),
            queue: Arc::new(Mutex::new(Queue::new())),
            logger: Arc::new(Mutex::new(Logger::new(node_id.clone()))),
            clock: Arc::new(VectorClock::new(&node_id, nodes)),
            applied_events: Mutex::new(HashMap::new()),
            event_buffer: Mutex::new(BinaryHeap::new())
        }
    }

    /// Enqueue with logging + clock
    pub fn enqueue(&self, item: T) -> Event<T> {
        let vector_time = self.clock.tick_snapshot();
        // Create event before applying to enable broadcasting
        let event = Event::new_enqueue(self.node_id.clone(), item.clone(), vector_time.clone());
        // Apply the operation locally
        self.apply_enqueue_op(&item, vector_time, Some(event.global_id), event.clone());
        event
    }

    /// Dequeue an item
    /// Optionally merge with external Lamport clock
    pub fn dequeue(&self) -> (Option<T>, Event<T>) {
       let vector_time = self.clock.tick_snapshot();

        // Perform the actual dequeue
        let mut queue = self.queue.lock().unwrap();
        let item = queue.dequeue();
        drop(queue);

        // Create event for broadcasting
        let event = Event::new_dequeue(self.node_id.clone(), item.clone(), vector_time.clone());

        // Log the operation
        let mut logger = self.logger.lock().unwrap();
        logger.log("dequeue", item.clone(), State::Delivered, vector_time, Some(event.global_id), event.clone());
        (item, event)

    }

    /// Apply remote event from another node
    pub fn apply_remote_event(&self, event: Event<T>) -> bool {
        // Update our clock with the event's timestamp
        self.clock.update(&event.clock);
        // Check for duplicates
        {
            let mut applied = self.applied_events.lock().unwrap();
            let node_events = applied.entry(event.origin_node.clone()).or_insert_with(HashSet::new);
            if node_events.contains(&event.global_id) {
                return false;  // Already applied
            }
        }

        // Check if we can apply this even immediately or need to buffer it
        if self.can_apply_event(&event) {
            self.apply_event_immediately(event);
            self.process_buffered_events();
            true
        } else{
            // Buffer the event for later processing
            let mut buffer = self.event_buffer.lock().unwrap();
            buffer.push(Reverse(event));
            false
        }
    }

    /// Check if an event can be applied (causal consistency)
    fn can_apply_event(&self, event: &Event<T>) -> bool {
        // With vector clocks, we should check if the event's vector clock
        // is consistent with our current state. For now, simplified logic:

        // Check if we've seen the immediately preceding event from the same node
        let my_clock = self.clock.snapshot();
        let event_node_time = event.clock.get(&event.origin_node).copied().unwrap_or(0);
        let my_node_time = my_clock.get(&event.origin_node).copied().unwrap_or(0);

        // Simple causality check: event should be exactly next from that node
        event_node_time == my_node_time + 1
    }

    /// Apply an event immediately
    fn apply_event_immediately(&self, event:Event<T>) {
        // Mark as applied
        {
            let mut applied = self.applied_events.lock().unwrap();
            let node_events = applied.entry(event.origin_node.clone()).or_insert_with(HashSet::new);
            node_events.insert(event.global_id);
        }

        // Apply the operation
        match event.op {
            EventOp::Enqueue => {
                if let Some(item) = event.item.clone() {
                    self.apply_enqueue_op(&item, event.clock.clone(), Some(event.global_id), event.clone());
                }
            }
            EventOp::Dequeue => {
                self.apply_dequeue_op(event.clock.clone(), Some(event.global_id), event.clone());
            }
        }
    }

    /// Process any buffered events that can now be applied
    fn process_buffered_events(&self) {
        let mut buffer = self. event_buffer.lock().unwrap();
        let mut to_apply = Vec::new();
        let mut remaining = BinaryHeap::new();

        while let Some(Reverse(event)) = buffer.pop() {
            if self.can_apply_event(&event) {
                to_apply.push(event);
            } else{
                remaining.push(Reverse(event));
            }
        }
        *buffer = remaining;
        drop(buffer);

        // Apply events outside the lock
        for event in to_apply {
            self.apply_event_immediately(event);
        }
    }

    /// Internal helper to apply enqueue operation
    fn apply_enqueue_op(&self, item: &T, clock:HashMap<String, u64>, event_id: Option<u64>,  event: Event<T>) {
        let mut queue = self.queue.lock().unwrap();
        queue.enqueue(item.clone());
        drop(queue);
        let mut logger = self.logger.lock().unwrap();
        logger.log("enqueue", Some(item.clone()), State::Committed, clock, event_id, event);
    }

    /// Internal helper to apply dequeue op
    fn apply_dequeue_op(&self, clock:HashMap<String, u64>, event_id:Option<u64>, event: Event<T>) {
        let mut queue = self.queue.lock().unwrap();
        let item = queue.dequeue();
        drop(queue);
        let mut logger = self.logger.lock().unwrap();
        logger.log("dequeue", item, State::Delivered, clock, event_id, event);
    }

    /// Get current queue state
    pub fn queue_state(&self) -> (usize, bool) {
        let queue = self.queue.lock().unwrap();
        (queue.len(), queue.is_empty())
    }

    /// Expose logs
    pub fn logs(&self) -> Vec<LogEntry<T>> {
      let logger = self.logger.lock().unwrap();
        logger.entries.clone()
    }

    /// Get current clock time
    pub fn clock(&self) -> u64 {
        self.clock.now()
    }

    /// Get node ID
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Get pending events in buffer
    pub fn pending_events_count(&self) -> usize {
        let buffer = self.event_buffer.lock().unwrap();
        buffer.len()
    }
}
