use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::fs::OpenOptions;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use crate::core::event::Event;
use serde::{Serialize, Deserialize};
use std::io::Write;

static LOG_ID_COUNTER: AtomicU64 = AtomicU64::new(1);
/// State of a queue operation
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum State {
    Pending,
    Committed,
    Delivered,
    Failed,
}

/// Log entry recording an operation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogEntry<T> {
    pub local_log_id: u64,
    pub local_node: String,
    pub op: String,                //"enqueue" or "dequeue"
    pub item: Option<T>,      // The item being enqueued/dequeued
    pub state: State,              // Current State
    pub clock:HashMap<String, u64>,              // Logical Clock
    pub event_global_id: Option<u64>,
    pub event: Option<Event<T>>,
}

impl <T: std::fmt::Debug> Display for LogEntry<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LogEntry {{ local_log_id: {}, local_node: {}, op: {}, item: {:?}, state: {:?}, clock: {:?}, event_global_id: {:?}, event: {:?}",
            self.local_log_id,
            self.local_node,
            self.op,
            self.item,
            self.state,
            self.clock,
            self.event_global_id,
            self.event,
        )
    }
}


#[derive(Clone, Debug)]
/// Logger storing all entries
pub struct Logger<T> {
    pub(crate) entries: Vec<LogEntry<T>>,
    local_node: String,
}

impl<T:Clone> Logger<T> {
    pub  fn new(local_node: String) -> Self {
        Self {entries:Vec::new(), local_node}
    }

    /// Log an operation
    pub fn log(&mut self, op: &str, item: Option<T>, state: State, clock: HashMap<String, u64>, event_global_id: Option<u64>, event: Event<T>) {
        // --- Negative-space assertion: op validity ---
        assert!(op == "enqueue" || op == "dequeue", "Operation must be enqueue or dequeue");

        // --- Negative-space assertion: state must match operation ---
        if op == "enqueue" {
            assert!(
                matches!(state, State::Pending | State::Committed),
                "Enqueue must start as Pending or Commited"
            );
        }
        if op == "dequeue" {
            assert!(
                matches!(state, State::Delivered),
                "Dequeue must result in Delivered"
            );
        }

        let local_log_id = LOG_ID_COUNTER.fetch_add(1, Ordering::SeqCst);

        // --- Log entry insertion ---
        let before = self.entries.len();
        self.entries.push(LogEntry {
            local_log_id,
            local_node: self.local_node.clone(),
            op: op.into(),
            item,
            state,
            clock,
            event_global_id ,
            event:Some(event),
        });

        // --- Negative-space assertion: log length increased exactly by 1 ---
        assert_eq!(
            self.entries.len(),
            before + 1,
            "Logger must increase by exactly one entry"
        );
    }

    pub fn update_entry_state(&mut self, log_id:u64, new_state:State) -> bool{
        if let Some(entry) = self.entries.iter_mut().find(|e| e.local_log_id ==log_id){
            entry.state = new_state;
            true
        } else{
            false
        }
    }

    pub fn get_entries_since(&self, clock: &HashMap<String, u64>) -> Vec<LogEntry<T>> {
        self.entries
            .iter()
            .filter(|entry| {
                // happened_after: entry.clock > given clock
                entry.clock.iter().any(|(node, &time)| {
                    let &other_time = clock.get(node).unwrap_or(&0);
                    time > other_time
                })
            })
            .cloned()
            .collect()
    }
}


pub fn append_logs<T: Serialize>(log: &Vec<LogEntry<T>>, path: &str) -> std::io::Result<()> {
    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)?;

    for entry in log {
        let json = serde_json::to_string(entry).expect("Serialization failed");
        writeln!(file, "{}", json)?; // one JSON object per line
    }
    Ok(())
}
/// Thread-safe wrapper
pub type SafeLogger<T> = Arc<Mutex<Logger<T>>>;