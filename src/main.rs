use DistributedQueueMini::core::log::append_logs;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use DistributedQueueMini::core::buildcore::DistributedQueueSystem;

fn main() {
    // Create 5 node IDs for example
    let node_ids: Vec<String> = (0..100).map(|i| format!("N{}", i)).collect();

    // Initialize DistributedQueueSystem for each node
    let mut nodes = Vec::new();
    for id in &node_ids {
        let other_nodes: Vec<&str> = node_ids
            .iter()
            .filter(|x| *x != id)
            .map(|s| s.as_str())
            .collect();
        nodes.push(Arc::new(DistributedQueueSystem::<String>::new_with_nodes(
            id.clone(),
            &other_nodes,
        )));
    }

    let mut handles = vec![];

    // Spawn threads for each node
    for node in nodes.clone() {
        let node_clone = node.clone();
        handles.push(thread::spawn(move || {
            // Enqueue 3 items
            for i in 1..=3 {
                let item = format!("{}-Item {}", node_clone.node_id(), i);
                node_clone.enqueue(item);
                thread::sleep(Duration::from_millis(10));
            }

            // Dequeue 2 items
            for _ in 0..2 {
                node_clone.dequeue();
                thread::sleep(Duration::from_millis(10));
            }
        }));
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    // Print the logs for all nodes in their original Debug form
    // Append the logs for all nodes as NDJSON
    for node in nodes {
        append_logs(&node.logs(), "output.ndjson").expect("Failed to append logs");
    }
}

