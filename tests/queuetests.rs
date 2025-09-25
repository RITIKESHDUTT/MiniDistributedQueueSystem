use DistributedQueueMini::core::buildcore::DistributedQueueSystem;
#[test]
fn test_duplicate_event_handling() {
    let node1 = DistributedQueueSystem::new("node1".to_string());
    let node2 = DistributedQueueSystem::new("node2".to_string());

    let event = node1.enqueue("item1".to_string());

    // Apply event once
    assert!(node2.apply_remote_event(event.clone()));
    assert_eq!(node2.queue_state().0, 1);

    // Try to apply same event again
    assert!(!node2.apply_remote_event(event));
    assert_eq!(node2.queue_state().0, 1); // Should remain unchanged

}