"""Example 14 -- Transactional Outbox Pattern.

Demonstrates reliable event publishing using the Outbox pattern.
Events are stored in the outbox alongside the aggregate write,
then a relay process polls and publishes them to external consumers.

This ensures at-least-once delivery: if the publisher fails,
entries remain unpublished and will be retried on the next poll.

Scenario:
  - Orders are placed and events stored in EventStore + Outbox
  - OutboxRelay polls and publishes events to a simulated message broker
  - Publisher failure is handled gracefully (entries preserved)
"""

from da2 import (
    Event,
    InMemoryEventStore,
    InMemoryOutbox,
    OutboxRelay,
)


class OrderPlaced(Event):
    def __init__(self, order_id: str, amount: float):
        self.order_id = order_id
        self.amount = amount


class OrderShipped(Event):
    def __init__(self, order_id: str):
        self.order_id = order_id


def main():
    print("=== Outbox Pattern Demo ===\n")

    store = InMemoryEventStore()
    outbox = InMemoryOutbox()

    # Simulate a message broker
    broker_messages: list[dict] = []

    def publish_to_broker(entries):
        """Simulated message broker publisher."""
        for entry in entries:
            msg = {
                "event_type": entry.event_type,
                "aggregate_id": entry.aggregate_id,
                "data": entry.data,
                "position": entry.position,
            }
            broker_messages.append(msg)
            print(f"  Published: {entry.event_type} (pos={entry.position})")

    relay = OutboxRelay(outbox, publisher=publish_to_broker, batch_size=10)

    # --- Write events and store in outbox ---
    print("1. Storing events...")
    store.append("order-1", [OrderPlaced("o1", 99.99)], expected_version=0)
    outbox.store(store.load_all())  # store all events in outbox

    store.append("order-2", [OrderPlaced("o2", 49.50)], expected_version=0)
    outbox.store(store.load_all(after_position=1))  # only new events

    print(f"   Outbox has {outbox.unpublished_count} unpublished entries\n")

    # --- Relay polls and publishes ---
    print("2. Relay publishes...")
    count = relay.poll_and_publish()
    print(f"   Published {count} entries")
    print(f"   Outbox unpublished: {outbox.unpublished_count}\n")

    # --- More events, including a shipping event ---
    print("3. More events arrive...")
    store.append("order-1", [OrderShipped("o1")], expected_version=1)
    outbox.store(store.load_all(after_position=2))

    count = relay.poll_and_publish()
    print(f"   Published {count} entries\n")

    # --- Verify ---
    print(f"Broker received {len(broker_messages)} messages:")
    for msg in broker_messages:
        print(f"  [{msg['aggregate_id']}] {msg['event_type']}")

    assert len(broker_messages) == 3
    assert outbox.unpublished_count == 0

    # --- Demonstrate failure handling ---
    print("\n4. Simulating publisher failure...")
    store.append("order-3", [OrderPlaced("o3", 200.00)], expected_version=0)
    outbox.store(store.load_all(after_position=3))

    def failing_publisher(entries):
        raise ConnectionError("Message broker unavailable")

    failing_relay = OutboxRelay(outbox, publisher=failing_publisher)
    try:
        failing_relay.poll_and_publish()
    except ConnectionError:
        print("   Publisher failed -- entries preserved for retry")

    print(f"   Outbox still has {outbox.unpublished_count} unpublished entries")

    # Retry with working publisher
    retry_relay = OutboxRelay(outbox, publisher=publish_to_broker)
    count = retry_relay.poll_and_publish()
    print(f"   Retry published {count} entries successfully")
    assert outbox.unpublished_count == 0

    print("\n=== Done ===")


if __name__ == "__main__":
    main()
