import json
import time
import uuid
import random
from datetime import datetime, timedelta, timezone
from confluent_kafka import Producer

# Kafka configuration
KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "linger.ms": 50
}

TOPIC = "rides.events.raw"

# Simulation settings
CITIES = ["Auckland", "Wellington", "Christchurch"]
EVENT_TYPES = [
    "ride_requested",
    "ride_accepted",
    "ride_completed",
    "fare_updated"
]

DUPLICATE_EVENT_PROB = 0.05     # 5% duplicates
LATE_EVENT_PROB = 0.15          # 15% late events
MAX_LATE_MINUTES = 90           # late by up to 90 mins

# Helpers
def utc_now():
    return datetime.now(timezone.utc)

def maybe_late(event_time):
    if random.random() < LATE_EVENT_PROB:
        delay = timedelta(minutes=random.randint(5, MAX_LATE_MINUTES))
        return event_time - delay
    return event_time

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")

# Event creation
def create_event(event_type, ride):
    event_time = maybe_late(ride["event_time"])

    payload = {}
    if event_type == "ride_completed":
        payload = {
            "fare": round(random.uniform(10, 60), 2),
            "duration_seconds": random.randint(300, 3600),
            "distance_km": round(random.uniform(2, 30), 1)
        }
    elif event_type == "fare_updated":
        payload = {
            "fare": round(random.uniform(10, 65), 2)
        }

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_time": event_time.isoformat(),
        "ride_id": ride["ride_id"],
        "user_id": ride["user_id"],
        "driver_id": ride["driver_id"],
        "city": ride["city"],
        "payload": payload
    }

# Ride lifecycle
def generate_ride_events():
    ride = {
        "ride_id": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4()),
        "driver_id": str(uuid.uuid4()),
        "city": random.choice(CITIES),
        "event_time": utc_now()
    }

    events = [
        create_event("ride_requested", ride),
        create_event("ride_accepted", ride),
        create_event("ride_completed", ride)
    ]

    # Sometimes add a late fare update
    if random.random() < 0.3:
        events.append(create_event("fare_updated", ride))

    return events

# Main loop
def main():
    producer = Producer(KAFKA_CONFIG)

    print("Starting ride event generator...")
    try:
        while True:
            events = generate_ride_events()

            for event in events:
                value = json.dumps(event)
                key = event["ride_id"]

                producer.produce(
                    topic=TOPIC,
                    key=key,
                    value=value,
                    on_delivery=delivery_report
                )

                # Intentional duplicates
                if random.random() < DUPLICATE_EVENT_PROB:
                    producer.produce(
                        topic=TOPIC,
                        key=key,
                        value=value
                    )

            producer.poll(0)
            time.sleep(random.uniform(0.2, 1.0))

    except KeyboardInterrupt:
        print("Stopping generator...")
    finally:
        producer.flush()

if __name__ == "__main__":
    main()
