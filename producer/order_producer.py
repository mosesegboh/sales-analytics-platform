import json
import random
import time
from datetime import datetime, timezone
from uuid import uuid4

from confluent_kafka import Producer
from faker import Faker


KAFKA_TOPIC = "orders.raw"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

fake = Faker()

products = [
    {"product_id": 101, "name": "Laptop", "price": 1200.00},
    {"product_id": 102, "name": "Monitor", "price": 300.00},
    {"product_id": 103, "name": "Keyboard", "price": 80.00},
    {"product_id": 104, "name": "Mouse", "price": 45.00},
    {"product_id": 105, "name": "Headphones", "price": 150.00},
]

countries = [
    "United Kingdom",
    "Estonia",
    "Germany",
    "France",
    "Finland",
    "Nigeria",
]


def create_order() -> dict:
    product = random.choice(products)
    quantity = random.randint(1, 5)

    return {
        "event_id": str(uuid4()),
        "event_type": "OrderCreated",
        "order_id": random.randint(100000, 999999),
        "customer_id": random.randint(1000, 9999),
        "customer_name": fake.name(),
        "customer_email": fake.email(),
        "product_id": product["product_id"],
        "product_name": product["name"],
        "quantity": quantity,
        "unit_price": product["price"],
        "total_amount": round(quantity * product["price"], 2),
        "currency": "EUR",
        "country": random.choice(countries),
        "order_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def delivery_report(error, message) -> None:
    if error is not None:
        print(f"Delivery failed: {error}")
        return

    print(
        f"Delivered to topic={message.topic()}, "
        f"partition={message.partition()}, "
        f"offset={message.offset()}"
    )


def main() -> None:
    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "sales-order-producer",
        }
    )

    try:
        while True:
            order = create_order()

            producer.produce(
                topic=KAFKA_TOPIC,
                key=str(order["customer_id"]),
                value=json.dumps(order),
                callback=delivery_report,
            )

            # Allows completed delivery callbacks to run.
            producer.poll(0)

            print(json.dumps(order, indent=2))
            time.sleep(2)

    except KeyboardInterrupt:
        print("\nStopping producer...")

    finally:
        # Wait for queued events to be delivered before exiting.
        producer.flush()


if __name__ == "__main__":
    main()
