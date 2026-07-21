import json

from confluent_kafka import Consumer, KafkaError, KafkaException


KAFKA_TOPIC = "orders.raw"

consumer = Consumer(
    {
        "bootstrap.servers": "localhost:9092",
        "group.id": "sales-order-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
)


def process_order(order: dict) -> None:
    print(
        f"Order {order['order_id']} | "
        f"{order['product_name']} | "
        f"Quantity: {order['quantity']} | "
        f"Total: {order['total_amount']} {order['currency']}"
    )


def main() -> None:
    consumer.subscribe([KAFKA_TOPIC])

    print(f"Listening to {KAFKA_TOPIC}...")

    try:
        while True:
            message = consumer.poll(timeout=1.0)

            if message is None:
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue

                raise KafkaException(message.error())

            try:
                order = json.loads(message.value().decode("utf-8"))

                process_order(order)

                consumer.commit(message=message, asynchronous=False)

                print(
                    f"Committed partition={message.partition()}, "
                    f"offset={message.offset()}"
                )

            except json.JSONDecodeError as error:
                print(f"Invalid JSON: {error}")

            except KeyError as error:
                print(f"Missing required field: {error}")

    except KeyboardInterrupt:
        print("\nStopping consumer...")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()