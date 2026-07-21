# Sales Analytics Platform

A starter sales analytics data platform for streaming order events with Kafka and preparing warehouse storage in Postgres.

## Current Stack

- Python producer for generating sample `OrderCreated` events.
- Python consumer for reading and acknowledging order events.
- Apache Kafka broker for event streaming.
- Kafka UI for local topic inspection.
- Postgres warehouse container for downstream analytics work.

## Project Structure

```text
.
|-- consumer/
|   `-- order_consumer.py
|-- data/
|-- database/
|-- producer/
|   `-- order_producer.py
|-- spark/
|-- tests/
|-- docker-compose.yml
`-- requirements.txt
```

## Requirements

- Docker and Docker Compose
- Python 3.12 or compatible Python 3 version
- `pip`

## Local Setup

Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install Python dependencies:

```bash
pip install -r requirements.txt
```

Start the local services:

```bash
docker compose up -d
```

Kafka UI is available at:

```text
http://localhost:8085
```

Postgres is exposed locally on port `5434` with:

```text
database: sales_warehouse
user: data_engineer
password: data_engineer_password
```

## Running The Stream

Start the consumer in one terminal:

```bash
python consumer/order_consumer.py
```

Start the producer in another terminal:

```bash
python producer/order_producer.py
```

The producer writes sample order events to the `orders.raw` Kafka topic every two seconds. The consumer reads from the same topic, prints a compact order summary, and commits offsets after successful processing.

## Branch Workflow

- `master` is the main release branch.
- `develop` is the integration branch.
- Feature work should use `feat/<short-description>` branch names.
- Completed feature work should be merged back into `develop` and `master` as the project progresses.

## Validation

Run a quick syntax check for the current Python scripts:

```bash
python3 -m py_compile producer/order_producer.py consumer/order_consumer.py
```
