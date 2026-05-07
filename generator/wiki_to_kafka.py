import json
import logging
import os
import signal
import sys
import time

import requests
from kafka import KafkaProducer
from sseclient import SSEClient

STREAM_URL = os.getenv("STREAM_URL", "https://stream.wikimedia.org/v2/stream/page-create")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "input")

running = True

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
logger = logging.getLogger("wiki-generator")


def handle_shutdown(signum, frame):
    global running
    logger.info("Shutdown signal received: %s", signum)
    running = False


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


def create_producer():
    while running:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                acks="all",
                retries=10,
                linger_ms=100,
            )
            logger.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except Exception as exc:
            logger.warning("Kafka not ready yet: %s", exc)
            time.sleep(5)
    return None


def stream_events():
    headers = {
        "Accept": "text/event-stream",
        "User-Agent": "hw10-wiki-generator/1.0",
    }
    response = requests.get(STREAM_URL, headers=headers, stream=True, timeout=60)
    response.raise_for_status()
    return SSEClient(response)


def main():
    producer = create_producer()
    if producer is None:
        logger.error("Could not create Kafka producer")
        sys.exit(1)

    while running:
        try:
            logger.info("Connecting to Wikimedia stream: %s", STREAM_URL)
            client = stream_events()

            for event in client.events():
                if not running:
                    break

                if not event.data:
                    continue

                try:
                    payload = json.loads(event.data)
                except json.JSONDecodeError:
                    logger.warning("Skipping non-JSON event")
                    continue

                meta = payload.get("meta", {})
                if meta.get("domain") == "canary":
                    continue

                producer.send(KAFKA_TOPIC, payload)

        except Exception as exc:
            logger.exception("Stream error, reconnecting in 5 seconds: %s", exc)
            time.sleep(5)

    producer.flush()
    producer.close()
    logger.info("Generator stopped")


if __name__ == "__main__":
    main()