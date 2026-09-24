import os
import json
import time
import logging
from confluent_kafka import Consumer, KafkaError

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "order-events")
KAFKA_GROUP     = os.getenv("KAFKA_GROUP", "payment-group")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [payment] %(message)s"
)
log = logging.getLogger("payment-consumer")

def build_consumer():
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })

def main():
    consumer = build_consumer()
    consumer.subscribe([KAFKA_TOPIC])
    log.info("Subscribed to topic '%s' as group '%s'", KAFKA_TOPIC, KAFKA_GROUP)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("Consumer error: %s", msg.error())
                continue

            key = msg.key().decode("utf-8") if msg.key() else None
            raw = msg.value().decode("utf-8") if msg.value() else "{}"

            try:
                event = json.loads(raw)
            except json.JSONDecodeError:
                log.warning("Non-JSON message, skipping: %s", raw)
                continue

            order_id = event.get("order_id", key)
            amount   = event.get("amount", "?")
            customer = event.get("customer", "?")
            product  = event.get("product", "?")

            # --- имитация обработки оплаты ---
            log.info("Processing payment for order_id=%s | %s | %s | amount=%s",
                     order_id, customer, product, amount)
            time.sleep(1.5)  # имитация работы
            log.info("Payment APPROVED for order_id=%s (partition=%d, offset=%d)",
                     order_id, msg.partition(), msg.offset())

    except KeyboardInterrupt:
        log.info("Shutting down...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()