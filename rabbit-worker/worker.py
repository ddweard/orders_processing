import os
import time
import logging
import pika

RABBIT_HOST  = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBIT_USER  = os.getenv("RABBITMQ_USER", "guest")
RABBIT_PASS  = os.getenv("RABBITMQ_PASS", "guest")
RABBIT_QUEUE = os.getenv("RABBITMQ_QUEUE", "order-notifications")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [rabbit-worker] %(message)s"
)
log = logging.getLogger("rabbit-worker")

def connect_with_retry(max_attempts=30, delay=2):
    credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(
        host=RABBIT_HOST,
        credentials=credentials,
        heartbeat=30,
        blocked_connection_timeout=30,
    )
    for attempt in range(1, max_attempts + 1):
        try:
            conn = pika.BlockingConnection(params)
            log.info("Connected to RabbitMQ (attempt %d)", attempt)
            return conn
        except pika.exceptions.AMQPConnectionError as e:
            log.warning("RabbitMQ not ready (%d/%d): %s", attempt, max_attempts, e)
            time.sleep(delay)
    raise RuntimeError("Could not connect to RabbitMQ")

def on_message(channel, method, properties, body):
    text = body.decode("utf-8", errors="replace")
    log.info("Notification received: %s", text)

    # имитация обработки уведомления
    time.sleep(0.5)
    log.info("Notification processed, ack sent (delivery_tag=%s)", method.delivery_tag)

    channel.basic_ack(delivery_tag=method.delivery_tag)

def main():
    conn = connect_with_retry()
    channel = conn.channel()

    channel.queue_declare(queue=RABBIT_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=RABBIT_QUEUE,
        on_message_callback=on_message,
        auto_ack=False,
    )

    log.info("Waiting for messages in queue '%s'. To exit press CTRL+C", RABBIT_QUEUE)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        log.info("Shutting down...")
        channel.stop_consuming()
    finally:
        conn.close()

if __name__ == "__main__":
    main()