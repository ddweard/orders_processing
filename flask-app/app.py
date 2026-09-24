import os
import time
import json
import logging

import mysql.connector
from mysql.connector import Error as MySQLError
from confluent_kafka import Producer
import pika
from flask import Flask, request, jsonify

# ---------- настройки ----------
MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB   = os.getenv("MYSQL_DB", "orders_db")
MYSQL_USER = os.getenv("MYSQL_USER", "app")
MYSQL_PASS = os.getenv("MYSQL_PASSWORD", "apppass")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "order-events")

RABBIT_HOST  = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBIT_USER  = os.getenv("RABBITMQ_USER", "guest")
RABBIT_PASS  = os.getenv("RABBITMQ_PASS", "guest")
RABBIT_QUEUE = os.getenv("RABBITMQ_QUEUE", "order-notifications")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("flask-app")

app = Flask(__name__)

# ---------- Kafka producer ----------
producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "client.id": "flask-order-producer",
    "acks": "all",
})

def kafka_delivery_report(err, msg):
    if err is not None:
        log.error("Kafka delivery failed: %s", err)
    else:
        log.info("Kafka delivered to %s [%d] @ %d",
                 msg.topic(), msg.partition(), msg.offset())

# ---------- MySQL helpers ----------
def get_db_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        database=MYSQL_DB,
        user=MYSQL_USER,
        password=MYSQL_PASS,
        autocommit=False,
    )

def wait_for_mysql(max_attempts=30, delay=2):
    """Подстраховка на случай, если MySQL ещё не готов."""
    for attempt in range(1, max_attempts + 1):
        try:
            conn = get_db_connection()
            conn.close()
            log.info("MySQL is ready (attempt %d)", attempt)
            return
        except MySQLError as e:
            log.warning("MySQL not ready (%d/%d): %s", attempt, max_attempts, e)
            time.sleep(delay)
    raise RuntimeError("MySQL did not become ready in time")

def save_order(customer: str, product: str, amount: float) -> int:
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO orders (customer, product, amount) VALUES (%s, %s, %s)",
            (customer, product, amount),
        )
        conn.commit()
        order_id = cur.lastrowid
        cur.close()
        return order_id
    finally:
        conn.close()

# ---------- RabbitMQ helper ----------
def publish_notification(text: str):
    credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(
        host=RABBIT_HOST,
        credentials=credentials,
        heartbeat=30,
        blocked_connection_timeout=30,
    )
    connection = pika.BlockingConnection(params)
    try:
        channel = connection.channel()
        channel.queue_declare(queue=RABBIT_QUEUE, durable=True)
        channel.basic_publish(
            exchange="",
            routing_key=RABBIT_QUEUE,
            body=text.encode("utf-8"),
            properties=pika.BasicProperties(
                delivery_mode=2,  # persistent
                content_type="text/plain",
            ),
        )
        log.info("RabbitMQ notification sent: %s", text)
    finally:
        connection.close()

# ---------- HTTP ----------
@app.route("/health", methods=["GET"])
def health():
    return jsonify(status="ok"), 200

@app.route("/orders", methods=["POST"])
def create_order():
    data = request.get_json(silent=True) or {}

    customer = (data.get("customer") or "").strip()
    product  = (data.get("product")  or "").strip()
    amount   = data.get("amount")

    # валидация
    if not customer or not product:
        return jsonify(error="customer and product are required"), 400
    try:
        amount = float(amount)
    except (TypeError, ValueError):
        return jsonify(error="amount must be a number"), 400
    if amount <= 0:
        return jsonify(error="amount must be > 0"), 400

    # 1) сохраняем в MySQL
    try:
        order_id = save_order(customer, product, amount)
        log.info("Order saved to MySQL: order_id=%s", order_id)
    except MySQLError as e:
        log.exception("MySQL insert failed")
        return jsonify(error=f"db error: {e}"), 500

    # 2) Kafka: ключ = order_id
    event = {
        "order_id": order_id,
        "customer": customer,
        "product":  product,
        "amount":   amount,
        "event":    "order_created",
    }
    try:
        producer.produce(
            topic=KAFKA_TOPIC,
            key=str(order_id).encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
            callback=kafka_delivery_report,
        )
        producer.poll(0)
        log.info("Kafka event produced for order_id=%s", order_id)
    except Exception as e:
        log.exception("Kafka produce failed")
        # заказ в БД уже есть — не роняем запрос, но сообщаем
        return jsonify(order_id=order_id,
                       warning=f"kafka error: {e}"), 202

    # 3) RabbitMQ: короткое уведомление
    try:
        publish_notification(f"New order #{order_id}: {product} for {customer}, amount={amount}")
    except Exception as e:
        log.exception("RabbitMQ publish failed")
        return jsonify(order_id=order_id,
                       warning=f"rabbitmq error: {e}"), 202

    return jsonify(order_id=order_id, status="created"), 201


if __name__ == "__main__":
    wait_for_mysql()
    # host 0.0.0.0 — чтобы nginx из другого контейнера достучался
    app.run(host="0.0.0.0", port=5000, debug=False)