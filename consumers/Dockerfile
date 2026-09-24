FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY payment_consumer.py .
COPY delivery_consumer.py .

CMD ["python", "payment_consumer.py"]