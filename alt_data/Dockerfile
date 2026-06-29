FROM python:3.11-slim
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY normalize      /app/normalize
COPY store          /app/store
COPY kafka_producer /app/kafka_producer
COPY alt_data       /app/alt_data

CMD ["python", "-m", "alt_data.src.main"]
