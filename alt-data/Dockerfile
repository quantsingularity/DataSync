FROM python:3.11-slim
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

COPY ../requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY ../normalize      /app/normalize
COPY ../store          /app/store
COPY ../kafka-producer /app/kafka-producer
COPY ../alt-data       /app/alt-data

CMD ["python", "-m", "alt-data.src.main"]
