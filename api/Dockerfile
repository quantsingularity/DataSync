FROM python:3.11-slim
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

# Build context is the repo root (see docker-compose.yml). Shared packages
# live at the root, so they must be copied from there - copying from a parent
# of the build context (../) is forbidden by Docker and broke the build.
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY normalize   /app/normalize
COPY store       /app/store
COPY api         /app/api

EXPOSE 8000
CMD ["uvicorn", "api.src.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
