FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y gcc netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Copy dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Expose API port
EXPOSE 8000

# Wait for DB → Run ETL → Start API
CMD sh -c "\
  echo 'Waiting for database...' && \
  until nc -z db 5432; do sleep 2; done && \
  echo 'Database is ready' && \
  python -m ingestion.runner && \
  uvicorn api.main:app --host 0.0.0.0 --port 8000 \
"
