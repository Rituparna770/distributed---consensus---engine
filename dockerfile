FROM python:3.11-slim

# Curl is handy for in-container debugging and health checks.
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app/src
EXPOSE 8000

# Default; docker-compose overrides per service.
CMD ["python", "src/node.py"]
