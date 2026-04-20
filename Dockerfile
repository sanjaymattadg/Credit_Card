FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

COPY pipeline/requirements.txt /app/pipeline/requirements.txt

RUN pip install --no-cache-dir -r /app/pipeline/requirements.txt

ENV PYTHONPATH=/app
