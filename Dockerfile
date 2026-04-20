FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "dbt-core==1.7.*" \
    "dbt-duckdb==1.7.*" \
    duckdb \
    pandas \
    pyarrow

COPY pipeline.py /app/pipeline.py
COPY bronze_loader.py /app/bronze_loader.py
COPY dbt_project/ /app/dbt_project/

CMD ["python", "pipeline.py"]
