FROM python:3.11-slim

# spark-submit needs a JVM; pip-installed pyspark provides the rest of the Spark
# distribution (including the spark-submit launcher itself). procps provides `ps`,
# which Spark's own load-spark-env.sh shells out to on startup -- without it here
# (python:3.11-slim has no `ps`), executor registration silently breaks and the
# driver/executor heartbeat NPEs forever (BlockManagerId.executorId() on a null id).
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jdk-headless procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY streaming/requirements.txt /app/streaming/requirements.txt
RUN pip install --no-cache-dir -r /app/streaming/requirements.txt

COPY common /app/common
COPY streaming /app/streaming

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# docker-compose.yml sets the real spark-submit command (with --packages for the
# Kafka/Postgres connectors); this is just a standalone fallback.
CMD ["spark-submit", "--master", "local[*]", "--packages", \
     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3", \
     "streaming/spark_job.py"]
