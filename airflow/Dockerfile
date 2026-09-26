FROM apache/airflow:2.9.3-python3.11

COPY airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY --chown=airflow:root common /opt/airflow/common

ENV PYTHONPATH=/opt/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=false
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=false

# docker-compose.yml mounts ./airflow/dags -> /opt/airflow/dags as a live volume,
# so DAG edits on the host are picked up without rebuilding this image.
