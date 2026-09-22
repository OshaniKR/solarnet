"""
Tiny helper for pushing gauge metrics to the Prometheus Pushgateway.

Used by components that are NOT long-lived HTTP servers (the simulators, the Spark
job, the Airflow DAG) and therefore can't be scraped directly by Prometheus. The API
service exposes its own /metrics endpoint instead and is scraped directly -- see
api/main.py and observability/prometheus.yml.

Deliberately pushes WITHOUT a per-call grouping key (job name only), so each push
*replaces* the previous values for that job rather than accumulating a new series
group forever. Prometheus's own repeated scraping of the Pushgateway is what turns
these snapshots into a time series -- the Pushgateway itself only needs to hold the
latest value. (Grouping by e.g. batch_id would leak memory in the Pushgateway over
a long-running demo -- worth mentioning as a limitation in the report.)
"""
import os

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

PUSHGATEWAY_URL = os.environ.get("PUSHGATEWAY_URL", "http://pushgateway:9091")


def push_metrics(job: str, metrics: dict) -> None:
    registry = CollectorRegistry()
    for name, value in metrics.items():
        gauge = Gauge(name, f"{name} (pushed by {job})", registry=registry)
        gauge.set(value)
    try:
        push_to_gateway(PUSHGATEWAY_URL, job=job, registry=registry)
    except Exception:
        # Observability must never take down the pipeline it is observing.
        print(f'{{"level":"WARNING","component":"metrics_utils","message":"push to {PUSHGATEWAY_URL} failed"}}')
