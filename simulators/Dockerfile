FROM python:3.11-slim

WORKDIR /app

COPY simulators/requirements.txt /app/simulators/requirements.txt
RUN pip install --no-cache-dir -r /app/simulators/requirements.txt

COPY common /app/common
COPY simulators /app/simulators

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# docker-compose.yml overrides this per-service (meter_sim.py vs tariff_sim.py)
CMD ["python", "simulators/meter_sim.py"]
