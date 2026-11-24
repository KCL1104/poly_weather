# One container runs everything: order-book recorder, METAR poller and all scheduled
# jobs (`polyweather run-all`). On a home computer use compose.yaml, which mounts the
# data folder; on Railway the attached volume is picked up automatically.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install ".[s3]"

# Runs as root on purpose: Railway mounts volumes as root, and a folder shared from
# the host must be writable whoever owns it.
CMD ["polyweather", "run-all"]
