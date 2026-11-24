# One container runs everything: order-book recorder, METAR poller and all scheduled
# jobs (`polyweather run-all`). Mount a volume and point POLYWEATHER_DATA_DIR at it;
# on Railway the attached volume is picked up automatically.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install ".[s3]"

# Runs as root on purpose: Railway mounts volumes as root.
CMD ["polyweather", "run-all"]
