# syntax=docker/dockerfile:1
FROM python:3.14-slim

ARG PIP_INDEX_URL=https://pypi.org/simple
ENV PIP_INDEX_URL=${PIP_INDEX_URL} \
    PIP_NO_CACHE_DIR=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TICKFORGE_HTTP_HOST=0.0.0.0 \
    TICKFORGE_OUTPUT_DIR=/data

WORKDIR /app

COPY requirements-lock.txt ./
RUN pip install --no-cache-dir -r requirements-lock.txt

COPY tickforge ./tickforge
COPY config ./config
COPY scripts ./scripts
COPY tools ./tools
COPY samples ./samples

# The demo runs as root inside the container so that a bind-mounted host
# directory (./artifacts) is writable regardless of its UID.  A production
# image should use a fixed non-root UID and a volume with matching ownership.
RUN mkdir -p /data
EXPOSE 9100

HEALTHCHECK --interval=10s --timeout=3s --start-period=45s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:9100/healthz', timeout=2)"

ENTRYPOINT ["python", "-m", "tickforge"]
CMD ["live", "--config", "config/default.toml", "--duration", "900", "--out", "/data"]
