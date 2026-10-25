# syntax=docker/dockerfile:1

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Render's build context is this directory; keep the package path intact so
# `dispatcher.app:app` and the relative imports inside the package resolve.
COPY . ./dispatcher

EXPOSE 8080

# One process, deliberately. The worker pool, queue, circuit state, catalog
# cache and rate-limit buckets are all in memory; running multiple uvicorn
# workers would give each process its own independent view of the fleet.
# Uvicorn proxy headers stay off so caller-IP resolution is explicit: Render
# sets the first X-Forwarded-For entry to the real client IP and leaves
# caller-supplied entries after it. The app reads only that first entry.
CMD ["sh", "-c", "exec uvicorn dispatcher.app:app --host 0.0.0.0 --port ${PORT:-8080}"]
