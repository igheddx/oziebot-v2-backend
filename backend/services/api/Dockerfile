FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends curl \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY backend/packages/domain /app/packages/domain
COPY backend/packages/py-common /app/packages/py-common
COPY backend/services/strategy-engine /app/services/strategy-engine
COPY backend/services/api /app/services/api

RUN pip install --no-cache-dir --upgrade pip \
  && pip install --no-cache-dir -e /app/packages/domain \
  && pip install --no-cache-dir -e /app/packages/py-common \
  && pip install --no-cache-dir -e /app/services/strategy-engine \
  && pip install --no-cache-dir -e /app/services/api

ENV PYTHONPATH=/app/services/api/src
WORKDIR /app/services/api
RUN chmod +x /app/services/api/docker-entrypoint.sh
EXPOSE 8000
ENTRYPOINT ["/app/services/api/docker-entrypoint.sh"]
