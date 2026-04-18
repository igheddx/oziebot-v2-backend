FROM python:3.12-slim
WORKDIR /app
COPY backend/packages/domain /app/packages/domain
COPY backend/packages/py-common /app/packages/py-common
COPY backend/services/alerts-worker /app/services/alerts-worker
RUN pip install --no-cache-dir --upgrade pip \
  && pip install --no-cache-dir -e /app/packages/domain \
  && pip install --no-cache-dir -e /app/packages/py-common \
  && pip install --no-cache-dir -e /app/services/alerts-worker
CMD ["python", "-m", "oziebot_alerts_worker"]
