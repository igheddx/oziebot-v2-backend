FROM python:3.12-slim
WORKDIR /app
COPY backend/packages/domain /app/packages/domain
COPY backend/packages/py-common /app/packages/py-common
COPY backend/services/execution-engine /app/services/execution-engine
RUN pip install --no-cache-dir --upgrade pip \
  && pip install --no-cache-dir -e /app/packages/domain \
  && pip install --no-cache-dir -e /app/packages/py-common \
  && pip install --no-cache-dir -e /app/services/execution-engine
CMD ["python", "-m", "oziebot_execution_engine"]
