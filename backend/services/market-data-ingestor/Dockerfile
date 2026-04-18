FROM python:3.12-slim
WORKDIR /app
COPY backend/packages/domain /app/packages/domain
COPY backend/packages/py-common /app/packages/py-common
COPY backend/services/market-data-ingestor /app/services/market-data-ingestor
RUN pip install --no-cache-dir --upgrade pip \
  && pip install --no-cache-dir -e /app/packages/domain \
  && pip install --no-cache-dir -e /app/packages/py-common \
  && pip install --no-cache-dir -e /app/services/market-data-ingestor
CMD ["python", "-m", "oziebot_market_data_ingestor"]
