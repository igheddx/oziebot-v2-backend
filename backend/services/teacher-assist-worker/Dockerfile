FROM python:3.12-slim
WORKDIR /app
COPY backend/packages/domain /app/packages/domain
COPY backend/packages/py-common /app/packages/py-common
COPY backend/services/api /app/services/api
COPY backend/services/teacher-assist-worker /app/services/teacher-assist-worker
RUN pip install --no-cache-dir --upgrade pip \
  && pip install --no-cache-dir -e /app/packages/domain \
  && pip install --no-cache-dir -e /app/packages/py-common \
  && pip install --no-cache-dir -e /app/services/api \
  && pip install --no-cache-dir -e /app/services/teacher-assist-worker
CMD ["python", "-m", "oziebot_teacher_assist_worker"]
