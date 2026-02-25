# docker/dockerfile.backend
FROM python:3.11-slim

# 작업 디렉토리
WORKDIR /app

# backend 코드 복사
COPY backend /app/backend

# 의존성 설치
RUN pip install --no-cache-dir \
    fastapi \
    uvicorn[standard] \
    redis

# 포트 오픈
EXPOSE 8000

# 백엔드 서버 실행
CMD ["uvicorn", "backend.api_server:app", "--host", "0.0.0.0", "--port", "8000"]
