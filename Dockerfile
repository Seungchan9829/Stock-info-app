FROM python:3.12

# 1. 작업 디렉토리
WORKDIR /app

# 2. pip 업그레이드 (선택)
RUN pip install --upgrade pip

# 3. 의존성 설치 전 requirements.txt 복사
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# 4. 앱 소스 복사
COPY . .

CMD gunicorn app:app \
    --bind 0.0.0.0:${PORT} \
    --workers ${WEB_CONCURRENCY} \
    --timeout ${GUNICORN_TIMEOUT}