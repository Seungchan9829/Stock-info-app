# 어떤 이미지로 할지 설정
FROM python:3.12

# 작업 디렉토리 설정
WORKDIR /app

# 라이브러리 복사 , COPY <소스경로> <목적지 경로> : 
COPY requirements.txt .

# 패키지 설정
RUN pip install --no-cache-dir -r requirements.txt

# 4. 앱 소스 복사
COPY . .

# 컨테이너가 실행될 때 기본적으로 실행할 명령어 / 프로그램 지정
CMD ["python", "-m", "worker.main_worker"]
