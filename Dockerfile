# 1. 파이썬 실행 환경 설정
FROM python:3.9-slim

# 2. 컨테이너 내 작업 디렉토리 설정
WORKDIR /app

# 3. 라이브러리 설치 (캐시 최적화를 위해 먼저 수행)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. 전체 소스 코드 복사 (app 폴더 포함)
COPY . .

# 5. 서버 실행 (FastAPI 실행을 위해 uvicorn 사용)
# app/main.py에 app 인스턴스가 있으므로 'app.main:app'으로 지정합니다.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]