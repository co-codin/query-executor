FROM python:3.10.12-slim-bullseye AS builder

WORKDIR /tmp
COPY requirements.txt .
RUN pip3 install --upgrade pip && \
    pip3 install --no-cache-dir -r requirements.txt

FROM python:3.10.12-slim-bullseye

COPY --from=builder /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages
COPY executor_service /app/executor_service/
WORKDIR /app

CMD ["python", "-m", "executor_service"]
