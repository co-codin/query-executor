FROM python:3.10

WORKDIR /app

COPY requirements.txt requirements.dev.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.dev.txt

COPY executor_service ./executor_service/
CMD ["python", "-m", "executor_service"]

