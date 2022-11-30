FROM python:3.8.7
ARG SERVICE_PORT=8000
RUN pip install --no-cache-dir -U pip

COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

COPY executor_service/ /app/executor_service/

EXPOSE $SERVICE_PORT

WORKDIR /app
CMD ["python", "-m", "executor_service"]
