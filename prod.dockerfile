FROM python:3.8.7

COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

COPY executor_service/ /app/executor_service/

WORKDIR /app
CMD ["python", "-m", "executor_service"]
