FROM python:3.10

RUN pip install --no-cache-dir -U pip

COPY requirements.txt requirements.dev.txt /tmp/

RUN pip install -r /tmp/requirements.dev.txt

EXPOSE 8000

WORKDIR /app
CMD ["python", "-m", "executor_service"]
