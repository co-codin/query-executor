FROM python:3.10

WORKDIR /tmp
COPY requirements.txt requirements.dev.txt ./

RUN pip3 install --upgrade pip && \
    pip3 install --no-cache-dir -r requirements.txt

EXPOSE 8000

WORKDIR /app
CMD ["python", "-m", "executor_service"]
