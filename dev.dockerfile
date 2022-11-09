FROM python:3.8.7

WORKDIR /usr/local/app/

RUN pip install --no-cache-dir -U pip

COPY config.py logger_config.py requirements.txt requirements.dev.txt ./
COPY executor_service ./executor_service/

RUN pip install -r requirements.dev.txt

EXPOSE 8000

CMD ["venv/bin/python", "-m", "executor_service"]
