FROM python:3.8.7

WORKDIR /usr/local/app/

RUN pip install --no-cache-dir -U pip

COPY .env config.py logger_config.py requirements.txt ./
COPY models ./models/
COPY executor_service ./executor_service/

RUN pip install -r requirements.txt

EXPOSE 8000

CMD ["venv/bin/python", "-m", "executor_service"]
