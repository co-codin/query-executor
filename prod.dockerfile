FROM python:3.8.7
ARG SERVICE_PORT=8000
RUN pip install --no-cache-dir -U pip

COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

RUN mkdir -p /usr/local/app/
WORKDIR /usr/local/app/

COPY config.py logger_config.py ./
COPY executor_service ./executor_service/

RUN mkdir -p /var/logs/
RUN mkdir logs

EXPOSE $SERVICE_PORT
CMD ["venv/bin/python", "-m", "executor_service"]

#CMD ["uvicorn", "app.main:app" , "--host", "0.0.0.0", "--port", "8000"]
#CMD ["python3","-m","app.main.py"]


