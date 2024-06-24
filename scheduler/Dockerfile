#Arquivo scheduler/Dockerfile
FROM python:3.9-slim

WORKDIR /app

USER root

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY scheduler.py .

CMD ["python", "scheduler.py"]
