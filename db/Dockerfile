#Arquivo db/Dockerfile
FROM python:3.9-slim

WORKDIR /db

USER root

RUN apt-get update && apt-get install -y netcat-openbsd libpq-dev gcc

COPY wait-for-it.sh /usr/bin/wait-for-it.sh
RUN chmod +x /usr/bin/wait-for-it.sh

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT ["sh", "-c", "/usr/bin/wait-for-it.sh --host=postgres --port=5432 --timeout=60 && python createDataBase.py"]
