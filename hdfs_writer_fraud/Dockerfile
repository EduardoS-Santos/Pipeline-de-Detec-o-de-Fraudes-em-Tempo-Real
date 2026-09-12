FROM python:3.11-slim

WORKDIR /app

# Instala as bibliotecas do Kafka e do HDFS (WebHDFS)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY writer.py .

CMD ["python", "-u","writer.py"]