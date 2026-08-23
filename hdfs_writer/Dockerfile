FROM python:3.11-slim

WORKDIR /app

# Instala as bibliotecas do Kafka e do HDFS (WebHDFS)
RUN pip install --no-cache-dir kafka-python hdfs

COPY writer.py .

CMD ["python", "writer.py"]