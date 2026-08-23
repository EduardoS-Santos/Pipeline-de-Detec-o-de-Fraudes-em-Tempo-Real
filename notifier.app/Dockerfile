FROM python:3.11-slim

WORKDIR /app

# Instala o cliente Kafka e a biblioteca de requisições HTTP
RUN pip install --no-cache-dir kafka-python requests

# Copia o código do notificador
COPY notifier.py .

# Executa o notificador
CMD ["python", "notifier.py"]