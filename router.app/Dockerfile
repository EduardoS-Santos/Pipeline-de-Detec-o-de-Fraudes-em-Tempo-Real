FROM python:3.11-slim

WORKDIR /app

# Instala a biblioteca do Kafka
RUN pip install --no-cache-dir kafka-python lz4 requests

# Copia o código do roteador
COPY router.py .

# Executa o roteador
CMD ["python", "router.py"]