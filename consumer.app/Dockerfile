FROM eclipse-temurin:17-jdk-alpine

# Instala Python, PIP, BASH e a biblioteca gcompat (soluciona o erro do Snappy)
RUN apk add --no-cache python3 py3-pip bash gcompat libc6-compat

WORKDIR /app

RUN pip install --no-cache-dir pyspark==3.5.0 --break-system-packages

COPY fraud_detection.py .

CMD ["python3", "fraud_detection.py"]