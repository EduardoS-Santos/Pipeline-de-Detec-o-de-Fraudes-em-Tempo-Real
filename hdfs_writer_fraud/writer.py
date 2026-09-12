import os
import json
import time
import logging
import io
import pandas as pd
from datetime import datetime
from kafka import KafkaConsumer
from hdfs import InsecureClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Variáveis de Ambiente configuráveis pelo Docker
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'transacoes.ok')
HDFS_TARGET_DIR = os.getenv('HDFS_TARGET_DIR', '/data/transacoes_ok')
GROUP_ID = os.getenv('CONSUMER_GROUP_ID', 'hdfs-writer-group')

# --- CONFIGURAÇÕES DE ALTA PERFORMANCE PARA BI / HDFS ---
# Acumula até 500.000 registros por lote Parquet
BATCH_SIZE = int(10000)  
# Ou aguarda até 5 minutos (300 segundos) para fazer o flush
FLUSH_INTERVAL_SEC = int(60) 

def safe_deserializer(m):
    return json.loads(m.decode('utf-8')) if m else None

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=['kafka:9092'],
    group_id=GROUP_ID,
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    max_poll_interval_ms=300000,
    value_deserializer=safe_deserializer
)

client = InsecureClient('http://namenode:9870', user='root')

buffer = []
last_flush_time = time.time()

logging.info(f"🚀 HDFS Writer ativo escutando [{KAFKA_TOPIC}] -> HDFS [{HDFS_TARGET_DIR}]")
logging.info(f"⚙️ Configuração do Lote: {BATCH_SIZE} msgs OU {FLUSH_INTERVAL_SEC}s de limite.")

for message in consumer:
    record = message.value
    if record:
        buffer.append(record)

    # 🎯 Dispara a gravação quando o lote enche (500k) ou estoura o tempo limite (5 minutos)
    if len(buffer) >= BATCH_SIZE or (time.time() - last_flush_time > FLUSH_INTERVAL_SEC and len(buffer) > 0):
        try:
            today_str = datetime.now().strftime('%Y-%m-%d')
            partition_path = f"{HDFS_TARGET_DIR}/date_partition={today_str}"
            
            # Garante que a pasta existe no HDFS
            client.makedirs(partition_path)

            file_name = f"part_{int(time.time() * 1000)}.parquet"
            hdfs_file_path = f"{partition_path}/{file_name}"

            # 🛠️ Converte o lote em memória (BytesIO) para Parquet com compressão Snappy
            df = pd.DataFrame(buffer)
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False, compression='snappy', engine='pyarrow')
            parquet_bytes = parquet_buffer.getvalue()

            # Escreve o arquivo consolidado no HDFS
            client.write(hdfs_file_path, data=parquet_bytes, overwrite=True)
            
            consumer.commit()
            logging.info(f"✅ Arquivo Parquet otimizado gerado com {len(buffer)} registros em {hdfs_file_path}")
            
            # Limpa o lote e atualiza o cronômetro
            buffer.clear()
            last_flush_time = time.time()

        except Exception as e:
            logging.error(f"❌ Erro ao gravar Parquet no HDFS: {e}")