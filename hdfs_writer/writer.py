import json
import time
import logging
import pandas as pd
from kafka import KafkaConsumer
from hdfs import InsecureClient

logging.basicConfig(level=logging.INFO)
hdfs_client = InsecureClient('http://namenode:9870', user='root')

consumer = KafkaConsumer(
    'transacoes.ok',
    bootstrap_servers=['kafka:9092'],
    group_id='hdfs-writer-group',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None
)

buffer = []
LAST_FLUSH = time.time()
BATCH_SIZE = 100000  # 👈 Grava a cada 100 mil transações
FLUSH_INTERVAL = 60  # 👈 Ou a cada 60 segundos

for message in consumer:
    tx_ok = message.value
    if tx_ok:
        buffer.append(tx_ok)

    now = time.time()
    # Só grava se atingir 100k mensagens OU estourar 1 minuto
    if len(buffer) >= BATCH_SIZE or (now - LAST_FLUSH >= FLUSH_INTERVAL and len(buffer) > 0):
        
        # 1. Converte o lote em DataFrame do Pandas
        df = pd.DataFrame(buffer)
        
        # 2. Converte para o formato Parquet compactado
        parquet_bytes = df.to_parquet(index=False, compression='snappy')
        
        # 3. Caminho particionado por data (Padrão de Data Lake)
        partition_date = time.strftime("%Y-%m-%d")
        file_path = f"/data/transacoes_ok/date={partition_date}/batch_{int(now)}.parquet"
        
        try:
            # Escrita direta do arquivo Parquet no HDFS
            hdfs_client.write(file_path, data=parquet_bytes, overwrite=True)
            logging.info(f"📁 Lote de {len(buffer)} msgs salvo com SUCESSO no HDFS: {file_path}")
            
            consumer.commit()
            buffer.clear()
            LAST_FLUSH = now
        except Exception as e:
            logging.error(f"❌ Erro ao escrever Parquet no HDFS: {e}")