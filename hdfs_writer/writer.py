import json
import time
import logging
from kafka import KafkaConsumer
from hdfs import InsecureClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

hdfs_client = InsecureClient('http://namenode:9870', user='root')

consumer = KafkaConsumer(
    'transacoes.ok',
    bootstrap_servers=['kafka:9092'],
    group_id='hdfs-writer-group',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None
)

logging.info("💾 Gravador HDFS Ativo!")

buffer = []
for message in consumer:
    tx_ok = message.value
    if not tx_ok:
        continue

    buffer.append(json.dumps(tx_ok))

    if len(buffer) >= 10:
        file_path = f"/data/transacoes_ok/batch_{int(time.time())}.json"
        content = "\n".join(buffer) + "\n"
        
        try:
            hdfs_client.write(file_path, data=content.encode('utf-8'), overwrite=True)
            logging.info(f"📁 Salvo lote com {len(buffer)} aprovadas no HDFS: {file_path}")
            consumer.commit()
            buffer.clear()
        except Exception as e:
            logging.error(f"❌ Erro ao escrever no HDFS: {e}")