import json
import logging
import requests
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def safe_deserializer(m):
    return json.loads(m.decode('utf-8')) if m else None

consumer = KafkaConsumer(
    'transacoes.fraud',
    bootstrap_servers=['kafka:9092'],
    group_id='n8n-notifier-group',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    value_deserializer=safe_deserializer
)

logging.info("📬 Notificador do n8n Ativo!")

for message in consumer:
    fraud_data = message.value
    if not fraud_data:
        continue

    try:
        logging.info(f"👉 Disparando Webhook para fraude: {fraud_data['transaction_id']}")
        res = requests.post(
            "http://n8n-orchestrator:5678/webhook/fraude-alert",
            json=fraud_data,
            timeout=5
        )
        if res.status_code == 200:
            logging.info(f"✅ Alerta enviado ao n8n com sucesso!")
            consumer.commit()
        else:
            logging.warning(f"⚠️ n8n respondeu com status: {res.status_code}")
    except Exception as e:
        logging.error(f"❌ Falha de conexão com o n8n: {e}")