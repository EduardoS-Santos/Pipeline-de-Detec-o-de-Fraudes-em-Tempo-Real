import json
import time
import logging
import requests
from datetime import datetime
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def safe_deserializer(m):
    return json.loads(m.decode('utf-8')) if m else None

# Aumento do max_poll_interval_ms para 10 minutos (600000ms) evitando timeout
consumer = KafkaConsumer(
    'transacoes.fraud',
    bootstrap_servers=['kafka:9092'],
    group_id='n8n-notifier-group',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    max_poll_interval_ms=600000, 
    value_deserializer=safe_deserializer
)

logging.info("📬 Notificador do n8n Ativo (Modo Lote Estrito - 500 msgs/dia)!")

buffer = []
daily_sent_count = 0
current_day = datetime.now().day

BATCH_LIMIT = 100
MAX_DAILY_BATCHES = 5

for message in consumer:
    fraud_data = message.value
    if not fraud_data:
        continue

    today = datetime.now().day
    if today != current_day:
        current_day = today
        daily_sent_count = 0
        logging.info("🔄 Virada de dia! Contador de envios zerado.")

    # Se já atingiu 5 lotes (500 e-mails) no dia, ignora novos processamentos
    if daily_sent_count >= MAX_DAILY_BATCHES:
        logging.warning("🛑 Cota diária de 500 alertas atingida! Commitando offset e aguardando...")
        consumer.commit()
        time.sleep(300) # Dorme 5 minutos antes de checar novidades
        continue

    buffer.append(fraud_data)

    if len(buffer) >= BATCH_LIMIT:
        try:
            logging.warning(f"🚨 Enviando LOTE #{daily_sent_count + 1}/5 ({len(buffer)} itens) para o n8n...")
            
            payload = {
                "batch_id": f"BATCH_{int(time.time())}",
                "total_records": len(buffer),
                "frauds": buffer
            }

            # Timeout estendido para aguardar o envio dos 100 e-mails em fila no n8n
            res = requests.post(
                "http://n8n-orchestrator:5678/webhook/fraude-alert",
                json=payload,
                timeout=300 
            )

            if res.status_code == 200:
                logging.info(f"✅ Lote #{daily_sent_count + 1} processado com SUCESSO!")
                consumer.commit()
                daily_sent_count += 1
                buffer.clear()
            else:
                logging.warning(f"⚠️ n8n respondeu com erro {res.status_code}")

        except Exception as e:
            logging.error(f"❌ Falha de conexão ao enviar para o n8n: {e}")