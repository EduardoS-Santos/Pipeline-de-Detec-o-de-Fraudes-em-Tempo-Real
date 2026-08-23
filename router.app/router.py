import json
import math
import time
import logging
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def haversine(lat1, lon1, lat2, lon2):
    if None in (lat1, lon1, lat2, lon2):
        return 0.0
    R = 6371.0  # Raio da Terra em KM
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def safe_json_deserializer(m):
    if not m:
        return None
    try:
        return json.loads(m.decode('utf-8'))
    except Exception as e:
        logging.error(f"❌ Erro ao desserializar JSON: {e}")
        return None

consumer = KafkaConsumer(
    'transacoes.brutas',
    bootstrap_servers=['kafka:9092'],
    group_id='fraud-router-group',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    value_deserializer=safe_json_deserializer
)

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    acks='all',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

logging.info("⚡ Roteador de Transações Iniciado!")

while True:
    records_batch = consumer.poll(timeout_ms=1000, max_records=50)
    if not records_batch:
        continue

    for topic_partition, messages in records_batch.items():
        for msg in messages:
            tx = msg.value
            if not tx:
                continue

            # Extração dos dados do Schema Bruto
            tx_id = tx.get("transaction_id")
            user_id = tx.get("user_id")
            amount = tx.get("amount")
            lat, lon = tx.get("lat"), tx.get("lon")
            prev_lat, prev_lon = tx.get("prev_lat"), tx.get("prev_lon")

            # Cálculo de distância
            dist_km = round(haversine(lat, lon, prev_lat, prev_lon), 1)
            now_timestamp = int(time.time())

            # --- ROTEAMENTO CONFORME OS SCHEMAS ---
            if dist_km > 200.0:
                # Schema Transações Suspeitas
                payload_fraud = {
                    "transaction_id": tx_id,
                    "user_id": user_id,
                    "amount": round(amount, 2),
                    "status": "FLAGGED_FRAUD",
                    "distance_km": dist_km,
                    "target_email": "testenn8n2026@gmail.com",
                    "processed_at": now_timestamp
                }
                producer.send('transacoes.fraud', value=payload_fraud).get(timeout=5)
                logging.warning(f"🚨 SUSPEITA ({dist_km} km): {tx_id} -> transacoes.fraud")
            else:
                # Schema Transações Aprovadas
                payload_ok = {
                    "transaction_id": tx_id,
                    "user_id": user_id,
                    "amount": round(amount, 2),
                    "status": "APPROVED",
                    "distance_km": dist_km,
                    "processed_at": now_timestamp
                }
                producer.send('transacoes.ok', value=payload_ok).get(timeout=5)
                logging.info(f"✅ APROVADA ({dist_km} km): {tx_id} -> transacoes.ok")

        consumer.commit()