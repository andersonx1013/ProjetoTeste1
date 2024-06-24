#Arquivo scheduler/scheduler.py
from kafka import KafkaConsumer
import json
import logging
from datetime import datetime
import requests
import time
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = 'Kafka_teste:9092'
TOPIC = 'cadastro-produtos'
API_ENDPOINT = 'http://api:5000/api/produtos'

failed_messages = []  # Lista para armazenar mensagens que falharam ao serem enviadas para a API
lock = threading.Lock()  # Lock para acessar a lista de mensagens falhadas

logger.info(f"Conectando ao broker Kafka em {KAFKA_BROKER}")

try:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None
    )
    logger.info("Conexão ao broker Kafka estabelecida com sucesso")
except Exception as e:
    logger.error(f"Falha ao conectar ao broker Kafka: {e}")
    consumer = None

def transform_data(data):
    required_fields = ["productId", "productName", "productDescription", "price", "currency", "stockQuantity", "category"]
    for field in required_fields:
        if field not in data:
            raise ValueError(f"Campo ausente: {field}")
    
    transformed_data = {
        "id": data["productId"],
        "name": data["productName"],
        "description": data["productDescription"],
        "pricing": {
            "amount": data["price"],
            "currency": data["currency"]
        },
        "availability": {
            "quantity": data["stockQuantity"],
            "timestamp": datetime.now().isoformat()
        },
        "category": data["category"]
    }
    return transformed_data

def send_to_api(data):
    while True:
        try:
            response = requests.post(API_ENDPOINT, json=data)
            response.raise_for_status()
            logger.info(f"Dados enviados para a API com sucesso: {response.status_code}")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Falha ao enviar dados para a API: {e}. Tentando novamente em 5 segundos... (Itens na fila: {len(failed_messages)})")
            time.sleep(5)
            return False

def process_failed_messages():
    global failed_messages
    while True:
        if failed_messages:
            logger.info(f"Tentando reenviar {len(failed_messages)} mensagens falhadas")
            with lock:
                for message in failed_messages[:]:
                    if send_to_api(message):
                        failed_messages.remove(message)
                        logger.info(f"Mensagem reenviada com sucesso: {message}")
                    else:
                        break  # Saia do loop se falhar novamente, tentará novamente na próxima rodada
        time.sleep(5)  # Espera antes de tentar novamente

# Thread para processar mensagens falhadas
threading.Thread(target=process_failed_messages, daemon=True).start()

logger.info("Iniciando o consumo de mensagens")

if consumer:
    for message in consumer:
        try:
            if message.value is None:
                logger.error("Mensagem vazia recebida, ignorando.")
                continue
            product_data = message.value
            logger.info(f"Mensagem original recebida: {product_data}")

            # Transformar os dados
            transformed_data = transform_data(product_data)
            logger.info(f"Mensagem transformada: {transformed_data}")

            # Enviar dados transformados para a API
            if not send_to_api(transformed_data):
                with lock:
                    failed_messages.append(transformed_data)
                    logger.info(f"Mensagens falhadas atualmente: {len(failed_messages)}")
                    for msg in failed_messages:
                        logger.info(f"Mensagem falhada: {json.dumps(msg, indent=2, ensure_ascii=False)}")

        except ValueError as e:
            logger.error(f"Falha ao processar a mensagem: {e}")
        except json.JSONDecodeError as e:
            logger.error(f"Falha ao decodificar JSON: {e}")
        except Exception as e:
            logger.error(f"Falha ao processar a mensagem: {e}")
