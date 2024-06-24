#Arquivo scheduler/config/settings.py
import logging

KAFKA_BROKER = 'Kafka_teste:9092'  # Use o nome do contêiner Kafka

TOPIC = 'cadastro-produtos'
API_ENDPOINT = 'http://api:5000/api/produtos'  # Use o nome do contêiner da API

# Configuração do logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

logger.debug(f"KAFKA_BROKER set to: {KAFKA_BROKER}")
logger.debug(f"TOPIC set to: {TOPIC}")
logger.debug(f"API_ENDPOINT set to: {API_ENDPOINT}")
