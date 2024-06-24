from flask import Flask, request, jsonify
import psycopg2
import os
from kafka import KafkaProducer
import json
import logging
import time

app = Flask(__name__)

# Configuração do PostgreSQL
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")

# Configuração do Kafka
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "Kafka_teste:9092")
TOPIC = os.getenv("TOPIC", "produtos-persistidos")

# Configuração do logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Tentar se conectar ao Kafka até 5 vezes
producer = None
for attempt in range(5):
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logger.info("Conexão com o Kafka estabelecida com sucesso")
        break
    except Exception as e:
        logger.error(f"Tentativa {attempt + 1} de conexão com Kafka falhou: {e}")
        time.sleep(5)

if producer is None:
    raise Exception("Não foi possível conectar ao Kafka após várias tentativas")

def persist_data(data):
    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)
        cur = conn.cursor()
        insert_query = """
        INSERT INTO produtos (id, name, description, pricing_amount, pricing_currency, availability_quantity, availability_timestamp, category)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
        """
        cur.execute(insert_query, (
            data["id"],
            data["name"],
            data["description"],
            data["pricing"]["amount"],
            data["pricing"]["currency"],
            data["availability"]["quantity"],
            data["availability"]["timestamp"],
            data["category"]
        ))
        conn.commit()
        cur.close()
        conn.close()
        logger.debug("Data persisted successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to persist data: {e}")
        return False

def produce_to_kafka(data):
    try:
        logger.debug("Tentando enviar dados para o Kafka")
        future = producer.send(TOPIC, value=data)
        result = future.get(timeout=10)  # Aguarde a confirmação da mensagem
        logger.debug(f"Dados enviados para o Kafka com sucesso: {result}")
    except Exception as e:
        logger.error(f"Falha ao produzir dados para o Kafka: {e}")
        raise

@app.route('/api/produtos', methods=['POST'])
def add_produto():
    data = request.get_json()
    if not data:
        logger.error("Dados inválidos recebidos: Nenhum dado recebido")
        return jsonify({"status": "error", "message": "Invalid data"}), 400

    logger.info(f"Dados recebidos: {json.dumps(data, indent=2, ensure_ascii=False)}")

    # Persistir os dados no PostgreSQL
    if persist_data(data):
        # Produzir os dados persistidos no Kafka
        try:
            produce_to_kafka(data)
        except Exception as e:
            logger.error(f"Failed to produce to Kafka: {e}")
            return jsonify({"status": "error", "message": f"Failed to produce to Kafka: {e}"}), 500
        return jsonify({"status": "success", "data": data}), 200
    else:
        logger.error("Failed to persist data")
        return jsonify({"status": "error", "message": "Failed to persist data"}), 500

@app.route('/api/test_kafka', methods=['GET'])
def test_kafka():
    try:
        future = producer.send(TOPIC, value={"message": "test"})
        result = future.get(timeout=10)
        return jsonify({"status": "success", "message": f"Kafka is reachable: {result}"}), 200
    except Exception as e:
        logger.error(f"Failed to reach Kafka: {e}")
        return jsonify({"status": "error", "message": f"Failed to reach Kafka: {e}"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
