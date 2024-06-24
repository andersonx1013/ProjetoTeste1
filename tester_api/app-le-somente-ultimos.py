from flask import Flask, request, jsonify
import logging
import json
from kafka import KafkaProducer, KafkaConsumer
import os

app = Flask(__name__)

# Configuração do logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurações do Kafka
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'Kafka_teste:9092')
TOPIC_CADASTRO = os.environ.get('TOPIC_CADASTRO', 'cadastro-produtos')
TOPIC_PERSISTIDOS = os.environ.get('TOPIC_PERSISTIDOS', 'produtos-persistidos')

# Inicialização do KafkaProducer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def consume_messages(topic, timeout=5):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    messages = []
    try:
        while True:
            msg_pack = consumer.poll(timeout_ms=timeout*1000)
            if not msg_pack:
                break
            for tp, messages_list in msg_pack.items():
                for message in messages_list:
                    logger.info(f"Mensagem lida: {message.value}")
                    messages.append(message.value)
        logger.info(f"Total de mensagens lidas: {len(messages)}")
    except Exception as e:
        logger.error(f"Falha ao ler mensagens: {e}")
    finally:
        consumer.close()
    return messages

@app.route('/api/send', methods=['POST'])
def send_message():
    data = request.get_json()
    logger.info(f"Received data: {json.dumps(data, indent=2, ensure_ascii=False)}")

    if not data:
        logger.error("Invalid data received")
        return jsonify({"status": "error", "message": "Invalid data"}), 400

    try:
        producer.send(TOPIC_CADASTRO, value=data)
        producer.flush()
        logger.info("Dados enviados com sucesso para o Kafka")
        return jsonify({"status": "success", "message": "Data processed and sent to Kafka", "data": data}), 200
    except Exception as e:
        logger.error(f"Falha ao processar dados: {e}")
        return jsonify({"status": "error", "message": f"Failed to process data: {e}"}), 500

@app.route('/api/cadastro-produtos', methods=['GET'])
def get_cadastro_produtos():
    logger.info("===== /api/cadastro-produtos =======")
    try:
        messages = consume_messages(TOPIC_CADASTRO)
        logger.info(f"{len(messages)} mensagens lidas do Kafka")
        return jsonify({"status": "success", "messages": messages}), 200
    except Exception as e:
        logger.error(f"Falha ao ler mensagens: {e}")
        return jsonify({"status": "error", "message": f"Failed to read messages: {e}"}), 500

@app.route('/api/produtos-persistidos', methods=['GET'])
def get_persisted_products():
    logger.info("===== /api/produtos-persistidos =======")
    try:
        messages = consume_messages(TOPIC_PERSISTIDOS)
        logger.info(f"{len(messages)} mensagens lidas do Kafka")
        return jsonify({"status": "success", "messages": messages}), 200
    except Exception as e:
        logger.error(f"Falha ao ler mensagens: {e}")
        return jsonify({"status": "error", "message": f"Failed to read messages: {e}"}), 500

if __name__ == '__main__':
    logger.info("Iniciando a aplicação Flask")
    app.run(host='0.0.0.0', port=5001)
