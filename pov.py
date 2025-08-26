import pika
import logging
import json
from flask import Flask, request, jsonify
import pika

app = Flask(__name__)

# Configuração do RabbitMQ
RABBITMQ_HOST = 'localhost'
EXCHANGE_NAME = 'vr_exchange'
QUEUE_NAME = 'vr_fila'

# Conectar ao RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()
channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='direct')
channel.queue_declare(queue=QUEUE_NAME)
channel.queue_bind(queue=QUEUE_NAME, exchange=EXCHANGE_NAME)

@app.route('/send', methods=['POST'])
def send_message():
    data = request.json
    message = data.get('message', '')

    if not message:
        return jsonify({"status": "error", "message": "No message provided"}), 400

    # Serializar para JSON
    body = json.dumps({"message": message})

    channel.basic_publish(exchange=EXCHANGE_NAME, routing_key=QUEUE_NAME, body=body)
    print(f'Sent: {body}')
    return jsonify({"status": "success", "message": "Message sent to RabbitMQ"}), 200

if __name__ == '__main__':
    app.run(debug=True)

