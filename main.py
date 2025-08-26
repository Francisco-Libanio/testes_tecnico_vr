#!/usr/bin/env python
import json
import logging
import uuid
import logging
from logging import StreamHandler

from flask import Flask, request, jsonify
import pika
from marshmallow import Schema, fields, ValidationError, validate

app = Flask(__name__)


# ===== variaveis =====
RABBITMQ_HOST = "localhost"
EXCHANGE_NAME = "vr_exchange"
QUEUE_NAME = "fila.notificacao.entrada.francisco"
ROUTING_KEY = QUEUE_NAME 

notificacoes = {}  # dict[str, dict] => traceId -> dados

TIPOS_VALIDOS = ("EMAIL", "SMS", "PUSH")

logging.basicConfig(level=logging.DEBUG,
                    format='%(levelname)s - %(message)s',
                    handlers=[StreamHandler()])


logging.info('INICIANDO')
# definindo o objeto para verificação
class NotificacaoSchema(Schema):
    mensagemId = fields.UUID(required=False)
    conteudoMensagem = fields.String(
        required=True,
        validate=validate.Length(min=1, error="conteudoMensagem não pode ser vazio")
    )
    tipoNotificacao = fields.String(
        required=True,
        validate=validate.OneOf(TIPOS_VALIDOS, error=f"tipoNotificacao deve ser um de {TIPOS_VALIDOS}")
    )

schema = NotificacaoSchema()

# ===== Utilitários de RabbitMQ =====
def _open_channel():
    """Abre conexão e canal novos (thread-safe para Flask)."""
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    # Conferindo a existencia da fila e da exchange
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct", durable=False)
    channel.queue_declare(queue=QUEUE_NAME, durable=False)
    channel.queue_bind(queue=QUEUE_NAME, exchange=EXCHANGE_NAME, routing_key=ROUTING_KEY)

    return connection, channel

def publica_no_rmq(body: str):
    """Publica e fecha a conexão (simples e seguro para múltiplas threads/processos)."""
    connection, channel = _open_channel()
    try:
        channel.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=ROUTING_KEY,
            body=body,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,  # persistindo a mensagem na fila
            ),
        )
    finally:
        try:
            channel.close()
        finally:
            connection.close()


@app.route("/send", methods=["POST"])
def send_message():
    # Verificando se a requisição é Content-Type
    if not request.is_json:
        logging.error('Erro em Content-Type verifique a requisição')
        return jsonify({"status": "error", "message": "Content-Type deve ser application/json"}), 415
        

    # verificando se a requisição contem um json válido
    payload = request.get_json(silent=True)
    if payload is None:
        logging.error('JSON invalido')
        return jsonify({"status": "error", "message": "JSON inválido"}), 400

    # Validando com com marshmallow
    try:
        data = schema.load(payload)  # valida e normaliza (ex.: UUID)
    except ValidationError as err:
        logging.error(f'erro de validação {err.messages}')
        return jsonify({"status": "error", "errors": err.messages}), 400
    


    # Gera IDs
    traceId = str(uuid.uuid4())
    mensagemId = str(data.get("mensagemId", uuid.uuid4()))

    # rastreamento
    notificacoes[traceId] = {
        "mensagemId": mensagemId,
        "conteudoMensagem": data["conteudoMensagem"],
        "tipoNotificacao": data["tipoNotificacao"],
        "status": "RECEBIDO",
    }

    logging.debug(f'CHECANDO {notificacoes}')
    # breakpoint
    # Montando body JSON final para a fila
    body = json.dumps(
        {
            "traceId": traceId,
            "mensagemId": mensagemId,
            "conteudoMensagem": data["conteudoMensagem"],
            "tipoNotificacao": data["tipoNotificacao"],
        },
        ensure_ascii=False,
    )

    # Publicando no RabbitMQ
    try:
        publica_no_rmq(body)
    except pika.exceptions.AMQPError as e:
        app.logger.exception("Falha ao publicar no RabbitMQ")
        return jsonify({"status": "error", "message": f"Falha ao publicar no RabbitMQ: {e.__class__.__name__}"}), 502

    app.logger.info("Enviado para RabbitMQ: %s", body)
    return jsonify({"status": "accepted", "traceId":traceId, "mensagemId":mensagemId}), 202 #Accepted



@app.route("/api/notificacao/status/<traceId>", methods=["GET"])
def consultar_status(traceId: str):
    notificacao = notificacoes.get(traceId)

    if not notificacao:
        return jsonify({"status": "error", "message": f"traceId {traceId} não encontrado"}), 404

    resposta = {
        "traceId": traceId,
        "mensagemId": notificacao["mensagemId"],
        "conteudoMensagem": notificacao["conteudoMensagem"],
        "tipoNotificacao": notificacao["tipoNotificacao"],
        "status": notificacao["status"],  # sempre o mais recente
    }

    return jsonify(resposta), 200
    

if __name__ == "__main__":
    
    app.run(host="127.0.0.1", port=5000, debug=True)
