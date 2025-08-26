import json
import logging
import random
import time
from logging import StreamHandler

import pika

logging.basicConfig(
    level=logging.DEBUG,
    format='%(levelname)s - %(message)s',
    handlers=[StreamHandler()],
)

notificacoes = {}


def callback(ch, method, properties, body):
    message = json.loads(body.decode())
    traceId = message.get('traceId')

    logging.info(f'[CONSUMER] Recebido: {message}')

    # 10-15% de chance de falha
    if random.random() < 0.15:
        logging.info('[CONSUMER] Falha simulada no processamento inicial')
        # notificacoes[traceId]["status"] = "FALHA_PROCESSAMENTO_INICIAL"
        # logging.info(f'checando {notificacoes[traceId]["status"]}')
        # breakpoint()
        # Publica na retry queue
        ch.basic_publish(
            exchange='vr_exchange',
            routing_key='fila.notificacao.retry.francisco',
            body=json.dumps(message).encode(),
        )
        logging.info(
            '[CONSUMER] Mensagem enviada para retry: fila.notificacao.retry.francisco'
        )

    else:
        # Simulando processamento
        logging.info('[CONSUMER] Processando mensagem...')
        time.sleep(5)

        # notificacoes[traceId]["status"] = "PROCESSADO_INTERMEDIARIO"
        # breakpoint()
        # Publicar na fila de validação
        ch.basic_publish(
            exchange='vr_exchange',
            routing_key='fila.notificacao.validacao.francisco',
            body=json.dumps(message).encode(),
        )
        logging.info(
            '[CONSUMER] Mensagem enviada para validação: fila.notificacao.validacao.francisco'
        )

    # Confirma processamento da mensagem
    ch.basic_ack(delivery_tag=method.delivery_tag)


def consume():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost')
    )
    channel = connection.channel()

    # Exchange principal
    channel.exchange_declare(exchange='vr_exchange', exchange_type='direct')

    # Fila de entrada
    channel.queue_declare(queue='fila.notificacao.entrada.francisco')
    channel.queue_bind(
        queue='fila.notificacao.entrada.francisco', exchange='vr_exchange'
    )

    # Fila de retry
    channel.queue_declare(queue='fila.notificacao.retry.francisco')
    channel.queue_bind(
        queue='fila.notificacao.retry.francisco', exchange='vr_exchange'
    )

    # Fila de validação
    channel.queue_declare(queue='fila.notificacao.validacao.francisco')
    channel.queue_bind(
        queue='fila.notificacao.validacao.francisco', exchange='vr_exchange'
    )

    channel.basic_consume(
        queue='fila.notificacao.entrada.francisco',
        on_message_callback=callback,
        auto_ack=False,  # iusando basic_ack
    )

    print('[CONSUMER] Esperando mensagens. Para sair, pressione CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    consume()
