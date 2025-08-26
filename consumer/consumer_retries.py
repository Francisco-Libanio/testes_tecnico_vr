import pika
import json
import time
import random
import logging
from logging import StreamHandler


logging.basicConfig(
    level=logging.DEBUG,
    format='%(levelname)s - %(message)s',
    handlers=[StreamHandler()],
)
notificacoes = {}


def callback_retry(ch, method, properties, body):
    message = json.loads(body.decode())
    traceId = message.get('traceId')
    logging.info(f'[RETRY] Recebido para reprocessamento: {message}')

    # Simula atraso antes do reprocessamento
    time.sleep(2)

    # determinando falha
    if random.random() < 0.2:
        logging.error('[RETRY]  Reprocessamento falhou novamente')
        if traceId in notificacoes:
            notificacoes[traceId]['status'] = 'FALHA_FINAL_REPROCESSAMENTO'

        # Publicando na Dead Letter Queue
        ch.basic_publish(
            exchange='vr_exchange',
            routing_key='fila.notificacao.dlq.francisco',
            body=json.dumps(message).encode(),
        )
        logging.info(
            f'[RETRY] Mensagem enviada para DLQ: fila.notificacao.dlq.francisco'
        )

    else:
        logging.info('[RETRY] Reprocessamento bem-sucedido')
        if traceId in notificacoes:
            notificacoes[traceId]['status'] = 'PROCESSADO_INTERMEDIARIO'

        # Publica na próxima etapa (validacao)
        ch.basic_publish(
            exchange='vr_exchange',
            routing_key='fila.notificacao.validacao.francisco',
            body=json.dumps(message).encode(),
        )
        logging.info(
            f'[RETRY] Mensagem enviada para validação: fila.notificacao.validacao.francisco'
        )

    # Confirmando processamento da mensagem
    ch.basic_ack(delivery_tag=method.delivery_tag)


def consume_retry():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost')
    )
    channel = connection.channel()

    # Exchange principal
    channel.exchange_declare(exchange='vr_exchange', exchange_type='direct')

    # Fila de retry
    channel.queue_declare(queue='fila.notificacao.retry.francisco')
    channel.queue_bind(
        queue='fila.notificacao.retry.francisco', exchange='vr_exchange'
    )

    # Fila DLQ
    channel.queue_declare(queue='fila.notificacao.dlq.francisco')
    channel.queue_bind(
        queue='fila.notificacao.dlq.francisco', exchange='vr_exchange'
    )

    # Fila de validação
    channel.queue_declare(queue='fila.notificacao.validacao.francisco')
    channel.queue_bind(
        queue='fila.notificacao.validacao.francisco', exchange='vr_exchange'
    )

    channel.basic_consume(
        queue='fila.notificacao.retry.francisco',
        on_message_callback=callback_retry,
        auto_ack=False,
    )

    print('[RETRY] Esperando mensagens de retry. Para sair, pressione CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    consume_retry()
