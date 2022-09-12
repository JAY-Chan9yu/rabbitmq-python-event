from typing import Callable

import pika
from pika.exchange_type import ExchangeType


class BasicPikaClient:

    def __init__(
        self,
        vhost: str,
        rabbitmq_broker_url: str,
        rabbitmq_user: str,
        rabbitmq_password: str,
        port: int = 5672
    ):
        self._url = rabbitmq_broker_url
        self._port = port
        self._vhost = vhost
        self._cred = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(self._url, self._port, self._vhost, self._cred)
        )
        self.channel = self.connection.channel()

    def close(self):
        self.channel.close()
        self.connection.close()


class EventProducer(BasicPikaClient):
    """
    이벤트 생성자
    """

    def declare_queue(self, queue_name: str):
        """queue 선언, 존재하면 pass"""
        print(f"Trying to declare queue({queue_name})...")
        self.channel.queue_declare(queue=queue_name)

    def declare_exchange(self, exchange_name: str, exchange_type: ExchangeType):
        """exchange 선언, 존재하면 pass"""
        print(f"Trying to declare queue({exchange_name})...")
        self.channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type.name)

    def send_message(self, exchange: str, routing_key: str, body: bytes):
        self.channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=body
        )
        print(f"Sent message. Exchange: {exchange}, Routing Key: {routing_key}, Body: {body}")


class EventConsumer(BasicPikaClient):
    """
    이벤트 소비자
    """

    def get_message(self, queue: str):
        """하나의 메세지만 가져와서 처리 """
        method_frame, header_frame, body = self.channel.basic_get(queue)

        if method_frame:
            print(method_frame, header_frame, body)
            self.channel.basic_ack(method_frame.delivery_tag)
            return method_frame, header_frame, body
        else:
            print('No message returned')

    def consume_messages(self, queue: str, callback_func: Callable):
        """메세지를 계속 소비 """
        self.channel.basic_consume(queue=queue, on_message_callback=callback_func, auto_ack=True)
        print(' [*] Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()
