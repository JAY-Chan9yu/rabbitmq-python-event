from time import sleep

from pika.exchange_type import ExchangeType

from event_broker import EventProducer


event_producer = EventProducer(
    vhost='default',
    rabbitmq_broker_url='127.0.0.1',
    rabbitmq_user='root',
    rabbitmq_password='root',
    port=5672
)

# Declare a queue
event_producer.declare_queue("t_msg_1")
# Declare a exchange
event_producer.declare_exchange(exchange_name='log', exchange_type=ExchangeType.topic)

try:
    while True:
        # Send a message to the queue
        event_producer.send_message(exchange="log", routing_key="log.1", body=b'Hello World!')
        sleep(1)

except KeyboardInterrupt:
    print('stop')
    event_producer.close()
