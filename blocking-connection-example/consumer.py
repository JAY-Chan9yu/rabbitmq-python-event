from event_broker import EventConsumer


def callback(ch, method, properties, body):
    # body = pickle.loads(body)
    print(f"[x] Received {body}")


basic_message_receiver = EventConsumer(
    vhost='default',
    rabbitmq_broker_url='127.0.0.1',
    rabbitmq_user='root',
    rabbitmq_password='root'
)

# basic_message_receiver.channel.exchange_declare(exchange='log', exchange_type='topic')

try:
    # Consume the message that was sent.
    basic_message_receiver.consume_messages("t_msg_1", callback)
    # Close connections.
    basic_message_receiver.close()

except KeyboardInterrupt:
    print('stop')
    basic_message_receiver.close()
