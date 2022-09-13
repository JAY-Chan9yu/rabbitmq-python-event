# 참고 : https://github.com/lukebakken/pika-1296
import asyncio

from random import randrange

from async_event_broker import RabbitMQ


async def do_random():
	while True:
		await asyncio.sleep(1)
		print("[*] sleep 1 second!")


async def do_rabbitmq():
	print('----- start rabbitmq -----')
	rmq = RabbitMQ()
	rmq.start()
	rmq.consume_keywords(on_message)


def on_message(_unused_channel, basic_deliver, properties, body):
	# print('Received message # %s from %s: %s', basic_deliver.delivery_tag, properties.app_id, body)

	async def test():
		random_val = randrange(0, 10)

		if random_val < 5:
			# 5초 뒤에 출력 (비동기적으로 실행 하는지 테스트)
			await asyncio.sleep(5)
			print(f'------------------[sleep] Received message # {basic_deliver.delivery_tag} from {properties.app_id}: {body}')
		else:
			await asyncio.sleep(1)
			print(f' Received message # {basic_deliver.delivery_tag} from {properties.app_id}: {body}')

	asyncio.create_task(test())


async def main():
	random_task = asyncio.create_task(do_random())
	asyncio.create_task(do_rabbitmq())
	await asyncio.gather(random_task)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
