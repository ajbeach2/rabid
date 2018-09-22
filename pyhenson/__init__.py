import asyncio
import json
from henson import Application
from henson_amqp import AMQP
from . import settings

__all__ = ('app',)

amqp = AMQP()

# Consume messages
async def run(app, message):
	print(message)

app = Application('pyhenson', callback=run)
app.settings.from_object(settings)
amqp.init_app(app)
app.consumer = amqp.consumer()


# produce messages
async def _send_message(message,routing_key=None):
    # TODO: This should be done in a separate step.
    producer = amqp.producer()
    while True:
        serialized_message = json.dumps(message)
        await producer.send(serialized_message, routing_key=routing_key)


loop = asyncio.get_event_loop()
loop.run_until_complete(_send_message('hello world'))
loop.close()
