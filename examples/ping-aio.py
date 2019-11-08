import asyncio
import sys
from random import choice

import uvloop
from eventail.async_service.aio import Service
from eventail.log_criticity import DEBUG, ERROR, INFO, NOTICE

MESSAGES = [
    "hello",
    "proof of concept",
    "how are you?",
    "ça cause français?",
    "allo?",
    "y'a quelqu'un?",
    "arrête de répéter ce que je dit !",
    "t'es lourd!",
    "stop that!",
    "so childish…",
]


class Ping(Service):

    PREFETCH_COUNT = 10

    def __init__(self, url, logical_service, loop=None):
        self.return_key = logical_service + ".EchoMessage"
        super().__init__(
            url, ["ShutdownStarted"], [self.return_key], logical_service, loop=loop
        )

    async def handle_result(
        self, key, message, conversation_id, status, correlation_id
    ):
        await self.log(
            DEBUG, f"Received {key} {status}", conversation_id=conversation_id
        )
        if key == self.return_key:
            await self.log(
                INFO,
                f"Got echo {message} {correlation_id}",
                conversation_id=conversation_id,
            )
        else:
            # should never happen: means we misconfigured the routing keys
            await self.log(
                ERROR,
                f"Unexpected message {key} {status}",
                conversation_id=conversation_id,
            )

    async def on_ShutdownStarted(self, payload, conversation_id):
        await self.log(INFO, "Received signal for shutdown.")
        await self.stop()

    async def ping(self):
        i = 1
        while True:
            message = {"message": choice(MESSAGES)}
            conversation_id = correlation_id = "anyid" + str(i)
            await self.log(
                INFO,
                f"Sending {message} {correlation_id}",
                conversation_id=conversation_id,
            )
            # Will raise ValueError if unroutable and we'll stop.
            await self.send_command(
                "pong.EchoMessage",
                message,
                conversation_id,
                self.return_key,
                correlation_id,
            )
            i += 1
            await asyncio.sleep(1)

    async def healthcheck(self) -> None:
        while True:
            await self.log(NOTICE, "I'm fine!")
            await asyncio.sleep(60)

    async def on_ready(self) -> None:
        self.create_task(self.healthcheck())
        self.create_task(self.ping())


if __name__ == "__main__":
    uvloop.install()
    service_name = sys.argv[1]
    urls = sys.argv[2:] if len(sys.argv) > 2 else ["amqp://localhost"]

    loop = asyncio.get_event_loop()
    service = Ping(urls, service_name, loop=loop)
    print("To exit press CTRL+C")
    loop.run_until_complete(service.run())  # auto reconnect in built-in
