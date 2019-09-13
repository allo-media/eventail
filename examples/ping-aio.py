import asyncio
import sys
from random import choice

import uvloop
from async_service.aio import Service

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

    async def handle_result(self, key, message, status, correlation_id):
        await self.log("debug", f"Received {key} {status}")
        if key == self.return_key:
            await self.log("info", f"Got echo {message} {correlation_id}")
        else:
            # should never happen: means we misconfigured the routing keys
            await self.log("error", f"Unexpected message {key} {status}")

    async def handle_event(self, event, payload):
        handler = getattr(self, event)
        if handler is not None:
            await handler(payload)
        else:
            # should never happens: means we misconfigured the routing keys
            await self.log("error", "unexpected message {}".format(event))

    async def ShutdownStarted(self, payload):
        await self.log("info", "Received signal for shutdown.")
        await self.stop()

    async def ping(self):
        i = 1
        while True:
            message = {"message": choice(MESSAGES)}
            correlation_id = "anyid" + str(i)
            await self.log("info", f"Sending {message} {correlation_id}")
            # Will raise ValueError if unroutable and we'll stop.
            await self.send_command(
                "pong.EchoMessage", message, self.return_key, correlation_id
            )
            i += 1
            await asyncio.sleep(1)

    async def healthcheck(self) -> None:
        while True:
            await self.log("health", "I'm fine!")
            await asyncio.sleep(60)

    async def on_ready(self) -> None:
        self.create_task(self.healthcheck())
        self.create_task(self.ping())


if __name__ == "__main__":
    uvloop.install()
    service_name = sys.argv[1]
    url = sys.argv[2] if len(sys.argv) > 2 else "amqp://localhost"

    loop = asyncio.get_event_loop()
    service = Ping(url, "ping", loop=loop)
    print("To exit press CTRL+C")
    loop.run_until_complete(service.run())  # auto reconnect in built-in
