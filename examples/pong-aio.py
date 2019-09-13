import asyncio
import sys

import uvloop
from async_service.aio import Service


class EchoService(Service):

    PREFETCH_COUNT = 10
    RETRY_DELAY = 2

    async def handle_command(self, command, message, reply_to, correlation_id):
        await self.log("debug", "Received {}".format(command))
        # auto-delegation pattern
        handler = getattr(self, command.split(".")[-1])
        if handler is not None:
            await handler(message, reply_to, correlation_id)
        else:
            # should never happens: means we misconfigured the routing keys
            await self.log("error", "unexpected message {}".format(command))
            await self.return_error(
                reply_to,
                {"reason": "unknown command", "message": "unknown {}".format(command)},
                correlation_id,
            )

    async def handle_event(self, event, payload):
        handler = getattr(self, event)
        if handler is not None:
            await handler(payload)
        else:
            # should never happens: means we misconfigured the routing keys
            await self.log("error", "unexpected message {}".format(event))

    async def EchoMessage(self, message, reply_to, correlation_id):
        await self.log("info", "Echoing {}".format(message))
        try:
            await self.return_success(reply_to, message, correlation_id, mandatory=True)
        except ValueError:
            await self.log("Error", f"Unroutable {reply_to}")

    async def ShutdownStarted(self, payload):
        await self.log("info", "Received signal for shutdown.")
        await self.stop()

    async def healthcheck(self) -> None:
        while True:
            await self.log("health", "I'm fine!")
            await asyncio.sleep(60)

    async def on_ready(self) -> None:
        self.create_task(self.healthcheck())


if __name__ == "__main__":
    uvloop.install()
    url = sys.argv[1] if len(sys.argv) > 1 else "amqp://localhost"

    loop = asyncio.get_event_loop()
    service = EchoService(
        url, ["ShutdownStarted"], ["pong.EchoMessage"], "pong", loop=loop
    )
    print("To exit press CTRL+C")
    loop.run_until_complete(service.run())  # auto reconnect in built-in
