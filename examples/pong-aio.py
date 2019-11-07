import asyncio
import sys

import uvloop
from py_eda_tools.async_service.aio import Service
from py_eda_tools.async_service.log_criticity import INFO, NOTICE


class EchoService(Service):

    PREFETCH_COUNT = 10
    RETRY_DELAY = 2

    async def on_EchoMessage(self, message, conversation_id, reply_to, correlation_id):
        assert "message" in message, "missing key 'message' in message!"
        await self.log(INFO, f"Echoing {message}", conversation_id=conversation_id)
        try:
            await self.return_success(
                reply_to, message, conversation_id, correlation_id, mandatory=True
            )
        except ValueError:
            await self.log(
                "Error", f"Unroutable {reply_to}", conversation_id=conversation_id
            )

    async def on_ShutdownStarted(self, payload):
        await self.log(INFO, "Received signal for shutdown.")
        await self.stop()

    async def healthcheck(self) -> None:
        while True:
            await self.log(NOTICE, "I'm fine!")
            await asyncio.sleep(60)

    async def on_ready(self) -> None:
        self.create_task(self.healthcheck())


if __name__ == "__main__":
    uvloop.install()
    urls = sys.argv[1:] if len(sys.argv) > 1 else ["amqp://localhost"]

    loop = asyncio.get_event_loop()
    service = EchoService(
        urls, ["ShutdownStarted"], ["pong.EchoMessage"], "pong", loop=loop
    )
    print("To exit press CTRL+C")
    loop.run_until_complete(service.run())  # auto reconnect in built-in
