import asyncio
import sys

import aiohttp
import uvloop
from py_eda_tools.async_service.aio import Service


class EchoService(Service):

    PREFETCH_COUNT = 25
    RETRY_DELAY = 2

    async def on_EchoMessage(self, message, conversation_id, reply_to, correlation_id):
        await self.log("info", "Echoing {}".format(message))
        headers = {
            "Authorization": "Token d6a3115d12e8307e52b2ab5377de3504b5502cdd",
            "Accept": "application/json",
        }
        async with self.http_client.get(
            # "https://preprod-scribr.allo-media.net/api/v1/call/102-1567780009.7958414/",
            # "https://preprod-scribr.allo-media.net/api/v1/transcription/102-1567777440.7775469/latest/",
            "https://preprod-scribr.allo-media.net/api/v1/customer-path/182/",
            headers=headers,
        ) as r:
            message["suprise"] = await r.json()

        try:
            await self.return_success(
                reply_to, message, conversation_id, correlation_id, mandatory=True
            )
        except ValueError as e:
            if e.args[0] == 404:
                await self.log("Error", f"Unroutable {reply_to}")
            else:
                raise

    async def on_ShutdownStarted(self, payload):
        await self.log("info", "Received signal for shutdown.")
        await self.stop()

    async def healthcheck(self) -> None:
        while True:
            await self.log("health", "I'm fine!")
            await asyncio.sleep(60)

    async def on_ready(self) -> None:
        self.http_client = (
            aiohttp.ClientSession()
        )  # should be called from async function
        self.create_task(self.healthcheck())

    async def stop(self):
        await super().stop()
        await self.http_client.close()


if __name__ == "__main__":
    uvloop.install()
    url = sys.argv[1] if len(sys.argv) > 1 else "amqp://localhost"

    loop = asyncio.get_event_loop()
    service = EchoService(
        [url], ["ShutdownStarted"], ["pong.EchoMessage"], "pong", loop=loop
    )
    print("To exit press CTRL+C")
    loop.run_until_complete(service.run())  # auto reconnect in built-in
