import asyncio
import sys

import uvloop
from async_service.aio import Service


class EchoService(Service):

    PREFETCH_COUNT = 10

    async def handle_command(self, command, message, return_to, correlation_id):
        await self.log("debug", "Received {}".format(command))
        if command.split(".")[-1] == "EchoMessage":
            await self.log("info", "Echoing {}".format(message))
            try:
                await self.return_success(
                    return_to, message, correlation_id, mandatory=True
                )
            except ValueError:
                await self.log("Error", f"Unroutable {return_to}")
        else:
            # should never happens: means we misconfigured the routing keys
            await self.log("error", "unexpected message {}".format(command))
            await self.return_error(
                return_to,
                {"reason": "unknown command", "message": "unknown {}".format(command)},
                correlation_id,
            )

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
    service = EchoService(url, [], ["pong.EchoMessage"], "pong", loop=loop)
    print("To exit press CTRL+C")
    loop.run_until_complete(service.run())  # auto reconnect in built-in
