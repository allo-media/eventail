#
# MIT License
#
# Copyright (c) 2018-2019 Groupe Allo-Media
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
import asyncio
import sys

import uvloop
from eventail.async_service.aio import Service
from eventail.log_criticity import INFO, NOTICE


class EchoService(Service):

    PREFETCH_COUNT = 10
    RETRY_DELAY = 2
    HEARTBEAT = 120

    async def on_EchoMessage(
        self, message, conversation_id, reply_to, correlation_id, _meta
    ):
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

    async def on_ShutdownStarted(self, payload, conversation_id):
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
