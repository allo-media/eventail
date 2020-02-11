#!/usr/bin/env python
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
import sys

from eventail.async_service.pika import Service, ReconnectingSupervisor
from eventail.log_criticity import ERROR, INFO, NOTICE


class EchoService(Service):

    PREFETCH_COUNT = 10
    RETRY_DELAY = 2

    def on_EchoMessage(self, payload, conversation_id, reply_to, correlation_id):
        text = payload["message"].upper()
        self.log(
            INFO,
            "Echoing",
            "Sending back {}".format(text),
            conversation_id=conversation_id,
        )
        self.return_success(reply_to, {"echo": text}, conversation_id, correlation_id)

    def handle_returned_message(self, key, message, envelope):
        self.log(ERROR, "unroutable message", "{}.{}.{}".format(key, message, envelope))

    def on_ready(self):
        self.healthcheck()

    def healthcheck(self):
        self.log(NOTICE, "I'm fine!")
        self.call_later(60, self.healthcheck)


if __name__ == "__main__":
    # import logging

    # logger = logging.getLogger("async_service")
    # logger.addHandler(logging.StreamHandler())
    # logger.setLevel(logging.DEBUG)
    urls = sys.argv[1:] if len(sys.argv) > 1 else ["amqp://localhost"]
    echo = ReconnectingSupervisor(
        EchoService, urls, [], ["pong.EchoMessage"], "pong"
    )
    print("To exit press CTRL+C")
    echo.run()
    print("Bye!")
