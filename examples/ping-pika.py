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
from random import choice

from eventail.async_service.pika import Service, ReconnectingSupervisor
from eventail.log_criticity import CRITICAL, ERROR, INFO, NOTICE

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
    "bye!",
]


class Ping(Service):

    PREFETCH_COUNT = 10

    def __init__(self, host, logical_service):
        self.return_key = logical_service + ".EchoMessage"
        super().__init__(host, [], [self.return_key], logical_service)

    def on_ready(self):
        self.healthcheck()
        self.ping()

    def handle_result(self, key, message, conversation_id, status, correlation_id):
        self.log(
            INFO, "Received {} {}".format(key, status), conversation_id=conversation_id
        )
        if key == self.return_key:
            self.log(
                INFO,
                "Got echo: {} {}".format(message, correlation_id),
                conversation_id=conversation_id,
            )
        else:
            # should never happen: means we misconfigured the routing keys
            self.log(
                ERROR,
                "Unexpected message {} {}".format(key, status),
                conversation_id=conversation_id,
            )

    def handle_returned_message(self, key, message, envelope):
        self.log(CRITICAL, "unroutable {}.{}.{}".format(key, message, envelope))
        raise ValueError(f"Wrong routing key {key}")

    def ping(self):
        message = {"message": choice(MESSAGES)}
        conversation_id = correlation_id = "anyid" + str(self._message_number)
        self.log(
            INFO,
            "Sending: {} {}".format(message, self._message_number),
            conversation_id=conversation_id,
        )
        self.send_command(
            "pong.EchoMessage",
            message,
            conversation_id,
            self.return_key,
            correlation_id,
        )
        self.call_later(1, self.ping)

    def healthcheck(self):
        self.log(NOTICE, "I'm fine!")
        self.call_later(60, self.healthcheck)


if __name__ == "__main__":
    import logging

    logger = logging.getLogger("async_service")
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)

    service_name = sys.argv[1]
    urls = sys.argv[2:] if len(sys.argv) > 2 else ["amqp://localhost"]
    ping = ReconnectingSupervisor(Ping, urls, service_name)
    print("To exit press CTRL+C")
    ping.run()
    print("Bye!")
