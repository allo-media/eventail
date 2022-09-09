#!/usr/bin/env python
#
# MIT License
#
# Copyright (c) 2022 Groupe Allo-Media
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
from eventail.log_criticity import CRITICAL, INFO, NOTICE

MESSAGES = []


class Ping(Service):

    PREFETCH_COUNT = 10

    def on_unconfigured(self):
        self.publish_configuration("EchoMessagesRequested", {})

    def on_ready(self):
        self.healthcheck()
        self.ping()

    def on_EchoMessagesDefined(self, payload, _meta):
        MESSAGES.extend(payload["messages"])
        return True

    def on_EchoReturn(self, payload, conversation_id, status, correlation_id, _meta):
        self.log(
            INFO,
            "Got echo: {} {}".format(payload, correlation_id),
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
            self.logical_service + ".EchoReturn",
            correlation_id,
        )
        self.call_later(1, self.ping)

    def healthcheck(self):
        self.log(NOTICE, "I'm fine!", additional_fields={"healthcheck": "ok"})
        self.call_later(60, self.healthcheck)


if __name__ == "__main__":
    import logging

    logger = logging.getLogger("async_service")
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)

    service_name = sys.argv[1]
    urls = sys.argv[2:] if len(sys.argv) > 2 else ["amqp://localhost"]
    ping = ReconnectingSupervisor(
        Ping,
        urls,
        [],
        [service_name + ".EchoReturn"],
        service_name,
        config_routing_keys=["EchoMessagesDefined"],
    )
    print("To exit press CTRL+C")
    print("The application won't start to ping until you configured it.")
    print(
        "To configure it, use the publish_configuration.py Eventail command. The payload is given in this folder."
    )
    ping.run()
    print("Bye!")
