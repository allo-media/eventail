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
from typing import Any, Dict
import argparse
import pprint

from eventail.async_service.pika import Service


JSON_MODEL = Dict[str, Any]


class Monitor(Service):
    def handle_result(
        self,
        key: str,
        payload: JSON_MODEL,
        conversation_id: str,
        status: str,
        correlation_id: str,
    ) -> None:
        print("Got a Result:", key)
        print("conversation ID:", conversation_id)
        print("Correlation ID:", correlation_id)
        print("Status", status)
        print("Payload:")
        pprint.pprint(payload)
        print("---------")
        print()

    def handle_command(
        self,
        command: str,
        payload: JSON_MODEL,
        conversation_id: str,
        reply_to: str,
        correlation_id: str,
    ) -> None:
        print("Got a Command", command)
        print("conversation ID:", conversation_id)
        print("Correlation ID", correlation_id)
        print("Return to", reply_to)
        print("Payload:")
        pprint.pprint(payload)
        print("---------")
        print()

    def handle_event(
        self, event: str, payload: JSON_MODEL, conversation_id: str
    ) -> None:
        print("Got an Event", event)
        print("conversation ID:", conversation_id)
        print("Payload:")
        pprint.pprint(payload)
        print("---------")
        print()


if __name__ == "__main__":
    # import logging
    # logger = logging.getLogger("async_service")
    # logger.addHandler(logging.StreamHandler())
    # logger.setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser(
        description="Monitor selected Events and/or Commands on the given broker"
    )
    parser.add_argument("amqp_url", help="URL of the broker, including credentials")
    parser.add_argument(
        "--events",
        help="Event patterns to subscribe to (default to all)",
        nargs="*",
        default=["#"],
    )
    parser.add_argument(
        "--commands",
        help="Command patterns to subscribe to (default to all)",
        nargs="*",
        default=["#"],
    )
    args = parser.parse_args()
    monitor = Monitor([args.amqp_url], args.events, args.commands, "debug_monitor")
    monitor.use_exclusive_queues()
    print("Subscribing to events:", args.events)
    print("Subscribing to commands:", args.commands)
    print("Ctr-C to quit.")
    try:
        monitor.run()
    except KeyboardInterrupt:
        monitor.stop()
