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
from typing import Any, Dict, List
import argparse
import json
import os.path
import time

import cbor2 as cbor
import pika

from eventail.async_service.pika import Service


class EventSender(Service):
    def __init__(
        self,
        urls: List[str],
        event: str,
        payload: Dict[str, Any],
        use_json: bool = False,
    ) -> None:
        super().__init__(urls, [], [], "debug_event_publisher")
        self.event = event
        self.payload = payload
        if use_json:
            self.use_json()

    def on_ready(self):
        self.publish_event(self.event, self.payload, "debug" + str(time.time()))

    def on_delivery_confirmation(self, method_frame: pika.frame.Method) -> None:
        confirmation_type: str = method_frame.method.NAME.split(".")[1].lower()
        if confirmation_type == "ack":
            print("Message successfuly sent")
        else:
            print("The broker refused the message")
        self.stop()


if __name__ == "__main__":
    # import logging
    # logger = logging.getLogger("async_service")
    # logger.addHandler(logging.StreamHandler())
    # logger.setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser(
        description="Publish an Event and its payload on the given broker"
    )
    parser.add_argument(
        "amqp_url", help="URL of the broker, including credentials", type=str
    )
    parser.add_argument("event", help="Event Name")
    parser.add_argument(
        "payload",
        help="The path to the file containing the payload, in JSON or CBOR format (from file extension).",
    )
    args = parser.parse_args()
    _, ext = os.path.splitext(args.payload)
    unserialize = json.loads if ext == ".json" else cbor.loads
    with open(args.payload, "rb") as ins:
        data = ins.read()
    payload = unserialize(data)  # type: ignore
    event_sender = EventSender(
        [args.amqp_url], args.event, payload, use_json=ext == "json"
    )
    event_sender.run()
