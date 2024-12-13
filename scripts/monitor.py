#!/usr/bin/env python
#
# MIT License
#
# Copyright (c) 2018-2024 Groupe Allo-Media
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
import os
import pprint

import cbor2 as cbor
from eventail.async_service.pika import Service


JSON_MODEL = Dict[str, Any]


class Monitor(Service):
    def __init__(self, save, filter_fields, *args, **kwargs):
        self.save = save
        self.filter_fields = filter_fields
        super().__init__(*args, **kwargs)

    def dump(self, key, conversation_id, payload):
        if self.save:
            filename = f"{key}.{conversation_id}.cbor"
            if os.path.exists(filename):
                filename = f"{key}.{conversation_id}.2.cbor"
            with open(filename, "wb") as output:
                cbor.dump(payload, output)
            print("Payload saved as", filename)
        else:
            print("Payload:")
            pprint.pprint(payload)

    def _should_skip(self, payload):
        return any(payload.get(field) != value for field, value in self.filter_fields)

    def handle_result(
        self,
        key: str,
        payload: JSON_MODEL,
        conversation_id: str,
        status: str,
        correlation_id: str,
        meta: Dict[str, str],
    ) -> None:
        if self._should_skip(payload):
            return
        print("Got a Result:", key)
        print("Conversation ID:", conversation_id)
        print("Correlation ID:", correlation_id)
        print("Status", status)
        print("Metadata:", meta)
        self.dump(key + "-result", conversation_id, payload)
        print("---------")
        print()

    def handle_command(
        self,
        command: str,
        payload: JSON_MODEL,
        conversation_id: str,
        reply_to: str,
        correlation_id: str,
        meta: Dict[str, str],
    ) -> None:
        if self._should_skip(payload):
            return
        print("Got a Command", command)
        print("Conversation ID:", conversation_id)
        print("Correlation ID", correlation_id)
        print("Return to", reply_to)
        print("Metadata:", meta)
        self.dump(command, conversation_id, payload)
        print("---------")
        print()

    def handle_event(
        self,
        event: str,
        payload: JSON_MODEL,
        conversation_id: str,
        meta: Dict[str, str],
    ) -> None:
        if self._should_skip(payload):
            return
        print("Got an Event", event)
        print("Conversation ID:", conversation_id)
        print("Metadata:", meta)
        self.dump(event, conversation_id, payload)
        print("---------")
        print()

    def handle_config(
        self,
        config: str,
        payload: JSON_MODEL,
        meta: Dict[str, str],
    ) -> bool:
        if config != "MonitorStarted" and self._should_skip(payload):
            return True
        print("Got a configuration", config)
        print("Metadata:", meta)
        self.dump(config, "", payload)
        print("---------")
        print()
        return True

    def on_unconfigured(self):
        # Trick to unlock the monitor.
        self.publish_configuration("MonitorStarted", {})

    def on_ready(self) -> None:
        print("Started")


def fields(string: str):
    return string.split("=")


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
        help="Event patterns to subscribe to (default to none)",
        nargs="*",
        default=[],
    )
    parser.add_argument(
        "--commands",
        help="Command patterns to subscribe to (default to none)",
        nargs="*",
        default=[],
    )
    parser.add_argument(
        "--configurations",
        help="Configuration patterns to subscribe to (default to none)",
        nargs="*",
        default=[],
    )
    parser.add_argument(
        "--save", action="store_true", help="Save payloads in CBOR format."
    )
    parser.add_argument(
        "--filter_fields",
        help="Filter payload for specific fields values, for example client_id=123 campaign_id=456",
        nargs="*",
        type=fields,
        default=[],
    )
    args = parser.parse_args()
    monitor = Monitor(
        args.save,
        args.filter_fields,
        [args.amqp_url],
        args.events,
        args.commands,
        "debug_monitor",
        config_routing_keys=args.configurations + ["MonitorStarted"],
    )
    monitor.use_exclusive_queues()
    print("Subscribing to events:", args.events)
    print("Subscribing to commands:", args.commands)
    print("Subscribing to configurations:", args.configurations)
    print("Press Ctrl-C to quit.")
    try:
        monitor.run()
    except KeyboardInterrupt:
        monitor.stop()
