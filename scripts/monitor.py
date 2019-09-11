#!/usr/bin/env python
from typing import Any, Dict
import argparse
import pprint

from async_service.pika import Service


JSON_MODEL = Dict[str, Any]


class Monitor(Service):
    def handle_result(
        self, key: str, payload: JSON_MODEL, status: str, correlation_id: str
    ) -> None:
        print("Got a Result:", key)
        print("Correlation ID:", correlation_id)
        print("Status", status)
        print("Payload:")
        pprint.pprint(payload)
        print("---------")
        print()

    def handle_command(
        self, command: str, payload: JSON_MODEL, return_to: str, correlation_id: str
    ) -> None:
        print("Got a Command", command)
        print("Correlation ID", correlation_id)
        print("Return to", return_to)
        print("Payload:")
        pprint.pprint(payload)
        print("---------")
        print()

    def handle_event(self, event: str, payload: JSON_MODEL) -> None:
        print("Got an Event", event)
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
    monitor = Monitor(args.amqp_url, args.events, args.commands, "debug_monitor")
    monitor.use_exclusive_queues()
    print("Subscribing to events:", args.events)
    print("Subscribing to commands:", args.commands)
    print("Ctr-C to quit.")
    try:
        monitor.run()
    except KeyboardInterrupt:
        monitor.stop()
