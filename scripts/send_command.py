#!/usr/bin/env python
from typing import Any, Dict, List
import argparse
import json
import pprint
import os.path

import cbor

from py_eda_tools.async_service.pika import Service


JSON_MODEL = Dict[str, Any]


class RPC(Service):
    def __init__(
        self,
        urls: List[str],
        service_command: str,
        payload: JSON_MODEL,
        use_json: bool = False,
    ) -> None:
        super().__init__(urls, [], ["debug.return"], "debug_cmd_sender")
        self.service_command = service_command
        self.payload = payload
        self.use_exclusive_queues()  # Important !!!
        if use_json:
            self.use_json()

    def on_ready(self):
        self.send_command(
            self.service_command, self.payload, "debug", "debug.return", "1"
        )

    def handle_result(
        self,
        key: str,
        payload: JSON_MODEL,
        conversation_id: str,
        status: str,
        correlation_id: str,
    ):
        if correlation_id == "1":
            print("Correlation ID is O.K.")
        if conversation_id == "debug":
            print("Conversation ID is O.K.")
        if status == "success":
            print("Success!")
        else:
            print("Error!")
        pprint.pprint(payload)
        self.stop()


if __name__ == "__main__":
    # import logging
    # logger = logging.getLogger("async_service")
    # logger.addHandler(logging.StreamHandler())
    # logger.setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser(
        description="Send a service command and its payload on the given broker and waits for its result."
    )
    parser.add_argument("amqp_url", help="URL of the broker, including credentials")
    parser.add_argument("command", help="Command in the form service.command")
    parser.add_argument(
        "payload",
        help="The path to the file containing the payload, in JSON or CBOR format (from file extension).",
    )
    args = parser.parse_args()
    _, ext = os.path.splitext(args.payload)
    unserialize = json.loads if ext == ".json" else cbor.loads
    with open(args.payload, "rb") as ins:
        data = ins.read()
    payload = unserialize(data)
    rpc = RPC([args.amqp_url], args.command, payload, use_json=ext == "json")
    rpc.run()
