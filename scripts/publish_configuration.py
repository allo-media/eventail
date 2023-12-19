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
import argparse
import json
import os.path

import cbor2 as cbor

from eventail.sync_publisher import Endpoint


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Publish a configuration event and its payload on the given broker"
    )
    parser.add_argument(
        "amqp_url", help="URL of the broker, including credentials", type=str
    )
    parser.add_argument("event", help="Configuration event name")
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

    endpoint = Endpoint(args.amqp_url, "debug_configuration_event_publisher")
    endpoint.publish_configuration(args.event, payload)
