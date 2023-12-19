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
import argparse
import json
import pprint

import cbor2 as cbor

import pika
from pika.exceptions import (
    ChannelClosed,
    ConnectionClosed,
    AMQPConnectionError,
    AMQPHeartbeatTimeout,
)


class Inspector:
    def __init__(self, host, queue, count=0, save=False):
        connection = pika.BlockingConnection(pika.URLParameters(host))
        channel = connection.channel()
        self.save = save

        result = channel.queue_declare(queue, passive=True)
        queue_name = result.method.queue
        self._count = result.method.message_count if count == 0 else count
        self._seen = 0

        channel.basic_consume(
            queue=queue_name, on_message_callback=self.callback, auto_ack=False
        )

        self._channel = channel
        self._connection = connection

    def callback(self, ch, method, properties, body):
        decode = (
            json.loads if properties.content_type == "application/json" else cbor.loads
        )
        print("[{}]".format(method.routing_key))
        pprint.pprint(properties)
        pprint.pprint(method)
        print()
        if self.save:
            with open(
                method.routing_key
                + properties.headers["conversation_id"]
                + (
                    ".json"
                    if properties.content_type == "application/json"
                    else ".cbor"
                ),
                "wb",
            ) as payload:
                payload.write(body)
        else:
            pprint.pprint(decode(body))
        print("-----------")
        self._seen += 1
        if self._seen == self._count:
            self._channel.stop_consuming()
            self._connection.close()

    def run(self):
        try:
            self._channel.start_consuming()
        except KeyboardInterrupt:
            return True
        except (
            ChannelClosed,
            ConnectionClosed,
            AMQPConnectionError,
            AMQPHeartbeatTimeout,
        ):
            return False
        finally:
            if not self._connection.is_closed:
                self._connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Dump the content of a queue without consuming it."
    )
    parser.add_argument("amqp_url", help="URL of the broker, including credentials.")
    parser.add_argument("queue", help="Name of queue to inspect.")
    parser.add_argument(
        "--count",
        help="Number of message to dump (default is 0 = all).",
        type=int,
        default=0,
    )
    parser.add_argument(
        "--save", action="store_true", help="save payloads in original format."
    )
    # parser.add_argument(
    #     "--filter",
    #     help="Log patterns to subscribe to (default to all)",
    #     nargs="*",
    #     default=["#"],
    # )
    args = parser.parse_args()
    expected_stop = False
    print("Ctrl-C to quit.")
    print("Dumping queue:", args.queue)
    inspector = Inspector(args.amqp_url, args.queue, args.count, args.save)
    inspector.run()
    print("bye!")
