#!/usr/bin/env python
import argparse
import json
import pprint
import time

import cbor

import pika
from pika.exceptions import (
    ChannelClosed,
    ConnectionClosed,
    AMQPConnectionError,
    AMQPHeartbeatTimeout,
)


class Inspector:

    def __init__(self, host, queue, count=0):
        connection = pika.BlockingConnection(pika.URLParameters(host))
        channel = connection.channel()

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
        decode = json.loads if properties.content_type == 'application/json' else cbor.loads
        print("[{}]".format(method.routing_key))
        pprint.pprint(properties)
        pprint.pprint(method)
        print()
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
    parser.add_argument("--count", help="Number of message to dump (default is 0 = all).", type=int, default=0)
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
    inspector = Inspector(args.amqp_url, args.queue, args.count)
    inspector.run()
    print("bye!")
