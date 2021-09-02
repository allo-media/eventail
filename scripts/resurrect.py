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
from typing import List, Tuple


import pika
from pika.exceptions import (
    ChannelClosed,
    ConnectionClosed,
    AMQPConnectionError,
    AMQPHeartbeatTimeout,
)


class Resurrection:
    def __init__(
        self, url: str, queue: str, batch_size: int = 1, count: int = 0
    ) -> None:
        connection = pika.BlockingConnection(pika.URLParameters(url))
        channel = connection.channel()

        self._batch_size = batch_size
        result = channel.queue_declare(queue, passive=True)
        channel.basic_qos(prefetch_count=self._batch_size)
        queue_name = result.method.queue
        self._count = result.method.message_count if count == 0 else count
        self._seen = 0
        self.messages: List[
            Tuple[pika.spec.Basic.Deliver, pika.spec.BasicProperties, bytes]
        ] = []

        channel.basic_consume(
            queue=queue_name, on_message_callback=self.callback, auto_ack=False
        )

        self._channel = channel
        self._connection = connection

    def callback(
        self,
        ch: pika.channel.Channel,
        method: pika.spec.Basic.Deliver,
        properties: pika.spec.BasicProperties,
        body: bytes,
    ) -> None:
        # we cache the message to avoid loops if
        # some resurrected messages come back dead again.
        self.messages.append((method, properties, body))
        print("Buffering message", method)
        self._seen += 1
        if self._seen == self._count:
            print("replay")
            self.replay()
            print("stop consuming")
            self._channel.stop_consuming()
        elif self._seen % self._batch_size == 0:
            print("replay batch")
            self.replay()

    def replay(self):
        for method, properties, body in self.messages:
            print("Replaying", method)
            print()
            self._channel.basic_publish(
                exchange=properties.headers["x-first-death-exchange"],
                routing_key=method.routing_key,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                    content_type=properties.content_type,
                    reply_to=properties.reply_to,
                    correlation_id=properties.correlation_id,
                    headers=properties.headers,
                ),
            )
            # Confirm consumption only if successfuly resent
            self._channel.basic_ack(method.delivery_tag)
        self.messages.clear()

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
        ) as e:
            print(e)
            return False
        else:
            return True
        finally:
            if not self._connection.is_closed:
                self._connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Resend dead letters.")
    parser.add_argument("amqp_url", help="URL of the broker, including credentials.")
    parser.add_argument("queue", help="Name of dead-letter queue.")
    parser.add_argument(
        "--count",
        help="Number of message to resurrect (default is 0 = all).",
        type=int,
        default=0,
    )
    parser.add_argument(
        "--batch_size",
        help="for more efficiency, if the messages are small, process them in batches of this size (default is 1).",
        type=int,
        default=1,
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
    print("Resurrecting from:", args.queue)
    inspector = Resurrection(args.amqp_url, args.queue, args.batch_size, args.count)
    if inspector.run():
        print("Done!")
    else:
        print("connection error (closed)")
