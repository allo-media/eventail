#!/usr/bin/env python
import argparse
import time

import pika
from pika.exceptions import (
    ChannelClosed,
    ConnectionClosed,
    AMQPConnectionError,
    AMQPHeartbeatTimeout,
)


class Logger:

    LOG_EXCHANGE = "logs"
    LOG_EXCHANGE_TYPE = "topic"

    def __init__(self, host, routing_keys):
        connection = pika.BlockingConnection(pika.URLParameters(host))
        channel = connection.channel()

        channel.exchange_declare(
            exchange=self.LOG_EXCHANGE,
            exchange_type=self.LOG_EXCHANGE_TYPE,
            durable=True,
        )

        # We declare a transient queue because we don't want to fill-up rabbitmq
        # with logs if the logger is down
        result = channel.queue_declare("", exclusive=True)
        queue_name = result.method.queue

        for key in routing_keys:
            channel.queue_bind(exchange="logs", queue=queue_name, routing_key=key)
        # Logger queue is auto ack for minimum overhead as we don't care losing some
        # messages (very rare as we rarely fail)
        channel.basic_consume(
            queue=queue_name, on_message_callback=self.callback, auto_ack=True
        )
        self._channel = channel
        self._connection = connection

    def callback(self, ch, method, properties, body):
        print("[{}] {}".format(method.routing_key, body.decode("utf-8")))

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
        description="Display selected logs in realtime on the given broker"
    )
    parser.add_argument("amqp_url", help="URL of the broker, including credentials")
    parser.add_argument(
        "--filter",
        help="Log patterns to subscribe to (default to all)",
        nargs="*",
        default=["#"],
    )
    args = parser.parse_args()
    expected_stop = False
    print("Ctrl-C to quit.")
    print("Subcribing to logs:", args.filter)
    while not expected_stop:
        try:
            logger = Logger(args.amqp_url, args.filter)
        except AMQPConnectionError:
            time.sleep(2)
            continue
        expected_stop = logger.run()
    print("bye!")
