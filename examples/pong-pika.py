#!/usr/bin/env python
import sys

from async_service.pika import Service, ReconnectingSupervisor


class EchoService(Service):

    PREFETCH_COUNT = 10

    def handle_command(self, command, message, return_to, correlation_id):
        self.log("debug", "Received {}".format(command))
        if command.split(".")[-1] == 'EchoMessage':
            self.log("info", "Echoing {}".format(message))
            self.return_success(return_to, message, correlation_id)
        else:
            # should never happens: means we misconfigured the routing keys
            self.log("error", "unexpected message {}".format(command))
            self.return_error(
                return_to,
                {"reason": "unknown command", "message": "unknown {}".format(command)},
                correlation_id)

    def handle_returned_message(self, key, message, envelope):
        self.log("error", "unroutable {}.{}.{}".format(
            key, message, envelope))

    def on_ready(self):
        self.healthcheck()

    def healthcheck(self):
        self.log("health", "I'm fine!")
        self.call_later(60, self.healthcheck)


if __name__ == '__main__':
    # import logging
    # logger = logging.getLogger("async_service")
    # logger.addHandler(logging.StreamHandler())
    # logger.setLevel(logging.DEBUG)
    url = sys.argv[1] if len(sys.argv) > 1 else "amqp://localhost"
    echo = ReconnectingSupervisor(EchoService, url, [], ["pong.EchoMessage"], "pong")
    print('To exit press CTRL+C')
    echo.run()
    print("Bye!")
