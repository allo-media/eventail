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
"""
A base class implementing AM service architecture and its requirements for a synchronous publisher Endpoint.
"""
import logging
import os
import socket
import time
from typing import Any, Dict, List

import cbor
from kombu import Connection, Exchange
from kombu.pools import producers, set_limit

from eventail.log_criticity import CRITICITY_LABELS

JSON_MODEL = Dict[str, Any]
LOGGER = logging.getLogger("sync_endpoint")
set_limit(2)  # two connections are more than enough; `async_service` uses just one.


class Endpoint:
    """A synchronous publishing endpoint for AlloMedia EDA."""

    ID = os.getpid()
    HOSTNAME = socket.gethostname()
    EVENT_EXCHANGE = "events"
    LOG_EXCHANGE = "logs"
    EVENT_EXCHANGE_TYPE = "topic"
    LOG_EXCHANGE_TYPE = "topic"

    def __init__(self, amqp_urls: List[str], logical_service: str) -> None:
        """Initialize endpoint.

        * ``amqp_urls`` is a list of broker urls that will be tried in turn (round robin style) to send messages.
        * ``logical_service``: the logical service this endpoint belongs to.
        """

        self.logical_service = logical_service

        self._connection = Connection(
            amqp_urls, transport_options={"confirm_publish": True}
        )
        self.event_exchange = Exchange(
            self.EVENT_EXCHANGE, type=self.EVENT_EXCHANGE_TYPE, durable=True
        )
        self.log_exchange = Exchange(
            self.LOG_EXCHANGE, self.LOG_EXCHANGE_TYPE, durable=True
        )
        self._force_json = False

    def force_json(self):
        """Force serialization of payload into JSON."""
        self._force_json = True

    def log(
        self, criticity: int, short: str, full: str = "", conversation_id: str = ""
    ) -> None:
        """Log to the log bus.

        Parameters are unicode strings, except for the `criticity` level,
        which is an int in the syslog scale.
        """
        # no persistent messages, no retry
        level_name = CRITICITY_LABELS[criticity % 8]
        log = {
            "version": "1.1",
            "short_message": short,
            "full_message": full,
            "level": criticity,
            "_levelname": level_name,
            "host": f"{self.logical_service}@{self.HOSTNAME}",
            "timestamp": time.time(),
            "_conversation_id": conversation_id,
            "_logical_service": self.logical_service,
            "_worker_pid": self.ID,
        }
        LOGGER.debug("Application logged: %s\n%s", short, full)

        with producers[self._connection].acquire(block=True) as producer:
            producer.publish(
                log,
                delivery_mode=1,  # not persistent
                exchange=self.log_exchange,
                serializer="json",
                routing_key="{}.{}".format(self.logical_service, level_name),
                declare=[self.log_exchange],
                retry=False,
            )

    def publish_event(
        self, event: str, message: JSON_MODEL, conversation_id: str
    ) -> None:
        """Publish an event on the bus.

        The ``event`` is the name of the event,
        and the `message` is any data conforming to the JSON model.
        """

        with producers[self._connection].acquire(block=True) as producer:
            headers = {"conversation_id": str(conversation_id)}
            producer.publish(
                message if self._force_json else cbor.dumps(message),
                delivery_mode=2,  # persistent
                exchange=self.event_exchange,
                routing_key=event,
                declare=[self.event_exchange],
                headers=headers,
                content_type=None if self._force_json else "application/cbor",
                content_encoding=None if self._force_json else "binary",
                serializer="json" if self._force_json else None,
                retry=True,
                retry_policy={
                    "interval_start": 0,  # First retry immediately,
                    "interval_step": 1,  # then increase by 1s for every retry.
                    "interval_max": 30,  # but don't exceed 30s between retries.
                },
            )
