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
import asyncio
import json
import os
import signal
import socket
import traceback
from contextlib import asynccontextmanager
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Coroutine,
    Dict,
    List,
    Optional,
    Sequence,
)
from urllib.parse import urlencode

import aiormq
import cbor
from aiormq import exceptions, spec
from aiormq.abc import DeliveredMessage

from eventail.gelf import GELF
from eventail.log_criticity import ALERT, EMERGENCY, ERROR, WARNING

JSON_MODEL = Dict[str, Any]
HEADER = Dict[str, str]


class Service:

    ID = os.getpid()
    HOSTNAME = socket.gethostname()
    EVENT_EXCHANGE = "events"
    CMD_EXCHANGE = "commands"
    LOG_EXCHANGE = "logs"
    EVENT_EXCHANGE_TYPE = "topic"
    CMD_EXCHANGE_TYPE = "topic"
    LOG_EXCHANGE_TYPE = "topic"
    RETRY_DELAY = 5  # in seconds
    #: Heartbeat interval, must be superior to the expected blocking processing time (in seconds).
    #: Beware that the actual delay is negotiated with the broker, and the lower value is taken, so
    #: configure Rabbitmq accordingly.
    HEARTBEAT = 60
    #: In production, experiment with higher prefetch values
    #: for higher consumer throughput
    PREFETCH_COUNT = 3

    def __init__(
        self,
        amqp_urls: List[str],
        event_routing_keys: Sequence[str],
        command_routing_keys: Sequence[str],
        logical_service: str,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.

        :param str amqp_url: The AMQP url to connect with

        """
        self._urls = amqp_urls
        self._event_routing_keys = event_routing_keys
        self._command_routing_keys = command_routing_keys
        self.logical_service = logical_service
        self.loop: asyncio.AbstractEventLoop = (
            loop if loop is not None else asyncio.get_running_loop()
        )
        self.exclusive_queues = False
        self._serialize: Callable[..., bytes] = cbor.dumps
        self._mime_type = "application/cbor"
        self._should_reconnect = True
        self.stopped = asyncio.Event(loop=loop)
        self._connection: aiormq.Connection
        self._channel: aiormq.Channel
        self._log_channel: aiormq.Channel
        self._event_consumer_tag: str
        self._command_consumer_tag: str

        for s in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT):
            self.loop.add_signal_handler(s, lambda: self.create_task(self.stop()))

    def on_connection_closed(self, closing: asyncio.Future) -> None:
        if self._should_reconnect:
            self.create_task(self.connect())
        else:
            self.stopped.set()

    async def on_message(self, message: DeliveredMessage) -> None:

        properties = message.header.properties

        headers: Dict[str, bytes] = properties.headers
        decoder = cbor if properties.content_type == "application/cbor" else json
        routing_key: str = message.delivery.routing_key
        exchange: str = message.delivery.exchange
        if headers is None or "conversation_id" not in headers:
            self.log(EMERGENCY, f"Missing headers on {routing_key}")
            # unrecoverable error, send to dead letter
            message.channel.basic_nack(message.delivery.delivery_tag, requeue=False)
            return
        # the underlying lib uses bytes
        conversation_id = headers["conversation_id"].decode("ascii")
        try:
            payload: JSON_MODEL = decoder.loads(message.body) if message.body else None
        except ValueError:
            await self.log(
                EMERGENCY,
                "Unable to decode payload for {}".format(routing_key),
                conversation_id=conversation_id,
            )
            # Unrecoverable, put to dead letter
            await message.channel.basic_nack(
                message.delivery.delivery_tag, requeue=False
            )
            return

        if exchange == self.CMD_EXCHANGE:
            correlation_id = properties.correlation_id
            reply_to = properties.reply_to
            status = headers.get("status", b"").decode("ascii") if headers else ""
            if not (reply_to or status):
                await self.log(
                    EMERGENCY,
                    f"invalid enveloppe for command/result: {routing_key}",
                    conversation_id=conversation_id,
                )
                # Unrecoverable, put to dead letter
                await message.channel.basic_nack(
                    message.delivery.delivery_tag, requeue=False
                )
                return
            if reply_to:
                async with self.ack_policy(
                    message.channel,
                    message.delivery,
                    conversation_id,
                    reply_to,
                    correlation_id,
                ):
                    await self.handle_command(
                        routing_key, payload, conversation_id, reply_to, correlation_id
                    )
            else:
                async with self.ack_policy(
                    message.channel,
                    message.delivery,
                    conversation_id,
                    reply_to,
                    correlation_id,
                ):
                    await self.handle_result(
                        routing_key, payload, conversation_id, status, correlation_id
                    )
        else:
            async with self.ack_policy(
                message.channel, message.delivery, conversation_id, "", ""
            ):
                await self.handle_event(routing_key, payload, conversation_id)

    @asynccontextmanager
    async def ack_policy(
        self,
        ch: aiormq.Channel,
        deliver: spec.Basic.Deliver,
        conversation_id: str,
        reply_to: str,
        correlation_id: str,
    ) -> AsyncGenerator[None, None]:
        try:
            yield None
        except Exception:
            error = traceback.format_exc()
            await self.log(
                ALERT,
                f"Unhandled error while processing message {deliver.routing_key}",
                error,
                conversation_id=conversation_id,
            )
            # retry once
            if not deliver.redelivered:
                await ch.basic_nack(delivery_tag=deliver.delivery_tag, requeue=True)
            else:
                # dead letter
                await self.log(
                    EMERGENCY,
                    f"Giving up on {deliver.routing_key}",
                    error,
                    conversation_id=conversation_id,
                )
                await ch.basic_nack(delivery_tag=deliver.delivery_tag, requeue=False)
        else:
            await ch.basic_ack(delivery_tag=deliver.delivery_tag)

    async def _emit(
        self,
        exchange: str,
        routing_key: str,
        message: JSON_MODEL,
        conversation_id: str,
        mandatory: bool,
        reply_to: str = "",
        correlation_id: str = "",
        headers: Optional[HEADER] = None,
    ) -> None:
        """Send a message.

        The ``message`` is any data conforming to the JSON model.

        If the broker is in trouble, retry regularly until it accepts the message.

        If the message is unroutable, a ValueError is raised.
        """
        if headers is None:
            headers = {}
        headers["conversation_id"] = conversation_id
        while True:
            try:
                await self._channel.basic_publish(
                    self._serialize(message),
                    exchange=exchange,
                    routing_key=routing_key,
                    mandatory=mandatory,
                    properties=spec.Basic.Properties(
                        delivery_mode=2,  # make message persistent
                        content_type=self._mime_type,
                        reply_to=reply_to,
                        correlation_id=correlation_id,
                        headers=headers,
                    ),
                )
            except exceptions.DeliveryError as e:
                # message was not delivered
                # if it is returned as unroutable, raise a ValueError
                if e.message is not None:
                    raise ValueError(404, f"{routing_key} is unroutable")
                # retry later
                await asyncio.sleep(self.RETRY_DELAY, loop=self.loop)
            except RuntimeError as e:
                if "closed" not in e.args[0]:
                    await self.stop()
                    raise
                else:
                    break
            else:
                break

    async def connect(self) -> None:
        """Connect to RabbitMQ, declare the topology and consumers."""
        # Perform connection
        connected = False
        url_idx = 0
        hb_query = "?{}".format(urlencode({"heartbeat": self.HEARTBEAT}))
        while not (connected or self.stopped.is_set()):
            try:
                connection = await aiormq.connect(
                    self._urls[url_idx] + hb_query, loop=self.loop
                )
            except ConnectionError as e:
                if e.errno == 111:
                    await asyncio.sleep(self.RETRY_DELAY, loop=self.loop)
                else:
                    self.stopped.set()
            except Exception:
                self.stopped.set()
                raise
            else:
                connected = True
            url_idx = (url_idx + 1) % len(self._urls)
        if not connected:
            return
        self._connection = connection
        connection.closing.add_done_callback(self.on_connection_closed)

        # Creating channels
        self._channel = await connection.channel(publisher_confirms=True)
        self._log_channel = await connection.channel(publisher_confirms=False)
        await self._channel.basic_qos(prefetch_count=self.PREFETCH_COUNT)
        # setup exchanges
        await self._channel.exchange_declare(
            exchange=self.EVENT_EXCHANGE,
            exchange_type=self.EVENT_EXCHANGE_TYPE,
            durable=True,
        )
        await self._channel.exchange_declare(
            exchange=self.CMD_EXCHANGE,
            exchange_type=self.CMD_EXCHANGE_TYPE,
            durable=True,
        )
        await self._log_channel.exchange_declare(
            exchange=self.LOG_EXCHANGE,
            exchange_type=self.LOG_EXCHANGE_TYPE,
            durable=True,
        )

        # Declare, bind and consume queues
        if self._event_routing_keys:
            events_ok = await self._channel.queue_declare(
                "" if self.exclusive_queues else (self.logical_service + ".events"),
                durable=not self.exclusive_queues,
                exclusive=self.exclusive_queues,
            )
            for key in self._event_routing_keys:
                await self._channel.queue_bind(
                    events_ok.queue, self.EVENT_EXCHANGE, routing_key=key
                )
            # returns ConsumeOK, which has consumer_tag attribute
            res = await self._channel.basic_consume(events_ok.queue, self.on_message)
            self._event_consumer_tag = res.consumer_tag

        if self._command_routing_keys:
            cmds_ok = await self._channel.queue_declare(
                "" if self.exclusive_queues else (self.logical_service + ".commands"),
                durable=not self.exclusive_queues,
                exclusive=self.exclusive_queues,
            )
            for key in self._command_routing_keys:
                await self._channel.queue_bind(
                    cmds_ok.queue, self.CMD_EXCHANGE, routing_key=key
                )
            res = await self._channel.basic_consume(cmds_ok.queue, self.on_message)
            self._command_consumer_tag = res.consumer_tag
        await self.on_ready()

    # Public API

    def use_json(self) -> None:
        """Force sending message serialized in plain JSON instead of CBOR."""
        self._serialize = lambda message: json.dumps(message).encode("utf-8")
        self._mime_type = "application/json"

    def use_exclusive_queues(self) -> None:
        """Force usage of exclusive queues.

        This is useful for debug tools that should not leave a queue behind them (overflow ristk)
        and not interfere between instances.
        """
        self.exclusive_queues = True

    async def run(self) -> None:
        await self.connect()
        await self.stopped.wait()

    async def stop(self) -> None:
        """Cleanly stop the application.

        This method is automatically triggered if we receive one of
        these UNIX signals: signal.SIGHUP, signal.SIGTERM, signal.SIGINT.
        """
        print("Shutting down in 3 seconds.")
        self._should_reconnect = False
        if not hasattr(self, "_connection") or self._connection.is_closed:
            self.stopped.set()
            return
        await self.log(WARNING, "Shutting down…")
        await self._channel.basic_cancel(self._event_consumer_tag)
        await self._channel.basic_cancel(self._command_consumer_tag)
        # wait for ongoing publishings?
        await asyncio.sleep(3, loop=self.loop)
        await self._connection.close()

    async def log(
        self,
        criticity: int,
        short: str,
        full: str = "",
        conversation_id: str = "",
        additional_fields: Dict = {},
    ) -> None:
        """Log to the log bus.


        Parameters:
         - `criticity`: int, in the syslog scale
         - `short`: str, short description of log
         - `full`: str, the full message of the log (appears as `message` in Graylog)
         - `additional_fields: Dict, data to be merged into the GELF payload as additional fields
        """
        gelf = GELF(self, criticity, short, full, conversation_id, additional_fields)
        # no persistent messages, no delivery confirmations
        try:
            await self._log_channel.basic_publish(
                exchange=self.LOG_EXCHANGE,
                routing_key=gelf.routing_key,
                body=gelf.payload,
            )
        except RuntimeError as e:
            if "closed" not in e.args[0]:
                await self.stop()
                raise

    async def send_command(
        self,
        command: str,
        message: JSON_MODEL,
        conversation_id: str,
        reply_to: str,
        correlation_id: str,
        mandatory: bool = False,
    ) -> None:
        """Send a command message.

        The `message` is any data conforming to the JSON model.
        if `mandatory` is True and you have implemented
        `handle_returned_message`, then it will be called if your message
        is unroutable."""
        await self._emit(
            self.CMD_EXCHANGE,
            command,
            message,
            conversation_id,
            mandatory,
            reply_to=reply_to,
            correlation_id=correlation_id,
        )

    async def return_success(
        self,
        destination: str,
        message: JSON_MODEL,
        conversation_id: str,
        correlation_id: str,
        mandatory: bool = False,
    ) -> None:
        """Send a successful result message.

        The `message` is any data conforming to the JSON model.
        if `mandatory` is True and you have implemented
        `handle_returned_message`, then it will be called if your message
        is unroutable."""
        headers = {"status": "success"}
        await self._emit(
            self.CMD_EXCHANGE,
            destination,
            message,
            conversation_id,
            mandatory,
            correlation_id=correlation_id,
            headers=headers,
        )

    async def return_error(
        self,
        destination: str,
        message: JSON_MODEL,
        conversation_id: str,
        correlation_id: str,
        mandatory: bool = False,
    ) -> None:
        """Send a failure notification.

        The `message` is any data conforming to the JSON model.
        if `mandatory` is True and you have implemented
        `handle_returned_message`, then it will be called if your message
        is unroutable."""
        headers = {"status": "error"}
        await self._emit(
            self.CMD_EXCHANGE,
            destination,
            message,
            conversation_id,
            mandatory,
            correlation_id=correlation_id,
            headers=headers,
        )

    async def publish_event(
        self,
        event: str,
        message: JSON_MODEL,
        conversation_id: str,
        mandatory: bool = False,
    ) -> None:
        """Publish an event on the bus.

        The ``event`` is the name of the event,
        and the `message` is any data conforming to the JSON model.

        If `mandatory` is True and you have implemented
        `handle_returned_message`, then it will be called if your message
        is unroutable.
        The default is False because some events maybe unused yet.
        """
        await self._emit(
            self.EVENT_EXCHANGE, event, message, conversation_id, mandatory
        )

    def create_task(self, coro: Coroutine) -> Coroutine:
        """Launch a task."""
        return getattr(self, "_channel", asyncio).create_task(coro)

    async def handle_event(
        self, event: str, payload: JSON_MODEL, conversation_id: str
    ) -> None:
        """Handle incoming event (may be overwritten by subclasses).

        The `payload` is already decoded and is a python data structure compatible with the JSON data model.
        You should never do any filtering here: use the routing keys intead
        (see ``__init__()``).

        The default implementation dispatches the messages by calling coroutine methods in the form
        ``self.on_KEY(payload)`` where key is the routing key.
        """
        handler = getattr(self, "on_" + event)
        if handler is not None:
            await handler(payload, conversation_id)
        else:
            await self.log(
                ERROR,
                f"unexpected event {event}; check your subscriptions!",
                conversation_id=conversation_id,
            )

    async def handle_command(
        self,
        command: str,
        payload: JSON_MODEL,
        conversation_id: str,
        reply_to: str,
        correlation_id: str,
    ) -> None:
        """Handle incoming commands (may be overwriten by subclasses).

        The `payload` is already decoded and is a python data structure compatible with the JSON data model.
        You should never do any filtering here: use the routing keys intead (see ``__init__()``).
        Expected errors should be returned with the ``return_error`` method.

        The default implementation dispatches the messages by calling coroutine methods in the form
        ``self.on_COMMAND(payload, reply_to, correlation_id)`` where COMMAND is what is left
        after stripping the ``service.`` prefix from the routing key.

        """
        handler = getattr(self, "on_" + command.split(".")[-1])
        if handler is not None:
            await handler(payload, conversation_id, reply_to, correlation_id)
        else:
            # should never happens: means we misconfigured the routing keys
            await self.log(
                ERROR,
                f"unexpected command {command}; check your subscriptions!",
                conversation_id=conversation_id,
            )

    async def handle_result(
        self,
        key: str,
        payload: JSON_MODEL,
        conversation_id: str,
        status: str,
        correlation_id: str,
    ) -> None:
        """Handle incoming result (may be overwritten by subclasses).

        The `payload` is already decoded and is a python data structure compatible with the JSON data model.
        You should never do any filtering here: use the routing keys intead (see ``__init__()``).

        The ``key`` is the routing key and ``status`` is either "success" or "error".

        The default implementation dispatches the messages by calling coroutine methods in the form
        ``self.on_KEY(payload, status, correlation_id)`` where KEY is what is left
        after stripping the ``service.`` prefix from the routing key.
        """
        handler = getattr(self, "on_" + key.split(".")[-1])
        if handler is not None:
            await handler(payload, conversation_id, status, correlation_id)
        else:
            # should never happens: means we misconfigured the routing keys
            await self.log(
                ERROR,
                f"unexpected result {key}; check your subscriptions!",
                conversation_id=conversation_id,
            )

    # Abstract methods

    async def on_ready(self) -> None:
        """Code to execute once the service comes online.

        (to be implemented by subclasses)
        """
        pass
