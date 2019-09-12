import asyncio
import json
import os
from typing import Any, Callable, Coroutine, Dict, Optional, Sequence

import aiormq
import cbor
from aiormq import exceptions, types

JSON_MODEL = Dict[str, Any]
HEADER = Dict[str, str]


class Service:

    ID = os.getpid()
    EVENT_EXCHANGE = "events"
    CMD_EXCHANGE = "commands"
    LOG_EXCHANGE = "logs"
    EVENT_EXCHANGE_TYPE = "topic"
    CMD_EXCHANGE_TYPE = "topic"
    LOG_EXCHANGE_TYPE = "topic"
    RETRY_DELAY = 5  # in seconds
    # In production, experiment with higher prefetch values
    # for higher consumer throughput
    PREFETCH_COUNT = 3

    def __init__(
        self,
        amqp_url: str,
        event_routing_keys: Sequence[str],
        command_routing_keys: Sequence[str],
        logical_service: str,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.

        :param str amqp_url: The AMQP url to connect with

        """
        self._url = amqp_url
        self._event_routing_keys = event_routing_keys
        self._command_routing_keys = command_routing_keys
        self.logical_service = logical_service
        self.loop = loop
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

    def on_connection_closed(self, closing: asyncio.Future) -> None:
        if self._should_reconnect:
            self.create_task(self.connect())
        else:
            self.stopped.set()

    async def on_message(self, message: types.DeliveredMessage) -> None:

        properties = message.header.properties

        headers: HEADER = properties.headers
        decoder = cbor if properties.content_type == "application/cbor" else json
        routing_key: str = message.delivery.routing_key
        exchange: str = message.delivery.exchange
        try:
            payload: JSON_MODEL = decoder.loads(message.body) if message.body else None
        except ValueError:
            await self.log(
                "critical", "Unable to decode payload for {}".format(routing_key)
            )
            # Unrecoverable, put to dead letter
            await message.channel.basic_nack(
                message.delivery.delivery_tag, requeue=False
            )
            return

        if exchange == self.CMD_EXCHANGE:
            correlation_id = properties.correlation_id
            reply_to = properties.reply_to
            status = headers.get("status", "") if headers else ""
            if not (reply_to or status):
                await self.log(
                    "error", "invalid enveloppe for command/result: {}".format(headers)
                )
                # Unrecoverable, put to dead letter
                await message.channel.basic_nack(
                    message.delivery.delivery_tag, requeue=False
                )
                return
            if reply_to:
                await self.on_command(
                    message.channel,
                    routing_key,
                    payload,
                    reply_to,
                    correlation_id,
                    message.delivery,
                )
            else:
                await self.on_result(
                    message.channel,
                    routing_key,
                    payload,
                    status,
                    correlation_id,
                    message.delivery,
                )
        else:
            await self.on_event(message.channel, routing_key, payload, message.delivery)

    async def on_command(
        self,
        ch: aiormq.Channel,
        routing_key: str,
        payload: JSON_MODEL,
        reply_to: str,
        correlation_id: str,
        delivery: types.spec.Basic.Deliver,
    ) -> None:
        try:
            await self.handle_command(routing_key, payload, reply_to, correlation_id)
        except Exception as e:
            # unexpected error
            await self.log(
                "error",
                "Unexpected error while processing command {}: {}".format(
                    routing_key, e
                ),
            )
            # requeue once
            if not delivery.redelivered:
                ch.basic_nack(delivery.delivery_tag, requeue=True)
            else:
                # return error to sender
                await self.return_error(
                    reply_to,
                    {"reason": "unhandled exception", "message": str(e)},
                    correlation_id,
                )
                await ch.basic_ack(delivery.delivery_tag)
        else:
            await ch.basic_ack(delivery.delivery_tag)

    async def on_result(
        self,
        ch: aiormq.Channel,
        routing_key: str,
        payload: JSON_MODEL,
        status: str,
        correlation_id: str,
        delivery: types.spec.Basic.Deliver,
    ) -> None:
        try:
            await self.handle_result(routing_key, payload, status, correlation_id)
        except Exception as e:
            await self.log(
                "error",
                "Unexpected error while processing result {}: {}".format(
                    routing_key, e
                ),
            )
            # requeue once
            if not delivery.redelivered:
                ch.basic_nack(delivery.delivery_tag, requeue=True)
            else:
                # dead letter
                self.log("error", "Giving up on {}: {}".format(routing_key, e))
                ch.basic_nack(delivery.delivery_tag, requeue=False)
        else:
            await ch.basic_ack(delivery.delivery_tag)

    async def on_event(
        self,
        ch: aiormq.Channel,
        routing_key: str,
        payload: JSON_MODEL,
        delivery: types.spec.Basic.Deliver,
    ) -> None:
        try:
            await self.handle_event(routing_key, payload)
        except Exception as e:
            await self.log(
                "error",
                "Unexpected error while processing result {}: {}".format(
                    routing_key, e
                ),
            )
            # requeue once
            if not delivery.redelivered:
                ch.basic_nack(delivery.delivery_tag, requeue=True)
            else:
                # dead letter
                self.log("error", "Giving up on {}: {}".format(routing_key, e))
                ch.basic_nack(delivery.delivery_tag, requeue=False)
        else:
            await ch.basic_ack(delivery.delivery_tag)

    async def _emit(
        self,
        exchange: str,
        routing_key: str,
        message: JSON_MODEL,
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
        while True:
            try:
                await self._channel.basic_publish(
                    self._serialize(message),
                    exchange=exchange,
                    routing_key=routing_key,
                    mandatory=mandatory,
                    properties=aiormq.spec.Basic.Properties(
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
                    raise ValueError(f"{routing_key} is unroutable")
                # retry later
                await asyncio.sleep(self.RETRY_DELAY, loop=self.loop)
            except RuntimeError as e:
                print("attempt")
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
        while not connected:
            try:
                connection = await aiormq.connect(self._url, loop=self.loop)
            except ConnectionError as e:
                if e.errno == 111:
                    await asyncio.sleep(self.RETRY_DELAY, loop=self.loop)
                else:
                    self.stopped.set()
            else:
                connected = True
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
                res = await self._channel.basic_consume(
                    events_ok.queue, self.on_message
                )
                self._event_consumer_tag = res.consumer_tag

        if self._command_routing_keys:
            cmds_ok = await self._channel.queue_declare(
                "" if self.exclusive_queues else (self.logical_service + ".cmds"),
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
        self._should_reconnect = False
        await self._channel.basic_cancel(self._event_consumer_tag)
        await self._channel.basic_cancel(self._command_consumer_tag)
        # wait for ongoing publishings?
        await asyncio.sleep(self.RETRY_DELAY + 1, loop=self.loop)
        await self._channel.close()
        await self._log_channel.close()
        await self._connection.close()

    async def log(self, criticity: str, message: str) -> None:
        """Log to the log bus.

        Parameters are unicode strings.
        """
        # no persistent messages, no delivery confirmations

        try:
            await self._log_channel.basic_publish(
                "[{}-{}] {}".format(self.logical_service, self.ID, message).encode(
                    "utf-8"
                ),
                exchange=self.LOG_EXCHANGE,
                routing_key="{}.{}".format(self.logical_service, criticity),
            )
        except RuntimeError as e:
            if "closed" not in e.args[0]:
                await self.stop()
                raise

    async def send_command(
        self,
        command: str,
        message: JSON_MODEL,
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
            mandatory,
            reply_to=reply_to,
            correlation_id=correlation_id,
        )

    async def return_success(
        self,
        destination: str,
        message: JSON_MODEL,
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
            mandatory,
            correlation_id=correlation_id,
            headers=headers,
        )

    async def return_error(
        self,
        destination: str,
        message: JSON_MODEL,
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
            mandatory,
            correlation_id=correlation_id,
            headers=headers,
        )

    async def publish_event(
        self, event: str, message: JSON_MODEL, mandatory: bool = False
    ) -> None:
        """Publish an event on the bus.

        The ``event`` is the name of the event,
        and the `message` is any data conforming to the JSON model.

        If `mandatory` is True and you have implemented
        `handle_returned_message`, then it will be called if your message
        is unroutable.
        The default is False because some events maybe unused yet.
        """
        await self._emit(self.EVENT_EXCHANGE, event, message, mandatory)

    # Abstract methods

    async def on_ready(self) -> None:
        """Code to execute once the service comes online.

        (to be implemented by subclasses)
        """
        pass

    def create_task(self, coro: Coroutine) -> Coroutine:
        """Launch a task."""
        return self._channel.create_task(coro)

    async def handle_event(self, event: str, payload: JSON_MODEL) -> None:
        """Handle incoming event (to be implemented by subclasses).

        The `payload` is already decoded and is a python data structure compatible with the JSON data model.
        You should never do any filtering here: use the routing keys intead
        (see ``__init__()``).
        """
        return NotImplemented

    async def handle_command(
        self, command: str, payload: JSON_MODEL, reply_to: str, correlation_id: str
    ) -> None:
        """Handle incoming commands (to be implemented by subclasses).

        The `payload` is already decoded and is a python data structure compatible with the JSON data model.
        You should never do any filtering here: use the routing keys intead (see ``__init__()``).
        Expected errors should be returned with the ``return_error`` method.
        """
        return NotImplemented

    async def handle_result(
        self, key: str, payload: JSON_MODEL, status: str, correlation_id: str
    ) -> None:
        """Handle incoming result (to be implemented by subclasses).

        The `payload` is already decoded and is a python data structure compatible with the JSON data model.
        You should never do any filtering here: use the routing keys intead (see ``__init__()``).

        The ``key`` is the routing key and ``status`` is either "success" or "error".
        """
        return NotImplemented
