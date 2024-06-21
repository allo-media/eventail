# -*- coding: utf-8 -*-
#
# MIT License
#
# Copyright (c) 2018-2021 Groupe Allo-Media
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
A base class implementing AM service architecture and its requirements.
Inspired from pika complete examples.
"""

import functools
import json
import logging
import os
import signal
import socket
import traceback
from contextlib import AbstractContextManager
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import cbor2 as cbor
import pika

from eventail.gelf import GELF
from eventail.log_criticity import ALERT, EMERGENCY, ERROR, WARNING

LOGGER = logging.getLogger("async_service")


JSON_MODEL = Dict[str, Any]
HEADER = Dict[str, str]


class Service(object):
    """This is an example service that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.

    If RabbitMQ closes the connection, this class will stop and indicate
    that reconnection is necessary. You should look at the output, as
    there are limited reasons why the connection may be closed, which
    usually are tied to permission related issues or socket timeouts.

    If the channel is closed, it will indicate a problem with one of the
    commands that were issued and that should surface in the output as well.

    To leverage the binary nature of AMQP messages, we use CBOR instead of
    JSON as data serialization (transparent). Moreover, CBOR is much faster
    and much more compact than JSON.
    """

    ID = os.getpid()
    HOSTNAME = socket.gethostname()
    EVENT_EXCHANGE = "events"
    CMD_EXCHANGE = "commands"
    LOG_EXCHANGE = "logs"
    CONFIG_EXCHANGE = "configurations"
    EVENT_EXCHANGE_TYPE = "topic"
    CMD_EXCHANGE_TYPE = "topic"
    LOG_EXCHANGE_TYPE = "topic"
    CONFIG_EXCHANGE_TYPE = "topic"
    RETRY_DELAY = 15  # in seconds
    #: Heartbeat interval, must be superior to the expected blocking processing time (in seconds).
    #: Beware that the actual delay is negotiated with the broker, and the lower value is taken, so
    #: configure Rabbitmq accordingly.
    HEARTBEAT = 60
    #: When rabbitmq is low on resources, it may temporarily block the connection.
    #: We can specify a timeout if it is not acceptable to the service (in seconds)
    BLOCKED_TIMEOUT = 3600
    #: In production, experiment with higher prefetch values
    #: for higher consumer throughput. As rule of thumb, you can set it to the number
    #: of messages the service can process whithin one second, as long as they can fit
    #: easily in memory.
    PREFETCH_COUNT = 3

    def __init__(
        self,
        amqp_urls: List[str],
        event_routing_keys: Sequence[str],
        command_routing_keys: Sequence[str],
        logical_service: str,
        config_routing_keys: Sequence[str] = [],
    ) -> None:
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.

        :param str amqp_urls: List of AMQP urls.

        The service will try to connect to one of them, in a round-robin fashion.

        """
        self._urls = amqp_urls
        self._event_routing_keys = event_routing_keys
        self._command_routing_keys = command_routing_keys
        self._config_routing_keys = config_routing_keys
        self.logical_service = logical_service
        self.url_idx = 0
        self._event_queue = logical_service + ".events"
        self._command_queue = logical_service + ".commands"
        self._config_queue = ""  # no name yet, as it is an exclusive queue
        self.exclusive_queues = False
        self._serialize: Callable[..., bytes] = cbor.dumps
        self._mime_type = "application/cbor"
        self._connection: pika.SelectConnection
        self._channel: pika.channel.Channel
        self._log_channel: pika.channel.Channel
        self._configured = not config_routing_keys

        for s in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT):
            signal.signal(s, lambda _s, _f: self.stop())

    def reset_connection_state(self) -> None:
        self._bind_count = (
            (len(self._event_routing_keys) or 1)
            + (len(self._command_routing_keys) or 1)
            + (len(self._config_routing_keys) or 1)
        )
        self.should_reconnect = False
        self.was_consuming = False
        self._configured = not self._config_routing_keys

        self._closing = False
        self._event_consumer_tag: Optional[str] = None
        self._command_consumer_tag: Optional[str] = None
        self._config_consumer_tag: Optional[str] = None
        self._consuming = False

        # for events publishing only
        self._deliveries: Dict[
            int, Tuple[str, str, JSON_MODEL, str, bool, Optional[HEADER]]
        ] = {}
        self._acked = 0
        self._nacked = 0
        self._message_number = 0
        self._pending_ack: List[Tuple[int, int]] = []

    def connect(self) -> pika.SelectConnection:
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.SelectConnection

        """
        self.reset_connection_state()
        url = self._urls[self.url_idx]
        self.url_idx = (self.url_idx + 1) % len(self._urls)
        LOGGER.info("Connecting to %s", url.split("@")[-1])
        connection_params = pika.URLParameters(url)
        connection_params.heartbeat = self.HEARTBEAT
        connection_params.blocked_connection_timeout = self.BLOCKED_TIMEOUT

        return pika.SelectConnection(
            parameters=connection_params,
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed,
        )

    def close_connection(self) -> None:
        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            LOGGER.info("Connection is closing or already closed")
        else:
            LOGGER.info("Closing connection")
            self._connection.close()

    def on_connection_open(self, _unused_connection: pika.BaseConnection) -> None:
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :param pika.SelectConnection _unused_connection: The connection

        """
        LOGGER.info("Connection opened")
        self.open_channels()

    def on_connection_open_error(
        self, _unused_connection: pika.BaseConnection, err: Exception
    ) -> None:
        """This method is called by pika if the connection to RabbitMQ
        can't be established.

        :param pika.SelectConnection _unused_connection: The connection
        :param Exception err: The error

        """
        LOGGER.error("Connection open failed: %s", err)
        self.reconnect(True)

    def on_connection_closed(
        self, _unused_connection: pika.BaseConnection, reason: Exception
    ) -> None:
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param Exception reason: exception representing reason for loss of
            connection.

        """
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self.reconnect(True)

    def reconnect(self, should_reconnect=True) -> None:
        """Will be invoked if the connection can't be opened or is
        closed. Indicates that a reconnect is necessary then stops the
        ioloop.

        """
        self.should_reconnect = should_reconnect
        self.stop(should_reconnect)

    def open_channels(self) -> None:
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.

        """
        LOGGER.info("Creating channels")
        self._connection.channel(
            on_open_callback=functools.partial(self.on_channel_open, main=True)
        )
        self._connection.channel(
            on_open_callback=functools.partial(self.on_channel_open, main=False)
        )

    def on_channel_open(self, channel: pika.channel.Channel, main: bool) -> None:
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchanges to use.

        :param pika.channel.Channel channel: The channel object

        """
        LOGGER.info("Channel opened")
        if main:
            self._channel = channel
            self.setup_exchange(self.EVENT_EXCHANGE, self.EVENT_EXCHANGE_TYPE, channel)
            self.setup_exchange(self.CMD_EXCHANGE, self.CMD_EXCHANGE_TYPE, channel)
            self.setup_exchange(
                self.CONFIG_EXCHANGE, self.CONFIG_EXCHANGE_TYPE, channel
            )
        else:
            self._log_channel = channel
            self.setup_exchange(self.LOG_EXCHANGE, self.LOG_EXCHANGE_TYPE, channel)

        self.add_on_channel_close_callback(channel)

    def add_on_channel_close_callback(self, channel: pika.channel.Channel) -> None:
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        LOGGER.info("Adding channel close callback")
        channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(
        self, channel: pika.channel.Channel, reason: Exception
    ) -> None:
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel: The closed channel
        :param Exception reason: why the channel was closed

        """
        LOGGER.warning("Channel %i was closed: %s", channel, reason)
        self.close_connection()

    def setup_exchange(
        self, exchange_name: str, exchange_type: str, channel: pika.channel.Channel
    ) -> None:
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        LOGGER.info("Declaring exchange: %s", exchange_name)
        # Note: using functools.partial is not required, it is demonstrating
        # how arbitrary data can be passed to the callback when it is called
        cb = functools.partial(self.on_exchange_declareok, exchange_name=exchange_name)
        channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=exchange_type,
            callback=cb,
            durable=True,
        )

    def on_exchange_declareok(
        self, _unused_frame: pika.frame.Method, exchange_name: str
    ) -> None:
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.frame.Method unused_frame: Exchange.DeclareOk response frame
        :param str|unicode userdata: Extra user data (exchange name)

        """
        LOGGER.info("Exchange declared: %s", exchange_name)
        if (
            exchange_name == self.EVENT_EXCHANGE
            and self._event_routing_keys
            or exchange_name == self.CMD_EXCHANGE
            and self._command_routing_keys
            or exchange_name == self.CONFIG_EXCHANGE
            and self._config_routing_keys
        ):
            self.setup_queue(exchange_name)
        elif exchange_name != self.LOG_EXCHANGE:
            self._bind_count -= 1
            if self._bind_count == 0:
                self.set_qos()

    def setup_queue(self, exchange_name: str) -> None:
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode exchange: The name of exchange to bind.

        """
        cb = functools.partial(self.on_queue_declareok, exchange_name=exchange_name)
        if self.exclusive_queues or exchange_name == self.CONFIG_EXCHANGE:
            LOGGER.info("Declaring exclusive on exchange %s", exchange_name)
            self._channel.queue_declare("", exclusive=True, durable=False, callback=cb)
        else:
            queue = (
                self._event_queue
                if exchange_name == self.EVENT_EXCHANGE
                else self._command_queue
            )
            LOGGER.info("Declaring queue %s on exchange %s", queue, exchange_name)
            self._channel.queue_declare(queue=queue, durable=True, callback=cb)

    def on_queue_declareok(self, frame: pika.frame.Method, exchange_name: str) -> None:
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        :param pika.frame.Method frame: The Queue.DeclareOk frame

        """
        queue_name = frame.method.queue
        routing_keys: Sequence[str]
        if exchange_name == self.EVENT_EXCHANGE:
            routing_keys = self._event_routing_keys
            self._event_queue = queue_name
        elif exchange_name == self.CMD_EXCHANGE:
            routing_keys = self._command_routing_keys
            self._command_queue = queue_name
        else:
            routing_keys = self._config_routing_keys
            self._config_queue = queue_name
        LOGGER.info("Binding %s to %s with %s", exchange_name, queue_name, routing_keys)
        for key in routing_keys:
            self._channel.queue_bind(
                queue_name, exchange_name, routing_key=key, callback=self.on_bindok
            )

    def on_bindok(self, _unused_frame: pika.frame.Method) -> None:
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will set the prefetch count for the channel.

        :param pika.frame.Method _unused_frame: The Queue.BindOk response frame

        """
        LOGGER.info("Queue bound")
        self._bind_count -= 1
        if self._bind_count == 0:
            self.set_qos()

    def set_qos(self) -> None:
        """This method sets up the consumer prefetch to only be delivered
        PREFETCH_COUNT at a time. The consumer must acknowledge this message
        before RabbitMQ will deliver another one. You should experiment
        with different prefetch values to achieve desired performance.

        """
        self._channel.basic_qos(
            prefetch_count=self.PREFETCH_COUNT, callback=self.on_basic_qos_ok
        )

    def on_basic_qos_ok(self, _unused_frame: pika.frame.Method) -> None:
        """Invoked by pika when the Basic.QoS method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method _unused_frame: The Basic.QosOk response frame

        """
        LOGGER.info("QOS set to: %d", self.PREFETCH_COUNT)
        self.enable_delivery_confirmations()
        self.start_consuming()

    def enable_delivery_confirmations(self) -> None:
        """Send the Confirm.Select RPC method to RabbitMQ to enable delivery
        confirmations on the channel. The only way to turn this off is to close
        the channel and create a new one.

        When the message is confirmed from RabbitMQ, the
        on_delivery_confirmation method will be invoked passing in a Basic.Ack
        or Basic.Nack method from RabbitMQ that will indicate which messages it
        is confirming or rejecting.

        """
        LOGGER.info("Issuing Confirm.Select RPC command")
        self._channel.confirm_delivery(self.on_delivery_confirmation)

    def on_delivery_confirmation(self, method_frame: pika.frame.Method) -> None:
        """Invoked by pika when RabbitMQ responds to a Basic.Publish RPC
        command, passing in either a Basic.Ack or Basic.Nack frame with
        the delivery tag of the message that was published. The delivery tag
        is an integer counter indicating the message number that was sent
        on the channel via Basic.Publish. Here we're just doing house keeping
        to keep track of stats and remove message numbers that we expect
        a delivery confirmation of from the list used to keep track of messages
        that are pending confirmation.

        BEWARE: the `ack` and `nack` received here are emitted by the broker,
        not by other services! They mean the broker accepted/received the
        message or not.
        Unroutable messages won't raise a `nack`.
        If you want to be notified of unroutable messages,
        you need to set `mandatory=True` on the emitted message and
        implement `handle_returned_message`. The unroutable message
        will then be returned to this callback.

        :param pika.frame.Method method_frame: Basic.Ack or Basic.Nack frame

        """
        confirmation_type: str = method_frame.method.NAME.split(".")[1].lower()
        delivery_tag: int = method_frame.method.delivery_tag
        multiple: bool = method_frame.method.multiple
        LOGGER.info("Received %s for delivery tag: %i", confirmation_type, delivery_tag)
        confirm_range: List[int]
        if multiple:
            # Python dict are sorted by insertion order since version 3.7.
            # As we insert keys in tag increasing order, we're good.
            confirm_range = [i for i in self._deliveries.keys() if i <= delivery_tag]
        else:
            confirm_range = [delivery_tag]
        num_confirms = len(confirm_range)
        if confirmation_type == "ack":
            self._acked += num_confirms
        elif confirmation_type == "nack":
            self._nacked += num_confirms
            # The broker in is trouble, resend later
            for i in confirm_range:
                self.call_later(
                    self.RETRY_DELAY, lambda args=self._deliveries[i]: self._emit(*args)
                )
                del self._deliveries[i]
            # Pending rcv acks that depend on at least one of those nacked publishes should be rescheduled too
            low_bound = confirm_range[0]
            for i, (_, bound) in enumerate(self._pending_ack):
                if bound >= low_bound:
                    to_reschedule = [tag for tag, _ in self._pending_ack[i:]]
                    del self._pending_ack[i:]
                    self.call_later(
                        self.RETRY_DELAY + 0.1,
                        lambda args=to_reschedule: map(self._auto_ack, args),
                    )
                    break
            return
        # ACK only
        for i in confirm_range:
            del self._deliveries[i]
        acks = 0
        if self._deliveries:
            low_bound = next(iter(self._deliveries.keys()))
            for i, (pending, bound) in enumerate(self._pending_ack):
                if bound < low_bound:
                    self._channel.basic_ack(delivery_tag=pending)
                else:
                    acks = i
                    del self._pending_ack[:i]
                    break
        else:
            for pending, _ in self._pending_ack:
                self._channel.basic_ack(delivery_tag=pending)
            acks = len(self._pending_ack)
            self._pending_ack.clear()

        LOGGER.info(
            "Published %i messages, %i have yet to be confirmed, %i were acked and %i were nacked",
            self._message_number,
            len(self._deliveries),
            self._acked,
            self._nacked,
        )
        LOGGER.info(
            f"Acknowledged {acks} processed messages, {len(self._pending_ack)} still pending"
        )

    def start_consuming(self) -> None:
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.

        """
        LOGGER.info("Issuing consumer related RPC commands")
        self.add_on_cancel_callback()
        self.add_on_return_callback()
        if self._configured:
            LOGGER.info("Consuming business events")
            if self._event_routing_keys:
                self._event_consumer_tag = self._channel.basic_consume(
                    self._event_queue, self.on_message
                )
                self._consuming = True
            if self._command_routing_keys:
                self._command_consumer_tag = self._channel.basic_consume(
                    self._command_queue, self.on_message
                )
                self._consuming = True
            self.on_ready()
        else:
            LOGGER.info("Consuming configuration events")
            self._config_consumer_tag = self._channel.basic_consume(
                self._config_queue, self.on_message
            )
            self._consuming = True
            self.on_unconfigured()
            LOGGER.info("Waiting for configuration")
        self.was_consuming = True

    def add_on_cancel_callback(self) -> None:
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.

        """
        LOGGER.info("Adding consumer cancellation callback")
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def add_on_return_callback(self) -> None:
        """Add a callback that will be invoked to return an unroutable message."""
        LOGGER.info("Adding return callback")
        self._channel.add_on_return_callback(self.on_message_returned)

    def on_consumer_cancelled(self, method_frame: pika.frame.Method) -> None:
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        LOGGER.info("Consumer was cancelled remotely, shutting down: %r", method_frame)
        if not (self._channel.is_closed or self._channel.is_closing):
            self._channel.close()

    def on_message_returned(
        self,
        ch: pika.channel.Channel,
        basic_return: pika.spec.Basic.Return,
        properties: pika.spec.BasicProperties,
        body: bytes,
    ):
        """Invoked by pika when a message is returned.

        A message maybe returned if:
        * it was sent with the `mandatory` flag on True;
        * the broker was unable to route it to a queue.

        :param pika.channel.Channel ch: The channel object
        :param pika.Spec.Basic.Return basic_deliver: method
        :param pika.Spec.BasicProperties: properties
        :param bytes body: The message body
        """
        decoder = cbor if properties.content_type == "application/cbor" else json
        # If we are not able to decode our own payload, better crash the service now
        payload: JSON_MODEL = decoder.loads(body) if body else None
        routing_key: str = basic_return.routing_key
        envelope: Dict[str, str] = {}
        if properties.reply_to:
            envelope["reply_to"] = properties.reply_to
        if properties.correlation_id:
            envelope["correlation_id"] = properties.correlation_id
        if properties.headers:
            envelope.update(properties.headers)
        LOGGER.info("Received returned message: %s", routing_key)
        try:
            self.handle_returned_message(routing_key, payload, envelope)
        except Exception as e:
            # unexpected error
            self.log(
                EMERGENCY,
                "in handle_returned_message [{}] {}".format(self.logical_service, e),
                conversation_id=envelope.get("conversation_id", ""),
            )
            # Crash the service now
            self.stop()

    def on_message(
        self,
        ch: pika.channel.Channel,
        basic_deliver: pika.spec.Basic.Deliver,
        properties: pika.spec.BasicProperties,
        body: bytes,
    ) -> None:
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param pika.channel.Channel ch: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param bytes body: The message body

        """
        headers: HEADER = properties.headers if properties.headers is not None else {}
        decoder = cbor if properties.content_type == "application/cbor" else json
        routing_key: str = basic_deliver.routing_key
        exchange: str = basic_deliver.exchange
        if exchange != self.CONFIG_EXCHANGE and "conversation_id" not in headers:
            self.log(EMERGENCY, f"Missing headers on {routing_key}")
            # unrecoverable error, send to dead letter
            ch.basic_nack(delivery_tag=basic_deliver.delivery_tag, requeue=False)
            return
        conversation_id = headers.pop("conversation_id", "")

        try:
            payload: JSON_MODEL = decoder.loads(body) if body else None
        except ValueError:
            self.log(
                EMERGENCY,
                f"Unable to decode payload for {routing_key}; dead lettering.",
                conversation_id=conversation_id,
            )
            # Unrecoverable, put to dead letter
            ch.basic_nack(delivery_tag=basic_deliver.delivery_tag, requeue=False)
            return
        LOGGER.info("Received message from %s: %s", exchange, routing_key)

        # meta
        headers["timestamp"] = properties.timestamp
        headers["expiration"] = properties.expiration
        headers["user_id"] = properties.user_id
        headers["app_id"] = properties.app_id
        headers["delivery_tag"] = basic_deliver.delivery_tag

        if exchange == self.CMD_EXCHANGE:
            correlation_id = properties.correlation_id
            reply_to = properties.reply_to
            status = headers.pop("status", "") if headers else ""
            if not (reply_to or status):
                self.log(
                    EMERGENCY,
                    "invalid enveloppe for command/result: {}; dead lettering.".format(
                        headers
                    ),
                    conversation_id=conversation_id,
                )
                # Unrecoverable. Put to dead letter
                ch.basic_nack(delivery_tag=basic_deliver.delivery_tag, requeue=False)
                return
            if reply_to:
                with self.ack_policy(
                    ch, basic_deliver, conversation_id, correlation_id
                ) as ap:
                    ap.manual_ack(
                        self.handle_command(
                            routing_key,
                            payload,
                            conversation_id,
                            reply_to,
                            correlation_id,
                            meta=headers,
                        )
                    )
            else:
                with self.ack_policy(
                    ch, basic_deliver, conversation_id, correlation_id
                ) as ap:
                    ap.manual_ack(
                        self.handle_result(
                            routing_key,
                            payload,
                            conversation_id,
                            status,
                            correlation_id,
                            meta=headers,
                        )
                    )
        elif exchange == self.EVENT_EXCHANGE:
            with self.ack_policy(ch, basic_deliver, conversation_id, "") as ap:
                ap.manual_ack(
                    self.handle_event(
                        routing_key, payload, conversation_id, meta=headers
                    )
                )
        else:
            with self.ack_policy(ch, basic_deliver, "", ""):
                configured = self.handle_config(routing_key, payload, meta=headers)
                if configured and not self._configured:
                    self._configured = True
                    self.start_consuming()

    def ack_policy(
        self,
        ch: pika.channel.Channel,
        deliver: pika.spec.Basic.Deliver,
        conversation_id: str,
        correlation_id: str,
    ) -> "AckPolicy":
        return AckPolicy(self, ch, deliver, conversation_id, correlation_id)

    def stop_consuming(self) -> None:
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        if not (self._channel.is_closed or self._channel.is_closing):
            LOGGER.info("Sending a Basic.Cancel RPC command to RabbitMQ")
            for consumer_tag in (
                self._event_consumer_tag,
                self._command_consumer_tag,
                self._config_consumer_tag,
            ):
                if consumer_tag is not None:
                    cb = functools.partial(self.on_cancelok, userdata=consumer_tag)
                    self._channel.basic_cancel(consumer_tag, cb)

    def on_cancelok(self, _unused_frame: pika.frame.Method, userdata: str) -> None:
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method _unused_frame: The Basic.CancelOk frame
        :param str|unicode userdata: Extra user data (consumer tag)

        """
        self._consuming = False
        LOGGER.info(
            "RabbitMQ acknowledged the cancellation of the consumer: %s", userdata
        )
        self.close_channel()

    def close_channel(self) -> None:
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        LOGGER.info("Closing the channels")
        self._channel.close()
        self._log_channel.close()

    def _emit(
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

        The `message` is any data conforming to the JSON model.
        """
        if headers is None:
            headers = {}
        headers["conversation_id"] = conversation_id
        self._channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=self._serialize(message),
            mandatory=mandatory,
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
                content_type=self._mime_type,
                reply_to=reply_to,
                correlation_id=correlation_id,
                headers=headers,
            ),
        )
        self._message_number += 1
        self._deliveries[self._message_number] = (
            exchange,
            routing_key,
            message,
            conversation_id,
            mandatory,
            headers,
        )
        LOGGER.info("Published message # %i", self._message_number)

    def _auto_ack(self, delivery_tag: int):
        if self._deliveries:
            # self._deliveries may still contain outgoing messages published by
            # the handler of the incoming delivery `delivery_tag`.
            self._pending_ack.append((delivery_tag, self._message_number))
        else:
            self._channel.basic_ack(delivery_tag=delivery_tag)

    # Public interface

    def use_json(self) -> None:
        """Force sending message serialized in plain JSON instead of CBOR."""
        self._serialize = lambda message: json.dumps(message).encode("utf-8")
        self._mime_type = "application/json"

    def use_exclusive_queues(self) -> None:
        """Force usage of exclusive queues.

        This is useful for debug tools that should not leave a queue behind them (overflow risk)
        and not interfere between instances.
        """
        self.exclusive_queues = True

    def log(
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
         - `additional_fields`: Dict, data to be merged into the GELF payload as additional fields
        """
        gelf = GELF(self, criticity, short, full, conversation_id, additional_fields)
        LOGGER.debug("Application logged: %s\n%s", short, full)
        # no persistent messages, no delivery confirmations
        self._log_channel.basic_publish(
            exchange=self.LOG_EXCHANGE,
            routing_key=gelf.routing_key,
            body=gelf.payload,
        )

    def send_command(
        self,
        command: str,
        message: JSON_MODEL,
        conversation_id: str,
        reply_to: str,
        correlation_id: str,
        mandatory: bool = True,
    ) -> None:
        """Send a command message.

        The `message` is any data conforming to the JSON model.
        if `mandatory` is True (default) and you have implemented
        `handle_returned_message`, then it will be called if your message
        is unroutable."""
        self._emit(
            self.CMD_EXCHANGE,
            command,
            message,
            conversation_id,
            mandatory,
            reply_to=reply_to,
            correlation_id=correlation_id,
        )

    def return_success(
        self,
        destination: str,
        message: JSON_MODEL,
        conversation_id: str,
        correlation_id: str,
        mandatory: bool = True,
    ) -> None:
        """Send a successful result message.

        The `message` is any data conforming to the JSON model.
        if `mandatory` is True (default) and you have implemented
        `handle_returned_message`, then it will be called if your message
        is unroutable."""
        headers = {"status": "success"}
        self._emit(
            self.CMD_EXCHANGE,
            destination,
            message,
            conversation_id,
            mandatory,
            correlation_id=correlation_id,
            headers=headers,
        )

    def return_error(
        self,
        destination: str,
        message: JSON_MODEL,
        conversation_id: str,
        correlation_id: str,
        mandatory: bool = True,
    ) -> None:
        """Send a failure result message.

        The `message` is any data conforming to the JSON model.
        If `mandatory` is True (default) and you have implemented
        `handle_returned_message`, then it will be called if your message
        is unroutable."""
        headers = {"status": "error"}
        self._emit(
            self.CMD_EXCHANGE,
            destination,
            message,
            conversation_id,
            mandatory,
            correlation_id=correlation_id,
            headers=headers,
        )

    def publish_event(
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
        self._emit(self.EVENT_EXCHANGE, event, message, conversation_id, mandatory)

    def publish_configuration(
        self,
        event: str,
        message: JSON_MODEL,
    ) -> None:
        """Publish a configuration on the bus.

        The ``event`` is the name of the configuration event,
        and the `message` is any data conforming to the JSON model.
        """
        self._emit(self.CONFIG_EXCHANGE, event, message, "", False)

    def call_later(self, delay: float, callback: Callable) -> object:
        """Call `callback` after `delay` seconds.

        Return a handle that can be passed to `self.cancel_timer()`"""
        return self._connection.ioloop.call_later(delay, callback)

    def cancel_timer(self, timer: object) -> None:
        self._connection.ioloop.remove_timeout(timer)

    def run(self) -> None:
        """Run the service by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.
        """
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self, reconnect=False) -> None:
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again if this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.

        This method is automatically triggered if we receive one of
        these UNIX signals: signal.SIGHUP, signal.SIGTERM, signal.SIGINT.
        """
        self.should_reconnect = reconnect
        if not self._closing:
            if not self._connection.is_closed:
                self.log(WARNING, "Shutting down…")
            self._closing = True
            LOGGER.info("Stopping")
            if self._consuming:
                self.stop_consuming()
                try:
                    self._connection.ioloop.start()
                except RuntimeError:
                    # already running!
                    pass
            else:
                self._connection.ioloop.stop()
            LOGGER.info("Stopped")

    def manual_ack(self, delivery_tag, multiple=False) -> None:
        """Manually confirm the processing of the incoming message identified by `delivery_tag`.

        The handler that processed the message first must have returned a truth value to indicate
        the message will be manually acked.

        **Never mix** manual acknowledgment with automatic end-of-handler acknowledgement.
        """
        self._channel.basic_ack(delivery_tag=delivery_tag, multiple=multiple)

    def manual_nack(self, delivery_tag, requeue=False, multiple=False) -> None:
        """Manually reject the incoming message identified by `delivery_tag`.

        The handler that processed the message first must have returned a truth value to indicate
        the message will be manually acked.

        **Never mix** manual acknowledgment with automatic end-of-handler acknowledgement.
        """
        self._channel.basic_nack(
            delivery_tag=delivery_tag, requeue=requeue, multiple=multiple
        )

    def handle_event(
        self,
        event: str,
        payload: JSON_MODEL,
        conversation_id: str,
        meta: Dict[str, str],
    ) -> Any:
        """Handle incoming event (may be overwritten by subclasses).

        The `payload` is already decoded and is a python data structure compatible with the JSON data model.
        You should never do any filtering here: use the routing keys intead
        (see ``__init__()``).

        The default implementation dispatches the messages by calling methods in the form
        ``self.on_KEY(payload)`` where key is the routing key and returning their return value.

        Return a falsy value (default is `None`) for automatic acknowledgement of processed message or
        a truthy one for manual acknowdegment later.

        **Never mix** manual acknowledgment with automatic end-of-handler acknowledgement.
        """
        handler = getattr(self, "on_" + event)
        if handler is not None:
            return handler(payload, conversation_id, meta)
        else:
            self.log(
                ERROR,
                f"unexpected event {event}; check your subscriptions!",
                conversation_id=conversation_id,
            )

    def handle_command(
        self,
        command: str,
        payload: JSON_MODEL,
        conversation_id: str,
        reply_to: str,
        correlation_id: str,
        meta: Dict[str, str],
    ) -> Any:
        """Handle incoming commands (may be overwriten by subclasses).

        The `payload` is already decoded and is a python data structure compatible with the JSON data model.
        You should never do any filtering here: use the routing keys intead (see ``__init__()``).
        Expected errors should be returned with the ``return_error`` method.

        The default implementation dispatches the messages by calling methods in the form
        ``self.on_COMMAND(payload, reply_to, correlation_id)`` where COMMAND is what is left
        after stripping the ``service.`` prefix from the routing key, and returning their return value.


        Return a falsy value (default is `None`) for automatic acknowledgement of processed message or
        a truthy one for manual acknowdegment later.

        **Never mix** manual acknowledgment with automatic end-of-handler acknowledgement.
        """
        handler = getattr(self, "on_" + command.split(".")[-1])
        if handler is not None:
            return handler(payload, conversation_id, reply_to, correlation_id, meta)
        else:
            # should never happens: means we misconfigured the routing keys
            self.log(
                ERROR,
                f"unexpected command {command}; check your subscriptions!",
                conversation_id=conversation_id,
            )

    def handle_result(
        self,
        key: str,
        payload: JSON_MODEL,
        conversation_id: str,
        status: str,
        correlation_id: str,
        meta: Dict[str, str],
    ) -> Any:
        """Handle incoming result (may be overwritten by subclasses).

        The `payload` is already decoded and is a python data structure compatible with the JSON data model.
        You should never do any filtering here: use the routing keys intead (see ``__init__()``).

        The ``key`` is the routing key and ``status`` is either "success" or "error".

        The default implementation dispatches the messages by calling methods in the form
        ``self.on_KEY(payload, status, correlation_id)`` where KEY is what is left
        after stripping the ``service.`` prefix from the routing key, and returning their return value.

        Return a falsy value (default is `None`) for automatic acknowledgement of processed message or
        a truthy one for manual acknowdegment later.

        **Never mix** manual acknowledgment with automatic end-of-handler acknowledgement.
        """
        handler = getattr(self, "on_" + key.split(".")[-1])
        if handler is not None:
            return handler(payload, conversation_id, status, correlation_id, meta)
        else:
            # should never happens: means we misconfigured the routing keys
            self.log(
                ERROR,
                f"unexpected result {key}; check your subscriptions!",
                conversation_id=conversation_id,
            )

    def handle_config(
        self, configuration: str, payload: JSON_MODEL, meta: Dict[str, str]
    ) -> bool:
        """Handle incoming configuration (may be overwritten by subclasses).

        The `payload` is already decoded and is a python data structure compatible with the JSON data model.
        You should never do any filtering here: use the routing keys intead (see ``__init__()``).

        ``configuration`` is the routing key.

        This callback must return a boolean flag to tell Eventail whether the service configuration is done
        and that the service can now process business events.

        The default implementation dispatches the messages by calling methods in the form
        ``self.on_KEY(payload) -> bool`` where key is the routing key.
        """
        handler = getattr(self, "on_" + configuration)
        if handler is not None:
            return handler(payload, meta)
        else:
            self.log(ERROR, "unexpected result {key}; check your subscriptions!")
            return False

    # Abstract methods

    def handle_returned_message(
        self, key: str, payload: JSON_MODEL, envelope: Dict[str, str]
    ):
        """Invoked when a message is returned (to be implemented by subclasses).

        A message maybe returned if:
        * it was sent with the `mandatory` flag on True;
        * and the broker was unable to route it to a queue.
        """
        pass

    def on_ready(self) -> None:
        """Code to execute once the business service is configured and comes online.

        (to be implemented by subclasses)
        """
        pass

    def on_unconfigured(self) -> None:
        """This handler is called when the service needs to be configured before it comes online.

        This callback is invoked only if there are configuration subscriptions (at least one).

        It is typically used to publish one or more configuration requests on the EDA configuration bus.

        (to be implemented by subclasses)
        """
        pass


class AckPolicy(AbstractContextManager):
    def __init__(
        self,
        endpoint: Service,
        ch: pika.channel.Channel,
        deliver: pika.spec.Basic.Deliver,
        conversation_id: str,
        correlation_id: str,
    ) -> None:
        self.endpoint = endpoint
        self.ch = ch
        self.deliver = deliver
        self.conversation_id = conversation_id
        self.correlation_id = correlation_id
        self._ack_on_exit = True

    def __exit__(
        self,
        exc_type: Union[type, None],
        exc_value: Union[BaseException, None],
        exc_tb,
    ) -> bool:
        if exc_tb is not None:
            error = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
            self.endpoint.log(
                ALERT,
                f"Unhandled error while processing message {self.deliver.routing_key}",
                error,
                conversation_id=self.conversation_id,
            )
            # retry once
            if not self.deliver.redelivered:
                self.ch.basic_nack(delivery_tag=self.deliver.delivery_tag, requeue=True)
            else:
                # dead letter
                self.endpoint.log(
                    EMERGENCY,
                    f"Giving up on {self.deliver.routing_key}",
                    error,
                    conversation_id=self.conversation_id,
                )
                self.ch.basic_nack(
                    delivery_tag=self.deliver.delivery_tag, requeue=False
                )
            return True
        elif self._ack_on_exit:
            self.endpoint._auto_ack(self.deliver.delivery_tag)
        return False

    def manual_ack(self, in_progress: Any):
        self._ack_on_exit = not bool(in_progress)
