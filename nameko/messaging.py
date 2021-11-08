"""
Provides core messaging decorators and dependency providers.
"""
from __future__ import absolute_import

import re
import asyncio
import aiormq
from functools import partial
from logging import getLogger

from kombu.common import maybe_declare

from nameko import config, serialization
from nameko.amqp.consume import Consumer as ConsumerCore, AIOConsumer as AIOConsumerCore
from nameko.amqp.publish import (
    Publisher as PublisherCore,
    AIOPublisher as AIOPublisherCore,
)
from nameko.amqp.publish import get_connection
from nameko.constants import (
    AMQP_SSL_CONFIG_KEY,
    AMQP_URI_CONFIG_KEY,
    DEFAULT_AMQP_URI,
    DEFAULT_HEARTBEAT,
    DEFAULT_PREFETCH_COUNT,
    HEADER_PREFIX,
    HEARTBEAT_CONFIG_KEY,
    LOGIN_METHOD_CONFIG_KEY,
    PREFETCH_COUNT_CONFIG_KEY,
)
from nameko.exceptions import ContainerBeingKilled
from nameko.extensions import DependencyProvider, Entrypoint


_log = getLogger(__name__)


def encode_to_headers(context_data, prefix=HEADER_PREFIX):
    return {
        "{}.{}".format(prefix, key): value
        for key, value in context_data.items()
        if value is not None
    }


def decode_from_headers(headers, prefix=HEADER_PREFIX):
    return {
        re.sub(r"^{}\.".format(prefix), "", key): value
        for key, value in headers.items()
    }


class AIOPublisher(DependencyProvider):

    publisher_cls = AIOPublisherCore

    def __init__(self, exchange_name=None, declare=None, **publisher_options):
        """Provides an AMQP message publisher method via dependency injection.

        In AMQP, messages are published to *exchanges* and routed to bound
        *queues*. This dependency accepts the `exchange` to publish to and
        will ensure that it is declared before publishing.

        Optionally, you may use the `declare` keyword argument to pass a list
        of other :class:`kombu.Exchange` or :class:`kombu.Queue` objects to
        declare before publishing.

        :Parameters:
            exchange_name : :str
                The name of the exchange to publish to.
            declare : list
                List of :class:`kombu.Exchange` or :class:`kombu.Queue` objects
                to declare before publishing.
            **publisher_options
                Options to configure the
                :class:`~nameko.amqqp.publish.Publisher` that sends the
                message.


        If `exchange` is not provided, the message will be published to the
        default exchange.

        Example::

            class Foobar(object):

                publish = Publisher(exchange=...)

                def spam(self, data):
                    self.publish('spam:' + data)
        """
        self.exchange_name = exchange_name
        self.publisher_options = publisher_options

        self.declare = declare[:] if declare is not None else []

        # if self.exchange:
        #     self.declare.append(self.exchange)

        default_uri = config.get(AMQP_URI_CONFIG_KEY, DEFAULT_AMQP_URI)
        self.amqp_uri = self.publisher_options.pop("uri", default_uri)

    async def setup(self):

        default_ssl = config.get(AMQP_SSL_CONFIG_KEY)
        ssl = self.publisher_options.pop("ssl", default_ssl)

        default_serializer = self.container.serializer
        serializer = self.publisher_options.pop("serializer", default_serializer)
        self.connection = await aiormq.connect(
            self.amqp_uri, loop=asyncio.get_event_loop()
        )
        self.channel = await self.connection.channel()

        self.publisher = self.publisher_cls(
            self.channel,
            ssl=ssl,
            serializer=serializer,
            content_type="application/json",
            exchange_name=self.exchange_name,
            **self.publisher_options
        )

    def get_dependency(self, worker_ctx):
        async def publish(msg, **kwargs):
            extra_headers = encode_to_headers(worker_ctx.context_data)
            await self.publisher.publish(msg, extra_headers=extra_headers, **kwargs)

        return publish


class Publisher(DependencyProvider):

    publisher_cls = PublisherCore

    def __init__(self, exchange=None, declare=None, **publisher_options):
        """Provides an AMQP message publisher method via dependency injection.

        In AMQP, messages are published to *exchanges* and routed to bound
        *queues*. This dependency accepts the `exchange` to publish to and
        will ensure that it is declared before publishing.

        Optionally, you may use the `declare` keyword argument to pass a list
        of other :class:`kombu.Exchange` or :class:`kombu.Queue` objects to
        declare before publishing.

        :Parameters:
            exchange : :class:`kombu.Exchange`
                Destination exchange
            declare : list
                List of :class:`kombu.Exchange` or :class:`kombu.Queue` objects
                to declare before publishing.
            **publisher_options
                Options to configure the
                :class:`~nameko.amqqp.publish.Publisher` that sends the
                message.


        If `exchange` is not provided, the message will be published to the
        default exchange.

        Example::

            class Foobar(object):

                publish = Publisher(exchange=...)

                def spam(self, data):
                    self.publish('spam:' + data)
        """
        self.exchange = exchange
        self.publisher_options = publisher_options

        self.declare = declare[:] if declare is not None else []

        if self.exchange:
            self.declare.append(self.exchange)

        default_uri = config.get(AMQP_URI_CONFIG_KEY, DEFAULT_AMQP_URI)
        self.amqp_uri = self.publisher_options.pop("uri", default_uri)

    def setup(self):

        default_ssl = config.get(AMQP_SSL_CONFIG_KEY)
        ssl = self.publisher_options.pop("ssl", default_ssl)

        default_login_method = config.get(LOGIN_METHOD_CONFIG_KEY)
        login_method = self.publisher_options.pop("login_method", default_login_method)

        with get_connection(self.amqp_uri, ssl=ssl, login_method=login_method) as conn:
            for entity in self.declare:
                maybe_declare(entity, conn.channel())

        default_serializer = self.container.serializer
        serializer = self.publisher_options.pop("serializer", default_serializer)

        self.publisher = self.publisher_cls(
            self.amqp_uri,
            ssl=ssl,
            serializer=serializer,
            exchange=self.exchange,
            declare=self.declare,
            **self.publisher_options
        )

    def get_dependency(self, worker_ctx):
        def publish(msg, **kwargs):
            extra_headers = encode_to_headers(worker_ctx.context_data)
            self.publisher.publish(msg, extra_headers=extra_headers, **kwargs)

        return publish


class AIOConsumer(Entrypoint):

    consumer_cls = AIOConsumerCore

    def __init__(
        self,
        queue,
        requeue_on_error=False,
        expected_exceptions=(),
        sensitive_arguments=(),
        **consumer_options
    ):
        """
        Decorates a method as a message consumer.

        Messages from the queue will be deserialized depending on their content
        type and passed to the the decorated method.
        When the consumer method returns without raising any exceptions,
        the message will automatically be acknowledged.
        If any exceptions are raised during the consumption and
        `requeue_on_error` is True, the message will be requeued.

        If `requeue_on_error` is true, handlers will return the event to the
        queue if an error occurs while handling it. Defaults to false.

        Example::

            @consume(...)
            def handle_message(self, body):

                if not self.spam(body):
                    raise Exception('message will be requeued')

                self.shrub(body)

        Args:
            queue: The queue to consume from.
        """
        self.queue = queue
        self.requeue_on_error = requeue_on_error
        self.consumer_options = consumer_options
        # TODO it's bad that we eat all the remaining keyword arguments as consumer opts
        super(AIOConsumer, self).__init__(
            expected_exceptions=expected_exceptions,
            sensitive_arguments=sensitive_arguments,
        )

    @property
    def amqp_uri(self):
        return config[AMQP_URI_CONFIG_KEY]

    async def setup(self):
        ssl = config.get(AMQP_SSL_CONFIG_KEY)

        heartbeat = self.consumer_options.pop(
            "heartbeat", config.get(HEARTBEAT_CONFIG_KEY, DEFAULT_HEARTBEAT)
        )
        prefetch_count = self.consumer_options.pop(
            "prefetch_count",
            config.get(PREFETCH_COUNT_CONFIG_KEY, DEFAULT_PREFETCH_COUNT),
        )
        accept = self.consumer_options.pop("accept", serialization.setup().accept)

        queues = [self.queue]
        callbacks = [self.handle_message]

        self.consumer = self.consumer_cls(
            self.amqp_uri,
            ssl=ssl,
            queues=queues,
            callbacks=callbacks,
            heartbeat=heartbeat,
            prefetch_count=prefetch_count,
            accept=accept,
            **self.consumer_options
        )

    async def start(self):
        await self.consumer.run()

    def stop(self):
        self.consumer.stop()

    async def handle_message(self, message):
        print("message: ", message)
        body = message.body
        print("body: ", body)
        args = (body,)
        kwargs = {}

        context_data = decode_from_headers(message.header.properties.headers)
        print("context_data: ", context_data)

        handle_result = partial(self.handle_result, message)

        async def spawn_worker():
            try:
                await self.container.spawn_worker(
                    self,
                    args,
                    kwargs,
                    context_data=context_data,
                    handle_result=handle_result,
                )
            except ContainerBeingKilled:
                self.consumer.requeue_message(message)

        service_name = self.container.service_name
        method_name = self.method_name

        # we must spawn a thread here to prevent handle_message blocking
        # when the worker pool is exhausted; if this happens the AMQP consumer
        # is also blocked and fails to send heartbeats, eventually causing it
        # to be disconnected
        # TODO replace global worker pool limits with per-entrypoint limits,
        # then remove this waiter thread
        ident = u"{}.wait_for_worker_pool[{}.{}]".format(
            type(self).__name__, service_name, method_name
        )
        await self.container.spawn_managed_thread(spawn_worker, identifier=ident)

    async def handle_result(self, message, worker_ctx, result=None, exc_info=None):
        await self.handle_message_processed(message, result, exc_info)
        return result, exc_info

    async def handle_message_processed(self, message, result=None, exc_info=None):
        await self.consumer.ack_message(message)
        # if exc_info is not None and self.requeue_on_error:
        #     self.consumer.requeue_message(message)
        # else:
        #     self.consumer.ack_message(message)


class Consumer(Entrypoint):

    consumer_cls = ConsumerCore

    def __init__(
        self,
        queue,
        requeue_on_error=False,
        expected_exceptions=(),
        sensitive_arguments=(),
        **consumer_options
    ):
        """
        Decorates a method as a message consumer.

        Messages from the queue will be deserialized depending on their content
        type and passed to the the decorated method.
        When the consumer method returns without raising any exceptions,
        the message will automatically be acknowledged.
        If any exceptions are raised during the consumption and
        `requeue_on_error` is True, the message will be requeued.

        If `requeue_on_error` is true, handlers will return the event to the
        queue if an error occurs while handling it. Defaults to false.

        Example::

            @consume(...)
            def handle_message(self, body):

                if not self.spam(body):
                    raise Exception('message will be requeued')

                self.shrub(body)

        Args:
            queue: The queue to consume from.
        """
        self.queue = queue
        self.requeue_on_error = requeue_on_error
        self.consumer_options = consumer_options
        # TODO it's bad that we eat all the remaining keyword arguments as consumer opts
        super(Consumer, self).__init__(
            expected_exceptions=expected_exceptions,
            sensitive_arguments=sensitive_arguments,
        )

    @property
    def amqp_uri(self):
        return config[AMQP_URI_CONFIG_KEY]

    def setup(self):
        ssl = config.get(AMQP_SSL_CONFIG_KEY)
        login_method = config.get(LOGIN_METHOD_CONFIG_KEY)

        heartbeat = self.consumer_options.pop(
            "heartbeat", config.get(HEARTBEAT_CONFIG_KEY, DEFAULT_HEARTBEAT)
        )
        prefetch_count = self.consumer_options.pop(
            "prefetch_count",
            config.get(PREFETCH_COUNT_CONFIG_KEY, DEFAULT_PREFETCH_COUNT),
        )
        accept = self.consumer_options.pop("accept", serialization.setup().accept)

        queues = [self.queue]
        callbacks = [self.handle_message]

        self.consumer = self.consumer_cls(
            self.amqp_uri,
            ssl=ssl,
            login_method=login_method,
            queues=queues,
            callbacks=callbacks,
            heartbeat=heartbeat,
            prefetch_count=prefetch_count,
            accept=accept,
            **self.consumer_options
        )

    def start(self):
        self.container.spawn_managed_thread(self.consumer.run)
        self.consumer.wait_until_consumer_ready()

    def stop(self):
        self.consumer.stop()

    def handle_message(self, body, message):
        args = (body,)
        kwargs = {}

        context_data = decode_from_headers(message.headers)

        handle_result = partial(self.handle_result, message)

        def spawn_worker():
            try:
                self.container.spawn_worker(
                    self,
                    args,
                    kwargs,
                    context_data=context_data,
                    handle_result=handle_result,
                )
            except ContainerBeingKilled:
                self.consumer.requeue_message(message)

        service_name = self.container.service_name
        method_name = self.method_name

        # we must spawn a thread here to prevent handle_message blocking
        # when the worker pool is exhausted; if this happens the AMQP consumer
        # is also blocked and fails to send heartbeats, eventually causing it
        # to be disconnected
        # TODO replace global worker pool limits with per-entrypoint limits,
        # then remove this waiter thread
        ident = u"{}.wait_for_worker_pool[{}.{}]".format(
            type(self).__name__, service_name, method_name
        )
        self.container.spawn_managed_thread(spawn_worker, identifier=ident)

    def handle_result(self, message, worker_ctx, result=None, exc_info=None):
        self.handle_message_processed(message, result, exc_info)
        return result, exc_info

    def handle_message_processed(self, message, result=None, exc_info=None):

        if exc_info is not None and self.requeue_on_error:
            self.consumer.requeue_message(message)
        else:
            self.consumer.ack_message(message)


consume = AIOConsumer.decorator
