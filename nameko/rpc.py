from __future__ import absolute_import, unicode_literals

import sys
import uuid
import asyncio
from functools import partial
from logging import getLogger
import aiormq
import json

import kombu.serialization
from amqp.exceptions import NotFound
from kombu import Exchange, Queue

from nameko import config, serialization
from nameko.amqp.consume import AIOConsumer
from nameko.amqp.publish import (
    AIOPublisher,
    UndeliverableMessage,
    get_connection,
)
from nameko.constants import (
    AMQP_SSL_CONFIG_KEY,
    AMQP_URI_CONFIG_KEY,
    DEFAULT_AMQP_URI,
    DEFAULT_HEARTBEAT,
    DEFAULT_PREFETCH_COUNT,
    HEARTBEAT_CONFIG_KEY,
    PREFETCH_COUNT_CONFIG_KEY,
    RPC_EXCHANGE_CONFIG_KEY,
)
from nameko.exceptions import (
    ContainerBeingKilled,
    MalformedRequest,
    MethodNotFound,
    ReplyQueueExpiredWithPendingReplies,
    UnknownService,
    UnserializableValueError,
    deserialize,
    serialize,
)
from nameko.extensions import (
    DependencyProvider,
    Entrypoint,
    ProviderCollector,
    SharedExtension,
)
from nameko.messaging import decode_from_headers, encode_to_headers


_log = getLogger(__name__)


RPC_QUEUE_TEMPLATE = "rpc-{}"
RPC_REPLY_QUEUE_TEMPLATE = "rpc.reply-{}-{}"
RPC_REPLY_QUEUE_TTL = 300000  # ms (5 mins)

RESTRICTED_PUBLISHER_OPTIONS = (
    "exchange",
    "routing_key",
    "mandatory",
    "reply_to",
    "correlation_id",
)
"""
Publisher options that cannot be overridden when configuring an RPC client
"""


def get_rpc_exchange():
    # TODO: refactor this ugliness
    exchange_name = config.get(RPC_EXCHANGE_CONFIG_KEY, "nameko-rpc")
    exchange = Exchange(exchange_name, durable=True, type="topic")
    return exchange


class RpcConsumer(SharedExtension, ProviderCollector):

    consumer_cls = AIOConsumer

    def __init__(self, **consumer_options):
        self.queue = None
        self.consumer_options = consumer_options
        super(RpcConsumer, self).__init__()

    @property
    def amqp_uri(self):
        return config.get(AMQP_URI_CONFIG_KEY, DEFAULT_AMQP_URI)

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
        )

    async def start(self):
        await self.consumer.run()
        service_name = self.container.service_name
        queue_name = RPC_QUEUE_TEMPLATE.format(service_name)
        routing_key = "{}.*".format(service_name)
        exchange_name = config.get(RPC_EXCHANGE_CONFIG_KEY, "nameko-rpc")
        # Declare an exchange
        await self.consumer.channel.exchange_declare(
            exchange=exchange_name,
            exchange_type="topic",
            durable=True,
        )

        declare_queue_result = await self.consumer.channel.queue_declare(
            queue=queue_name,
            durable=True,
        )
        self.queue = declare_queue_result.queue
        await self.consumer.channel.queue_bind(
            self.queue, exchange_name, routing_key=routing_key
        )
        await self.consumer.channel.basic_consume(self.queue, self.handle_message)

    def stop(self):
        self.consumer.stop()
        # super stop? (waits for all other providers)

    def unregister_provider(self, provider):
        self.stop()
        super(RpcConsumer, self).unregister_provider(provider)

    def get_provider_for_method(self, routing_key):
        service_name = self.container.service_name

        for provider in self._providers:
            key = "{}.{}".format(service_name, provider.method_name)
            if key == routing_key:
                return provider
        else:
            method_name = routing_key.split(".")[-1]
            raise MethodNotFound(method_name)

    async def handle_message(self, message: aiormq.types.DeliveredMessage):
        routing_key = message.delivery.routing_key
        body = json.loads(message.body.decode("ascii"))
        try:
            provider = self.get_provider_for_method(routing_key)
            await provider.handle_message(body, message)
        except Exception:
            exc_info = sys.exc_info()
            await self.handle_result(message, None, exc_info)

    async def handle_result(self, message, result, exc_info):

        exchange_name = config.get(RPC_EXCHANGE_CONFIG_KEY, "nameko-rpc")
        ssl = config.get(AMQP_SSL_CONFIG_KEY)

        responder = Responder(self.amqp_uri, exchange_name, message, ssl=ssl)
        result, exc_info = await responder.send_response(result, exc_info)

        await self.consumer.ack_message(message)

        return result, exc_info


class Rpc(Entrypoint):
    """
    A limitation of using a shared queue for all RPC entrypoints is
    that we can't accept per-entrypoint consumer options. The best solution
    to this is to start using a queue per entrypoint, but this will require
    a consumer (and if using kombu, a connection) per entrypoint.

    For the time being consumer options are not supported in RPC entrypoints.
    """

    rpc_consumer = RpcConsumer()

    async def setup(self):
        self.rpc_consumer.register_provider(self)

    def stop(self):
        self.rpc_consumer.unregister_provider(self)

    async def start(self):
        """TODO: should be removed after update Extension class"""
        return

    async def handle_message(self, body, message):
        try:
            args = body["args"]
            kwargs = body["kwargs"]
        except KeyError:
            raise MalformedRequest("Message missing `args` or `kwargs`")

        self.check_signature(args, kwargs)

        context_data = decode_from_headers(message.header.properties.headers)

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
                self.rpc_consumer.consumer.requeue_message(message)

        service_name = self.container.service_name
        method_name = self.method_name

        # we must spawn a thread here to prevent handle_message blocking
        # when the worker pool is exhausted; if this happens the AMQP consumer
        # is also blocked and fails to send heartbeats, eventually causing it
        # to be disconnected
        # TODO replace global worker pool limits with per-entrypoint limits,
        # then remove this waiter thread
        ident = "{}.wait_for_worker_pool[{}.{}]".format(
            type(self).__name__, service_name, method_name
        )
        await self.container.spawn_managed_thread(spawn_worker, identifier=ident)

    async def handle_result(self, message, worker_ctx, result, exc_info):
        result, exc_info = await self.rpc_consumer.handle_result(
            message, result, exc_info
        )
        return result, exc_info


rpc = Rpc.decorator


class Responder(object):
    """Helper object for publishing replies to RPC calls."""

    publisher_cls = AIOPublisher

    def __init__(self, amqp_uri, exchange_name, message, ssl=None):
        self.amqp_uri = amqp_uri
        self.message = message
        self.exchange_name = exchange_name
        self.ssl = ssl

    async def send_response(self, result, exc_info):

        error = None
        if exc_info is not None:
            error = serialize(exc_info[1])

        # send response encoded the same way as was the request message
        content_type = self.message.header.properties.content_type
        content_type = "application/json"
        serializer = kombu.serialization.registry.type_to_name[content_type]

        # disaster avoidance serialization check: `result` must be
        # serializable, otherwise the container will commit suicide assuming
        # unrecoverable errors (and the message will be requeued for another
        # victim)

        try:
            kombu.serialization.dumps(result, serializer)
        except Exception:
            exc_info = sys.exc_info()
            # `error` below is guaranteed to serialize to json
            error = serialize(UnserializableValueError(result))
            result = None

        payload = {"result": result, "error": error}

        routing_key = self.message.header.properties.reply_to
        correlation_id = self.message.header.properties.correlation_id

        publisher = self.publisher_cls(self.message.channel, ssl=self.ssl)

        await publisher.publish(
            payload,
            serializer=serializer,
            exchange_name=self.exchange_name,
            routing_key=routing_key,
            correlation_id=correlation_id,
            content_type=content_type,
        )
        return result, exc_info


class ReplyListener(SharedExtension):
    """SharedExtension for consuming replies to RPC requests.

    Creates a queue and consumes from it in a managed thread. RPC requests
    should register their `correlation_id` with
    :meth:`~ReplyListener.register_for_reply` in order for the `ReplyListener`
    to capture the reply.
    """

    class ReplyConsumer(AIOConsumer):
        """Subclass Consumer to add disconnection check"""

        def __init__(self, check_for_lost_replies, *args, **kwargs):
            self.check_for_lost_replies = check_for_lost_replies
            super(ReplyListener.ReplyConsumer, self).__init__(*args, **kwargs)

        def get_consumers(self, consumer_cls, channel):
            """
            Check for messages lost while the reply listener was disconnected
            from the broker.
            """
            self.check_for_lost_replies()

            return super(ReplyListener.ReplyConsumer, self).get_consumers(
                consumer_cls, channel
            )

    consumer_cls = ReplyConsumer

    def __init__(self, **consumer_options):
        self.queue = None
        self.pending = {}
        self.consumer_options = consumer_options
        super(ReplyListener, self).__init__()

    @property
    def amqp_uri(self):
        return config.get(AMQP_URI_CONFIG_KEY, DEFAULT_AMQP_URI)

    async def setup(self):

        if self.queue is not None:
            return

        reply_queue_uuid = uuid.uuid4()
        service_name = self.container.service_name

        queue_name = RPC_REPLY_QUEUE_TEMPLATE.format(service_name, reply_queue_uuid)

        self.routing_key = str(reply_queue_uuid)

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
            self.check_for_lost_replies,
            self.amqp_uri,
            ssl=ssl,
            queues=queues,
            callbacks=callbacks,
            heartbeat=heartbeat,
            prefetch_count=prefetch_count,
            accept=accept,
        )
        await self.consumer.run()
        exchange_name = config.get(RPC_EXCHANGE_CONFIG_KEY, "nameko-rpc")
        # Declare an exchange
        await self.consumer.channel.exchange_declare(
            exchange=exchange_name,
            exchange_type="topic",
            durable=True,
        )
        declare_queue_result = await self.consumer.channel.queue_declare(
            queue=queue_name, durable=True, arguments={"x-expires": RPC_REPLY_QUEUE_TTL}
        )
        self.queue = declare_queue_result.queue
        await self.consumer.channel.queue_bind(
            self.queue, exchange_name, routing_key=self.routing_key
        )
        await self.consumer.channel.basic_consume(self.queue, self.handle_message)

    def stop(self):
        self.consumer.stop()

    def check_for_lost_replies(self):
        if self.pending:
            try:
                ssl = config.get(AMQP_SSL_CONFIG_KEY)
                with get_connection(self.amqp_uri, ssl) as conn:
                    self.queue.bind(conn).queue_declare(passive=True)
            except NotFound:
                raise ReplyQueueExpiredWithPendingReplies(
                    "Lost replies for correlation ids:\n{}".format(
                        "\n".join(self.pending.keys())
                    )
                )

    def register_for_reply(self, correlation_id):
        """Register an RPC call with the given `correlation_id` for a reply.

        Returns a function that can be used to retrieve the reply, blocking
        until it has been received.
        """
        loop = asyncio.get_event_loop()
        future = loop.create_future()
        self.pending[correlation_id] = future
        return future

    async def handle_message(self, message: aiormq.types.DeliveredMessage):
        await self.consumer.ack_message(message)
        body = json.loads(message.body.decode("ascii"))
        correlation_id = message.header.properties.correlation_id
        future = self.pending.pop(correlation_id, None)
        if future is not None:
            future.set_result(body)
        else:
            _log.debug("Unknown correlation id: %s", correlation_id)


class ClusterRpc(DependencyProvider):
    """DependencyProvider for injecting an RPC client to a cluster of
    services into a service.

    :Parameters:
        **publisher_options
            Options to configure the :class:`~nameko.amqqp.publish.Publisher`
            that sends the message.
    """

    publisher_cls = AIOPublisher

    reply_listener = ReplyListener()

    def __init__(self, **publisher_options):

        for option in RESTRICTED_PUBLISHER_OPTIONS:
            publisher_options.pop(option, None)
        self.publisher_options = publisher_options

        default_uri = config.get(AMQP_URI_CONFIG_KEY, DEFAULT_AMQP_URI)
        self.amqp_uri = self.publisher_options.get("uri", default_uri)

    async def setup(self):

        default_serializer = self.container.serializer
        serializer = self.publisher_options.pop("serializer", default_serializer)

        default_ssl = config.get(AMQP_SSL_CONFIG_KEY)
        ssl = self.publisher_options.pop("ssl", default_ssl)

        self.publisher_options.pop("uri", None)
        await self.reply_listener.setup()
        self.publisher = self.publisher_cls(
            self.reply_listener.consumer.channel,
            ssl=ssl,
            serializer=serializer,
            declare=[self.reply_listener.queue],
            reply_to=self.reply_listener.routing_key,
            content_type="application/json",
            exchange_name=config.get(RPC_EXCHANGE_CONFIG_KEY, "nameko-rpc"),
            **self.publisher_options
        )

    def get_dependency(self, worker_ctx):

        publish = self.publisher.publish
        register_for_reply = self.reply_listener.register_for_reply

        return Client(publish, register_for_reply, worker_ctx.context_data)


class ServiceRpc(ClusterRpc):
    """DependencyProvider for injecting an RPC client to a specific service
    into a service.

    As per :class:`~nameko.rpc.ClusterRpc` but with a pre-specified target
    service.

    :Parameters:
        target_service : str
            Target service name
    """

    def __init__(self, target_service, **kwargs):
        self.target_service = target_service
        super(ServiceRpc, self).__init__(**kwargs)

    def get_dependency(self, worker_ctx):
        client = super(ServiceRpc, self).get_dependency(worker_ctx)
        return getattr(client, self.target_service)


class Client(object):
    """Helper object for making RPC calls.

    The target service and method name may be specified at construction time
    or by attribute or dict access, for example:

        # target at construction time
        client = Client(
            publish, register_for_reply, context_data,
            "target_service", "target_method"
        )
        client(*args, **kwargs)

        # equivalent with attribute access
        client = Client(publish, register_for_reply, context_data)
        client = client.target_service.target_method  # now fully-specified
        client(*args, **kwargs)

    Calling a fully-specified `Client` will make the RPC call and block for the
    result. The `call_async` method initiates the call but returns an
    `RpcReply` object that can be used later to retrieve the result.

    :Parameters:
        publish : callable
            Function to publish an RPC request
        register_for_reply : callable
            Function to register a new call with a reply listener. Returns
            another function that should be used to retrieve the response.
        context_data: dict
            Worker context data to be sent as extra headers
        service_name : str
            Optional target service name, if known
        method_name : str
            Optional target method name, if known
    """

    def __init__(
        self,
        publish,
        register_for_reply,
        context_data,
        service_name=None,
        method_name=None,
    ):
        self.publish = publish
        self.register_for_reply = register_for_reply
        self.context_data = context_data
        self.service_name = service_name
        self.method_name = method_name

    def __getattr__(self, name):
        if self.method_name is not None:
            raise AttributeError(name)

        if self.service_name:
            target_service = self.service_name
            target_method = name
        else:
            target_service = name
            target_method = None

        clone = Client(
            self.publish,
            self.register_for_reply,
            self.context_data,
            target_service,
            target_method,
        )
        return clone

    @property
    def fully_specified(self):
        return self.service_name is not None and self.method_name is not None

    @property
    def identifier(self):
        return "{}.{}".format(self.service_name or "*", self.method_name or "*")

    def __getitem__(self, name):
        """Enable dict-like access on the client."""
        return getattr(self, name)

    async def __call__(self, *args, **kwargs):
        rpc_call = await self._call(*args, **kwargs)
        response = await rpc_call
        error = response.get("error")
        if error:
            raise deserialize(error)
        return response["result"]

    def call_async(self, *args, **kwargs):
        rpc_call = self._call(*args, **kwargs)
        return rpc_call

    async def _call(self, *args, **kwargs):
        if not self.fully_specified:
            raise ValueError(
                "Cannot call unspecified method {}".format(self.identifier)
            )

        _log.debug("invoking %s", self)

        # We use the `mandatory` flag in `producer.publish` below to catch rpc
        # calls to non-existent services, which would otherwise wait forever
        # for a reply that will never arrive.
        #
        # However, the basic.return ("no one is listening for topic") is sent
        # asynchronously and conditionally, so we can't wait() on the channel
        # for it (will wait forever on successful delivery).
        #
        # Instead, we make use of (the rabbitmq extension) confirm_publish
        # (https://www.rabbitmq.com/confirms.html), which _always_ sends a
        # reply down the channel. Moreover, in the case where no queues are
        # bound to the exchange (service unknown), the basic.return is sent
        # first, so by the time kombu returns (after waiting for the confim)
        # we can reliably check for returned messages.

        # Note that deactivating publish-confirms in the Client will disable
        # this functionality and therefore :class:`UnknownService` will never
        # be raised (and the caller will hang).

        correlation_id = str(uuid.uuid4())

        extra_headers = encode_to_headers(self.context_data)
        send_request = partial(
            self.publish,
            routing_key=self.identifier,
            mandatory=True,
            correlation_id=correlation_id,
            extra_headers=extra_headers,
        )
        get_response_future = self.register_for_reply(correlation_id)

        rpc_call = RpcCall(correlation_id, send_request, get_response_future)

        try:
            future = await rpc_call.send_request(*args, **kwargs)
        except UndeliverableMessage:
            raise UnknownService(self.service_name)

        return future


class RpcCall(object):
    """Encapsulates a single RPC request and response.

    :Parameters:
        correlation_id : str
            Identifier for this call
        send_request : callable
            Function that initiates the request
        get_response_future : asyncio.Future
            ???
    """

    _response = None

    def __init__(self, correlation_id, send_request, get_response_future):
        self.correlation_id = correlation_id
        self._send_request = send_request
        self._get_response_future = get_response_future

    async def send_request(self, *args, **kwargs):
        """Send the RPC request to the remote service"""
        payload = {"args": args, "kwargs": kwargs}
        await self._send_request(payload)
        return self._get_response_future

    def get_response(self):
        """Retrieve the response for this RPC call. Blocks if the response
        has not been received.
        """
        if self._response is not None:
            return self._response

        self._response = self._get_response()
        return self._response

    def result(self):
        """Return the result of this RPC call, blocking if the response
        has not been received.

        Raises a `RemoteError` if the remote service returned an error
        response.
        """
        response = self.get_response()
        response = json.loads(response)
        error = response.get("error")
        if error:
            raise deserialize(error)
        return response["result"]


RpcProxy = ServiceRpc  # backwards compat
Proxy = Client  # backwards compat
