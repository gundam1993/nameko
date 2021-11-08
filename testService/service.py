#!/usr/bin/env python
# -*- coding:utf-8 -*-

from nameko.events import EventDispatcher, event_handler
from nameko.rpc import rpc, RpcProxy


class Test:
    name="test"

    users_rpc = RpcProxy("users")
    event_dispatcher = EventDispatcher()

    @rpc
    async def ping(self):
        result_co = self.users_rpc.health_check()
        result = await result_co
        print('result: ', result)
        await self.event_dispatcher("test_event", {"data": "test"})
        return "pong"

    @event_handler("test", "test_event")
    async def handle_test_event(self, data):
        print('data: ', data)
