from distributed.client import default_client, Future
from tornado.locks import Condition
from tornado.queues import Queue
from tornado import gen

from .core import Stream


def __stream_map__(self, func):
    return default_client().submit(func, self)


def __stream_reduce__(self, func, accumulator):
    return default_client().submit(func, accumulator, self)


Future.__stream_map__ = __stream_map__
Future.__stream_reduce__ = __stream_reduce__


class scatter(Stream):
    def __init__(self, child, limit=10, client=None):
        self.client = client or default_client()
        self.queue = Queue(maxsize=limit)
        self.condition = Condition()

        Stream.__init__(self, child)

        self.client.loop.add_callback(self.cb)

    def update(self, x, who=None):
        return self.queue.put(x)

    @gen.coroutine
    def cb(self):
        while True:
            x = yield self.queue.get()
            L = [x]
            while not self.queue.empty():
                L.append(self.queue.get_nowait())
            futures = yield self.client._scatter(L)
            for f in futures:
                yield self.emit(f)
            if self.queue.empty():
                self.condition.notify_all()

    @gen.coroutine
    def flush(self):
        while not self.queue.empty():
            yield self.condition.wait()


class gather(Stream):
    def __init__(self, child, limit=10, client=None):
        self.client = client or default_client()
        self.queue = Queue(maxsize=limit)
        self.condition = Condition()

        Stream.__init__(self, child)

        self.client.loop.add_callback(self.cb)

    def update(self, x, who=None):
        return self.queue.put(x)

    @gen.coroutine
    def cb(self):
        while True:
            x = yield self.queue.get()
            L = [x]
            while not self.queue.empty():
                L.append(self.queue.get_nowait())
            results = yield self.client._gather(L)
            for x in results:
                yield self.emit(x)
            if self.queue.empty():
                self.condition.notify_all()

    @gen.coroutine
    def flush(self):
        while not self.queue.empty():
            yield self.condition.wait()
