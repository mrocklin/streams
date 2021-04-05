from __future__ import absolute_import, division, print_function

from operator import getitem

from dask.compatibility import apply
from dask.distributed import get_worker
from distributed.client import default_client
from tornado import gen

from . import core, sources
from .core import Stream


class DaskStream(Stream):
    """ A Parallel stream using Dask

    This object is fully compliant with the ``streamz.core.Stream`` object but
    uses a Dask client for execution.  Operations like ``map`` and
    ``accumulate`` submit functions to run on the Dask instance using
    ``dask.distributed.Client.submit`` and pass around Dask futures.
    Time-based operations like ``timed_window``, buffer, and so on operate as
    normal.

    Typically one transfers between normal Stream and DaskStream objects using
    the ``Stream.scatter()`` and ``DaskStream.gather()`` methods.

    Examples
    --------
    >>> from dask.distributed import Client
    >>> client = Client()

    >>> from streamz import Stream
    >>> source = Stream()
    >>> source.scatter().map(func).accumulate(binop).gather().sink(...)

    See Also
    --------
    dask.distributed.Client
    """
    def __init__(self, *args, **kwargs):
        kwargs["ensure_io_loop"] = True
        super().__init__(*args, **kwargs)


@DaskStream.register_api()
class map(DaskStream):
    def __init__(self, upstream, func, *args, **kwargs):
        self.func = func
        self.kwargs = kwargs
        self.args = args

        DaskStream.__init__(self, upstream)

    def update(self, x, who=None, metadata=None):
        client = default_client()
        result = client.submit(self.func, x, *self.args, **self.kwargs)
        return self._emit(result, metadata=metadata)


@DaskStream.register_api()
class accumulate(DaskStream):
    def __init__(self, upstream, func, start=core.no_default,
                 returns_state=False, **kwargs):
        self.func = func
        self.state = start
        self.returns_state = returns_state
        self.kwargs = kwargs
        self.with_state = kwargs.pop('with_state', False)
        DaskStream.__init__(self, upstream)

    def update(self, x, who=None, metadata=None):
        if self.state is core.no_default:
            self.state = x
            if self.with_state:
                return self._emit((self.state, x), metadata=metadata)
            else:
                return self._emit(x, metadata=metadata)
        else:
            client = default_client()
            result = client.submit(self.func, self.state, x, **self.kwargs)
            if self.returns_state:
                state = client.submit(getitem, result, 0)
                result = client.submit(getitem, result, 1)
            else:
                state = result
            self.state = state
            if self.with_state:
                return self._emit((self.state, result), metadata=metadata)
            else:
                return self._emit(result, metadata=metadata)


@core.Stream.register_api()
@DaskStream.register_api()
class scatter(DaskStream):
    """ Convert local stream to Dask Stream

    All elements flowing through the input will be scattered out to the cluster
    """
    @gen.coroutine
    def update(self, x, who=None, metadata=None):
        client = default_client()

        self._retain_refs(metadata)
        # We need to make sure that x is treated as it is by dask
        # However, client.scatter works internally different for
        # lists and dicts. So we always use a list here to be sure
        # we know the format exactly. We do not use a key to avoid
        # issues like https://github.com/python-streamz/streams/issues/397.
        future_as_list = yield client.scatter([x], asynchronous=True, hash=False)
        future = future_as_list[0]
        f = yield self._emit(future, metadata=metadata)
        self._release_refs(metadata)

        raise gen.Return(f)


@DaskStream.register_api()
class gather(core.Stream):
    """ Wait on and gather results from DaskStream to local Stream

    This waits on every result in the stream and then gathers that result back
    to the local stream.  Warning, this can restrict parallelism.  It is common
    to combine a ``gather()`` node with a ``buffer()`` to allow unfinished
    futures to pile up.

    Examples
    --------
    >>> local_stream = dask_stream.buffer(20).gather()

    See Also
    --------
    buffer
    scatter
    """
    @gen.coroutine
    def update(self, x, who=None, metadata=None):
        client = default_client()

        self._retain_refs(metadata)
        result = yield client.gather(x, asynchronous=True)
        result2 = yield self._emit(result, metadata=metadata)
        self._release_refs(metadata)

        raise gen.Return(result2)


@DaskStream.register_api()
class starmap(DaskStream):
    def __init__(self, upstream, func, **kwargs):
        self.func = func
        stream_name = kwargs.pop('stream_name', None)
        self.kwargs = kwargs

        DaskStream.__init__(self, upstream, stream_name=stream_name)

    def update(self, x, who=None, metadata=None):
        client = default_client()
        result = client.submit(apply, self.func, x, self.kwargs)
        return self._emit(result, metadata=metadata)


@DaskStream.register_api()
class buffer(DaskStream, core.buffer):
    pass


@DaskStream.register_api()
class combine_latest(DaskStream, core.combine_latest):
    pass


@DaskStream.register_api()
class delay(DaskStream, core.delay):
    pass


@DaskStream.register_api()
class latest(DaskStream, core.latest):
    pass


@DaskStream.register_api()
class partition(DaskStream, core.partition):
    pass


@DaskStream.register_api()
class rate_limit(DaskStream, core.rate_limit):
    pass


@DaskStream.register_api()
class sliding_window(DaskStream, core.sliding_window):
    pass


@DaskStream.register_api()
class timed_window(DaskStream, core.timed_window):
    pass


@DaskStream.register_api()
class union(DaskStream, core.union):
    pass


@DaskStream.register_api()
class zip(DaskStream, core.zip):
    pass


@DaskStream.register_api(staticmethod)
class filenames(DaskStream, sources.filenames):
    pass


@DaskStream.register_api(staticmethod)
class from_textfile(DaskStream, sources.from_textfile):
    pass


@DaskStream.register_api()
class to_kafka(DaskStream):
    """ Write values to Kafka directly from the Dask worker

    Parameters
    ----------
    topic : string
        The topic which to write
    producer_config : dict
        Settings to set up the stream, see
        https://docs.confluent.io/current/clients/confluent-kafka-python/#configuration
        https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        Examples:
        bootstrap.servers: Connection string (host:port) to Kafka

    """
    def __init__(self, upstream, topic, producer_config, **kwargs):
        self.topic = topic
        self.producer_config = producer_config

        stream_name = kwargs.pop('stream_name', None)
        DaskStream.__init__(self, upstream, stream_name=stream_name)

    def update(self, x, who=None):

        def get_producer(config):
            w = get_worker()
            if hasattr(w, 'producer'):
                return w.producer

            import confluent_kafka as ck
            w.producer = getattr(ck, 'Producer')(config)
            return w.producer

        def produce(topic, value, producer_config):
            producer = get_producer(producer_config)
            producer.produce(topic, value)
            producer.flush()
            return value

        client = default_client()
        result = client.submit(produce, self.topic, x, self.producer_config)
        self._emit(result)
