from __future__ import absolute_import, division, print_function
from functools import wraps

from .core import _truthy
from .core import get_io_loop
from .clients import DEFAULT_BACKENDS
from operator import getitem

from tornado import gen

from dask.compatibility import apply
from distributed.client import default_client

from .core import Stream
from . import core, sources

from collections import Sequence


NULL_COMPUTE = "~~NULL_COMPUTE~~"


def return_null(func):
    @wraps(func)
    def inner(x, *args, **kwargs):
        tv = func(x, *args, **kwargs)
        if tv:
            return x
        else:
            return NULL_COMPUTE

    return inner


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
        if 'loop' not in kwargs:
            kwargs['loop'] = default_client().loop
        super(DaskStream, self).__init__(*args, **kwargs)


@DaskStream.register_api()
class map(DaskStream):
    def __init__(self, upstream, func, *args, **kwargs):
        self.func = func
        self.kwargs = kwargs
        self.args = args

        DaskStream.__init__(self, upstream)

    def update(self, x, who=None):
        client = default_client()
        result = client.submit(self.func, x, *self.args, **self.kwargs)
        return self._emit(result)


@DaskStream.register_api()
class accumulate(DaskStream):
    def __init__(self, upstream, func, start=core.no_default,
                 returns_state=False, **kwargs):
        self.func = func
        self.state = start
        self.returns_state = returns_state
        self.kwargs = kwargs
        DaskStream.__init__(self, upstream)

    def update(self, x, who=None):
        if self.state is core.no_default:
            self.state = x
            return self._emit(self.state)
        else:
            client = default_client()
            result = client.submit(self.func, self.state, x, **self.kwargs)
            if self.returns_state:
                state = client.submit(getitem, result, 0)
                result = client.submit(getitem, result, 1)
            else:
                state = result
            self.state = state
            return self._emit(result)


@core.Stream.register_api()
@DaskStream.register_api()
class scatter(DaskStream):
    """ Convert local stream to Dask Stream

    All elements flowing through the input will be scattered out to the cluster
    """
    @gen.coroutine
    def update(self, x, who=None):
        client = default_client()
        future = yield client.scatter(x, asynchronous=True)
        f = yield self._emit(future)
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

    def __init__(self, *args, backend="dask", **kwargs):
        super().__init__(*args, **kwargs)
        upstream_backends = set(
            [getattr(u, "default_client", None) for u in self.upstreams]
        )
        if None in upstream_backends:
            upstream_backends.remove(None)
        if len(upstream_backends) > 1:
            raise RuntimeError("Mixing backends is not supported")
        elif upstream_backends:
            self.default_client = upstream_backends.pop()
        else:
            self.default_client = DEFAULT_BACKENDS.get(backend, backend)
        if "loop" not in kwargs and getattr(
            self.default_client(), "loop", None
        ):
            loop = self.default_client().loop
            self._set_loop(loop)
            if kwargs.get("ensure_io_loop", False) and not self.loop:
                self._set_asynchronous(False)
            if self.loop is None and self.asynchronous is not None:
                self._set_loop(get_io_loop(self.asynchronous))

    @gen.coroutine
    def update(self, x, who=None):
        client = self.default_client()
        result = yield client.gather(x, asynchronous=True)
        if (
            not (
                isinstance(result, Sequence)
                and any(r == NULL_COMPUTE for r in result)
            )
            and result != NULL_COMPUTE
        ):
            result2 = yield self._emit(result)
            raise gen.Return(result2)


@DaskStream.register_api()
class starmap(DaskStream):
    def __init__(self, upstream, func, **kwargs):
        self.func = func
        stream_name = kwargs.pop('stream_name', None)
        self.kwargs = kwargs

        DaskStream.__init__(self, upstream, stream_name=stream_name)

    def update(self, x, who=None):
        client = default_client()
        result = client.submit(apply, self.func, x, self.kwargs)
        return self._emit(result)


@DaskStream.register_api()
class filter(DaskStream):
    def __init__(self, upstream, predicate, *args, **kwargs):
        if predicate is None:
            predicate = _truthy
        self.predicate = return_null(predicate)
        stream_name = kwargs.pop("stream_name", None)
        self.kwargs = kwargs
        self.args = args

        DaskStream.__init__(self, upstream, stream_name=stream_name)

    def update(self, x, who=None):
        client = self.default_client()
        result = client.submit(self.predicate, x, *self.args, **self.kwargs)
        return self._emit(result)


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
