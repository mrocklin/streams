try:
    from cytoolz import accumulate
except ImportError:
    from toolz import accumulate

import functools
from .core import no_default

class Batch(tuple):
    def __stream_map__(self, func, **kwargs):
        if kwargs:
            func = functools.partial(func, **kwargs)
        return Batch(map(func, self))

    def __stream_accumulate__(self, func, accumulator):
        if accumulator is not no_default:
            seq = accumulate(func, self, accumulator)
        else:
            seq = accumulate(func, self)
        next(seq)  # burn first element, this is the old accumulator
        seq = Batch(seq)
        acc = seq[-1] if seq else accumulator
        return acc, seq

    def __stream_merge__(self, *others):
        return Batch(zip(self, *others))
