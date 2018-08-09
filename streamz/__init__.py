from __future__ import absolute_import, division, print_function

from .core import *
from .graph import *
from .sources import *
from .thread import *
try:
    from .dask import DaskStream, scatter
except ImportError:
    pass

__version__ = '0.3.0'
