import pytest
from streamz.dataframe.utils import is_dataframe_like, is_series_like, \
                is_index_like, get_base_frame_type, get_dataframe_package

import pandas as pd
import numpy as np


def test_utils_get_base_frame_type_pandas():
    with pytest.raises(TypeError):
        get_base_frame_type("DataFrame", is_dataframe_like, None)

    df = pd.DataFrame({'x': np.arange(10, dtype=float), 'y': [1.0, 2.0] * 5})

    assert pd.DataFrame == get_base_frame_type("DataFrame", is_dataframe_like, df)
    with pytest.raises(TypeError):
        get_base_frame_type("Series", is_series_like, df)
    with pytest.raises(TypeError):
        get_base_frame_type("Index", is_index_like, df)

    with pytest.raises(TypeError):
        get_base_frame_type("DataFrame", is_dataframe_like, df.x)
    assert pd.Series == get_base_frame_type("Series", is_series_like, df.x)
    with pytest.raises(TypeError):
        get_base_frame_type("Index", is_index_like, df.x)

    with pytest.raises(TypeError):
        get_base_frame_type("DataFrame", is_dataframe_like, df.index)
    with pytest.raises(TypeError):
        get_base_frame_type("Series", is_series_like, df.index)
    assert issubclass(get_base_frame_type("Index", is_index_like, df.index), pd.Index)


def test_utils_get_base_frame_type_cudf():  # pragma: no cover
    cudf = pytest.importorskip("cudf")

    df = cudf.DataFrame({'x': np.arange(10, dtype=float), 'y': [1.0, 2.0] * 5})

    assert cudf.DataFrame == get_base_frame_type("DataFrame", is_dataframe_like, df)
    with pytest.raises(TypeError):
        get_base_frame_type("Series", is_series_like, df)
    with pytest.raises(TypeError):
        get_base_frame_type("Index", is_index_like, df)

    with pytest.raises(TypeError):
        get_base_frame_type("DataFrame", is_dataframe_like, df.x)
    assert cudf.Series == get_base_frame_type("Series", is_series_like, df.x)
    with pytest.raises(TypeError):
        get_base_frame_type("Index", is_index_like, df.x)

    with pytest.raises(TypeError):
        get_base_frame_type("DataFrame", is_dataframe_like, df.index)
    with pytest.raises(TypeError):
        get_base_frame_type("Series", is_series_like, df.index)
    assert issubclass(get_base_frame_type("Index", is_index_like, df.index), cudf.Index)


def test_get_dataframe_package_pandas():
    df = pd.DataFrame({'x': np.arange(10, dtype=float), 'y': [1.0, 2.0] * 5})
    assert pd == get_dataframe_package(df)
    assert pd == get_dataframe_package(df.x)
    assert pd == get_dataframe_package(df.index)


def test_get_dataframe_package_cudf():  # pragma: no cover
    cudf = pytest.importorskip("cudf")
    df = cudf.DataFrame({'x': np.arange(10, dtype=float), 'y': [1.0, 2.0] * 5})
    assert cudf == get_dataframe_package(df)
    assert cudf == get_dataframe_package(df.x)
    assert cudf == get_dataframe_package(df.index)
