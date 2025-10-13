import contextlib
import pytest
import shapely
import pyarrow as pa
import numpy as np

from geoarrow.rust.core import from_shapely, points, polygons, from_wkb


@pytest.mark.parametrize("method", ["wkb", "ragged"])
def test_from_points(method):
    coords = np.array([[1, 4], [2, 5]], dtype="float64")
    geoms = shapely.points(coords)
    assert isinstance(geoms, np.ndarray)

    expected = points(coords)

    actual = from_shapely(geoms, method=method, coord_type="interleaved")

    assert actual == expected


@pytest.mark.parametrize("method", ["wkb", "ragged"])
def test_from_polygons(method):
    coords_ = np.array(
        [
            [[0, 3], [2, 3], [2, 5], [0, 5], [0, 3]],
            [[2, 1], [4, 1], [4, 3], [2, 3], [2, 1]],
        ],
        dtype="float64",
    )
    coords = coords_.reshape(-1, 2)
    ring_offsets = np.array([0, 5, 10])
    geom_offsets = np.array([0, 1, 2])

    geoms = shapely.polygons(coords_)
    assert isinstance(geoms, np.ndarray)

    expected = polygons(coords, geom_offsets=geom_offsets, ring_offsets=ring_offsets)

    actual = from_shapely(geoms, method=method, coord_type="interleaved")
    assert actual == expected


@pytest.mark.parametrize("method", ["wkb", "ragged"])
def test_from_geometry(method):
    geoms = np.array([shapely.box(0, 0, 2, 3), shapely.Point([2, 3])])

    responses = {
        "ragged": pytest.raises(ValueError, match="type combination is not supported"),
        "wkb": contextlib.nullcontext(),
    }

    expected = from_wkb(pa.array(shapely.to_wkb(geoms)))

    with responses[method]:
        actual = from_shapely(geoms, method=method)
        assert actual == expected
