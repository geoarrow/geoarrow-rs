import numpy as np
import pyarrow as pa
import pytest
from geoarrow.rust.core import linestrings, points


def test_points_2d():
    coords = np.random.rand(10, 2)
    point_arr = points(coords)
    point_arr = pa.array(point_arr)
    assert point_arr[0][0].as_py() == coords[0, 0]
    assert point_arr[0][1].as_py() == coords[0, 1]

    coords_retour = point_arr.values.to_numpy().reshape(-1, 2)
    assert np.allclose(coords, coords_retour)

    with pytest.raises(ValueError, match="Buffer is not C contiguous"):
        points((coords[:, 0], coords[:, 1]))

    x = np.ascontiguousarray(coords[:, 0])
    y = np.ascontiguousarray(coords[:, 1])
    point_arr2 = points((x, y))
    point_arr2 = pa.array(point_arr2)
    assert point_arr2[0][0].as_py() == coords[0, 0]
    assert point_arr2[0][1].as_py() == coords[0, 1]

    coords_retour2 = np.column_stack(
        [
            point_arr2.field("x"),
            point_arr2.field("y"),
        ]
    )
    assert np.allclose(coords, coords_retour2)


@pytest.mark.skip("Come back to this once we remove the const generic from array types")
def test_points_3d():
    coords = np.random.rand(10, 3)
    point_arr = points(coords)
    point_arr = pa.array(point_arr)
    assert point_arr[0][0].as_py() == coords[0, 0]
    assert point_arr[0][1].as_py() == coords[0, 1]
    assert point_arr[0][2].as_py() == coords[0, 2]


def test_linestrings():
    coords = np.random.rand(10, 2)
    geom_offsets = np.array([0, 2, 6, 10], dtype=np.int32)
    geom_arr = linestrings(coords, geom_offsets)
    geom_arr = pa.array(geom_arr)
    assert len(geom_arr) == 3
    assert len(geom_arr[0]) == 2
    assert len(geom_arr[1]) == 4
    assert len(geom_arr[2]) == 4

    assert np.allclose(coords, geom_arr.values.values.to_numpy().reshape(-1, 2))
