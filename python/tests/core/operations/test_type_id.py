import geopandas as gpd
import numpy as np
import shapely
from arro3.core import ChunkedArray
from geoarrow.rust.core import GeoArray, GeoChunkedArray, get_type_id


def test_points():
    geoms = shapely.points([1, 2, 3], [4, 5, 6])
    arr = GeoArray.from_arrow(gpd.GeoSeries(geoms).to_arrow("geoarrow"))
    out = get_type_id(arr)
    assert (np.asarray(out) == 1).all()


def test_points_wkb():
    geoms = shapely.points([1, 2, 3], [4, 5, 6])
    arr = GeoArray.from_arrow(gpd.GeoSeries(geoms).to_arrow("wkb"))
    out = get_type_id(arr)
    assert (np.asarray(out) == 1).all()


def test_mixed_wkb():
    geoms = [
        shapely.geometry.Point(1, 4),
        shapely.geometry.LineString([(2, 5), (3, 6)]),
        shapely.geometry.Polygon([(0, 0), (1, 1), (1, 0), (0, 0)]),
        shapely.geometry.MultiPoint([shapely.geometry.Point(1, 4)]),
        shapely.geometry.MultiLineString(
            [shapely.geometry.LineString([(2, 5), (3, 6)])]
        ),
        shapely.geometry.MultiPolygon(
            [
                shapely.geometry.Polygon([(0, 0), (1, 1), (1, 0), (0, 0)]),
            ]
        ),
        shapely.geometry.GeometryCollection(
            [
                shapely.geometry.Point(1, 4),
                shapely.geometry.LineString([(2, 5), (3, 6)]),
                shapely.geometry.Polygon([(0, 0), (1, 1), (1, 0), (0, 0)]),
            ]
        ),
    ]
    arr = GeoArray.from_arrow(gpd.GeoSeries(geoms).to_arrow("wkb"))
    out = get_type_id(arr)
    assert (np.asarray(out) == np.array([1, 2, 3, 4, 5, 6, 7])).all()


def test_mixed_wkb_3d():
    geoms = [
        shapely.geometry.Point(1, 4, 7),
        shapely.geometry.LineString([(2, 5, 8), (3, 6, 9)]),
        shapely.geometry.Polygon([(0, 0, 0), (1, 1, 1), (1, 0, 0), (0, 0, 0)]),
        shapely.geometry.MultiPoint([shapely.geometry.Point(1, 4, 7)]),
        shapely.geometry.MultiLineString(
            [shapely.geometry.LineString([(2, 5, 8), (3, 6, 9)])]
        ),
        shapely.geometry.MultiPolygon(
            [
                shapely.geometry.Polygon([(0, 0, 0), (1, 1, 1), (1, 0, 0), (0, 0, 0)]),
            ]
        ),
        shapely.geometry.GeometryCollection(
            [
                shapely.geometry.Point(1, 4, 7),
                shapely.geometry.LineString([(2, 5, 8), (3, 6, 9)]),
                shapely.geometry.Polygon([(0, 0, 0), (1, 1, 1), (1, 0, 0), (0, 0, 0)]),
            ]
        ),
    ]
    arr = GeoArray.from_arrow(gpd.GeoSeries(geoms).to_arrow("wkb"))
    out = get_type_id(arr)
    assert (np.asarray(out) == np.array([11, 12, 13, 14, 15, 16, 17])).all()


def test_multipoints():
    geoms = shapely.multipoints(
        [
            shapely.geometry.Point(1, 4),
            shapely.geometry.Point(2, 5),
            shapely.geometry.Point(3, 6),
        ]
    )
    arr = GeoArray.from_arrow(gpd.GeoSeries(geoms).to_arrow("geoarrow"))
    out = get_type_id(arr)
    assert (np.asarray(out) == 4).all()


def test_points_chunked():
    geoms1 = shapely.points([1, 2, 3], [4, 5, 6])
    geoms2 = shapely.points([10, 20, 30], [40, 50, 60])
    arr1 = GeoArray.from_arrow(gpd.GeoSeries(geoms1).to_arrow("geoarrow"))
    arr2 = GeoArray.from_arrow(gpd.GeoSeries(geoms2).to_arrow("geoarrow"))
    ca = GeoChunkedArray.from_arrow(ChunkedArray([arr1, arr2]))
    out = get_type_id(ca).read_all()
    assert (np.asarray(out) == 1).all()
