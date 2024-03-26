import geopandas as gpd
import pyarrow.parquet as pq
import shapely
from geoarrow.rust.core import write_parquet, from_geopandas


def test_write_native_points():
    points = shapely.points([1, 2, 3], [4, 5, 6])
    gdf = gpd.GeoDataFrame({"col1": ["a", "b", "c"]}, geometry=points, crs="EPSG:4326")
    table = from_geopandas(gdf)
    write_parquet(table, "test.parquet", encoding="native")

    schema = pq.read_schema("test.parquet")
    assert (
        schema.field("geometry").metadata[b"ARROW:extension:name"] == b"geoarrow.point"
    )

    # TODO: assert same CRS
    # meta = pq.read_metadata("test.parquet").metadata[b'geo']
    # meta = json.loads(meta)


def test_write_native_multi_points():
    points = shapely.points([1, 2, 3], [4, 5, 6])
    multi_points = shapely.multipoints(
        [
            points[0],
            points[1],
            points[1],
            points[2],
            points[2],
            points[0],
        ],
        indices=[0, 0, 1, 1, 2, 2],
    )

    gdf = gpd.GeoDataFrame(
        {"col1": ["a", "b", "c"]}, geometry=multi_points, crs="EPSG:4326"
    )
    table = from_geopandas(gdf)
    write_parquet(table, "test.parquet", encoding="native")

    schema = pq.read_schema("test.parquet")
    assert (
        schema.field("geometry").metadata[b"ARROW:extension:name"]
        == b"geoarrow.multipoint"
    )
