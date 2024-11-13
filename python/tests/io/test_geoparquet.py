import json

import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq
import shapely
from geoarrow.rust.core import from_geopandas
from geoarrow.rust.io import ParquetFile, read_parquet, write_parquet
from geoarrow.rust.io.store import LocalStore
from pyproj import CRS


def test_write_native_points():
    points = shapely.points([1, 2, 3], [4, 5, 6])
    gdf = gpd.GeoDataFrame({"col1": ["a", "b", "c"]}, geometry=points, crs="EPSG:4326")
    table = from_geopandas(gdf)
    write_parquet(table, "test.parquet", encoding="native")

    schema = pq.read_schema("test.parquet")
    assert (
        schema.field("geometry").metadata[b"ARROW:extension:name"] == b"geoarrow.point"
    )

    retour = read_parquet("test.parquet")
    assert pa.table(retour)["geometry"][0]["x"].as_py() == 1
    assert pa.table(retour)["geometry"][0]["y"].as_py() == 4
    # Native coords get returned as separated coord type
    # assert pa.table(table) == pa.table(retour)
    assert (
        retour.schema.field("geometry").metadata_str["ARROW:extension:name"]
        == "geoarrow.point"
    )

    # TODO: assert same CRS
    # meta = pq.read_metadata("test.parquet").metadata[b"geo"]
    # meta = json.loads(meta)
    # TODO: shouldn't this be a dict, not a string?
    # CRS.from_json(meta["columns"]["geometry"] ["crs"])


def test_write_native_points_3d():
    fname = "test_z.parquet"
    points = shapely.points([1, 2, 3], [4, 5, 6], [7, 8, 9])
    gdf = gpd.GeoDataFrame({"col1": ["a", "b", "c"]}, geometry=points, crs="EPSG:4326")
    table = from_geopandas(gdf)
    write_parquet(table, fname, encoding="native")

    pq_meta = pq.read_metadata(fname)
    json.loads(pq_meta.metadata[b"geo"])
    schema = pq.read_schema(fname)
    assert (
        schema.field("geometry").metadata[b"ARROW:extension:name"] == b"geoarrow.point"
    )

    retour = read_parquet(fname)
    # Native coords get returned as separated coord type
    # assert pa.table(table) == pa.table(retour)
    assert pa.table(retour)["geometry"][0]["x"].as_py() == 1
    assert pa.table(retour)["geometry"][0]["y"].as_py() == 4
    assert pa.table(retour)["geometry"][0]["z"].as_py() == 7


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


def test_read_write_crs():
    points = shapely.points([1, 2, 3], [4, 5, 6])
    crs = CRS.from_user_input("EPSG:4326")
    gdf = gpd.GeoDataFrame({"col1": ["a", "b", "c"]}, geometry=points, crs=crs)
    gdf.to_parquet("test.parquet")

    store = LocalStore(".")
    file = ParquetFile("test.parquet", store)
    assert file.crs() == crs

    table = read_parquet("test.parquet")
    ext_meta = table.schema.field("geometry").metadata_str["ARROW:extension:metadata"]
    ext_meta = json.loads(ext_meta)
    assert crs == CRS.from_json_dict(ext_meta["crs"])

    # Test writing and reading back
    write_parquet(table, "test2.parquet", encoding="native")
    table = read_parquet("test2.parquet")
    ext_meta = table.schema.field("geometry").metadata_str["ARROW:extension:metadata"]
    ext_meta = json.loads(ext_meta)
    assert crs == CRS.from_json_dict(ext_meta["crs"])
