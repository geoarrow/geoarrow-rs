import json
from pathlib import Path
from tempfile import NamedTemporaryFile

import geopandas as gpd
import numpy as np
import pyarrow.parquet as pq
import pytest
import requests
import shapely
import shapely.testing
from arro3.core import Table, struct_field
from arro3.io import read_parquet
from geoarrow.rust.core import GeoArray
from geoarrow.rust.io import GeoParquetDataset, GeoParquetFile, GeoParquetWriter
from obstore.store import AzureStore, HTTPStore, LocalStore
from pyproj import CRS


@pytest.mark.asyncio
async def test_parquet_file():
    store = HTTPStore.from_url(
        "https://raw.githubusercontent.com/opengeospatial/geoparquet/v1.0.0/examples"
    )
    file = await GeoParquetFile.open_async("example.parquet", store=store)
    assert file.crs() == CRS.from_authority("OGC", "CRS84")
    assert file.num_rows == 5
    assert (
        file.schema_arrow().field("geometry").metadata_str["ARROW:extension:name"]
        == "geoarrow.multipolygon"
    )
    assert (
        file.schema_arrow(parse_to_native=False)
        .field("geometry")
        .metadata_str["ARROW:extension:name"]
        == "geoarrow.wkb"
    )

    table = await file.read_async()
    assert table.num_rows == 5

    # Test writing
    with NamedTemporaryFile() as tmpfile:
        with GeoParquetWriter(tmpfile.name, table.schema) as writer:
            writer.write_table(table)

        file = await GeoParquetFile.open_async(tmpfile.name, store=LocalStore())
        _table2 = file.read()


@pytest.mark.asyncio
async def test_parquet_dataset():
    manifest_url = "https://raw.githubusercontent.com/OvertureMaps/explore-site/refs/heads/main/site/src/manifests/2025-04-23.json"
    manifest = requests.get(manifest_url).json()

    store = AzureStore(
        account_name="overturemapswestus2",
        container_name="release",
        skip_signature=True,
    )
    path = "2025-04-23.0"
    buildings_theme = [x for x in manifest["themes"] if x["name"] == "buildings"][0]
    path += buildings_theme["relative_path"]
    buildings_type = [x for x in buildings_theme["types"] if x["name"] == "building"][0]
    path += buildings_type["relative_path"]

    files = await store.list(path).collect_async()

    dataset = GeoParquetDataset.open(files, store=store)
    # There's no CRS for this dataset?
    assert dataset.crs() is None


def test_write_wkb():
    geoms = shapely.points([1, 2, 3], [4, 5, 6])
    arr = GeoArray.from_arrow(gpd.GeoSeries(geoms).to_arrow("geoarrow"))
    table = Table.from_arrays([arr], names=["geometry"])
    with GeoParquetWriter("points.parquet", table.schema) as writer:
        writer.write_table(table)

    gpq_meta = json.loads(pq.read_metadata("points.parquet").metadata[b"geo"])
    assert gpq_meta["version"] == "1.1.0"
    assert gpq_meta["primary_column"] == "geometry"
    assert gpq_meta["columns"]["geometry"] == {
        "encoding": "WKB",
        "geometry_types": ["Point"],
        "bbox": [1.0, 4.0, 3.0, 6.0],
    }

    shapely.testing.assert_geometries_equal(
        geoms, shapely.from_wkb(read_parquet("points.parquet").read_all()["geometry"])
    )


def test_write_geoarrow():
    geoms = shapely.points([1, 2, 3], [4, 5, 6])
    arr = GeoArray.from_arrow(gpd.GeoSeries(geoms).to_arrow("geoarrow"))
    table = Table.from_arrays([arr], names=["geometry"])
    with GeoParquetWriter(
        "points.parquet", table.schema, encoding="geoarrow"
    ) as writer:
        writer.write_table(table)

    gpq_meta = json.loads(pq.read_metadata("points.parquet").metadata[b"geo"])
    assert gpq_meta["version"] == "1.1.0"
    assert gpq_meta["primary_column"] == "geometry"
    assert gpq_meta["columns"]["geometry"] == {
        "encoding": "point",
        "geometry_types": ["Point"],
        "bbox": [1.0, 4.0, 3.0, 6.0],
    }

    geoarrow_back = read_parquet("points.parquet").read_all()["geometry"].chunks[0]
    np_coords = np.column_stack(
        [struct_field(geoarrow_back, 0), struct_field(geoarrow_back, 1)]
    )
    shapely.testing.assert_geometries_equal(
        geoms, shapely.from_ragged_array(shapely.GeometryType.POINT, np_coords)
    )


def test_write_geoarrow_xyz():
    geoms = shapely.points([1, 2, 3], [4, 5, 6], [7, 8, 9])
    arr = GeoArray.from_arrow(gpd.GeoSeries(geoms).to_arrow("geoarrow"))
    table = Table.from_arrays([arr], names=["geometry"])
    with GeoParquetWriter(
        "points.parquet", table.schema, encoding="geoarrow"
    ) as writer:
        writer.write_table(table)

    gpq_meta = json.loads(pq.read_metadata("points.parquet").metadata[b"geo"])
    assert gpq_meta["version"] == "1.1.0"
    assert gpq_meta["primary_column"] == "geometry"
    assert gpq_meta["columns"]["geometry"] == {
        "encoding": "point",
        "geometry_types": ["Point Z"],
        "bbox": [1.0, 4.0, 7.0, 3.0, 6.0, 9.0],
    }

    geoarrow_back = read_parquet("points.parquet").read_all()["geometry"].chunks[0]
    np_coords = np.column_stack(
        [
            struct_field(geoarrow_back, 0),
            struct_field(geoarrow_back, 1),
            struct_field(geoarrow_back, 2),
        ]
    )
    shapely.testing.assert_geometries_equal(
        geoms, shapely.from_ragged_array(shapely.GeometryType.POINT, np_coords)
    )


def test_write_crs():
    geoms = shapely.points([1, 2, 3], [4, 5, 6])
    crs = CRS.from_user_input("EPSG:4326")
    series = gpd.GeoSeries(geoms, crs=crs)
    arr = GeoArray.from_arrow(series.to_arrow("geoarrow"))
    table = Table.from_arrays([arr], names=["geometry"])
    with GeoParquetWriter("points.parquet", table.schema) as writer:
        writer.write_table(table)

    gpq_meta = json.loads(pq.read_metadata("points.parquet").metadata[b"geo"])
    assert gpq_meta["version"] == "1.1.0"
    assert gpq_meta["primary_column"] == "geometry"
    assert gpq_meta["columns"]["geometry"]["encoding"] == "WKB"
    assert gpq_meta["columns"]["geometry"]["geometry_types"] == ["Point"]
    assert gpq_meta["columns"]["geometry"]["bbox"] == [1.0, 4.0, 3.0, 6.0]
    assert CRS.from_json_dict(gpq_meta["columns"]["geometry"]["crs"]) == crs

    store = LocalStore(Path())
    file = GeoParquetFile.open("points.parquet", store=store)
    assert file.crs() == crs
