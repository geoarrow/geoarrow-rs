from tempfile import NamedTemporaryFile

import pytest
import requests
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
