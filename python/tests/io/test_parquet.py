import pytest
from geoarrow.rust.io import GeoParquetDataset, GeoParquetFile, GeoParquetWriter
from obstore.store import HTTPStore


@pytest.mark.asyncio
async def test_parquet_file():
    store = HTTPStore.from_url(
        "https://raw.githubusercontent.com/opengeospatial/geoparquet/v1.0.0/examples"
    )
    file = await GeoParquetFile.open_async("example.parquet", store=store)
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
