import geopandas as gpd
import shapely
from geoarrow.rust.core import from_geopandas
from pyproj import CRS
from geoarrow.rust.core._crs import get_crs
from arro3.core import RecordBatchReader, ArrayReader


def test_get_crs():
    points = shapely.points([1, 2, 3], [4, 5, 6])
    crs = CRS.from_epsg(4326)
    gdf = gpd.GeoDataFrame({"col1": ["a", "b", "c"]}, geometry=points, crs=crs)
    table = from_geopandas(gdf)

    # Schema
    assert get_crs(table.schema) == crs

    # Field
    assert get_crs(table.schema.field("geometry")) == crs

    # Table
    assert get_crs(table) == crs

    # ChunkedArray
    assert get_crs(table["geometry"]) == crs

    # RecordBatch
    batch = table.to_batches()[0]
    assert get_crs(batch) == crs

    # Array
    array = batch["geometry"]
    assert get_crs(array) == crs

    # RecordBatchReader
    reader = RecordBatchReader.from_batches(table.schema, [batch])
    assert get_crs(reader) == crs
    assert not reader.closed

    # ArrayReader
    reader = ArrayReader.from_arrays(array.field, [array])
    assert get_crs(reader) == crs
    assert not reader.closed
