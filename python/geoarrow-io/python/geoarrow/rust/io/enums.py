from enum import Enum


class StrEnum(str, Enum):
    def __str__(self):
        return str(self.value)


class GeoParquetEncoding(StrEnum):
    """Options for geometry encoding in GeoParquet."""

    WKB = "wkb"
    """Use [Well-Known Binary (WKB)][wkb_wiki] encoding when writing GeoParquet files.

    [wkb_wiki]: https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry#Well-known_binary

    This is the preferred option for maximum portability. See upstream [specification reference].

    [specification reference]: https://github.com/opengeospatial/geoparquet/blob/v1.1.0%2Bp1/format-specs/geoparquet.md#wkb
    """

    GEOARROW = "geoarrow"
    """Use native GeoArrow-based geometry types when writing GeoParquet files.

    !!! note
        GeoParquet ecosystem support is not as widespread for the GeoArrow encoding as for the WKB encoding.

    This is only valid when all geometries are one of the [supported][geoarrow_spec_reference] single-geometry type encodings (i.e., `"point"`, `"linestring"`, `"polygon"`, `"multipoint"`, `"multilinestring"`, `"multipolygon"`).

    [geoarrow_spec_reference]: https://github.com/opengeospatial/geoparquet/blob/v1.1.0%2Bp1/format-specs/geoparquet.md#native-encodings-based-on-geoarrow

    Using this encoding may provide better performance. Performance is most likely to be improved when writing points. Writing points _plus_ an external bounding-box column requires storing each x-y coordinate pair 3 times instead of one, so this could provide significant file size savings. There has not yet been widespread testing for other geometry types.

    These encodings correspond to the [separated (struct) representation of coordinates](https://geoarrow.org/format.html#coordinate-separated) for single-geometry type encodings. This encoding results in useful column statistics when row groups and/or files contain related features.
    """
