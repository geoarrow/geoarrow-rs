from arro3.core.types import ArrowSchemaExportable
from geoarrow.rust.core.enums import CoordType, Dimension, Edges
from geoarrow.rust.core.types import (
    CRSInput,
    CoordTypeInput,
    DimensionInput,
    EdgesInput,
)
from pyproj.crs.crs import CRS

__all__ = [
    "GeoType",
    "point",
    "linestring",
    "polygon",
    "multipoint",
    "multilinestring",
    "multipolygon",
    "geometrycollection",
    "geometry",
    "box",
    "wkb",
    "wkt",
]

class GeoType:
    """A GeoArrow data type.

    This implements the [Arrow PyCapsule
    interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html),
    allowing it to be seamlessly converted to other Arrow implementations.

    ### Converting to pyarrow

    `geoarrow-types` is recommended to register GeoArrow extension types into the pyarrow type registry.

    ```py
    from geoarrow.types.type_pyarrow import register_extension_types
    from geoarrow.rust.core import point

    pa.field(point("xy"))
    # pyarrow.Field<: extension<geoarrow.point<PointType>>>

    # You may want to set the name on the field upon importing:
    pa.field(point("xy")).with_name("geometry")
    # pyarrow.Field<geometry: extension<geoarrow.point<PointType>>>

    # Because the extension types were registered from geoarrow-types,
    # the resulting type is geoarrow-aware
    pa.field(point("xy", crs="epsg:4326")).type.crs
    # ProjJsonCrs(EPSG:4326)
    ```
    """

    def __arrow_c_schema__(self) -> object: ...
    def __eq__(self, other: object) -> bool: ...
    def __repr__(self) -> str: ...
    @classmethod
    def from_arrow(cls, data: ArrowSchemaExportable) -> GeoType: ...
    @classmethod
    def from_arrow_pycapsule(cls, capsule: object) -> GeoType: ...
    @property
    def coord_type(self) -> CoordType | None:
        """The coordinate type of the type.

        This will only be set if the type is a "native" GeoArrow type with a known
        coordinate type. Serialized arrays such as WKB/WKT arrays do not have a
        coordinate type.
        """
    @property
    def dimension(self) -> Dimension | None:
        """The dimension of the type.

        This will only be set if the type is a "native" GeoArrow type with a known
        dimension type. Geometry arrays and serialized arrays such as WKB/WKT arrays do
        not have a statically-known dimension.
        """
    @property
    def crs(self) -> CRS | None:
        """The CRS of the type."""
    @property
    def edges(self) -> Edges | None:
        """The edge interpolation of this type."""
    def with_crs(
        self, crs: CRSInput | None = None, *, edges: EdgesInput | None = None
    ) -> GeoType:
        """Return a new type with the given CRS and edge interpolation.

        Args:
            crs: the CRS of the type. Defaults to None.

        Keyword Args:
            edges: the edge interpolation of the type. Defaults to None.
        """

def point(
    dimension: DimensionInput,
    *,
    coord_type: CoordTypeInput = CoordType.SEPARATED,
    crs: CRSInput | None = None,
    edges: EdgesInput | None = None,
) -> GeoType:
    """Create a new Arrow type for a GeoArrow Point array.

    Args:
        dimension: The dimension of the array.

    Keyword Args:
        coord_type: The coordinate type of the array. Defaults to [`CoordType.SEPARATED`][geoarrow.rust.core.types.CoordType.SEPARATED].
        crs: the CRS of the type. Defaults to None.
        edges: the edge interpolation of the type. Defaults to None.

    Examples:
        ```py
        from geoarrow.rust.core import point

        point("xy")
        # GeoType(Point(dimension="XY", coord_type="separated"))

        point("xy", coord_type="interleaved")
        # GeoType(Point(dimension="XY", coord_type="interleaved"))
        ```

    """

def linestring(
    dimension: DimensionInput,
    *,
    coord_type: CoordTypeInput = CoordType.SEPARATED,
    crs: CRSInput | None = None,
    edges: EdgesInput | None = None,
) -> GeoType:
    """Create a new Arrow type for a GeoArrow LineString array.

    Args:
        dimension: The dimension of the array.

    Keyword Args:
        coord_type: The coordinate type of the array. Defaults to [`CoordType.SEPARATED`][geoarrow.rust.core.types.CoordType.SEPARATED].
        crs: the CRS of the type. Defaults to None.
        edges: the edge interpolation of the type. Defaults to None.
    """

def polygon(
    dimension: DimensionInput,
    *,
    coord_type: CoordTypeInput = CoordType.SEPARATED,
    crs: CRSInput | None = None,
    edges: EdgesInput | None = None,
) -> GeoType:
    """Create a new Arrow type for a GeoArrow Polygon array.

    Args:
        dimension: The dimension of the array.

    Keyword Args:
        coord_type: The coordinate type of the array. Defaults to [`CoordType.SEPARATED`][geoarrow.rust.core.types.CoordType.SEPARATED].
        crs: the CRS of the type. Defaults to None.
        edges: the edge interpolation of the type. Defaults to None.
    """

def multipoint(
    dimension: DimensionInput,
    *,
    coord_type: CoordTypeInput = CoordType.SEPARATED,
    crs: CRSInput | None = None,
    edges: EdgesInput | None = None,
) -> GeoType:
    """Create a new Arrow type for a GeoArrow MultiPoint array.

    Args:
        dimension: The dimension of the array.

    Keyword Args:
        coord_type: The coordinate type of the array. Defaults to [`CoordType.SEPARATED`][geoarrow.rust.core.types.CoordType.SEPARATED].
        crs: the CRS of the type. Defaults to None.
        edges: the edge interpolation of the type. Defaults to None.
    """

def multilinestring(
    dimension: DimensionInput,
    *,
    coord_type: CoordTypeInput = CoordType.SEPARATED,
    crs: CRSInput | None = None,
    edges: EdgesInput | None = None,
) -> GeoType:
    """Create a new Arrow type for a GeoArrow MultiLineString array.

    Args:
        dimension: The dimension of the array.

    Keyword Args:
        coord_type: The coordinate type of the array. Defaults to [`CoordType.SEPARATED`][geoarrow.rust.core.types.CoordType.SEPARATED].
        crs: the CRS of the type. Defaults to None.
        edges: the edge interpolation of the type. Defaults to None.
    """

def multipolygon(
    dimension: DimensionInput,
    *,
    coord_type: CoordTypeInput = CoordType.SEPARATED,
    crs: CRSInput | None = None,
    edges: EdgesInput | None = None,
) -> GeoType:
    """Create a new Arrow type for a GeoArrow MultiPolygon array.

    Args:
        dimension: The dimension of the array.

    Keyword Args:
        coord_type: The coordinate type of the array. Defaults to [`CoordType.SEPARATED`][geoarrow.rust.core.types.CoordType.SEPARATED].
        crs: the CRS of the type. Defaults to None.
        edges: the edge interpolation of the type. Defaults to None.
    """

def geometrycollection(
    dimension: DimensionInput,
    *,
    coord_type: CoordTypeInput = CoordType.SEPARATED,
    crs: CRSInput | None = None,
    edges: EdgesInput | None = None,
) -> GeoType:
    """Create a new Arrow type for a GeoArrow GeometryCollection array.

    Args:
        dimension: The dimension of the array.

    Keyword Args:
        coord_type: The coordinate type of the array. Defaults to [`CoordType.SEPARATED`][geoarrow.rust.core.types.CoordType.SEPARATED].
        crs: the CRS of the type. Defaults to None.
        edges: the edge interpolation of the type. Defaults to None.
    """

def geometry(
    *,
    coord_type: CoordType = CoordType.SEPARATED,
    crs: CRSInput | None = None,
    edges: EdgesInput | None = None,
) -> GeoType:
    """Create a new Arrow type for a GeoArrow Geometry array.

    Keyword Args:
        coord_type: The coordinate type of the array. Defaults to [`CoordType.SEPARATED`][geoarrow.rust.core.types.CoordType.SEPARATED].
        crs: the CRS of the type. Defaults to None.
        edges: the edge interpolation of the type. Defaults to None.
    """

def box(
    dimension: DimensionInput,
    *,
    crs: CRSInput | None = None,
    edges: EdgesInput | None = None,
) -> GeoType:
    """Create a new Arrow type for a GeoArrow Box array.

    Args:
        dimension: The dimension of the array.

    Keyword Args:
        crs: the CRS of the type. Defaults to None.
        edges: the edge interpolation of the type. Defaults to None.
    """

def wkb(
    *,
    crs: CRSInput | None = None,
    edges: EdgesInput | None = None,
) -> GeoType:
    """Create a new Arrow type for a GeoArrow WKB array.

    This type is backed by an Arrow BinaryArray with `i32` offsets, allowing a maximum
    array size of 2 GB per array chunk.

    Args:
        crs: the CRS of the type. Defaults to None.
        edges: the edge interpolation of the type. Defaults to None.
    """

def large_wkb(
    *,
    crs: CRSInput | None = None,
    edges: EdgesInput | None = None,
) -> GeoType:
    """Create a new Arrow type for a GeoArrow WKB array.

    This type is backed by an Arrow LargeBinaryArray with `i64` offsets, allowing more
    than 2GB of data per array chunk.

    Args:
        crs: the CRS of the type. Defaults to None.
        edges: the edge interpolation of the type. Defaults to None.
    """

def wkb_view(
    *,
    crs: CRSInput | None = None,
    edges: EdgesInput | None = None,
) -> GeoType:
    """Create a new Arrow type for a GeoArrow WKB array.

    This type is backed by an Arrow [BinaryViewArray](https://arrow.apache.org/docs/format/Columnar.html#variable-size-binary-view-layout).

    Args:
        crs: the CRS of the type. Defaults to None.
        edges: the edge interpolation of the type. Defaults to None.
    """

def wkt(
    *,
    crs: CRSInput | None = None,
    edges: EdgesInput | None = None,
) -> GeoType:
    """Create a new Arrow type for a GeoArrow WKT array.

    This type is backed by an Arrow StringArray with `i32` offsets, allowing a maximum
    array size of 2 GB per array chunk.

    Args:
        crs: the CRS of the type. Defaults to None.
        edges: the edge interpolation of the type. Defaults to None.
    """

def large_wkt(
    *,
    crs: CRSInput | None = None,
    edges: EdgesInput | None = None,
) -> GeoType:
    """Create a new Arrow type for a GeoArrow WKT array.

    This type is backed by an Arrow LargeString array with `i64` offsets, allowing more
    than 2GB of data per array chunk.

    Args:
        crs: the CRS of the type. Defaults to None.
        edges: the edge interpolation of the type. Defaults to None.
    """

def wkt_view(
    *,
    crs: CRSInput | None = None,
    edges: EdgesInput | None = None,
) -> GeoType:
    """Create a new Arrow type for a GeoArrow WKT array.

    This type is backed by an Arrow
    [StringView](https://arrow.apache.org/docs/format/Columnar.html#variable-size-binary-layout)
    array.

    Args:
        crs: the CRS of the type. Defaults to None.
        edges: the edge interpolation of the type. Defaults to None.
    """
