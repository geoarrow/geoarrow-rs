from arro3.core.types import ArrowStreamExportable
from geoarrow.rust.core._scalar import GeoScalar

from ._array import GeoArray
from ._data_type import GeoType
from .enums import CoordType
from .types import CoordTypeInput

class GeoChunkedArray:
    """A chunked GeoArrow array.

    This class is used to handle chunked arrays in GeoArrow, which can be
    composed of multiple chunks of data.
    """

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        For example, you can call [`pyarrow.chunked_array()`][pyarrow.chunked_array] to
        convert this array into a pyarrow array, without copying memory.
        """
    def __eq__(self, value: object) -> bool: ...
    def __getitem__(self, item: int) -> GeoScalar: ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    @classmethod
    def from_arrow(cls, data: ArrowStreamExportable) -> GeoChunkedArray:
        """Import from an Arrow chunked array/stream object.

        This uses the Arrow PyCapsule interface to import the array, so any producer
        that implements the protocol is supported.

        The existing array must have associated GeoArrow metadata.
        """
    @classmethod
    def from_arrow_pycapsule(cls, capsule: object) -> GeoChunkedArray: ...
    @property
    def null_count(self) -> int:
        """The number of null values in the chunked array."""
    @property
    def num_chunks(self) -> int:
        """Return the number of chunks in the array."""
    def chunk(self, i: int) -> GeoArray:
        """Return the i-th chunk of the array."""
    def chunks(self) -> list[GeoArray]:
        """Return all chunks of the array."""
    def cast(self, to_type: GeoType, /) -> GeoChunkedArray:
        """Cast to another `GeoType`.

        ### Criteria:

        - Dimension must be compatible:
            - If the source array and destination type are both dimension-aware, then
              their dimensions must match.
            - Casts from dimension-aware to dimensionless arrays (`GeometryArray`,
              `WkbArray`, `WkbViewArray`, `WktArray`, `WktViewArray`) are always
              allowed.
        - GeoArrow metadata (i.e. CRS and edges) on the source and destination types must match.

        ### Infallible casts:

        As long as the above criteria are met, these casts will always succeed without erroring.

        - The same geometry type with different coord types.
        - Any source array type to `Geometry`, `Wkb`, `LargeWkb`, `WkbView`, `Wkt`,
          `LargeWkt`, or `WktView`.
        - `Point` to `MultiPoint`
        - `LineString` to `MultiLineString`
        - `Polygon` to `MultiPolygon`

        ### Fallible casts:

        - `Geometry` to any other native type.
        - Parsing `WKB` or `WKT` to any native type other than `Geometry`.
        - `MultiPoint` to `Point`
        - `MultiLineString` to `LineString`
        - `MultiPolygon` to `Polygon`
        """
    def downcast(
        self, *, coord_type: CoordTypeInput = CoordType.SEPARATED
    ) -> GeoChunkedArray:
        """Downcast to its simplest, most-compact native geometry representation.

        If there is no simpler representation, the array is returned unchanged.
        """
    @property
    def type(self) -> GeoType:
        """Return the type of the chunked array."""
