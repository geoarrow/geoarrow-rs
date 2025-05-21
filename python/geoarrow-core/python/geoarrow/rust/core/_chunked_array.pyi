from arro3.core.types import ArrowStreamExportable

from ._array import GeoArrowArray
from ._data_type import GeoArrowType
from .types import CoordTypeInput

class ChunkedGeoArrowArray:
    """
    A class representing a chunked GeoArrow array.

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
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    @classmethod
    def from_arrow(cls, data: ArrowStreamExportable) -> ChunkedGeoArrowArray: ...
    @classmethod
    def from_arrow_pycapsule(cls, capsule: object) -> ChunkedGeoArrowArray: ...
    @property
    def null_count(self) -> int: ...
    @property
    def num_chunks(self) -> int:
        """Return the number of chunks in the array."""
    def chunk(self, i: int) -> GeoArrowArray:
        """Return the i-th chunk of the array."""
    def chunks(self) -> list[GeoArrowArray]:
        """Return all chunks of the array."""
    def cast(self, to_type: GeoArrowType, /) -> ChunkedGeoArrowArray: ...
    def downcast(self, coord_type: CoordTypeInput) -> ChunkedGeoArrowArray: ...
    @property
    def type(self) -> GeoArrowArray:
        """Return the type of the array."""
