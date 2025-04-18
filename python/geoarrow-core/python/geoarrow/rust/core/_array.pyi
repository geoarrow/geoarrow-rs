from arro3.core.types import ArrowArrayExportable

from ._data_type import GeoArrowType

class GeoArrowArray:
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> tuple[object, object]:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        For example, you can call [`pyarrow.array()`][pyarrow.array] to convert this
        array into a pyarrow array, without copying memory.
        """

    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    @classmethod
    def from_arrow(cls, data: ArrowArrayExportable) -> GeoArrowArray: ...
    @classmethod
    def from_arrow_pycapsule(
        cls,
        schema_capsule: object,
        array_capsule: object,
    ) -> GeoArrowArray: ...
    @property
    def null_count(self) -> int: ...
    @property
    def type(self) -> GeoArrowType: ...
