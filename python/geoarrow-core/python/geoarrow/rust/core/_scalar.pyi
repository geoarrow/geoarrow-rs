from ._data_type import GeoType

class GeoScalar:
    """
    An immutable geometry scalar using GeoArrow's in-memory representation.

    **Note**: for best performance, do as many operations as possible on arrays or
    chunked arrays instead of scalars.
    """
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

    def __eq__(self, value: object) -> bool: ...
    def __repr__(self) -> str: ...
    def _repr_svg_(self) -> str:
        """Render as SVG in IPython/Jupyter."""
    @property
    def __geo_interface__(self) -> dict[str, object]:
        """Implements the [Geo Interface].

        For example, you can pass this to [`shapely.geometry.shape`].

        [Geo Interface]: https://gist.github.com/sgillies/2217756
        """

    @property
    def is_null(self) -> bool:
        """Check if the scalar is null.

        Note that Arrow arrays hold a separate null bitmap, so this is separate from
        whether the geometry is empty.
        """
    @property
    def type(self) -> GeoType:
        """The type of the scalar."""
