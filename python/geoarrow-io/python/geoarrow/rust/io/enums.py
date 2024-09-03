from enum import Enum, auto


class StrEnum(str, Enum):
    def __new__(cls, value, *args, **kwargs):
        if not isinstance(value, (str, auto)):
            raise TypeError(
                f"Values of StrEnums must be strings: {value!r} is a {type(value)}"
            )
        return super().__new__(cls, value, *args, **kwargs)

    def __str__(self):
        return str(self.value)

    def _generate_next_value_(name, *_):
        return name.lower()


class GeoParquetEncoding(StrEnum):
    """Options for geometry encoding in GeoParquet."""

    WKB = auto()
    """Use Well-Known Binary (WKB) encoding when writing GeoParquet files.
    """

    Native = auto()
    """Use native GeoArrow geometry types when writing GeoParquet files.

    Supported as of GeoParquet version 1.1.

    This option provides for better read and write performance and for inferring spatial
    partitioning from remote files. But it does not yet have widespread support.
    """
