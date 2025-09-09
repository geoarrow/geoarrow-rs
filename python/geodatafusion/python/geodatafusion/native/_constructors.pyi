from typing import Literal

class Point:
    def __init__(
        self,
        *,
        coord_type: Literal["interleaved", "separated"] | None = None,
    ) -> None: ...
    def __datafusion_scalar_udf__(self) -> object: ...

class PointZ:
    def __init__(
        self,
        *,
        coord_type: Literal["interleaved", "separated"] | None = None,
    ) -> None: ...
    def __datafusion_scalar_udf__(self) -> object: ...

class PointM:
    def __init__(
        self,
        *,
        coord_type: Literal["interleaved", "separated"] | None = None,
    ) -> None: ...
    def __datafusion_scalar_udf__(self) -> object: ...

class PointZM:
    def __init__(
        self,
        *,
        coord_type: Literal["interleaved", "separated"] | None = None,
    ) -> None: ...
    def __datafusion_scalar_udf__(self) -> object: ...

class MakePoint:
    def __init__(
        self,
        *,
        coord_type: Literal["interleaved", "separated"] | None = None,
    ) -> None: ...
    def __datafusion_scalar_udf__(self) -> object: ...

class MakePointM:
    def __init__(
        self,
        *,
        coord_type: Literal["interleaved", "separated"] | None = None,
    ) -> None: ...
    def __datafusion_scalar_udf__(self) -> object: ...
