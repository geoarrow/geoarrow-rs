from typing import Literal

class Centroid:
    def __init__(
        self,
        *,
        coord_type: Literal["interleaved", "separated"] | None = None,
    ) -> None: ...
    def __datafusion_scalar_udf__(self) -> object: ...

class ConvexHull:
    def __init__(
        self,
        *,
        coord_type: Literal["interleaved", "separated"] | None = None,
    ) -> None: ...
    def __datafusion_scalar_udf__(self) -> object: ...

class OrientedEnvelope:
    def __init__(
        self,
        *,
        coord_type: Literal["interleaved", "separated"] | None = None,
    ) -> None: ...
    def __datafusion_scalar_udf__(self) -> object: ...

class PointOnSurface:
    def __init__(
        self,
        *,
        coord_type: Literal["interleaved", "separated"] | None = None,
    ) -> None: ...
    def __datafusion_scalar_udf__(self) -> object: ...

class Simplify:
    def __init__(
        self,
        *,
        coord_type: Literal["interleaved", "separated"] | None = None,
    ) -> None: ...
    def __datafusion_scalar_udf__(self) -> object: ...

class SimplifyPreserveTopology:
    def __init__(
        self,
        *,
        coord_type: Literal["interleaved", "separated"] | None = None,
    ) -> None: ...
    def __datafusion_scalar_udf__(self) -> object: ...

class SimplifyVW:
    def __init__(
        self,
        *,
        coord_type: Literal["interleaved", "separated"] | None = None,
    ) -> None: ...
    def __datafusion_scalar_udf__(self) -> object: ...
