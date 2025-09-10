from __future__ import annotations

from typing import TYPE_CHECKING

from datafusion import udf

from ._rust import *
from ._rust import ___version

__version__: str = ___version()

if TYPE_CHECKING:
    from datafusion import SessionContext


def register_all_geo(ctx: SessionContext):
    from . import geo

    # measurement
    ctx.register_udf(udf(geo.Area()))
    ctx.register_udf(udf(geo.Distance()))
    ctx.register_udf(udf(geo.Length()))

    # processing
    ctx.register_udf(udf(geo.Centroid()))
    ctx.register_udf(udf(geo.ConvexHull()))
    ctx.register_udf(udf(geo.OrientedEnvelope()))
    ctx.register_udf(udf(geo.PointOnSurface()))
    ctx.register_udf(udf(geo.Simplify()))
    ctx.register_udf(udf(geo.SimplifyPreserveTopology()))
    ctx.register_udf(udf(geo.SimplifyVW()))

    # relationships
    ctx.register_udf(udf(geo.Contains()))
    ctx.register_udf(udf(geo.CoveredBy()))
    ctx.register_udf(udf(geo.Covers()))
    ctx.register_udf(udf(geo.Disjoint()))
    ctx.register_udf(udf(geo.Intersects()))
    ctx.register_udf(udf(geo.Overlaps()))
    ctx.register_udf(udf(geo.Touches()))

    # validation
    ctx.register_udf(udf(geo.IsValid()))
    ctx.register_udf(udf(geo.IsValidReason()))


def register_all_geohash(ctx: SessionContext):
    from . import geohash

    ctx.register_udf(udf(geohash.GeoHash()))
    ctx.register_udf(udf(geohash.PointFromGeoHash()))
    ctx.register_udf(udf(geohash.Box2DFromGeoHash()))


def register_all_native(ctx: SessionContext):
    from . import native

    # accessors
    ctx.register_udf(udf(native.CoordDim()))
    ctx.register_udf(udf(native.NDims()))
    ctx.register_udf(udf(native.X()))
    ctx.register_udf(udf(native.Y()))
    ctx.register_udf(udf(native.Z()))
    ctx.register_udf(udf(native.M()))

    # bounding box
    ctx.register_udf(udf(native.Box2D()))
    ctx.register_udf(udf(native.Box3D()))
    ctx.register_udf(udf(native.XMin()))
    ctx.register_udf(udf(native.YMin()))
    ctx.register_udf(udf(native.ZMin()))
    ctx.register_udf(udf(native.XMax()))
    ctx.register_udf(udf(native.YMax()))
    ctx.register_udf(udf(native.ZMax()))
    ctx.register_udf(udf(native.MakeBox2D()))
    ctx.register_udf(udf(native.MakeBox3D()))

    # constructors
    ctx.register_udf(udf(native.Point()))
    ctx.register_udf(udf(native.PointZ()))
    ctx.register_udf(udf(native.PointM()))
    ctx.register_udf(udf(native.PointZM()))
    ctx.register_udf(udf(native.MakePoint()))
    ctx.register_udf(udf(native.MakePointM()))

    # io
    ctx.register_udf(udf(native.AsText()))
    ctx.register_udf(udf(native.AsBinary()))
    ctx.register_udf(udf(native.GeomFromText()))
    ctx.register_udf(udf(native.GeomFromWKB()))


def register_all(ctx: SessionContext):
    register_all_geo(ctx)
    register_all_geohash(ctx)
    register_all_native(ctx)
