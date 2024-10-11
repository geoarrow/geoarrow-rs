use std::sync::Arc;

use crate::array::mixed::builder::DEFAULT_PREFER_MULTI;
use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::{
    GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait, MultiLineStringTrait,
    MultiPointTrait, MultiPolygonTrait, PolygonTrait, RectTrait,
};
use crate::scalar::*;
use crate::trait_::ArrayAccessor;
use crate::NativeArray;

pub trait MapCoords {
    type Output;

    fn map_coords<F>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> geo::Coord + Sync,
    {
        self.try_map_coords(|coord| Ok::<_, GeoArrowError>(map_op(coord)))
    }

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>;
}

// Scalar impls

impl MapCoords for Coord<'_, 2> {
    type Output = geo::Coord;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        Ok(map_op(self)?)
    }
}

impl MapCoords for Point<'_, 2> {
    type Output = geo::Point;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        Ok(geo::Point(map_op(&self.coord())?))
    }
}

impl MapCoords for LineString<'_, 2> {
    type Output = geo::LineString;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        let output_coords = self
            .points()
            .map(|point| map_op(&point.coord()))
            .collect::<std::result::Result<Vec<_>, E>>()?;
        Ok(geo::LineString::new(output_coords))
    }
}

impl MapCoords for Polygon<'_, 2> {
    type Output = geo::Polygon;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        if self.exterior().is_none() {
            return Err(GeoArrowError::General(
                "Empty polygons not yet supported in MapCoords".to_string(),
            ));
        }
        let exterior = self.exterior().unwrap().try_map_coords(&map_op)?;
        let interiors = self
            .interiors()
            .map(|int| int.try_map_coords(&map_op))
            .collect::<Result<Vec<_>>>()?;
        Ok(geo::Polygon::new(exterior, interiors))
    }
}

impl MapCoords for MultiPoint<'_, 2> {
    type Output = geo::MultiPoint;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        let points = self
            .points()
            .map(|point| point.try_map_coords(&map_op))
            .collect::<Result<Vec<_>>>()?;
        Ok(geo::MultiPoint::new(points))
    }
}

impl MapCoords for MultiLineString<'_, 2> {
    type Output = geo::MultiLineString;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        let lines = self
            .line_strings()
            .map(|line_string| line_string.try_map_coords(&map_op))
            .collect::<Result<Vec<_>>>()?;
        Ok(geo::MultiLineString::new(lines))
    }
}

impl MapCoords for MultiPolygon<'_, 2> {
    // TODO: support empty polygons within a multi polygon
    type Output = geo::MultiPolygon;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        let polygons = self
            .polygons()
            .map(|polygon| polygon.try_map_coords(&map_op))
            .collect::<Result<Vec<_>>>()?;
        Ok(geo::MultiPolygon::new(polygons))
    }
}

impl MapCoords for Geometry<'_, 2> {
    type Output = geo::Geometry;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        match self.as_type() {
            GeometryType::Point(geom) => Ok(geo::Geometry::Point(geom.try_map_coords(&map_op)?)),
            GeometryType::LineString(geom) => {
                Ok(geo::Geometry::LineString(geom.try_map_coords(&map_op)?))
            }
            GeometryType::Polygon(geom) => {
                Ok(geo::Geometry::Polygon(geom.try_map_coords(&map_op)?))
            }
            GeometryType::MultiPoint(geom) => {
                Ok(geo::Geometry::MultiPoint(geom.try_map_coords(&map_op)?))
            }
            GeometryType::MultiLineString(geom) => Ok(geo::Geometry::MultiLineString(
                geom.try_map_coords(&map_op)?,
            )),
            GeometryType::MultiPolygon(geom) => {
                Ok(geo::Geometry::MultiPolygon(geom.try_map_coords(&map_op)?))
            }
            GeometryType::GeometryCollection(geom) => Ok(geo::Geometry::GeometryCollection(
                geom.try_map_coords(&map_op)?,
            )),
            GeometryType::Rect(geom) => Ok(geo::Geometry::Rect(geom.try_map_coords(&map_op)?)),
        }
    }
}

impl MapCoords for GeometryCollection<'_, 2> {
    type Output = geo::GeometryCollection;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        let geoms = self
            .geometries()
            .map(|geom| geom.try_map_coords(&map_op))
            .collect::<Result<Vec<_>>>()?;
        Ok(geo::GeometryCollection::new_from(geoms))
    }
}

impl MapCoords for Rect<'_, 2> {
    type Output = geo::Rect;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        let lower = self.lower();
        let upper = self.upper();
        let minx = lower[0];
        let miny = lower[1];
        let maxx = upper[0];
        let maxy = upper[1];
        let coords = vec![minx, miny, maxx, maxy];
        let coord_buffer = CoordBuffer::Interleaved(InterleavedCoordBuffer::new(coords.into()));
        let lower_coord = coord_buffer.value(0);
        let upper_coord = coord_buffer.value(1);

        let new_lower = map_op(&lower_coord)?;
        let new_upper = map_op(&upper_coord)?;
        Ok(geo::Rect::new(new_lower, new_upper))
    }
}

impl MapCoords for PointArray<2> {
    type Output = PointArray<2>;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        let mut builder = PointBuilder::with_capacity_and_options(
            self.buffer_lengths(),
            self.coord_type(),
            self.metadata(),
        );
        for maybe_geom in self.iter() {
            if let Some(geom) = maybe_geom {
                let result = geom.coord().try_map_coords(&map_op)?;
                builder.push_point(Some(&result));
            } else {
                builder.push_null()
            }
        }
        Ok(builder.finish())
    }
}

impl MapCoords for LineStringArray<2> {
    type Output = LineStringArray<2>;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        let mut builder = LineStringBuilder::with_capacity_and_options(
            self.buffer_lengths(),
            self.coord_type(),
            self.metadata(),
        );
        for maybe_geom in self.iter() {
            if let Some(geom) = maybe_geom {
                let result = geom.try_map_coords(&map_op)?;
                builder.push_line_string(Some(&result))?;
            } else {
                builder.push_null()
            }
        }
        Ok(builder.finish())
    }
}

impl MapCoords for PolygonArray<2> {
    type Output = PolygonArray<2>;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        let mut builder = PolygonBuilder::with_capacity_and_options(
            self.buffer_lengths(),
            self.coord_type(),
            self.metadata(),
        );
        for maybe_geom in self.iter() {
            if let Some(geom) = maybe_geom {
                let result = geom.try_map_coords(&map_op)?;
                builder.push_polygon(Some(&result))?;
            } else {
                builder.push_null()
            }
        }
        Ok(builder.finish())
    }
}

impl MapCoords for MultiPointArray<2> {
    type Output = MultiPointArray<2>;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        let mut builder = MultiPointBuilder::with_capacity_and_options(
            self.buffer_lengths(),
            self.coord_type(),
            self.metadata(),
        );
        for maybe_geom in self.iter() {
            if let Some(geom) = maybe_geom {
                let result = geom.try_map_coords(&map_op)?;
                builder.push_multi_point(Some(&result))?;
            } else {
                builder.push_null()
            }
        }
        Ok(builder.finish())
    }
}

impl MapCoords for MultiLineStringArray<2> {
    type Output = MultiLineStringArray<2>;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        let mut builder = MultiLineStringBuilder::with_capacity_and_options(
            self.buffer_lengths(),
            self.coord_type(),
            self.metadata(),
        );
        for maybe_geom in self.iter() {
            if let Some(geom) = maybe_geom {
                let result = geom.try_map_coords(&map_op)?;
                builder.push_multi_line_string(Some(&result))?;
            } else {
                builder.push_null()
            }
        }
        Ok(builder.finish())
    }
}

impl MapCoords for MultiPolygonArray<2> {
    type Output = MultiPolygonArray<2>;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        let mut builder = MultiPolygonBuilder::with_capacity_and_options(
            self.buffer_lengths(),
            self.coord_type(),
            self.metadata(),
        );
        for maybe_geom in self.iter() {
            if let Some(geom) = maybe_geom {
                let result = geom.try_map_coords(&map_op)?;
                builder.push_multi_polygon(Some(&result))?;
            } else {
                builder.push_null()
            }
        }
        Ok(builder.finish())
    }
}

impl MapCoords for MixedGeometryArray<2> {
    type Output = MixedGeometryArray<2>;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        let mut builder = MixedGeometryBuilder::with_capacity_and_options(
            self.buffer_lengths(),
            self.coord_type(),
            self.metadata(),
            DEFAULT_PREFER_MULTI,
        );
        for maybe_geom in self.iter() {
            if let Some(geom) = maybe_geom {
                let result = geom.try_map_coords(&map_op)?;
                builder.push_geometry(Some(&result))?;
            } else {
                builder.push_null()
            }
        }
        Ok(builder.finish())
    }
}

impl MapCoords for GeometryCollectionArray<2> {
    type Output = GeometryCollectionArray<2>;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        let mut builder = GeometryCollectionBuilder::with_capacity_and_options(
            self.buffer_lengths(),
            self.coord_type(),
            self.metadata(),
            DEFAULT_PREFER_MULTI,
        );
        for maybe_geom in self.iter() {
            if let Some(geom) = maybe_geom {
                let result = geom.try_map_coords(&map_op)?;
                builder.push_geometry_collection(Some(&result))?;
            } else {
                builder.push_null()
            }
        }
        Ok(builder.finish())
    }
}

impl MapCoords for RectArray<2> {
    type Output = RectArray<2>;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        let mut builder = RectBuilder::with_capacity_and_options(self.len(), self.metadata());
        for maybe_geom in self.iter() {
            if let Some(geom) = maybe_geom {
                let result = geom.try_map_coords(&map_op)?;
                builder.push_rect(Some(&result));
            } else {
                builder.push_null()
            }
        }
        Ok(builder.finish())
    }
}

impl MapCoords for &dyn NativeArray {
    type Output = Arc<dyn NativeArray>;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        use Dimension::*;
        use NativeType::*;

        let result: Arc<dyn NativeArray> = match self.data_type() {
            Point(_, XY) => Arc::new(self.as_point::<2>().try_map_coords(map_op)?),
            LineString(_, XY) => Arc::new(self.as_line_string::<2>().try_map_coords(map_op)?),
            Polygon(_, XY) => Arc::new(self.as_polygon::<2>().try_map_coords(map_op)?),
            MultiPoint(_, XY) => Arc::new(self.as_multi_point::<2>().try_map_coords(map_op)?),
            MultiLineString(_, XY) => {
                Arc::new(self.as_multi_line_string::<2>().try_map_coords(map_op)?)
            }
            MultiPolygon(_, XY) => Arc::new(self.as_multi_polygon::<2>().try_map_coords(map_op)?),
            Mixed(_, XY) => Arc::new(self.as_mixed::<2>().try_map_coords(map_op)?),
            GeometryCollection(_, XY) => {
                Arc::new(self.as_geometry_collection::<2>().try_map_coords(map_op)?)
            }
            Rect(XY) => Arc::new(self.as_rect::<2>().try_map_coords(map_op)?),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl MapCoords for ChunkedPointArray<2> {
    type Output = ChunkedPointArray<2>;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        Ok(ChunkedGeometryArray::new(
            self.try_map(|chunk| chunk.try_map_coords(&map_op))?,
        ))
    }
}

impl MapCoords for ChunkedLineStringArray<2> {
    type Output = ChunkedLineStringArray<2>;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        Ok(ChunkedGeometryArray::new(
            self.try_map(|chunk| chunk.try_map_coords(&map_op))?,
        ))
    }
}

impl MapCoords for ChunkedPolygonArray<2> {
    type Output = ChunkedPolygonArray<2>;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        Ok(ChunkedGeometryArray::new(
            self.try_map(|chunk| chunk.try_map_coords(&map_op))?,
        ))
    }
}

impl MapCoords for ChunkedMultiPointArray<2> {
    type Output = ChunkedMultiPointArray<2>;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        Ok(ChunkedGeometryArray::new(
            self.try_map(|chunk| chunk.try_map_coords(&map_op))?,
        ))
    }
}

impl MapCoords for ChunkedMultiLineStringArray<2> {
    type Output = ChunkedMultiLineStringArray<2>;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        Ok(ChunkedGeometryArray::new(
            self.try_map(|chunk| chunk.try_map_coords(&map_op))?,
        ))
    }
}

impl MapCoords for ChunkedMultiPolygonArray<2> {
    type Output = ChunkedMultiPolygonArray<2>;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        Ok(ChunkedGeometryArray::new(
            self.try_map(|chunk| chunk.try_map_coords(&map_op))?,
        ))
    }
}

impl MapCoords for ChunkedMixedGeometryArray<2> {
    type Output = ChunkedMixedGeometryArray<2>;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        Ok(ChunkedGeometryArray::new(
            self.try_map(|chunk| chunk.try_map_coords(&map_op))?,
        ))
    }
}

impl MapCoords for ChunkedGeometryCollectionArray<2> {
    type Output = ChunkedGeometryCollectionArray<2>;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        Ok(ChunkedGeometryArray::new(
            self.try_map(|chunk| chunk.try_map_coords(&map_op))?,
        ))
    }
}

impl MapCoords for ChunkedRectArray<2> {
    type Output = ChunkedRectArray<2>;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        Ok(ChunkedGeometryArray::new(
            self.try_map(|chunk| chunk.try_map_coords(&map_op))?,
        ))
    }
}

impl MapCoords for &dyn ChunkedNativeArray {
    type Output = Arc<dyn ChunkedNativeArray>;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord<2>) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        use Dimension::*;
        use NativeType::*;

        let result: Arc<dyn ChunkedNativeArray> = match self.data_type() {
            Point(_, XY) => Arc::new(self.as_point::<2>().try_map_coords(map_op)?),
            LineString(_, XY) => Arc::new(self.as_line_string::<2>().try_map_coords(map_op)?),
            Polygon(_, XY) => Arc::new(self.as_polygon::<2>().try_map_coords(map_op)?),
            MultiPoint(_, XY) => Arc::new(self.as_multi_point::<2>().try_map_coords(map_op)?),
            MultiLineString(_, XY) => {
                Arc::new(self.as_multi_line_string::<2>().try_map_coords(map_op)?)
            }
            MultiPolygon(_, XY) => Arc::new(self.as_multi_polygon::<2>().try_map_coords(map_op)?),
            Mixed(_, XY) => Arc::new(self.as_mixed::<2>().try_map_coords(map_op)?),
            GeometryCollection(_, XY) => {
                Arc::new(self.as_geometry_collection::<2>().try_map_coords(map_op)?)
            }
            Rect(XY) => Arc::new(self.as_rect::<2>().try_map_coords(map_op)?),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}
