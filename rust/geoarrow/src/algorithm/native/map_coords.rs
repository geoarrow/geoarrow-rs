use std::sync::Arc;

use crate::array::mixed::builder::DEFAULT_PREFER_MULTI;
use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::scalar::*;
use crate::trait_::ArrayAccessor;
use crate::NativeArray;
use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait,
};

/// Note: this will currently always create a _two-dimensional_ output array because it returns a [`geo::Coord`].
pub trait MapCoords {
    type Output;

    fn map_coords<F>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord + Sync,
    {
        self.try_map_coords(|coord| Ok::<_, GeoArrowError>(map_op(coord)))
    }

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>;
}

// Scalar impls

impl MapCoords for Coord {
    type Output = geo::Coord;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        Ok(map_op(self)?)
    }
}

impl MapCoords for Point {
    type Output = geo::Point;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        Ok(geo::Point(map_op(&self.coord().unwrap())?))
    }
}

impl MapCoords for LineString {
    type Output = geo::LineString;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        let output_coords = self
            .coords()
            .map(|coord| map_op(&coord))
            .collect::<std::result::Result<Vec<_>, E>>()?;
        Ok(geo::LineString::new(output_coords))
    }
}

impl MapCoords for Polygon {
    type Output = geo::Polygon;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
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

impl MapCoords for MultiPoint {
    type Output = geo::MultiPoint;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        let points = self
            .points()
            .map(|point| point.try_map_coords(&map_op))
            .collect::<Result<Vec<_>>>()?;
        Ok(geo::MultiPoint::new(points))
    }
}

impl MapCoords for MultiLineString {
    type Output = geo::MultiLineString;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        let lines = self
            .line_strings()
            .map(|line_string| line_string.try_map_coords(&map_op))
            .collect::<Result<Vec<_>>>()?;
        Ok(geo::MultiLineString::new(lines))
    }
}

impl MapCoords for MultiPolygon {
    // TODO: support empty polygons within a multi polygon
    type Output = geo::MultiPolygon;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        let polygons = self
            .polygons()
            .map(|polygon| polygon.try_map_coords(&map_op))
            .collect::<Result<Vec<_>>>()?;
        Ok(geo::MultiPolygon::new(polygons))
    }
}

impl MapCoords for Geometry {
    type Output = geo::Geometry;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        use GeometryType::*;

        match self.as_type() {
            Point(geom) => Ok(geo::Geometry::Point(geom.try_map_coords(&map_op)?)),
            LineString(geom) => Ok(geo::Geometry::LineString(geom.try_map_coords(&map_op)?)),
            Polygon(geom) => Ok(geo::Geometry::Polygon(geom.try_map_coords(&map_op)?)),
            MultiPoint(geom) => Ok(geo::Geometry::MultiPoint(geom.try_map_coords(&map_op)?)),
            MultiLineString(geom) => Ok(geo::Geometry::MultiLineString(
                geom.try_map_coords(&map_op)?,
            )),
            MultiPolygon(geom) => Ok(geo::Geometry::MultiPolygon(geom.try_map_coords(&map_op)?)),
            GeometryCollection(geom) => Ok(geo::Geometry::GeometryCollection(
                geom.try_map_coords(&map_op)?,
            )),
            Rect(geom) => Ok(geo::Geometry::Rect(geom.try_map_coords(&map_op)?)),
            Line(_) | Triangle(_) => todo!(),
        }
    }
}

impl MapCoords for GeometryCollection {
    type Output = geo::GeometryCollection;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        let geoms = self
            .geometries()
            .map(|geom| geom.try_map_coords(&map_op))
            .collect::<Result<Vec<_>>>()?;
        Ok(geo::GeometryCollection::new_from(geoms))
    }
}

impl MapCoords for Rect {
    type Output = geo::Rect;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        let lower = self.min();
        let upper = self.max();
        let minx = lower.x();
        let miny = lower.y();
        let maxx = upper.x();
        let maxy = upper.y();
        let coords = vec![minx, miny, maxx, maxy];
        let coord_buffer =
            CoordBuffer::Interleaved(InterleavedCoordBuffer::new(coords.into(), Dimension::XY));
        let lower_coord = coord_buffer.value(0);
        let upper_coord = coord_buffer.value(1);

        let new_lower = map_op(&lower_coord)?;
        let new_upper = map_op(&upper_coord)?;
        Ok(geo::Rect::new(new_lower, new_upper))
    }
}

impl MapCoords for PointArray {
    type Output = PointArray;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        let mut builder = PointBuilder::with_capacity_and_options(
            Dimension::XY,
            self.buffer_lengths(),
            self.coord_type(),
            self.metadata(),
        );
        for maybe_geom in self.iter() {
            if let Some(geom) = maybe_geom {
                let result = geom.coord().unwrap().try_map_coords(&map_op)?;
                builder.push_coord(Some(&result));
            } else {
                builder.push_null()
            }
        }
        Ok(builder.finish())
    }
}

impl MapCoords for LineStringArray {
    type Output = LineStringArray;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        let mut builder = LineStringBuilder::with_capacity_and_options(
            Dimension::XY,
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

impl MapCoords for PolygonArray {
    type Output = PolygonArray;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        let mut builder = PolygonBuilder::with_capacity_and_options(
            Dimension::XY,
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

impl MapCoords for MultiPointArray {
    type Output = MultiPointArray;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        let mut builder = MultiPointBuilder::with_capacity_and_options(
            Dimension::XY,
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

impl MapCoords for MultiLineStringArray {
    type Output = MultiLineStringArray;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        let mut builder = MultiLineStringBuilder::with_capacity_and_options(
            Dimension::XY,
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

impl MapCoords for MultiPolygonArray {
    type Output = MultiPolygonArray;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        let mut builder = MultiPolygonBuilder::with_capacity_and_options(
            Dimension::XY,
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

impl MapCoords for MixedGeometryArray {
    type Output = MixedGeometryArray;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        let mut builder = MixedGeometryBuilder::with_capacity_and_options(
            Dimension::XY,
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

impl MapCoords for GeometryCollectionArray {
    type Output = GeometryCollectionArray;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        let mut builder = GeometryCollectionBuilder::with_capacity_and_options(
            Dimension::XY,
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

impl MapCoords for RectArray {
    type Output = RectArray;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        let mut builder =
            RectBuilder::with_capacity_and_options(Dimension::XY, self.len(), self.metadata());
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
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        use Dimension::*;
        use NativeType::*;

        let result: Arc<dyn NativeArray> = match self.data_type() {
            Point(_, XY) => Arc::new(self.as_point().try_map_coords(map_op)?),
            LineString(_, XY) => Arc::new(self.as_line_string().try_map_coords(map_op)?),
            Polygon(_, XY) => Arc::new(self.as_polygon().try_map_coords(map_op)?),
            MultiPoint(_, XY) => Arc::new(self.as_multi_point().try_map_coords(map_op)?),
            MultiLineString(_, XY) => Arc::new(self.as_multi_line_string().try_map_coords(map_op)?),
            MultiPolygon(_, XY) => Arc::new(self.as_multi_polygon().try_map_coords(map_op)?),
            GeometryCollection(_, XY) => {
                Arc::new(self.as_geometry_collection().try_map_coords(map_op)?)
            }
            Rect(XY) => Arc::new(self.as_rect().try_map_coords(map_op)?),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl MapCoords for ChunkedPointArray {
    type Output = ChunkedPointArray;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        Ok(ChunkedGeometryArray::new(
            self.try_map(|chunk| chunk.try_map_coords(&map_op))?,
        ))
    }
}

impl MapCoords for ChunkedLineStringArray {
    type Output = ChunkedLineStringArray;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        Ok(ChunkedGeometryArray::new(
            self.try_map(|chunk| chunk.try_map_coords(&map_op))?,
        ))
    }
}

impl MapCoords for ChunkedPolygonArray {
    type Output = ChunkedPolygonArray;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        Ok(ChunkedGeometryArray::new(
            self.try_map(|chunk| chunk.try_map_coords(&map_op))?,
        ))
    }
}

impl MapCoords for ChunkedMultiPointArray {
    type Output = ChunkedMultiPointArray;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        Ok(ChunkedGeometryArray::new(
            self.try_map(|chunk| chunk.try_map_coords(&map_op))?,
        ))
    }
}

impl MapCoords for ChunkedMultiLineStringArray {
    type Output = ChunkedMultiLineStringArray;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        Ok(ChunkedGeometryArray::new(
            self.try_map(|chunk| chunk.try_map_coords(&map_op))?,
        ))
    }
}

impl MapCoords for ChunkedMultiPolygonArray {
    type Output = ChunkedMultiPolygonArray;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        Ok(ChunkedGeometryArray::new(
            self.try_map(|chunk| chunk.try_map_coords(&map_op))?,
        ))
    }
}

impl MapCoords for ChunkedMixedGeometryArray {
    type Output = ChunkedMixedGeometryArray;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        Ok(ChunkedGeometryArray::new(
            self.try_map(|chunk| chunk.try_map_coords(&map_op))?,
        ))
    }
}

impl MapCoords for ChunkedGeometryCollectionArray {
    type Output = ChunkedGeometryCollectionArray;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        Ok(ChunkedGeometryArray::new(
            self.try_map(|chunk| chunk.try_map_coords(&map_op))?,
        ))
    }
}

impl MapCoords for ChunkedRectArray {
    type Output = ChunkedRectArray;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
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
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        use Dimension::*;
        use NativeType::*;

        let result: Arc<dyn ChunkedNativeArray> = match self.data_type() {
            Point(_, XY) => Arc::new(self.as_point().try_map_coords(map_op)?),
            LineString(_, XY) => Arc::new(self.as_line_string().try_map_coords(map_op)?),
            Polygon(_, XY) => Arc::new(self.as_polygon().try_map_coords(map_op)?),
            MultiPoint(_, XY) => Arc::new(self.as_multi_point().try_map_coords(map_op)?),
            MultiLineString(_, XY) => Arc::new(self.as_multi_line_string().try_map_coords(map_op)?),
            MultiPolygon(_, XY) => Arc::new(self.as_multi_polygon().try_map_coords(map_op)?),
            GeometryCollection(_, XY) => {
                Arc::new(self.as_geometry_collection().try_map_coords(map_op)?)
            }
            Rect(XY) => Arc::new(self.as_rect().try_map_coords(map_op)?),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}
