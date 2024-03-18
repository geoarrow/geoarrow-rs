use std::sync::Arc;

use arrow_array::OffsetSizeTrait;

use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::{
    GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait, MultiLineStringTrait,
    MultiPointTrait, MultiPolygonTrait, PolygonTrait, RectTrait,
};
use crate::scalar::*;
use crate::trait_::GeometryArrayAccessor;
use crate::GeometryArrayTrait;

pub trait MapCoords {
    type Output;

    fn map_coords<F>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord,
    {
        self.try_map_coords(|coord| Ok::<_, GeoArrowError>(map_op(coord)))
    }

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E>,
        GeoArrowError: From<E>;
}

// Scalar impls

impl MapCoords for Coord<'_> {
    type Output = geo::Coord;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E>,
        GeoArrowError: From<E>,
    {
        Ok(map_op(self)?)
    }
}

impl MapCoords for Point<'_> {
    type Output = geo::Point;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E>,
        GeoArrowError: From<E>,
    {
        Ok(geo::Point(map_op(&self.coord())?))
    }
}

impl<O: OffsetSizeTrait> MapCoords for LineString<'_, O> {
    type Output = geo::LineString;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E>,
        GeoArrowError: From<E>,
    {
        let output_coords = self
            .coords()
            .map(|point| map_op(&point.coord()))
            .collect::<std::result::Result<Vec<_>, E>>()?;
        Ok(geo::LineString::new(output_coords))
    }
}

impl<O: OffsetSizeTrait> MapCoords for Polygon<'_, O> {
    type Output = geo::Polygon;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E>,
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

impl<O: OffsetSizeTrait> MapCoords for MultiPoint<'_, O> {
    type Output = geo::MultiPoint;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E>,
        GeoArrowError: From<E>,
    {
        let points = self
            .points()
            .map(|point| point.try_map_coords(&map_op))
            .collect::<Result<Vec<_>>>()?;
        Ok(geo::MultiPoint::new(points))
    }
}

impl<O: OffsetSizeTrait> MapCoords for MultiLineString<'_, O> {
    type Output = geo::MultiLineString;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E>,
        GeoArrowError: From<E>,
    {
        let lines = self
            .lines()
            .map(|line_string| line_string.try_map_coords(&map_op))
            .collect::<Result<Vec<_>>>()?;
        Ok(geo::MultiLineString::new(lines))
    }
}

impl<O: OffsetSizeTrait> MapCoords for MultiPolygon<'_, O> {
    // TODO: support empty polygons within a multi polygon
    type Output = geo::MultiPolygon;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E>,
        GeoArrowError: From<E>,
    {
        let polygons = self
            .polygons()
            .map(|polygon| polygon.try_map_coords(&map_op))
            .collect::<Result<Vec<_>>>()?;
        Ok(geo::MultiPolygon::new(polygons))
    }
}

impl<O: OffsetSizeTrait> MapCoords for Geometry<'_, O> {
    type Output = geo::Geometry;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E>,
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
            _ => todo!(), // GeometryType::GeometryCollection(geom)
        }
    }
}

impl<O: OffsetSizeTrait> MapCoords for GeometryCollection<'_, O> {
    type Output = geo::GeometryCollection;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E>,
        GeoArrowError: From<E>,
    {
        let geoms = self
            .geometries()
            .map(|geom| geom.try_map_coords(&map_op))
            .collect::<Result<Vec<_>>>()?;
        Ok(geo::GeometryCollection::new_from(geoms))
    }
}

impl MapCoords for Rect<'_> {
    type Output = geo::Rect;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E>,
        GeoArrowError: From<E>,
    {
        let (minx, miny) = self.lower();
        let (maxx, maxy) = self.upper();
        let coords = vec![minx, miny, maxx, maxy];
        let coord_buffer = CoordBuffer::Interleaved(InterleavedCoordBuffer::new(coords.into()));
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
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E>,
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

impl<O: OffsetSizeTrait> MapCoords for LineStringArray<O> {
    type Output = LineStringArray<O>;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E>,
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

impl<O: OffsetSizeTrait> MapCoords for PolygonArray<O> {
    type Output = PolygonArray<O>;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E>,
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

impl<O: OffsetSizeTrait> MapCoords for MultiPointArray<O> {
    type Output = MultiPointArray<O>;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E>,
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

impl<O: OffsetSizeTrait> MapCoords for MultiLineStringArray<O> {
    type Output = MultiLineStringArray<O>;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E>,
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

impl<O: OffsetSizeTrait> MapCoords for MultiPolygonArray<O> {
    type Output = MultiPolygonArray<O>;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E>,
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

impl<O: OffsetSizeTrait> MapCoords for MixedGeometryArray<O> {
    type Output = MixedGeometryArray<O>;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E>,
        GeoArrowError: From<E>,
    {
        let mut builder = MixedGeometryBuilder::with_capacity_and_options(
            self.buffer_lengths(),
            self.coord_type(),
            self.metadata(),
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

impl<O: OffsetSizeTrait> MapCoords for GeometryCollectionArray<O> {
    type Output = GeometryCollectionArray<O>;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E>,
        GeoArrowError: From<E>,
    {
        let mut builder = GeometryCollectionBuilder::with_capacity_and_options(
            self.buffer_lengths(),
            self.coord_type(),
            self.metadata(),
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
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E>,
        GeoArrowError: From<E>,
    {
        let mut builder = RectBuilder::with_capacity(self.len(), self.metadata());
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

impl MapCoords for &dyn GeometryArrayTrait {
    type Output = Arc<dyn GeometryArrayTrait>;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E>,
        GeoArrowError: From<E>,
    {
        let result: Arc<dyn GeometryArrayTrait> = match self.data_type() {
            GeoDataType::Point(_) => Arc::new(self.as_point().try_map_coords(map_op)?),
            GeoDataType::LineString(_) => Arc::new(self.as_line_string().try_map_coords(map_op)?),
            GeoDataType::LargeLineString(_) => {
                Arc::new(self.as_large_line_string().try_map_coords(map_op)?)
            }
            GeoDataType::Polygon(_) => Arc::new(self.as_polygon().try_map_coords(map_op)?),
            GeoDataType::LargePolygon(_) => {
                Arc::new(self.as_large_polygon().try_map_coords(map_op)?)
            }
            GeoDataType::MultiPoint(_) => Arc::new(self.as_multi_point().try_map_coords(map_op)?),
            GeoDataType::LargeMultiPoint(_) => {
                Arc::new(self.as_large_multi_point().try_map_coords(map_op)?)
            }
            GeoDataType::MultiLineString(_) => {
                Arc::new(self.as_multi_line_string().try_map_coords(map_op)?)
            }
            GeoDataType::LargeMultiLineString(_) => {
                Arc::new(self.as_large_multi_line_string().try_map_coords(map_op)?)
            }
            GeoDataType::MultiPolygon(_) => {
                Arc::new(self.as_multi_polygon().try_map_coords(map_op)?)
            }
            GeoDataType::LargeMultiPolygon(_) => {
                Arc::new(self.as_large_multi_polygon().try_map_coords(map_op)?)
            }
            GeoDataType::Mixed(_) => Arc::new(self.as_mixed().try_map_coords(map_op)?),
            GeoDataType::LargeMixed(_) => Arc::new(self.as_large_mixed().try_map_coords(map_op)?),
            GeoDataType::GeometryCollection(_) => {
                Arc::new(self.as_geometry_collection().try_map_coords(map_op)?)
            }
            GeoDataType::LargeGeometryCollection(_) => {
                Arc::new(self.as_large_geometry_collection().try_map_coords(map_op)?)
            }
            GeoDataType::Rect => Arc::new(self.as_rect().try_map_coords(map_op)?),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

pub trait MapCoordsChunked {
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

    fn try_map_coords_init<F, INIT, T, E>(&self, init: INIT, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&T, &crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        INIT: Fn() -> T + Sync + Send,
        GeoArrowError: From<E>;
}

impl MapCoordsChunked for ChunkedPointArray {
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

    fn try_map_coords_init<F, INIT, T, E>(&self, init: INIT, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&T, &crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        INIT: Fn() -> T + Sync + Send,
        GeoArrowError: From<E>,
    {
        let chunks = self.try_map_init(init, |base, chunk| {
            chunk.try_map_coords(|coord| map_op(base, coord))
        })?;
        Ok(ChunkedGeometryArray::new(chunks))
    }
}

macro_rules! impl_chunked {
    ($struct_name:ty) => {
        impl<O: OffsetSizeTrait> MapCoordsChunked for $struct_name {
            type Output = Self;

            fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
            where
                F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
                GeoArrowError: From<E>,
            {
                Ok(ChunkedGeometryArray::new(
                    self.try_map(|chunk| chunk.try_map_coords(&map_op))?,
                ))
            }

            fn try_map_coords_init<F, INIT, T, E>(
                &self,
                init: INIT,
                map_op: F,
            ) -> Result<Self::Output>
            where
                F: Fn(&T, &crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
                INIT: Fn() -> T + Sync + Send,
                GeoArrowError: From<E>,
            {
                let chunks = self.try_map_init(init, |base, chunk| {
                    chunk.try_map_coords(|coord| map_op(base, coord))
                })?;
                Ok(ChunkedGeometryArray::new(chunks))
            }
        }
    };
}

impl_chunked!(ChunkedLineStringArray<O>);
impl_chunked!(ChunkedPolygonArray<O>);
impl_chunked!(ChunkedMultiPointArray<O>);
impl_chunked!(ChunkedMultiLineStringArray<O>);
impl_chunked!(ChunkedMultiPolygonArray<O>);
impl_chunked!(ChunkedMixedGeometryArray<O>);
impl_chunked!(ChunkedGeometryCollectionArray<O>);

impl MapCoordsChunked for ChunkedRectArray {
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

    fn try_map_coords_init<F, INIT, T, E>(&self, init: INIT, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&T, &crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        INIT: Fn() -> T + Sync + Send,
        GeoArrowError: From<E>,
    {
        let chunks = self.try_map_init(init, |base, chunk| {
            chunk.try_map_coords(|coord| map_op(base, coord))
        })?;
        Ok(ChunkedGeometryArray::new(chunks))
    }
}

impl MapCoordsChunked for &dyn ChunkedGeometryArrayTrait {
    type Output = Arc<dyn ChunkedGeometryArrayTrait>;

    fn try_map_coords<F, E>(&self, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        GeoArrowError: From<E>,
    {
        let result: Arc<dyn ChunkedGeometryArrayTrait> = match self.data_type() {
            GeoDataType::Point(_) => Arc::new(self.as_point().try_map_coords(map_op)?),
            GeoDataType::LineString(_) => Arc::new(self.as_line_string().try_map_coords(map_op)?),
            GeoDataType::LargeLineString(_) => {
                Arc::new(self.as_large_line_string().try_map_coords(map_op)?)
            }
            GeoDataType::Polygon(_) => Arc::new(self.as_polygon().try_map_coords(map_op)?),
            GeoDataType::LargePolygon(_) => {
                Arc::new(self.as_large_polygon().try_map_coords(map_op)?)
            }
            GeoDataType::MultiPoint(_) => Arc::new(self.as_multi_point().try_map_coords(map_op)?),
            GeoDataType::LargeMultiPoint(_) => {
                Arc::new(self.as_large_multi_point().try_map_coords(map_op)?)
            }
            GeoDataType::MultiLineString(_) => {
                Arc::new(self.as_multi_line_string().try_map_coords(map_op)?)
            }
            GeoDataType::LargeMultiLineString(_) => {
                Arc::new(self.as_large_multi_line_string().try_map_coords(map_op)?)
            }
            GeoDataType::MultiPolygon(_) => {
                Arc::new(self.as_multi_polygon().try_map_coords(map_op)?)
            }
            GeoDataType::LargeMultiPolygon(_) => {
                Arc::new(self.as_large_multi_polygon().try_map_coords(map_op)?)
            }
            GeoDataType::Mixed(_) => Arc::new(self.as_mixed().try_map_coords(map_op)?),
            GeoDataType::LargeMixed(_) => Arc::new(self.as_large_mixed().try_map_coords(map_op)?),
            GeoDataType::GeometryCollection(_) => {
                Arc::new(self.as_geometry_collection().try_map_coords(map_op)?)
            }
            GeoDataType::LargeGeometryCollection(_) => {
                Arc::new(self.as_large_geometry_collection().try_map_coords(map_op)?)
            }
            GeoDataType::Rect => Arc::new(self.as_rect().try_map_coords(map_op)?),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }

    fn try_map_coords_init<F, INIT, T, E>(&self, init: INIT, map_op: F) -> Result<Self::Output>
    where
        F: Fn(&T, &crate::scalar::Coord) -> std::result::Result<geo::Coord, E> + Sync,
        INIT: Fn() -> T + Sync + Send,
        GeoArrowError: From<E>,
    {
        let result: Arc<dyn ChunkedGeometryArrayTrait> = match self.data_type() {
            GeoDataType::Point(_) => Arc::new(self.as_point().try_map_coords_init(init, map_op)?),
            GeoDataType::LineString(_) => {
                Arc::new(self.as_line_string().try_map_coords_init(init, map_op)?)
            }
            GeoDataType::LargeLineString(_) => Arc::new(
                self.as_large_line_string()
                    .try_map_coords_init(init, map_op)?,
            ),
            GeoDataType::Polygon(_) => {
                Arc::new(self.as_polygon().try_map_coords_init(init, map_op)?)
            }
            GeoDataType::LargePolygon(_) => {
                Arc::new(self.as_large_polygon().try_map_coords_init(init, map_op)?)
            }
            GeoDataType::MultiPoint(_) => {
                Arc::new(self.as_multi_point().try_map_coords_init(init, map_op)?)
            }
            GeoDataType::LargeMultiPoint(_) => Arc::new(
                self.as_large_multi_point()
                    .try_map_coords_init(init, map_op)?,
            ),
            GeoDataType::MultiLineString(_) => Arc::new(
                self.as_multi_line_string()
                    .try_map_coords_init(init, map_op)?,
            ),
            GeoDataType::LargeMultiLineString(_) => Arc::new(
                self.as_large_multi_line_string()
                    .try_map_coords_init(init, map_op)?,
            ),
            GeoDataType::MultiPolygon(_) => {
                Arc::new(self.as_multi_polygon().try_map_coords_init(init, map_op)?)
            }
            GeoDataType::LargeMultiPolygon(_) => Arc::new(
                self.as_large_multi_polygon()
                    .try_map_coords_init(init, map_op)?,
            ),
            GeoDataType::Mixed(_) => Arc::new(self.as_mixed().try_map_coords_init(init, map_op)?),
            GeoDataType::LargeMixed(_) => {
                Arc::new(self.as_large_mixed().try_map_coords_init(init, map_op)?)
            }
            GeoDataType::GeometryCollection(_) => Arc::new(
                self.as_geometry_collection()
                    .try_map_coords_init(init, map_op)?,
            ),
            GeoDataType::LargeGeometryCollection(_) => Arc::new(
                self.as_large_geometry_collection()
                    .try_map_coords_init(init, map_op)?,
            ),
            GeoDataType::Rect => Arc::new(self.as_rect().try_map_coords_init(init, map_op)?),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}
