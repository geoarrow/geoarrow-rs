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

    fn map_coords<F>(&self, map_op: F) -> Self::Output
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord + Sync;
}

// Scalar impls

impl MapCoords for Point<'_> {
    type Output = geo::Point;

    fn map_coords<F>(&self, map_op: F) -> Self::Output
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord + Sync,
    {
        geo::Point(map_op(&self.coord()))
    }
}

impl<O: OffsetSizeTrait> MapCoords for LineString<'_, O> {
    type Output = geo::LineString;

    fn map_coords<F>(&self, map_op: F) -> Self::Output
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord + Sync,
    {
        let output_coords = self
            .coords()
            .map(|point| map_op(&point.coord()))
            .collect::<Vec<_>>();
        geo::LineString::new(output_coords)
    }
}

impl<O: OffsetSizeTrait> MapCoords for Polygon<'_, O> {
    type Output = Result<geo::Polygon>;

    fn map_coords<F>(&self, map_op: F) -> Self::Output
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord + Sync,
    {
        if self.exterior().is_none() {
            return Err(GeoArrowError::General(
                "Empty polygons not yet supported in MapCoords".to_string(),
            ));
        }
        let exterior = self.exterior().unwrap().map_coords(&map_op);
        let interiors = self
            .interiors()
            .map(|int| int.map_coords(&map_op))
            .collect::<Vec<_>>();
        Ok(geo::Polygon::new(exterior, interiors))
    }
}

impl<O: OffsetSizeTrait> MapCoords for MultiPoint<'_, O> {
    type Output = geo::MultiPoint;

    fn map_coords<F>(&self, map_op: F) -> Self::Output
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord + Sync,
    {
        let points = self
            .points()
            .map(|point| point.map_coords(&map_op))
            .collect::<Vec<_>>();
        geo::MultiPoint::new(points)
    }
}

impl<O: OffsetSizeTrait> MapCoords for MultiLineString<'_, O> {
    type Output = geo::MultiLineString;

    fn map_coords<F>(&self, map_op: F) -> Self::Output
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord + Sync,
    {
        let lines = self
            .lines()
            .map(|line_string| line_string.map_coords(&map_op))
            .collect::<Vec<_>>();
        geo::MultiLineString::new(lines)
    }
}

impl<O: OffsetSizeTrait> MapCoords for MultiPolygon<'_, O> {
    // TODO: support empty polygons within a multi polygon
    type Output = Result<geo::MultiPolygon>;

    fn map_coords<F>(&self, map_op: F) -> Self::Output
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord + Sync,
    {
        let polygons = self
            .polygons()
            .map(|polygon| polygon.map_coords(&map_op))
            .collect::<Result<Vec<_>>>()?;
        Ok(geo::MultiPolygon::new(polygons))
    }
}

impl<O: OffsetSizeTrait> MapCoords for Geometry<'_, O> {
    type Output = Result<geo::Geometry>;

    fn map_coords<F>(&self, map_op: F) -> Self::Output
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord + Sync,
    {
        match self.as_type() {
            GeometryType::Point(geom) => Ok(geo::Geometry::Point(geom.map_coords(&map_op))),
            GeometryType::LineString(geom) => {
                Ok(geo::Geometry::LineString(geom.map_coords(&map_op)))
            }
            GeometryType::Polygon(geom) => Ok(geo::Geometry::Polygon(geom.map_coords(&map_op)?)),
            GeometryType::MultiPoint(geom) => {
                Ok(geo::Geometry::MultiPoint(geom.map_coords(&map_op)))
            }
            GeometryType::MultiLineString(geom) => {
                Ok(geo::Geometry::MultiLineString(geom.map_coords(&map_op)))
            }
            GeometryType::MultiPolygon(geom) => {
                Ok(geo::Geometry::MultiPolygon(geom.map_coords(&map_op)?))
            }
            GeometryType::GeometryCollection(geom) => {
                Ok(geo::Geometry::GeometryCollection(geom.map_coords(&map_op)?))
            }
            _ => todo!(), // GeometryType::GeometryCollection(geom)
        }
    }
}

impl<O: OffsetSizeTrait> MapCoords for GeometryCollection<'_, O> {
    type Output = Result<geo::GeometryCollection>;

    fn map_coords<F>(&self, map_op: F) -> Self::Output
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord + Sync,
    {
        let geoms = self
            .geometries()
            .map(|geom| geom.map_coords(&map_op))
            .collect::<Result<Vec<_>>>()?;
        Ok(geo::GeometryCollection::new_from(geoms))
    }
}

impl MapCoords for Rect<'_> {
    type Output = geo::Rect;

    fn map_coords<F>(&self, map_op: F) -> Self::Output
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord + Sync,
    {
        let (minx, miny) = self.lower();
        let (maxx, maxy) = self.upper();
        let coords = vec![minx, miny, maxx, maxy];
        let coord_buffer = CoordBuffer::Interleaved(InterleavedCoordBuffer::new(coords.into()));
        let lower_coord = coord_buffer.value(0);
        let upper_coord = coord_buffer.value(1);

        let new_lower = map_op(&lower_coord);
        let new_upper = map_op(&upper_coord);
        geo::Rect::new(new_lower, new_upper)
    }
}

impl MapCoords for PointArray {
    type Output = PointArray;

    fn map_coords<F>(&self, map_op: F) -> Self::Output
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord + Sync,
    {
        let mut builder = PointBuilder::with_capacity_and_options(
            self.buffer_lengths(),
            self.coord_type(),
            self.metadata(),
        );
        for maybe_geom in self.iter() {
            if let Some(geom) = maybe_geom {
                let coord = geom.coord();
                let result = map_op(&coord);
                builder.push_point(Some(&result));
            } else {
                builder.push_null()
            }
        }
        builder.finish()
    }
}

impl<O: OffsetSizeTrait> MapCoords for LineStringArray<O> {
    type Output = Result<LineStringArray<O>>;

    fn map_coords<F>(&self, map_op: F) -> Self::Output
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord + Sync,
    {
        let mut builder = LineStringBuilder::with_capacity_and_options(
            self.buffer_lengths(),
            self.coord_type(),
            self.metadata(),
        );
        for maybe_geom in self.iter() {
            if let Some(geom) = maybe_geom {
                let result = geom.map_coords(&map_op);
                builder.push_line_string(Some(&result))?;
            } else {
                builder.push_null()
            }
        }
        Ok(builder.finish())
    }
}

impl<O: OffsetSizeTrait> MapCoords for PolygonArray<O> {
    type Output = Result<PolygonArray<O>>;

    fn map_coords<F>(&self, map_op: F) -> Self::Output
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord + Sync,
    {
        let mut builder = PolygonBuilder::with_capacity_and_options(
            self.buffer_lengths(),
            self.coord_type(),
            self.metadata(),
        );
        for maybe_geom in self.iter() {
            if let Some(geom) = maybe_geom {
                let result = geom.map_coords(&map_op)?;
                builder.push_polygon(Some(&result))?;
            } else {
                builder.push_null()
            }
        }
        Ok(builder.finish())
    }
}

impl<O: OffsetSizeTrait> MapCoords for MultiPointArray<O> {
    type Output = Result<MultiPointArray<O>>;

    fn map_coords<F>(&self, map_op: F) -> Self::Output
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord + Sync,
    {
        let mut builder = MultiPointBuilder::with_capacity_and_options(
            self.buffer_lengths(),
            self.coord_type(),
            self.metadata(),
        );
        for maybe_geom in self.iter() {
            if let Some(geom) = maybe_geom {
                let result = geom.map_coords(&map_op);
                builder.push_multi_point(Some(&result))?;
            } else {
                builder.push_null()
            }
        }
        Ok(builder.finish())
    }
}

impl<O: OffsetSizeTrait> MapCoords for MultiLineStringArray<O> {
    type Output = Result<MultiLineStringArray<O>>;

    fn map_coords<F>(&self, map_op: F) -> Self::Output
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord + Sync,
    {
        let mut builder = MultiLineStringBuilder::with_capacity_and_options(
            self.buffer_lengths(),
            self.coord_type(),
            self.metadata(),
        );
        for maybe_geom in self.iter() {
            if let Some(geom) = maybe_geom {
                let result = geom.map_coords(&map_op);
                builder.push_multi_line_string(Some(&result))?;
            } else {
                builder.push_null()
            }
        }
        Ok(builder.finish())
    }
}

impl<O: OffsetSizeTrait> MapCoords for MultiPolygonArray<O> {
    type Output = Result<MultiPolygonArray<O>>;

    fn map_coords<F>(&self, map_op: F) -> Self::Output
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord + Sync,
    {
        let mut builder = MultiPolygonBuilder::with_capacity_and_options(
            self.buffer_lengths(),
            self.coord_type(),
            self.metadata(),
        );
        for maybe_geom in self.iter() {
            if let Some(geom) = maybe_geom {
                let result = geom.map_coords(&map_op)?;
                builder.push_multi_polygon(Some(&result))?;
            } else {
                builder.push_null()
            }
        }
        Ok(builder.finish())
    }
}

impl<O: OffsetSizeTrait> MapCoords for MixedGeometryArray<O> {
    type Output = Result<MixedGeometryArray<O>>;

    fn map_coords<F>(&self, map_op: F) -> Self::Output
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord + Sync,
    {
        let mut builder = MixedGeometryBuilder::with_capacity_and_options(
            self.buffer_lengths(),
            self.coord_type(),
            self.metadata(),
        );
        for maybe_geom in self.iter() {
            if let Some(geom) = maybe_geom {
                let result = geom.map_coords(&map_op)?;
                builder.push_geometry(Some(&result))?;
            } else {
                builder.push_null()
            }
        }
        Ok(builder.finish())
    }
}

impl<O: OffsetSizeTrait> MapCoords for GeometryCollectionArray<O> {
    type Output = Result<GeometryCollectionArray<O>>;

    fn map_coords<F>(&self, map_op: F) -> Self::Output
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord + Sync,
    {
        let mut builder = GeometryCollectionBuilder::with_capacity_and_options(
            self.buffer_lengths(),
            self.coord_type(),
            self.metadata(),
        );
        for maybe_geom in self.iter() {
            if let Some(geom) = maybe_geom {
                let result = geom.map_coords(&map_op)?;
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

    fn map_coords<F>(&self, map_op: F) -> Self::Output
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord + Sync,
    {
        let mut builder = RectBuilder::with_capacity(self.len(), self.metadata());
        for maybe_geom in self.iter() {
            if let Some(geom) = maybe_geom {
                let result = geom.map_coords(&map_op);
                builder.push_rect(Some(&result));
            } else {
                builder.push_null()
            }
        }
        builder.finish()
    }
}

impl MapCoords for &dyn GeometryArrayTrait {
    type Output = Result<Arc<dyn GeometryArrayTrait>>;

    fn map_coords<F>(&self, map_op: F) -> Self::Output
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord + Sync,
    {
        let result: Arc<dyn GeometryArrayTrait> = match self.data_type() {
            GeoDataType::Point(_) => Arc::new(self.as_point().map_coords(map_op)),
            GeoDataType::LineString(_) => Arc::new(self.as_line_string().map_coords(map_op)?),
            GeoDataType::LargeLineString(_) => {
                Arc::new(self.as_large_line_string().map_coords(map_op)?)
            }
            GeoDataType::Polygon(_) => Arc::new(self.as_polygon().map_coords(map_op)?),
            GeoDataType::LargePolygon(_) => Arc::new(self.as_large_polygon().map_coords(map_op)?),
            GeoDataType::MultiPoint(_) => Arc::new(self.as_multi_point().map_coords(map_op)?),
            GeoDataType::LargeMultiPoint(_) => {
                Arc::new(self.as_large_multi_point().map_coords(map_op)?)
            }
            GeoDataType::MultiLineString(_) => {
                Arc::new(self.as_multi_line_string().map_coords(map_op)?)
            }
            GeoDataType::LargeMultiLineString(_) => {
                Arc::new(self.as_large_multi_line_string().map_coords(map_op)?)
            }
            GeoDataType::MultiPolygon(_) => Arc::new(self.as_multi_polygon().map_coords(map_op)?),
            GeoDataType::LargeMultiPolygon(_) => {
                Arc::new(self.as_large_multi_polygon().map_coords(map_op)?)
            }
            GeoDataType::Mixed(_) => Arc::new(self.as_mixed().map_coords(map_op)?),
            GeoDataType::LargeMixed(_) => Arc::new(self.as_large_mixed().map_coords(map_op)?),
            GeoDataType::GeometryCollection(_) => {
                Arc::new(self.as_geometry_collection().map_coords(map_op)?)
            }
            GeoDataType::LargeGeometryCollection(_) => {
                Arc::new(self.as_large_geometry_collection().map_coords(map_op)?)
            }
            GeoDataType::Rect => Arc::new(self.as_rect().map_coords(map_op)),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl MapCoords for ChunkedPointArray {
    type Output = ChunkedPointArray;

    fn map_coords<F>(&self, map_op: F) -> Self::Output
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord + Sync,
    {
        ChunkedGeometryArray::new(self.map(|chunk| chunk.map_coords(&map_op)))
    }
}

impl<O: OffsetSizeTrait> MapCoords for ChunkedLineStringArray<O> {
    type Output = Result<ChunkedLineStringArray<O>>;

    fn map_coords<F>(&self, map_op: F) -> Self::Output
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord + Sync,
    {
        Ok(ChunkedGeometryArray::new(
            self.try_map(|chunk| chunk.map_coords(&map_op))?,
        ))
    }
}

impl<O: OffsetSizeTrait> MapCoords for ChunkedPolygonArray<O> {
    type Output = Result<ChunkedPolygonArray<O>>;

    fn map_coords<F>(&self, map_op: F) -> Self::Output
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord + Sync,
    {
        Ok(ChunkedGeometryArray::new(
            self.try_map(|chunk| chunk.map_coords(&map_op))?,
        ))
    }
}

impl<O: OffsetSizeTrait> MapCoords for ChunkedMultiPointArray<O> {
    type Output = Result<ChunkedMultiPointArray<O>>;

    fn map_coords<F>(&self, map_op: F) -> Self::Output
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord + Sync,
    {
        Ok(ChunkedGeometryArray::new(
            self.try_map(|chunk| chunk.map_coords(&map_op))?,
        ))
    }
}

impl<O: OffsetSizeTrait> MapCoords for ChunkedMultiLineStringArray<O> {
    type Output = Result<ChunkedMultiLineStringArray<O>>;

    fn map_coords<F>(&self, map_op: F) -> Self::Output
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord + Sync,
    {
        Ok(ChunkedGeometryArray::new(
            self.try_map(|chunk| chunk.map_coords(&map_op))?,
        ))
    }
}

impl<O: OffsetSizeTrait> MapCoords for ChunkedMultiPolygonArray<O> {
    type Output = Result<ChunkedMultiPolygonArray<O>>;

    fn map_coords<F>(&self, map_op: F) -> Self::Output
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord + Sync,
    {
        Ok(ChunkedGeometryArray::new(
            self.try_map(|chunk| chunk.map_coords(&map_op))?,
        ))
    }
}

impl<O: OffsetSizeTrait> MapCoords for ChunkedMixedGeometryArray<O> {
    type Output = Result<ChunkedMixedGeometryArray<O>>;

    fn map_coords<F>(&self, map_op: F) -> Self::Output
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord + Sync,
    {
        Ok(ChunkedGeometryArray::new(
            self.try_map(|chunk| chunk.map_coords(&map_op))?,
        ))
    }
}

impl<O: OffsetSizeTrait> MapCoords for ChunkedGeometryCollectionArray<O> {
    type Output = Result<ChunkedGeometryCollectionArray<O>>;

    fn map_coords<F>(&self, map_op: F) -> Self::Output
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord + Sync,
    {
        Ok(ChunkedGeometryArray::new(
            self.try_map(|chunk| chunk.map_coords(&map_op))?,
        ))
    }
}

impl MapCoords for ChunkedRectArray {
    type Output = ChunkedRectArray;

    fn map_coords<F>(&self, map_op: F) -> Self::Output
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord + Sync,
    {
        ChunkedGeometryArray::new(self.map(|chunk| chunk.map_coords(&map_op)))
    }
}

impl MapCoords for &dyn ChunkedGeometryArrayTrait {
    type Output = Result<Arc<dyn ChunkedGeometryArrayTrait>>;

    fn map_coords<F>(&self, map_op: F) -> Self::Output
    where
        F: Fn(&crate::scalar::Coord) -> geo::Coord + Sync,
    {
        let result: Arc<dyn ChunkedGeometryArrayTrait> = match self.data_type() {
            GeoDataType::Point(_) => Arc::new(self.as_point().map_coords(map_op)),
            GeoDataType::LineString(_) => Arc::new(self.as_line_string().map_coords(map_op)?),
            GeoDataType::LargeLineString(_) => {
                Arc::new(self.as_large_line_string().map_coords(map_op)?)
            }
            GeoDataType::Polygon(_) => Arc::new(self.as_polygon().map_coords(map_op)?),
            GeoDataType::LargePolygon(_) => Arc::new(self.as_large_polygon().map_coords(map_op)?),
            GeoDataType::MultiPoint(_) => Arc::new(self.as_multi_point().map_coords(map_op)?),
            GeoDataType::LargeMultiPoint(_) => {
                Arc::new(self.as_large_multi_point().map_coords(map_op)?)
            }
            GeoDataType::MultiLineString(_) => {
                Arc::new(self.as_multi_line_string().map_coords(map_op)?)
            }
            GeoDataType::LargeMultiLineString(_) => {
                Arc::new(self.as_large_multi_line_string().map_coords(map_op)?)
            }
            GeoDataType::MultiPolygon(_) => Arc::new(self.as_multi_polygon().map_coords(map_op)?),
            GeoDataType::LargeMultiPolygon(_) => {
                Arc::new(self.as_large_multi_polygon().map_coords(map_op)?)
            }
            GeoDataType::Mixed(_) => Arc::new(self.as_mixed().map_coords(map_op)?),
            GeoDataType::LargeMixed(_) => Arc::new(self.as_large_mixed().map_coords(map_op)?),
            GeoDataType::GeometryCollection(_) => {
                Arc::new(self.as_geometry_collection().map_coords(map_op)?)
            }
            GeoDataType::LargeGeometryCollection(_) => {
                Arc::new(self.as_large_geometry_collection().map_coords(map_op)?)
            }
            GeoDataType::Rect => Arc::new(self.as_rect().map_coords(map_op)),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}
