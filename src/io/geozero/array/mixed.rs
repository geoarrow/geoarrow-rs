use std::sync::Arc;

use crate::array::metadata::ArrayMetadata;
use crate::array::mixed::array::GeometryType;
use crate::array::{CoordType, MixedGeometryArray, MixedGeometryBuilder};
use crate::io::geozero::scalar::process_geometry;
use crate::trait_::{GeometryArrayAccessor, GeometryArrayBuilder};
use crate::GeometryArrayTrait;
use arrow_array::OffsetSizeTrait;
use geozero::{GeomProcessor, GeozeroGeometry};

impl<O: OffsetSizeTrait> GeozeroGeometry for MixedGeometryArray<O> {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        let num_geometries = self.len();
        processor.geometrycollection_begin(num_geometries, 0)?;

        for geom_idx in 0..num_geometries {
            process_geometry(&self.value(geom_idx), geom_idx, processor)?;
        }

        processor.geometrycollection_end(num_geometries - 1)?;
        Ok(())
    }
}

// TODO: Add "promote to multi" here
/// GeoZero trait to convert to GeoArrow MixedArray.
pub trait ToMixedArray<O: OffsetSizeTrait> {
    /// Convert to GeoArrow MixedArray
    fn to_mixed_geometry_array(&self) -> geozero::error::Result<MixedGeometryArray<O>>;

    /// Convert to a GeoArrow MixedArrayBuilder
    fn to_mixed_geometry_builder(&self) -> geozero::error::Result<MixedGeometryBuilder<O>>;
}

impl<T: GeozeroGeometry, O: OffsetSizeTrait> ToMixedArray<O> for T {
    fn to_mixed_geometry_array(&self) -> geozero::error::Result<MixedGeometryArray<O>> {
        Ok(self.to_mixed_geometry_builder()?.into())
    }

    fn to_mixed_geometry_builder(&self) -> geozero::error::Result<MixedGeometryBuilder<O>> {
        let mut stream_builder = MixedGeometryStreamBuilder::new();
        self.process_geom(&mut stream_builder)?;
        Ok(stream_builder.builder)
    }
}

/// A streaming builder for GeoArrow MixedGeometryArray.
///
/// This is useful in conjunction with [`geozero`] APIs because its coordinate stream requires the
/// consumer to keep track of which geometry type is currently being added to.
///
/// Converting an [`MixedGeometryStreamBuilder`] into a [`MixedGeometryArray`] is `O(1)`.
#[derive(Debug)]
pub struct MixedGeometryStreamBuilder<O: OffsetSizeTrait> {
    builder: MixedGeometryBuilder<O>,
    // Note: we don't know if, when `linestring_end` is called, that means a ring of a polygon has
    // finished or if a tagged line string has finished. This means we can't have an "unknown" enum
    // type, because we'll never be able to set it to unknown after a line string is done, meaning
    // that we can't rely on it being unknown or not.
    current_geom_type: GeometryType,
    /// Always add multi-geometries to make it easier to downcast later.
    prefer_multi: bool,
}

impl<O: OffsetSizeTrait> MixedGeometryStreamBuilder<O> {
    pub fn new() -> Self {
        Self {
            builder: MixedGeometryBuilder::<O>::new(),
            current_geom_type: GeometryType::Point,
            prefer_multi: true,
        }
    }

    pub fn new_with_options(
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
        prefer_multi: bool,
    ) -> Self {
        Self {
            builder: MixedGeometryBuilder::<O>::new_with_options(coord_type, metadata),
            current_geom_type: GeometryType::Point,
            prefer_multi,
        }
    }

    pub fn push_null(&mut self) {
        self.builder.push_null()
    }

    pub fn finish(self) -> MixedGeometryArray<O> {
        self.builder.finish()
    }
}

impl<O: OffsetSizeTrait> Default for MixedGeometryStreamBuilder<O> {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(unused_variables)]
impl<O: OffsetSizeTrait> GeomProcessor for MixedGeometryStreamBuilder<O> {
    fn xy(&mut self, x: f64, y: f64, idx: usize) -> geozero::error::Result<()> {
        match self.current_geom_type {
            GeometryType::Point => {
                if self.prefer_multi {
                    self.builder.multi_points.xy(x, y, idx)
                } else {
                    self.builder.points.xy(x, y, idx)
                }
            }
            GeometryType::LineString => {
                if self.prefer_multi {
                    self.builder.multi_line_strings.xy(x, y, idx)
                } else {
                    self.builder.line_strings.xy(x, y, idx)
                }
            }
            GeometryType::Polygon => {
                if self.prefer_multi {
                    self.builder.multi_polygons.xy(x, y, idx)
                } else {
                    self.builder.polygons.xy(x, y, idx)
                }
            }
            GeometryType::MultiPoint => self.builder.multi_points.xy(x, y, idx),
            GeometryType::MultiLineString => self.builder.multi_line_strings.xy(x, y, idx),
            GeometryType::MultiPolygon => self.builder.multi_polygons.xy(x, y, idx),
            GeometryType::GeometryCollection => todo!(),
        }
    }

    fn empty_point(&mut self, idx: usize) -> geozero::error::Result<()> {
        if self.prefer_multi {
            self.builder.add_multi_point_type();
            self.builder
                .multi_points
                .push_point(None::<&geo::Point<f64>>)
                .unwrap();
        } else {
            self.builder.add_point_type();
            self.builder.points.push_empty();
        }
        Ok(())
    }

    /// NOTE: It appears that point_begin is **only** called for "tagged" `Point` geometries. I.e.
    /// a coord of another geometry never has `point_begin` called. Each point of a multi point
    /// does not have `point_begin` called.
    fn point_begin(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.current_geom_type = GeometryType::Point;
        if self.prefer_multi {
            self.builder.add_multi_point_type();
            self.builder.multi_points.point_begin(idx)?;
        } else {
            self.builder.add_point_type();
            self.builder.points.point_begin(idx)?;
        }
        Ok(())
    }

    fn multipoint_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.current_geom_type = GeometryType::MultiPoint;
        self.builder.add_multi_point_type();
        self.builder.multi_points.multipoint_begin(size, idx)
    }

    fn linestring_begin(
        &mut self,
        tagged: bool,
        size: usize,
        idx: usize,
    ) -> geozero::error::Result<()> {
        if tagged {
            self.current_geom_type = GeometryType::LineString;
            if self.prefer_multi {
                self.builder.add_multi_line_string_type();
            } else {
                self.builder.add_line_string_type();
            }
        };

        match self.current_geom_type {
            GeometryType::LineString => {
                if self.prefer_multi {
                    self.builder
                        .multi_line_strings
                        .linestring_begin(tagged, size, idx)
                } else {
                    self.builder
                        .line_strings
                        .linestring_begin(tagged, size, idx)
                }
            }
            GeometryType::MultiLineString => self
                .builder
                .multi_line_strings
                .linestring_begin(tagged, size, idx),
            GeometryType::Polygon => {
                if self.prefer_multi {
                    self.builder
                        .multi_polygons
                        .linestring_begin(tagged, size, idx)
                } else {
                    self.builder.polygons.linestring_begin(tagged, size, idx)
                }
            }
            GeometryType::MultiPolygon => self
                .builder
                .multi_polygons
                .linestring_begin(tagged, size, idx),
            _ => panic!(
                "unexpected linestring_begin for {:?}",
                self.current_geom_type
            ),
        }
    }

    fn multilinestring_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.current_geom_type = GeometryType::MultiLineString;
        self.builder.add_multi_line_string_type();
        self.builder
            .multi_line_strings
            .multilinestring_begin(size, idx)
    }

    fn polygon_begin(
        &mut self,
        tagged: bool,
        size: usize,
        idx: usize,
    ) -> geozero::error::Result<()> {
        if tagged {
            self.current_geom_type = GeometryType::Polygon;
            if self.prefer_multi {
                self.builder.add_multi_polygon_type();
            } else {
                self.builder.add_polygon_type();
            }
        };

        match self.current_geom_type {
            GeometryType::Polygon => {
                if self.prefer_multi {
                    self.builder.multi_polygons.polygon_begin(tagged, size, idx)
                } else {
                    self.builder.polygons.polygon_begin(tagged, size, idx)
                }
            }
            GeometryType::MultiPolygon => {
                self.builder.multi_polygons.polygon_begin(tagged, size, idx)
            }
            _ => panic!("unexpected polygon_begin for {:?}", self.current_geom_type),
        }
    }

    fn multipolygon_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.current_geom_type = GeometryType::MultiPolygon;
        self.builder.add_multi_polygon_type();
        self.builder.multi_polygons.multipolygon_begin(size, idx)
    }
}

impl<O: OffsetSizeTrait> GeometryArrayBuilder for MixedGeometryStreamBuilder<O> {
    fn len(&self) -> usize {
        self.builder.len()
    }

    fn validity(&self) -> &arrow_buffer::NullBufferBuilder {
        // Take this method off trait
        todo!()
    }

    fn new() -> Self {
        Self::with_geom_capacity_and_options(0, Default::default(), Default::default())
    }

    fn into_array_ref(self) -> Arc<dyn arrow_array::Array> {
        self.builder.into_array_ref()
    }

    fn with_geom_capacity_and_options(
        _geom_capacity: usize,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        Self::new_with_options(coord_type, metadata, true)
    }

    fn set_metadata(&mut self, metadata: Arc<ArrayMetadata>) {
        self.builder.set_metadata(metadata)
    }

    fn finish(self) -> std::sync::Arc<dyn GeometryArrayTrait> {
        Arc::new(self.finish())
    }

    fn coord_type(&self) -> CoordType {
        self.builder.coord_type()
    }

    fn metadata(&self) -> Arc<ArrayMetadata> {
        self.builder.metadata()
    }
}
