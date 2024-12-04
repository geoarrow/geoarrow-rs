use std::sync::Arc;

use crate::array::metadata::ArrayMetadata;
use crate::array::{CoordType, GeometryArray, GeometryBuilder};
use crate::datatypes::Dimension;
use crate::io::geozero::scalar::process_geometry;
use crate::trait_::{ArrayAccessor, GeometryArrayBuilder};
use crate::{ArrayBase, NativeArray};
use geozero::error::GeozeroError;
use geozero::{GeomProcessor, GeozeroGeometry};

impl GeozeroGeometry for GeometryArray {
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
pub trait ToGeometryArray {
    /// Convert to GeoArrow MixedArray
    fn to_geometry_array(&self) -> geozero::error::Result<GeometryArray>;

    /// Convert to a GeoArrow MixedArrayBuilder
    fn to_geometry_builder(&self) -> geozero::error::Result<GeometryBuilder>;
}

impl<T: GeozeroGeometry> ToGeometryArray for T {
    fn to_geometry_array(&self) -> geozero::error::Result<GeometryArray> {
        Ok(self.to_geometry_builder()?.into())
    }

    fn to_geometry_builder(&self) -> geozero::error::Result<GeometryBuilder> {
        let mut stream_builder = GeometryStreamBuilder::new();
        self.process_geom(&mut stream_builder)?;
        Ok(stream_builder.builder)
    }
}

/// The current geometry type in which we're pushing coordinates.
#[derive(Debug, Clone, Copy, PartialEq)]
enum GeometryType {
    Point(Dimension),
    LineString(Dimension),
    Polygon(Dimension),
    MultiPoint(Dimension),
    MultiLineString(Dimension),
    MultiPolygon(Dimension),
    // We don't currently support ingesting GeometryCollection through here
}

/// A streaming builder for GeoArrow MixedGeometryArray.
///
/// This will currently ignore any dimensions beyond XY.
///
/// This is useful in conjunction with [`geozero`] APIs because its coordinate stream requires the
/// consumer to keep track of which geometry type is currently being added to.
///
/// Converting an [`MixedGeometryStreamBuilder`] into a [`MixedGeometryArray`] is `O(1)`.
#[derive(Debug)]
pub struct GeometryStreamBuilder {
    builder: GeometryBuilder,
    // Note: we don't know if, when `linestring_end` is called, that means a ring of a polygon has
    // finished or if a tagged line string has finished. This means we can't have an "unknown" enum
    // type, because we'll never be able to set it to unknown after a line string is done, meaning
    // that we can't rely on it being unknown or not.
    current_geom_type: GeometryType,
}

impl GeometryStreamBuilder {
    pub fn new() -> Self {
        Self::new_with_options(Default::default(), Default::default(), true)
    }

    pub fn new_with_options(
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
        prefer_multi: bool,
    ) -> Self {
        Self {
            builder: GeometryBuilder::new_with_options(coord_type, metadata, prefer_multi),
            // A fake geometry type that will be overridden with the first incoming geometry
            current_geom_type: GeometryType::Point(Dimension::XY),
        }
    }

    #[allow(dead_code)]
    pub fn push_null(&mut self) {
        self.builder.push_null()
    }

    pub fn finish(self) -> GeometryArray {
        self.builder.finish()
    }
}

impl Default for GeometryStreamBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(unused_variables)]
impl GeomProcessor for GeometryStreamBuilder {
    fn xy(&mut self, x: f64, y: f64, idx: usize) -> geozero::error::Result<()> {
        fn check_dim(dim: Dimension) -> geozero::error::Result<()> {
            if !matches!(dim, Dimension::XY) {
                return Err(GeozeroError::Geometry(format!(
                    "Expected dimension to be XY when GeomProcessor::xy is called, got dim {:?}",
                    dim
                )));
            }

            Ok(())
        }

        match self.current_geom_type {
            GeometryType::Point(dim) => {
                check_dim(dim)?;
                if self.builder.prefer_multi {
                    self.builder.mpoint_xy.xy(x, y, idx)
                } else {
                    self.builder.point_xy.xy(x, y, idx)
                }
            }
            GeometryType::LineString(dim) => {
                check_dim(dim)?;
                if self.builder.prefer_multi {
                    self.builder.mline_string_xy.xy(x, y, idx)
                } else {
                    self.builder.line_string_xy.xy(x, y, idx)
                }
            }
            GeometryType::Polygon(dim) => {
                check_dim(dim)?;
                if self.builder.prefer_multi {
                    self.builder.mpolygon_xy.xy(x, y, idx)
                } else {
                    self.builder.polygon_xy.xy(x, y, idx)
                }
            }
            GeometryType::MultiPoint(dim) => {
                check_dim(dim)?;
                self.builder.mpoint_xy.xy(x, y, idx)
            }
            GeometryType::MultiLineString(dim) => {
                check_dim(dim)?;
                self.builder.mline_string_xy.xy(x, y, idx)
            }
            GeometryType::MultiPolygon(dim) => {
                check_dim(dim)?;
                self.builder.mpolygon_xy.xy(x, y, idx)
            }
        }
    }

    fn empty_point(&mut self, idx: usize) -> geozero::error::Result<()> {
        if self.builder.prefer_multi {
            self.builder.add_multi_point_type(Dimension::XY);
            self.builder
                .mpoint_xy
                .push_point(None::<&geo::Point<f64>>)
                .unwrap();
        } else {
            self.builder.add_point_type(Dimension::XY);
            self.builder.point_xy.push_empty();
        }
        Ok(())
    }

    /// NOTE: It appears that point_begin is **only** called for "tagged" `Point` geometries. I.e.
    /// a coord of another geometry never has `point_begin` called. Each point of a multi point
    /// does not have `point_begin` called.
    fn point_begin(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.current_geom_type = GeometryType::Point(Dimension::XY);
        if self.builder.prefer_multi {
            self.builder.add_multi_point_type(Dimension::XY);
            self.builder.mpoint_xy.point_begin(idx)?;
        } else {
            self.builder.add_point_type(Dimension::XY);
            self.builder.point_xy.point_begin(idx)?;
        }
        Ok(())
    }

    fn multipoint_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.current_geom_type = GeometryType::MultiPoint(Dimension::XY);
        self.builder.add_multi_point_type(Dimension::XY);
        self.builder.mpoint_xy.multipoint_begin(size, idx)
    }

    fn linestring_begin(
        &mut self,
        tagged: bool,
        size: usize,
        idx: usize,
    ) -> geozero::error::Result<()> {
        if tagged {
            self.current_geom_type = GeometryType::LineString(Dimension::XY);
            if self.builder.prefer_multi {
                self.builder.add_multi_line_string_type(Dimension::XY);
            } else {
                self.builder.add_line_string_type(Dimension::XY);
            }
        };

        match self.current_geom_type {
            GeometryType::LineString(_) => {
                if self.builder.prefer_multi {
                    self.builder
                        .mline_string_xy
                        .linestring_begin(tagged, size, idx)
                } else {
                    self.builder
                        .line_string_xy
                        .linestring_begin(tagged, size, idx)
                }
            }
            GeometryType::MultiLineString(_) => self
                .builder
                .mline_string_xy
                .linestring_begin(tagged, size, idx),
            GeometryType::Polygon(_) => {
                if self.builder.prefer_multi {
                    self.builder.mpolygon_xy.linestring_begin(tagged, size, idx)
                } else {
                    self.builder.polygon_xy.linestring_begin(tagged, size, idx)
                }
            }
            GeometryType::MultiPolygon(_) => {
                self.builder.mpolygon_xy.linestring_begin(tagged, size, idx)
            }
            _ => panic!(
                "unexpected linestring_begin for {:?}",
                self.current_geom_type
            ),
        }
    }

    fn multilinestring_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.current_geom_type = GeometryType::MultiLineString(Dimension::XY);
        self.builder.add_multi_line_string_type(Dimension::XY);
        self.builder
            .mline_string_xy
            .multilinestring_begin(size, idx)
    }

    fn polygon_begin(
        &mut self,
        tagged: bool,
        size: usize,
        idx: usize,
    ) -> geozero::error::Result<()> {
        if tagged {
            self.current_geom_type = GeometryType::Polygon(Dimension::XY);
            if self.builder.prefer_multi {
                self.builder.add_multi_polygon_type(Dimension::XY);
            } else {
                self.builder.add_polygon_type(Dimension::XY);
            }
        };

        match self.current_geom_type {
            GeometryType::Polygon(_) => {
                if self.builder.prefer_multi {
                    self.builder.mpolygon_xy.polygon_begin(tagged, size, idx)
                } else {
                    self.builder.polygon_xy.polygon_begin(tagged, size, idx)
                }
            }
            GeometryType::MultiPolygon(_) => {
                self.builder.mpolygon_xy.polygon_begin(tagged, size, idx)
            }
            _ => panic!("unexpected polygon_begin for {:?}", self.current_geom_type),
        }
    }

    fn multipolygon_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.current_geom_type = GeometryType::MultiPolygon(Dimension::XY);
        self.builder.add_multi_polygon_type(Dimension::XY);
        self.builder.mpolygon_xy.multipolygon_begin(size, idx)
    }
}

impl GeometryArrayBuilder for GeometryStreamBuilder {
    fn len(&self) -> usize {
        self.builder.len()
    }

    fn nulls(&self) -> &arrow_buffer::NullBufferBuilder {
        // Take this method off trait
        todo!()
    }

    fn push_geometry(
        &mut self,
        _value: Option<&impl geo_traits::GeometryTrait<T = f64>>,
    ) -> crate::error::Result<()> {
        todo!()
    }

    fn new(dim: Dimension) -> Self {
        Self::with_geom_capacity_and_options(dim, 0, Default::default(), Default::default())
    }

    fn into_array_ref(self) -> Arc<dyn arrow_array::Array> {
        self.builder.into_array_ref()
    }

    fn with_geom_capacity_and_options(
        _dim: Dimension,
        _geom_capacity: usize,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        Self::new_with_options(coord_type, metadata, true)
    }

    fn set_metadata(&mut self, metadata: Arc<ArrayMetadata>) {
        self.builder.set_metadata(metadata)
    }

    fn finish(self) -> std::sync::Arc<dyn NativeArray> {
        Arc::new(self.finish())
    }

    fn coord_type(&self) -> CoordType {
        self.builder.coord_type()
    }

    fn metadata(&self) -> Arc<ArrayMetadata> {
        self.builder.metadata()
    }
}
