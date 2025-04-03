use std::fmt::Debug;
use std::sync::Arc;

use geozero::error::GeozeroError;
use geozero::geo_types::GeoWriter;
use geozero::{GeomProcessor, GeozeroGeometry};

use crate::array::metadata::ArrayMetadata;
use crate::array::{CoordType, GeometryArray, GeometryBuilder};
use geoarrow_schema::Dimension;
use crate::trait_::GeometryArrayBuilder;
use crate::NativeArray;

/// GeoZero trait to convert to GeoArrow [`GeometryArray`].
pub trait ToGeometryArray {
    /// Convert to GeoArrow [`GeometryArray`]
    fn to_geometry_array(&self) -> geozero::error::Result<GeometryArray> {
        Ok(self.to_geometry_builder()?.into())
    }

    /// Convert to a GeoArrow [`GeometryBuilder`]
    fn to_geometry_builder(&self) -> geozero::error::Result<GeometryBuilder>;
}

impl<T: GeozeroGeometry> ToGeometryArray for T {
    fn to_geometry_builder(&self) -> geozero::error::Result<GeometryBuilder> {
        let mut stream_builder = GeometryStreamBuilder::new();
        self.process_geom(&mut stream_builder)?;
        Ok(stream_builder.builder)
    }
}

/// A streaming builder for GeoArrow [`GeometryArray`].
///
/// This is useful in conjunction with [`geozero`] APIs because its coordinate stream requires the
/// consumer to keep track of which geometry type is currently being added to.
///
/// Converting an [`GeometryStreamBuilder`] into a [`GeometryArray`] is `O(1)`.
// #[derive(Debug)]
pub struct GeometryStreamBuilder {
    builder: GeometryBuilder,
    current_geometry: GeoWriter,
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
            current_geometry: GeoWriter::new(),
        }
    }

    #[allow(dead_code)]
    pub fn push_null(&mut self) {
        self.builder.push_null()
    }

    pub fn finish(self) -> GeometryArray {
        self.builder.finish()
    }

    fn push_current_geometry(&mut self) -> geozero::error::Result<()> {
        let geom = self
            .current_geometry
            .take_geometry()
            .ok_or(GeozeroError::Geometry("Take geometry failed".to_string()))?;
        self.builder
            .push_geometry(Some(&geom))
            .map_err(|err| GeozeroError::Geometry(err.to_string()))?;
        self.current_geometry = GeoWriter::new();
        Ok(())
    }
}

impl Default for GeometryStreamBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl Debug for GeometryStreamBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GeometryStreamBuilder")
    }
}

#[allow(unused_variables)]
impl GeomProcessor for GeometryStreamBuilder {
    fn xy(&mut self, x: f64, y: f64, idx: usize) -> geozero::error::Result<()> {
        self.current_geometry.xy(x, y, idx)
    }

    fn empty_point(&mut self, idx: usize) -> geozero::error::Result<()> {
        // This needs to be separate because GeoWriter doesn't know how to handle empty points
        Err(GeozeroError::Geometry(
            "Empty points not currently supported in generic geometry builder.".to_string(),
        ))
        // if self.builder.prefer_multi {
        //     self.builder.add_multi_point_type();
        //     self.builder
        //         .multi_points
        //         .push_point(None::<&geo::Point<f64>>)
        //         .unwrap();
        // } else {
        //     self.builder.add_point_type();
        //     self.builder.points.push_empty();
        // }
        // Ok(())
    }

    fn point_begin(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.current_geometry.point_begin(idx)
    }

    fn point_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.current_geometry.point_end(idx)?;
        self.push_current_geometry()
    }

    fn multipoint_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.current_geometry.multipoint_begin(size, idx)
    }

    fn multipoint_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.current_geometry.multipoint_end(idx)?;
        self.push_current_geometry()
    }

    fn linestring_begin(
        &mut self,
        tagged: bool,
        size: usize,
        idx: usize,
    ) -> geozero::error::Result<()> {
        self.current_geometry.linestring_begin(tagged, size, idx)
    }

    fn linestring_end(&mut self, tagged: bool, idx: usize) -> geozero::error::Result<()> {
        self.current_geometry.linestring_end(tagged, idx)?;
        self.push_current_geometry()
    }

    fn multilinestring_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.current_geometry.multilinestring_begin(size, idx)
    }

    fn multilinestring_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.current_geometry.multilinestring_end(idx)?;
        self.push_current_geometry()
    }

    fn polygon_begin(
        &mut self,
        tagged: bool,
        size: usize,
        idx: usize,
    ) -> geozero::error::Result<()> {
        self.current_geometry.polygon_begin(tagged, size, idx)
    }

    fn polygon_end(&mut self, tagged: bool, idx: usize) -> geozero::error::Result<()> {
        self.current_geometry.polygon_end(tagged, idx)?;
        self.push_current_geometry()
    }

    fn multipolygon_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.current_geometry.multipolygon_begin(size, idx)
    }

    fn multipolygon_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.current_geometry.multipolygon_end(idx)?;
        self.push_current_geometry()
    }

    fn geometrycollection_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.current_geometry.geometrycollection_begin(size, idx)
    }

    fn geometrycollection_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.current_geometry.geometrycollection_end(idx)?;
        self.push_current_geometry()
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
        value: Option<&impl geo_traits::GeometryTrait<T = f64>>,
    ) -> crate::error::Result<()> {
        self.builder.push_geometry(value)
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
