use crate::array::mixed::array::GeometryType;
use crate::array::{MixedGeometryArray, MixedGeometryBuilder};
use crate::io::geozero::scalar::geometry::process_geometry;
use crate::trait_::GeometryArrayAccessor;
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
    fn to_mutable_mixed_geometry_array(&self) -> geozero::error::Result<MixedGeometryBuilder<O>>;
}

impl<T: GeozeroGeometry, O: OffsetSizeTrait> ToMixedArray<O> for T {
    fn to_mixed_geometry_array(&self) -> geozero::error::Result<MixedGeometryArray<O>> {
        Ok(self.to_mutable_mixed_geometry_array()?.into())
    }

    fn to_mutable_mixed_geometry_array(&self) -> geozero::error::Result<MixedGeometryBuilder<O>> {
        let mut stream_builder = MixedGeometryStreamBuilder::new();
        self.process_geom(&mut stream_builder)?;
        Ok(stream_builder.builder)
    }
}

struct MixedGeometryStreamBuilder<O: OffsetSizeTrait> {
    builder: MixedGeometryBuilder<O>,
    // Note: we don't know if, when `linestring_end` is called, that means a ring of a polygon has
    // finished or if a tagged line string has finished. This means we can't have an "unknown" enum
    // type, because we'll never be able to set it to unknown after a line string is done, meaning
    // that we can't rely on it being unknown or not.
    current_geom_type: GeometryType,
}

impl<O: OffsetSizeTrait> MixedGeometryStreamBuilder<O> {
    fn new() -> Self {
        Self {
            builder: MixedGeometryBuilder::<O>::new(),
            current_geom_type: GeometryType::Point,
        }
    }
}

#[allow(unused_variables)]
impl<O: OffsetSizeTrait> GeomProcessor for MixedGeometryStreamBuilder<O> {
    fn xy(&mut self, x: f64, y: f64, idx: usize) -> geozero::error::Result<()> {
        match self.current_geom_type {
            GeometryType::Point => self.builder.points.xy(x, y, idx),
            GeometryType::LineString => self.builder.line_strings.xy(x, y, idx),
            GeometryType::Polygon => self.builder.polygons.xy(x, y, idx),
            GeometryType::MultiPoint => self.builder.multi_points.xy(x, y, idx),
            GeometryType::MultiLineString => self.builder.multi_line_strings.xy(x, y, idx),
            GeometryType::MultiPolygon => self.builder.multi_polygons.xy(x, y, idx),
        }
    }

    fn empty_point(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.builder.add_point_type();
        self.builder.points.push_empty();
        Ok(())
    }

    /// NOTE: It appears that point_begin is **only** called for "tagged" `Point` geometries. I.e.
    /// a coord of another geometry never has `point_begin` called. Each point of a multi point
    /// does not have `point_begin` called.
    fn point_begin(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.current_geom_type = GeometryType::Point;
        self.builder.add_point_type();
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
            self.builder.add_line_string_type();
        };

        match self.current_geom_type {
            GeometryType::LineString => self
                .builder
                .line_strings
                .linestring_begin(tagged, size, idx),
            GeometryType::MultiLineString => self
                .builder
                .multi_line_strings
                .linestring_begin(tagged, size, idx),
            GeometryType::Polygon => self.builder.polygons.linestring_begin(tagged, size, idx),
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
            self.builder.add_polygon_type();
        };

        match self.current_geom_type {
            GeometryType::Polygon => self.builder.polygons.polygon_begin(tagged, size, idx),
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
