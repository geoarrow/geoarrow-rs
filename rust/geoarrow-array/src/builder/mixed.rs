use std::sync::Arc;

use arrow_array::OffsetSizeTrait;
use geo_traits::*;
use geoarrow_schema::{
    CoordType, Dimension, LineStringType, Metadata, MultiLineStringType, MultiPointType,
    MultiPolygonType, PointType, PolygonType,
};

use crate::array::{MixedGeometryArray, WkbArray};
use crate::builder::{
    LineStringBuilder, MultiLineStringBuilder, MultiPointBuilder, MultiPolygonBuilder,
    PointBuilder, PolygonBuilder,
};
use crate::capacity::MixedCapacity;
use crate::error::{GeoArrowError, Result};
use crate::trait_::{ArrayAccessor, GeoArrowArray, GeometryArrayBuilder};

pub(crate) const DEFAULT_PREFER_MULTI: bool = false;

/// The GeoArrow equivalent to a `Vec<Option<Geometry>>`: a mutable collection of Geometries, all
/// of which have the same dimension.
///
/// This currently has the caveat that these geometries must be a _primitive_ geometry type. This
/// does not currently support nested GeometryCollection objects.
///
/// Converting an [`MixedGeometryBuilder`] into a [`MixedGeometryArray`] is `O(1)`.
///
/// # Invariants
///
/// - All arrays must have the same dimension
/// - All arrays must have the same coordinate layout (interleaved or separated)
#[derive(Debug)]
pub(crate) struct MixedGeometryBuilder {
    metadata: Arc<Metadata>,

    /// The dimension of this builder.
    ///
    /// All underlying arrays must contain a coordinate buffer of this same dimension.
    dim: Dimension,

    // Invariant: every item in `types` is `> 0 && < fields.len()`
    types: Vec<i8>,

    pub(crate) points: PointBuilder,
    pub(crate) line_strings: LineStringBuilder,
    pub(crate) polygons: PolygonBuilder,
    pub(crate) multi_points: MultiPointBuilder,
    pub(crate) multi_line_strings: MultiLineStringBuilder,
    pub(crate) multi_polygons: MultiPolygonBuilder,

    // Invariant: `offsets.len() == types.len()`
    offsets: Vec<i32>,

    /// Whether to prefer multi or single arrays for new geometries.
    ///
    /// E.g. if this is `true` and a Point geometry is added, it will be added to the
    /// MultiPointBuilder. If this is `false`, the Point geometry will be added to the
    /// PointBuilder.
    ///
    /// The idea is that always adding multi-geometries will make it easier to downcast later.
    pub(crate) prefer_multi: bool,
}

impl<'a> MixedGeometryBuilder {
    pub(crate) fn with_capacity_and_options(
        dim: Dimension,
        capacity: MixedCapacity,
        coord_type: CoordType,
        metadata: Arc<Metadata>,
        prefer_multi: bool,
    ) -> Self {
        // Don't store array metadata on child arrays
        Self {
            metadata,
            dim,
            types: vec![],
            points: PointBuilder::with_capacity(
                PointType::new(coord_type, dim, Default::default()),
                capacity.point,
            ),
            line_strings: LineStringBuilder::with_capacity(
                LineStringType::new(coord_type, dim, Default::default()),
                capacity.line_string,
            ),
            polygons: PolygonBuilder::with_capacity(
                PolygonType::new(coord_type, dim, Default::default()),
                capacity.polygon,
            ),
            multi_points: MultiPointBuilder::with_capacity(
                MultiPointType::new(coord_type, dim, Default::default()),
                capacity.multi_point,
            ),
            multi_line_strings: MultiLineStringBuilder::with_capacity(
                MultiLineStringType::new(coord_type, dim, Default::default()),
                capacity.multi_line_string,
            ),
            multi_polygons: MultiPolygonBuilder::with_capacity(
                MultiPolygonType::new(coord_type, dim, Default::default()),
                capacity.multi_polygon,
            ),
            offsets: vec![],
            prefer_multi,
        }
    }

    pub(crate) fn reserve(&mut self, capacity: MixedCapacity) {
        let total_num_geoms = capacity.total_num_geoms();
        self.types.reserve(total_num_geoms);
        self.offsets.reserve(total_num_geoms);
        self.points.reserve(capacity.point);
        self.line_strings.reserve(capacity.line_string);
        self.polygons.reserve(capacity.polygon);
        self.multi_points.reserve(capacity.multi_point);
        self.multi_line_strings.reserve(capacity.multi_line_string);
        self.multi_polygons.reserve(capacity.multi_polygon);
    }

    pub(crate) fn reserve_exact(&mut self, capacity: MixedCapacity) {
        let total_num_geoms = capacity.total_num_geoms();
        self.types.reserve_exact(total_num_geoms);
        self.offsets.reserve_exact(total_num_geoms);
        self.points.reserve_exact(capacity.point);
        self.line_strings.reserve_exact(capacity.line_string);
        self.polygons.reserve_exact(capacity.polygon);
        self.multi_points.reserve_exact(capacity.multi_point);
        self.multi_line_strings
            .reserve_exact(capacity.multi_line_string);
        self.multi_polygons.reserve_exact(capacity.multi_polygon);
    }

    // /// The canonical method to create a [`MixedGeometryBuilder`] out of its internal
    // /// components.
    // ///
    // /// # Implementation
    // ///
    // /// This function is `O(1)`.
    // ///
    // /// # Errors
    // ///
    // pub(crate) fn try_new(
    //     coords: CoordBufferBuilder,
    //     geom_offsets: BufferBuilder<O>,
    //     ring_offsets: BufferBuilder<O>,
    //     validity: Option<MutableBitmap>,
    // ) -> Result<Self> {
    //     check(
    //         &coords.clone().into(),
    //         &geom_offsets.clone().into(),
    //         &ring_offsets.clone().into(),
    //         validity.as_ref().map(|x| x.len()),
    //     )?;
    //     Ok(Self {
    //         coords,
    //         geom_offsets,
    //         ring_offsets,
    //         validity,
    //     })
    // }

    pub(crate) fn finish(self) -> MixedGeometryArray {
        MixedGeometryArray::new(
            self.types.into(),
            self.offsets.into(),
            Some(self.points.finish()),
            Some(self.line_strings.finish()),
            Some(self.polygons.finish()),
            Some(self.multi_points.finish()),
            Some(self.multi_line_strings.finish()),
            Some(self.multi_polygons.finish()),
            self.metadata,
        )
    }

    pub(crate) fn with_capacity_and_options_from_iter(
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait + 'a)>>,
        dim: Dimension,
        coord_type: CoordType,
        metadata: Arc<Metadata>,
        prefer_multi: bool,
    ) -> Result<Self> {
        let counter = MixedCapacity::from_geometries(geoms)?;
        Ok(Self::with_capacity_and_options(
            dim,
            counter,
            coord_type,
            metadata,
            prefer_multi,
        ))
    }

    #[allow(dead_code)]
    pub(crate) fn reserve_from_iter(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait + 'a)>>,
    ) -> Result<()> {
        let counter = MixedCapacity::from_geometries(geoms)?;
        self.reserve(counter);
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn reserve_exact_from_iter(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait + 'a)>>,
    ) -> Result<()> {
        let counter = MixedCapacity::from_geometries(geoms)?;
        self.reserve_exact(counter);
        Ok(())
    }

    /// Add a new Point to the end of this array.
    ///
    /// If `self.prefer_multi` is `true`, it will be stored in the `MultiPointBuilder` child
    /// array. Otherwise, it will be stored in the `PointBuilder` child array.
    #[inline]
    pub(crate) fn push_point(&mut self, value: Option<&impl PointTrait<T = f64>>) -> Result<()> {
        if self.prefer_multi {
            self.add_multi_point_type();
            self.multi_points.push_point(value)
        } else {
            self.add_point_type();
            self.points.push_point(value);
            Ok(())
        }
    }

    #[inline]
    fn add_point_type(&mut self) {
        self.offsets.push(self.points.len().try_into().unwrap());
        match self.dim {
            Dimension::XY => self.types.push(1),
            Dimension::XYZ => self.types.push(11),
            Dimension::XYM => self.types.push(21),
            Dimension::XYZM => self.types.push(31),
        }
    }

    /// Add a new LineString to the end of this array.
    ///
    /// If `self.prefer_multi` is `true`, it will be stored in the `MultiLineStringBuilder` child
    /// array. Otherwise, it will be stored in the `LineStringBuilder` child array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    pub(crate) fn push_line_string(
        &mut self,
        value: Option<&impl LineStringTrait<T = f64>>,
    ) -> Result<()> {
        if self.prefer_multi {
            self.add_multi_line_string_type();
            self.multi_line_strings.push_line_string(value)
        } else {
            self.add_line_string_type();
            self.line_strings.push_line_string(value)
        }
    }

    #[inline]
    fn add_line_string_type(&mut self) {
        self.offsets
            .push(self.line_strings.len().try_into().unwrap());
        match self.dim {
            Dimension::XY => self.types.push(2),
            Dimension::XYZ => self.types.push(12),
            Dimension::XYM => self.types.push(22),
            Dimension::XYZM => self.types.push(32),
        }
    }

    /// Add a new Polygon to the end of this array.
    ///
    /// If `self.prefer_multi` is `true`, it will be stored in the `MultiPolygonBuilder` child
    /// array. Otherwise, it will be stored in the `PolygonBuilder` child array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    pub(crate) fn push_polygon(
        &mut self,
        value: Option<&impl PolygonTrait<T = f64>>,
    ) -> Result<()> {
        if self.prefer_multi {
            self.add_multi_polygon_type();
            self.multi_polygons.push_polygon(value)
        } else {
            self.add_polygon_type();
            self.polygons.push_polygon(value)
        }
    }

    #[inline]
    fn add_polygon_type(&mut self) {
        self.offsets.push(self.polygons.len().try_into().unwrap());
        match self.dim {
            Dimension::XY => self.types.push(3),
            Dimension::XYZ => self.types.push(13),
            Dimension::XYM => self.types.push(23),
            Dimension::XYZM => self.types.push(33),
        }
    }

    /// Add a new MultiPoint to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    pub(crate) fn push_multi_point(
        &mut self,
        value: Option<&impl MultiPointTrait<T = f64>>,
    ) -> Result<()> {
        self.add_multi_point_type();
        self.multi_points.push_multi_point(value)
    }

    #[inline]
    fn add_multi_point_type(&mut self) {
        self.offsets
            .push(self.multi_points.len().try_into().unwrap());
        match self.dim {
            Dimension::XY => self.types.push(4),
            Dimension::XYZ => self.types.push(14),
            Dimension::XYM => self.types.push(24),
            Dimension::XYZM => self.types.push(34),
        }
    }

    /// Add a new MultiLineString to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    pub(crate) fn push_multi_line_string(
        &mut self,
        value: Option<&impl MultiLineStringTrait<T = f64>>,
    ) -> Result<()> {
        self.add_multi_line_string_type();
        self.multi_line_strings.push_multi_line_string(value)
    }

    #[inline]
    fn add_multi_line_string_type(&mut self) {
        self.offsets
            .push(self.multi_line_strings.len().try_into().unwrap());
        match self.dim {
            Dimension::XY => self.types.push(5),
            Dimension::XYZ => self.types.push(15),
            Dimension::XYM => self.types.push(25),
            Dimension::XYZM => self.types.push(35),
        }
    }

    /// Add a new MultiPolygon to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    pub(crate) fn push_multi_polygon(
        &mut self,
        value: Option<&impl MultiPolygonTrait<T = f64>>,
    ) -> Result<()> {
        self.add_multi_polygon_type();
        self.multi_polygons.push_multi_polygon(value)
    }

    #[inline]
    fn add_multi_polygon_type(&mut self) {
        self.offsets
            .push(self.multi_polygons.len().try_into().unwrap());
        match self.dim {
            Dimension::XY => self.types.push(6),
            Dimension::XYZ => self.types.push(16),
            Dimension::XYM => self.types.push(26),
            Dimension::XYZM => self.types.push(36),
        }
    }

    #[inline]
    pub(crate) fn push_geometry(
        &mut self,
        value: Option<&'a impl GeometryTrait<T = f64>>,
    ) -> Result<()> {
        use geo_traits::GeometryType::*;

        if let Some(geom) = value {
            match geom.as_type() {
                Point(g) => {
                    self.push_point(Some(g))?;
                }
                LineString(g) => {
                    self.push_line_string(Some(g))?;
                }
                Polygon(g) => {
                    self.push_polygon(Some(g))?;
                }
                MultiPoint(p) => self.push_multi_point(Some(p))?,
                MultiLineString(p) => self.push_multi_line_string(Some(p))?,
                MultiPolygon(p) => self.push_multi_polygon(Some(p))?,
                GeometryCollection(gc) => {
                    if gc.num_geometries() == 1 {
                        self.push_geometry(Some(&gc.geometry(0).unwrap()))?
                    } else {
                        return Err(GeoArrowError::General(
                            "nested geometry collections not supported".to_string(),
                        ));
                    }
                }
                Rect(_) | Triangle(_) | Line(_) => todo!(),
            };
        } else {
            self.push_null();
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn push_null(&mut self) {
        todo!("push null geometry")
    }

    /// Extend this builder with the given geometries
    pub(crate) fn extend_from_iter(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait<T = f64> + 'a)>>,
    ) {
        geoms
            .into_iter()
            .try_for_each(|maybe_geom| self.push_geometry(maybe_geom))
            .unwrap();
    }

    /// Create this builder from a slice of nullable Geometries.
    pub(crate) fn from_nullable_geometries(
        geoms: &[Option<impl GeometryTrait<T = f64>>],
        dim: Dimension,
        coord_type: CoordType,
        metadata: Arc<Metadata>,
        prefer_multi: bool,
    ) -> Result<Self> {
        let mut array = Self::with_capacity_and_options_from_iter(
            geoms.iter().map(|x| x.as_ref()),
            dim,
            coord_type,
            metadata,
            prefer_multi,
        )?;
        array.extend_from_iter(geoms.iter().map(|x| x.as_ref()));
        Ok(array)
    }
}

impl<O: OffsetSizeTrait> TryFrom<(WkbArray<O>, Dimension)> for MixedGeometryBuilder {
    type Error = GeoArrowError;

    fn try_from((value, dim): (WkbArray<O>, Dimension)) -> std::result::Result<Self, Self::Error> {
        assert_eq!(
            value.nulls().map_or(0, |validity| validity.null_count()),
            0,
            "Parsing a WkbArray with null elements not supported",
        );

        let metadata = value.data_type.metadata().clone();
        let wkb_objects = value
            .iter()
            .map(|x| x.transpose())
            .collect::<Result<Vec<_>>>()?;
        Self::from_nullable_geometries(
            &wkb_objects,
            dim,
            CoordType::default_interleaved(),
            metadata,
            true,
        )
    }
}

impl GeometryArrayBuilder for MixedGeometryBuilder {
    fn len(&self) -> usize {
        self.types.len()
    }

    fn push_null(&mut self) {
        self.push_null();
    }
}
