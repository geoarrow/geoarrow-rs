use std::sync::Arc;

use crate::array::metadata::ArrayMetadata;
use crate::array::unknown::array::UnknownGeometryArray;
use crate::array::unknown::capacity::UnknownCapacity;
use crate::array::{
    CoordType, LineStringBuilder, MultiLineStringBuilder, MultiPointBuilder, MultiPolygonBuilder,
    PointBuilder, PolygonBuilder, WKBArray,
};
use crate::datatypes::Dimension;
use crate::error::{GeoArrowError, Result};
use crate::scalar::WKB;
use crate::trait_::{ArrayAccessor, GeometryArrayBuilder, IntoArrow};
use crate::{ArrayBase, NativeArray};
use arrow_array::{OffsetSizeTrait, UnionArray};
use geo_traits::*;

pub(crate) const DEFAULT_PREFER_MULTI: bool = false;

/// The GeoArrow equivalent to a `Vec<Option<Geometry>>`: a mutable collection of Geometries.
///
/// Each Geometry can have a different dimension. All geometries must have the same coordinate
/// type.
///
/// This currently has the caveat that these geometries must be a _primitive_ geometry type. This
/// does not currently support nested GeometryCollection objects.
///
/// Converting an [`UnknownGeometryBuilder`] into a [`UnknownGeometryArray`] is `O(1)`.
///
/// # Invariants
///
/// - All arrays must have the same coordinate layout (interleaved or separated)
#[derive(Debug)]
pub struct UnknownGeometryBuilder {
    metadata: Arc<ArrayMetadata>,

    // Invariant: every item in `types` is `> 0 && < fields.len()`
    types: Vec<i8>,

    // In the future we'll additionally have xym, xyzm array variants.
    point_xy: PointBuilder,
    line_string_xy: LineStringBuilder,
    polygon_xy: PolygonBuilder,
    mpoint_xy: MultiPointBuilder,
    mline_string_xy: MultiLineStringBuilder,
    mpolygon_xy: MultiPolygonBuilder,

    point_xyz: PointBuilder,
    line_string_xyz: LineStringBuilder,
    polygon_xyz: PolygonBuilder,
    mpoint_xyz: MultiPointBuilder,
    mline_string_xyz: MultiLineStringBuilder,
    mpolygon_xyz: MultiPolygonBuilder,

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

impl<'a> UnknownGeometryBuilder {
    /// Creates a new empty [`UnknownGeometryBuilder`].
    pub fn new() -> Self {
        Self::new_with_options(Default::default(), Default::default(), DEFAULT_PREFER_MULTI)
    }

    pub fn new_with_options(
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
        prefer_multi: bool,
    ) -> Self {
        Self::with_capacity_and_options(Default::default(), coord_type, metadata, prefer_multi)
    }

    /// Creates a new [`MixedGeometryBuilder`] with given capacity and no validity.
    pub fn with_capacity(capacity: UnknownCapacity) -> Self {
        Self::with_capacity_and_options(
            capacity,
            Default::default(),
            Default::default(),
            DEFAULT_PREFER_MULTI,
        )
    }

    pub fn with_capacity_and_options(
        capacity: UnknownCapacity,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
        prefer_multi: bool,
    ) -> Self {
        // Don't store array metadata on child arrays
        Self {
            metadata,
            types: vec![],
            point_xy: PointBuilder::with_capacity_and_options(
                Dimension::XY,
                capacity.point_xy(),
                coord_type,
                Default::default(),
            ),
            line_string_xy: LineStringBuilder::with_capacity_and_options(
                Dimension::XY,
                capacity.line_string_xy(),
                coord_type,
                Default::default(),
            ),
            polygon_xy: PolygonBuilder::with_capacity_and_options(
                Dimension::XY,
                capacity.polygon_xy(),
                coord_type,
                Default::default(),
            ),
            mpoint_xy: MultiPointBuilder::with_capacity_and_options(
                Dimension::XY,
                capacity.mpoint_xy(),
                coord_type,
                Default::default(),
            ),
            mline_string_xy: MultiLineStringBuilder::with_capacity_and_options(
                Dimension::XY,
                capacity.mline_string_xy(),
                coord_type,
                Default::default(),
            ),
            mpolygon_xy: MultiPolygonBuilder::with_capacity_and_options(
                Dimension::XY,
                capacity.mpolygon_xy(),
                coord_type,
                Default::default(),
            ),
            point_xyz: PointBuilder::with_capacity_and_options(
                Dimension::XYZ,
                capacity.point_xyz(),
                coord_type,
                Default::default(),
            ),
            line_string_xyz: LineStringBuilder::with_capacity_and_options(
                Dimension::XYZ,
                capacity.line_string_xyz(),
                coord_type,
                Default::default(),
            ),
            polygon_xyz: PolygonBuilder::with_capacity_and_options(
                Dimension::XYZ,
                capacity.polygon_xyz(),
                coord_type,
                Default::default(),
            ),
            mpoint_xyz: MultiPointBuilder::with_capacity_and_options(
                Dimension::XYZ,
                capacity.mpoint_xyz(),
                coord_type,
                Default::default(),
            ),
            mline_string_xyz: MultiLineStringBuilder::with_capacity_and_options(
                Dimension::XYZ,
                capacity.mline_string_xyz(),
                coord_type,
                Default::default(),
            ),
            mpolygon_xyz: MultiPolygonBuilder::with_capacity_and_options(
                Dimension::XYZ,
                capacity.mpolygon_xyz(),
                coord_type,
                Default::default(),
            ),
            offsets: vec![],
            prefer_multi,
        }
    }

    pub fn reserve(&mut self, capacity: UnknownCapacity) {
        let total_num_geoms = capacity.total_num_geoms();
        self.types.reserve(total_num_geoms);
        self.offsets.reserve(total_num_geoms);

        self.point_xy.reserve(capacity.point_xy());
        self.line_string_xy.reserve(capacity.line_string_xy());
        self.polygon_xy.reserve(capacity.polygon_xy());
        self.mpoint_xy.reserve(capacity.mpoint_xy());
        self.mline_string_xy.reserve(capacity.mline_string_xy());
        self.mpolygon_xy.reserve(capacity.mpolygon_xy());

        self.point_xyz.reserve(capacity.point_xyz());
        self.line_string_xyz.reserve(capacity.line_string_xyz());
        self.polygon_xyz.reserve(capacity.polygon_xyz());
        self.mpoint_xyz.reserve(capacity.mpoint_xyz());
        self.mline_string_xyz.reserve(capacity.mline_string_xyz());
        self.mpolygon_xyz.reserve(capacity.mpolygon_xyz());
    }

    pub fn reserve_exact(&mut self, capacity: UnknownCapacity) {
        let total_num_geoms = capacity.total_num_geoms();

        self.types.reserve_exact(total_num_geoms);
        self.offsets.reserve_exact(total_num_geoms);

        self.point_xy.reserve_exact(capacity.point_xy());
        self.line_string_xy.reserve_exact(capacity.line_string_xy());
        self.polygon_xy.reserve_exact(capacity.polygon_xy());
        self.mpoint_xy.reserve_exact(capacity.mpoint_xy());
        self.mline_string_xy
            .reserve_exact(capacity.mline_string_xy());
        self.mpolygon_xy.reserve_exact(capacity.mpolygon_xy());

        self.point_xyz.reserve_exact(capacity.point_xyz());
        self.line_string_xyz
            .reserve_exact(capacity.line_string_xyz());
        self.polygon_xyz.reserve_exact(capacity.polygon_xyz());
        self.mpoint_xyz.reserve_exact(capacity.mpoint_xyz());
        self.mline_string_xyz
            .reserve_exact(capacity.mline_string_xyz());
        self.mpolygon_xyz.reserve_exact(capacity.mpolygon_xyz());
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
    // pub fn try_new(
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

    pub fn finish(self) -> UnknownGeometryArray {
        self.into()
    }

    pub fn with_capacity_from_iter(
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait + 'a)>>,
    ) -> Result<Self> {
        Self::with_capacity_and_options_from_iter(
            geoms,
            Default::default(),
            Default::default(),
            DEFAULT_PREFER_MULTI,
        )
    }

    pub fn with_capacity_and_options_from_iter(
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait + 'a)>>,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
        prefer_multi: bool,
    ) -> Result<Self> {
        let counter = UnknownCapacity::from_geometries(geoms, prefer_multi)?;
        Ok(Self::with_capacity_and_options(
            counter,
            coord_type,
            metadata,
            prefer_multi,
        ))
    }

    pub fn reserve_from_iter(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait + 'a)>>,
        prefer_multi: bool,
    ) -> Result<()> {
        let counter = UnknownCapacity::from_geometries(geoms, prefer_multi)?;
        self.reserve(counter);
        Ok(())
    }

    pub fn reserve_exact_from_iter(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait + 'a)>>,
        prefer_multi: bool,
    ) -> Result<()> {
        let counter = UnknownCapacity::from_geometries(geoms, prefer_multi)?;
        self.reserve_exact(counter);
        Ok(())
    }

    /// Add a new Point to the end of this array.
    ///
    /// If `self.prefer_multi` is `true`, it will be stored in the `MultiPointBuilder` child
    /// array. Otherwise, it will be stored in the `PointBuilder` child array.
    #[inline]
    pub fn push_point(&mut self, value: Option<&impl PointTrait<T = f64>>) -> Result<()> {
        if let Some(point) = value {
            if self.prefer_multi {
                self.add_multi_point_type(point.dim().try_into().unwrap());
                match point.dim() {
                    Dimensions::Xy | Dimensions::Unknown(2) => {
                        self.mpoint_xy.push_point(Some(point))?;
                    }
                    Dimensions::Xyz | Dimensions::Unknown(3) => {
                        self.mpoint_xyz.push_point(Some(point))?;
                    }
                    dim => {
                        return Err(GeoArrowError::General(format!(
                            "Unsupported dimension {dim:?}"
                        )))
                    }
                }
            } else {
                self.add_point_type(point.dim().try_into().unwrap());
                match point.dim() {
                    Dimensions::Xy | Dimensions::Unknown(2) => {
                        self.point_xy.push_point(Some(point));
                    }
                    Dimensions::Xyz | Dimensions::Unknown(3) => {
                        self.point_xyz.push_point(Some(point));
                    }
                    dim => {
                        return Err(GeoArrowError::General(format!(
                            "Unsupported dimension {dim:?}"
                        )))
                    }
                }
            }
        } else {
            self.push_null();
        };

        Ok(())
    }

    #[inline]
    pub(crate) fn add_point_type(&mut self, dim: Dimension) {
        match dim {
            Dimension::XY => {
                self.offsets.push(self.point_xy.len().try_into().unwrap());
                self.types.push(1)
            }
            Dimension::XYZ => {
                self.offsets.push(self.point_xyz.len().try_into().unwrap());
                self.types.push(11)
            }
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
    pub fn push_line_string(
        &mut self,
        value: Option<&impl LineStringTrait<T = f64>>,
    ) -> Result<()> {
        if let Some(line_string) = value {
            if self.prefer_multi {
                self.add_multi_line_string_type(line_string.dim().try_into().unwrap());
                match line_string.dim() {
                    Dimensions::Xy | Dimensions::Unknown(2) => {
                        self.mline_string_xy.push_line_string(Some(line_string))?;
                    }
                    Dimensions::Xyz | Dimensions::Unknown(3) => {
                        self.mline_string_xyz.push_line_string(Some(line_string))?;
                    }
                    dim => {
                        return Err(GeoArrowError::General(format!(
                            "Unsupported dimension {dim:?}"
                        )))
                    }
                }
            } else {
                self.add_line_string_type(line_string.dim().try_into().unwrap());
                match line_string.dim() {
                    Dimensions::Xy | Dimensions::Unknown(2) => {
                        self.line_string_xy.push_line_string(Some(line_string))?;
                    }
                    Dimensions::Xyz | Dimensions::Unknown(3) => {
                        self.line_string_xyz.push_line_string(Some(line_string))?;
                    }
                    dim => {
                        return Err(GeoArrowError::General(format!(
                            "Unsupported dimension {dim:?}"
                        )))
                    }
                }
            }
        } else {
            self.push_null();
        };

        Ok(())
    }

    #[inline]
    pub(crate) fn add_line_string_type(&mut self, dim: Dimension) {
        match dim {
            Dimension::XY => {
                self.offsets
                    .push(self.line_string_xy.len().try_into().unwrap());
                self.types.push(2)
            }
            Dimension::XYZ => {
                self.offsets
                    .push(self.line_string_xyz.len().try_into().unwrap());
                self.types.push(12)
            }
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
    pub fn push_polygon(&mut self, value: Option<&impl PolygonTrait<T = f64>>) -> Result<()> {
        if let Some(polygon) = value {
            if self.prefer_multi {
                self.add_multi_polygon_type(polygon.dim().try_into().unwrap());
                match polygon.dim() {
                    Dimensions::Xy | Dimensions::Unknown(2) => {
                        self.mpolygon_xy.push_polygon(Some(polygon))?;
                    }
                    Dimensions::Xyz | Dimensions::Unknown(3) => {
                        self.mpolygon_xyz.push_polygon(Some(polygon))?;
                    }
                    dim => {
                        return Err(GeoArrowError::General(format!(
                            "Unsupported dimension {dim:?}"
                        )))
                    }
                }
            } else {
                self.add_polygon_type(polygon.dim().try_into().unwrap());
                match polygon.dim() {
                    Dimensions::Xy | Dimensions::Unknown(2) => {
                        self.polygon_xy.push_polygon(Some(polygon))?;
                    }
                    Dimensions::Xyz | Dimensions::Unknown(3) => {
                        self.polygon_xyz.push_polygon(Some(polygon))?;
                    }
                    dim => {
                        return Err(GeoArrowError::General(format!(
                            "Unsupported dimension {dim:?}"
                        )))
                    }
                }
            }
        } else {
            self.push_null();
        };

        Ok(())
    }

    #[inline]
    pub(crate) fn add_polygon_type(&mut self, dim: Dimension) {
        match dim {
            Dimension::XY => {
                self.offsets.push(self.polygon_xy.len().try_into().unwrap());
                self.types.push(3)
            }
            Dimension::XYZ => {
                self.offsets
                    .push(self.polygon_xyz.len().try_into().unwrap());
                self.types.push(13)
            }
        }
    }

    /// Add a new MultiPoint to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    pub fn push_multi_point(
        &mut self,
        value: Option<&impl MultiPointTrait<T = f64>>,
    ) -> Result<()> {
        if let Some(multi_point) = value {
            self.add_multi_point_type(multi_point.dim().try_into().unwrap());
            match multi_point.dim() {
                Dimensions::Xy | Dimensions::Unknown(2) => {
                    self.mpoint_xy.push_multi_point(Some(multi_point))?;
                }
                Dimensions::Xyz | Dimensions::Unknown(3) => {
                    self.mpoint_xyz.push_multi_point(Some(multi_point))?;
                }
                dim => {
                    return Err(GeoArrowError::General(format!(
                        "Unsupported dimension {dim:?}"
                    )))
                }
            }
        } else {
            self.push_null();
        };

        Ok(())
    }

    #[inline]
    pub(crate) fn add_multi_point_type(&mut self, dim: Dimension) {
        match dim {
            Dimension::XY => {
                self.offsets.push(self.mpoint_xy.len().try_into().unwrap());
                self.types.push(4)
            }
            Dimension::XYZ => {
                self.offsets.push(self.mpoint_xyz.len().try_into().unwrap());
                self.types.push(14)
            }
        }
    }

    /// Add a new MultiLineString to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    pub fn push_multi_line_string(
        &mut self,
        value: Option<&impl MultiLineStringTrait<T = f64>>,
    ) -> Result<()> {
        if let Some(multi_line_string) = value {
            self.add_multi_line_string_type(multi_line_string.dim().try_into().unwrap());
            match multi_line_string.dim() {
                Dimensions::Xy | Dimensions::Unknown(2) => {
                    self.mline_string_xy
                        .push_multi_line_string(Some(multi_line_string))?;
                }
                Dimensions::Xyz | Dimensions::Unknown(3) => {
                    self.mline_string_xyz
                        .push_multi_line_string(Some(multi_line_string))?;
                }
                dim => {
                    return Err(GeoArrowError::General(format!(
                        "Unsupported dimension {dim:?}"
                    )))
                }
            }
        } else {
            self.push_null();
        };

        Ok(())
    }

    #[inline]
    pub(crate) fn add_multi_line_string_type(&mut self, dim: Dimension) {
        match dim {
            Dimension::XY => {
                self.offsets
                    .push(self.mline_string_xy.len().try_into().unwrap());
                self.types.push(5)
            }
            Dimension::XYZ => {
                self.offsets
                    .push(self.mline_string_xyz.len().try_into().unwrap());
                self.types.push(15)
            }
        }
    }

    /// Add a new MultiPolygon to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    pub fn push_multi_polygon(
        &mut self,
        value: Option<&impl MultiPolygonTrait<T = f64>>,
    ) -> Result<()> {
        if let Some(multi_polygon) = value {
            self.add_multi_polygon_type(multi_polygon.dim().try_into().unwrap());
            match multi_polygon.dim() {
                Dimensions::Xy | Dimensions::Unknown(2) => {
                    self.mpolygon_xy.push_multi_polygon(Some(multi_polygon))?;
                }
                Dimensions::Xyz | Dimensions::Unknown(3) => {
                    self.mpolygon_xyz.push_multi_polygon(Some(multi_polygon))?;
                }
                dim => {
                    return Err(GeoArrowError::General(format!(
                        "Unsupported dimension {dim:?}"
                    )))
                }
            }
        } else {
            self.push_null();
        };

        Ok(())
    }

    #[inline]
    pub(crate) fn add_multi_polygon_type(&mut self, dim: Dimension) {
        match dim {
            Dimension::XY => {
                self.offsets
                    .push(self.mpolygon_xy.len().try_into().unwrap());
                self.types.push(6)
            }
            Dimension::XYZ => {
                self.offsets
                    .push(self.mpolygon_xyz.len().try_into().unwrap());
                self.types.push(16)
            }
        }
    }

    #[inline]
    pub fn push_geometry(&mut self, value: Option<&'a impl GeometryTrait<T = f64>>) -> Result<()> {
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
    pub fn push_null(&mut self) {
        // Note: perhaps you could defer writing nulls until the first actual geometry has been
        // pushed. And then at that point you write and deferred nulls and then the current
        // geometry. And at any other point you can check which array already has data, and push a
        // null to that array.
        todo!("push null geometry")
    }

    pub fn extend_from_iter(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait<T = f64> + 'a)>>,
    ) {
        geoms
            .into_iter()
            .try_for_each(|maybe_geom| self.push_geometry(maybe_geom))
            .unwrap();
    }

    /// Create this builder from a slice of Geometries.
    pub fn from_geometries(
        geoms: &[impl GeometryTrait<T = f64>],
        coord_type: Option<CoordType>,
        metadata: Arc<ArrayMetadata>,
        prefer_multi: bool,
    ) -> Result<Self> {
        let mut array = Self::with_capacity_and_options_from_iter(
            geoms.iter().map(Some),
            coord_type.unwrap_or_default(),
            metadata,
            prefer_multi,
        )?;
        array.extend_from_iter(geoms.iter().map(Some));
        Ok(array)
    }

    /// Create this builder from a slice of nullable Geometries.
    pub fn from_nullable_geometries(
        geoms: &[Option<impl GeometryTrait<T = f64>>],
        coord_type: Option<CoordType>,
        metadata: Arc<ArrayMetadata>,
        prefer_multi: bool,
    ) -> Result<Self> {
        let mut array = Self::with_capacity_and_options_from_iter(
            geoms.iter().map(|x| x.as_ref()),
            coord_type.unwrap_or_default(),
            metadata,
            prefer_multi,
        )?;
        array.extend_from_iter(geoms.iter().map(|x| x.as_ref()));
        Ok(array)
    }

    pub(crate) fn from_wkb<W: OffsetSizeTrait>(
        wkb_objects: &[Option<WKB<'_, W>>],
        coord_type: Option<CoordType>,
        metadata: Arc<ArrayMetadata>,
        prefer_multi: bool,
    ) -> Result<Self> {
        let wkb_objects2 = wkb_objects
            .iter()
            .map(|maybe_wkb| maybe_wkb.as_ref().map(|wkb| wkb.parse()).transpose())
            .collect::<Result<Vec<_>>>()?;
        Self::from_nullable_geometries(&wkb_objects2, coord_type, metadata, prefer_multi)
    }
}

impl Default for UnknownGeometryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl IntoArrow for UnknownGeometryBuilder {
    type ArrowArray = UnionArray;

    fn into_arrow(self) -> Self::ArrowArray {
        todo!()
    }
}

impl From<UnknownGeometryBuilder> for UnknownGeometryArray {
    fn from(other: UnknownGeometryBuilder) -> Self {
        Self::new(
            other.types.into(),
            other.offsets.into(),
            other.point_xy.into(),
            other.line_string_xy.into(),
            other.polygon_xy.into(),
            other.mpoint_xy.into(),
            other.mline_string_xy.into(),
            other.mpolygon_xy.into(),
            other.point_xyz.into(),
            other.line_string_xyz.into(),
            other.polygon_xyz.into(),
            other.mpoint_xyz.into(),
            other.mline_string_xyz.into(),
            other.mpolygon_xyz.into(),
            other.metadata,
        )
    }
}

impl<G: GeometryTrait<T = f64>> TryFrom<&[G]> for UnknownGeometryBuilder {
    type Error = GeoArrowError;

    fn try_from(geoms: &[G]) -> Result<Self> {
        Self::from_geometries(geoms, Default::default(), Default::default(), true)
    }
}

impl<G: GeometryTrait<T = f64>> TryFrom<Vec<Option<G>>> for UnknownGeometryBuilder {
    type Error = GeoArrowError;

    fn try_from(geoms: Vec<Option<G>>) -> Result<Self> {
        Self::from_nullable_geometries(&geoms, Default::default(), Default::default(), true)
    }
}

impl<O: OffsetSizeTrait> TryFrom<WKBArray<O>> for UnknownGeometryBuilder {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> std::result::Result<Self, Self::Error> {
        assert_eq!(
            value.nulls().map_or(0, |validity| validity.null_count()),
            0,
            "Parsing a WKBArray with null elements not supported",
        );

        let metadata = value.metadata.clone();
        let wkb_objects: Vec<Option<WKB<'_, O>>> = value.iter().collect();
        Self::from_wkb(&wkb_objects, Default::default(), metadata, true)
    }
}

impl GeometryArrayBuilder for UnknownGeometryBuilder {
    fn len(&self) -> usize {
        self.types.len()
    }

    fn nulls(&self) -> &arrow_buffer::NullBufferBuilder {
        // Take this method off trait
        todo!()
    }

    fn new(_dim: Dimension) -> Self {
        Self::new()
    }

    fn into_array_ref(self) -> Arc<dyn arrow_array::Array> {
        Arc::new(self.into_arrow())
    }

    fn with_geom_capacity_and_options(
        _dim: Dimension,
        _geom_capacity: usize,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        // We don't know where to allocate the capacity
        Self::with_capacity_and_options(
            Default::default(),
            coord_type,
            metadata,
            DEFAULT_PREFER_MULTI,
        )
    }

    fn push_geometry(&mut self, value: Option<&impl GeometryTrait<T = f64>>) -> Result<()> {
        self.push_geometry(value)
    }

    fn finish(self) -> std::sync::Arc<dyn NativeArray> {
        Arc::new(self.finish())
    }

    fn coord_type(&self) -> CoordType {
        self.point_xy.coord_type()
    }

    fn set_metadata(&mut self, metadata: Arc<ArrayMetadata>) {
        self.metadata = metadata;
    }

    fn metadata(&self) -> Arc<ArrayMetadata> {
        self.metadata.clone()
    }
}
