use std::sync::Arc;

use arrow_array::{OffsetSizeTrait, UnionArray};
use geo_traits::*;
use geoarrow_schema::{CoordType, Dimension, Metadata};

use crate::capacity::GeometryCapacity;
use crate::error::{GeoArrowError, Result};
use crate::geometry::array::GeometryArray;
use crate::scalar::WKB;
use crate::trait_::{ArrayAccessor, GeometryArrayBuilder, IntoArrow};
use crate::{ArrayBase, NativeArray};
use crate::{
    GeometryCollectionBuilder, LineStringBuilder, MultiLineStringBuilder, MultiPointBuilder,
    MultiPolygonBuilder, PointBuilder, PolygonBuilder, WKBArray,
};

pub(crate) const DEFAULT_PREFER_MULTI: bool = false;

/// The GeoArrow equivalent to a `Vec<Option<Geometry>>`: a mutable collection of Geometries.
///
/// Each Geometry can have a different dimension. All geometries must have the same coordinate
/// type.
///
/// This currently has the caveat that these geometries must be a _primitive_ geometry type. This
/// does not currently support nested GeometryCollection objects.
///
/// Converting an [`GeometryBuilder`] into a [`GeometryArray`] is `O(1)`.
///
/// # Invariants
///
/// - All arrays must have the same coordinate layout (interleaved or separated)
#[derive(Debug)]
pub struct GeometryBuilder {
    metadata: Arc<Metadata>,

    // Invariant: every item in `types` is `> 0 && < fields.len()`
    types: Vec<i8>,

    // In the future we'll additionally have xym, xyzm array variants.
    point_xy: PointBuilder,
    line_string_xy: LineStringBuilder,
    polygon_xy: PolygonBuilder,
    mpoint_xy: MultiPointBuilder,
    mline_string_xy: MultiLineStringBuilder,
    mpolygon_xy: MultiPolygonBuilder,
    gc_xy: GeometryCollectionBuilder,

    point_xyz: PointBuilder,
    line_string_xyz: LineStringBuilder,
    polygon_xyz: PolygonBuilder,
    mpoint_xyz: MultiPointBuilder,
    mline_string_xyz: MultiLineStringBuilder,
    mpolygon_xyz: MultiPolygonBuilder,
    gc_xyz: GeometryCollectionBuilder,

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

    /// The number of nulls that has been deferred and are still to be written.
    ///
    /// Adding nulls is tricky. We often want to use this builder as a generic builder for data
    /// from unknown sources, which then gets downcasted to an array of a specific type.
    ///
    /// In a large majority of the time, this builder will have only data of a single type, which
    /// can then get downcasted to a simple array of a single geometry type and dimension. But in
    /// order for this process to be easy, we want the nulls to be assigned to the same array type
    /// as the actual data.
    ///
    /// When there's a valid geometry pushed before the null, we can add the null to an existing
    /// non-null array type, but if there are no valid geometries yet, we don't know which array to
    /// push the null to. This `deferred_nulls` is the number of initial null values that haven't
    /// yet been written to an array, because we don't know which array to write them to.
    deferred_nulls: usize,
}

impl<'a> GeometryBuilder {
    /// Creates a new empty [`GeometryBuilder`].
    pub fn new() -> Self {
        Self::new_with_options(
            CoordType::Interleaved,
            Default::default(),
            DEFAULT_PREFER_MULTI,
        )
    }

    /// Creates a new empty [`GeometryBuilder`] with the given options.
    pub fn new_with_options(
        coord_type: CoordType,
        metadata: Arc<Metadata>,
        prefer_multi: bool,
    ) -> Self {
        Self::with_capacity_and_options(Default::default(), coord_type, metadata, prefer_multi)
    }

    /// Creates a new [`GeometryBuilder`] with given capacity and no validity.
    pub fn with_capacity(capacity: GeometryCapacity) -> Self {
        Self::with_capacity_and_options(
            capacity,
            CoordType::Interleaved,
            Default::default(),
            DEFAULT_PREFER_MULTI,
        )
    }

    /// Creates a new empty [`GeometryBuilder`] with the given capacity and options.
    pub fn with_capacity_and_options(
        capacity: GeometryCapacity,
        coord_type: CoordType,
        metadata: Arc<Metadata>,
        prefer_multi: bool,
    ) -> Self {
        use Dimension::*;

        // Don't store array metadata on child arrays
        Self {
            metadata,
            types: vec![],
            point_xy: PointBuilder::with_capacity_and_options(
                XY,
                capacity.point_xy(),
                coord_type,
                Default::default(),
            ),
            line_string_xy: LineStringBuilder::with_capacity_and_options(
                XY,
                capacity.line_string_xy(),
                coord_type,
                Default::default(),
            ),
            polygon_xy: PolygonBuilder::with_capacity_and_options(
                XY,
                capacity.polygon_xy(),
                coord_type,
                Default::default(),
            ),
            mpoint_xy: MultiPointBuilder::with_capacity_and_options(
                XY,
                capacity.mpoint_xy(),
                coord_type,
                Default::default(),
            ),
            mline_string_xy: MultiLineStringBuilder::with_capacity_and_options(
                XY,
                capacity.mline_string_xy(),
                coord_type,
                Default::default(),
            ),
            mpolygon_xy: MultiPolygonBuilder::with_capacity_and_options(
                XY,
                capacity.mpolygon_xy(),
                coord_type,
                Default::default(),
            ),
            gc_xy: GeometryCollectionBuilder::with_capacity_and_options(
                XY,
                capacity.gc_xy(),
                coord_type,
                Default::default(),
                prefer_multi,
            ),
            point_xyz: PointBuilder::with_capacity_and_options(
                XYZ,
                capacity.point_xyz(),
                coord_type,
                Default::default(),
            ),
            line_string_xyz: LineStringBuilder::with_capacity_and_options(
                XYZ,
                capacity.line_string_xyz(),
                coord_type,
                Default::default(),
            ),
            polygon_xyz: PolygonBuilder::with_capacity_and_options(
                XYZ,
                capacity.polygon_xyz(),
                coord_type,
                Default::default(),
            ),
            mpoint_xyz: MultiPointBuilder::with_capacity_and_options(
                XYZ,
                capacity.mpoint_xyz(),
                coord_type,
                Default::default(),
            ),
            mline_string_xyz: MultiLineStringBuilder::with_capacity_and_options(
                XYZ,
                capacity.mline_string_xyz(),
                coord_type,
                Default::default(),
            ),
            mpolygon_xyz: MultiPolygonBuilder::with_capacity_and_options(
                XYZ,
                capacity.mpolygon_xyz(),
                coord_type,
                Default::default(),
            ),
            gc_xyz: GeometryCollectionBuilder::with_capacity_and_options(
                XYZ,
                capacity.gc_xyz(),
                coord_type,
                Default::default(),
                prefer_multi,
            ),
            offsets: vec![],
            prefer_multi,
            deferred_nulls: 0,
        }
    }

    /// Reserves capacity for at least `additional` more geometries.
    ///
    /// The collection may reserve more space to speculatively avoid frequent reallocations. After
    /// calling `reserve`, capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, capacity: GeometryCapacity) {
        let total_num_geoms = capacity.total_num_geoms();
        self.types.reserve(total_num_geoms);
        self.offsets.reserve(total_num_geoms);

        self.point_xy.reserve(capacity.point_xy());
        self.line_string_xy.reserve(capacity.line_string_xy());
        self.polygon_xy.reserve(capacity.polygon_xy());
        self.mpoint_xy.reserve(capacity.mpoint_xy());
        self.mline_string_xy.reserve(capacity.mline_string_xy());
        self.mpolygon_xy.reserve(capacity.mpolygon_xy());
        self.gc_xy.reserve(capacity.gc_xy());

        self.point_xyz.reserve(capacity.point_xyz());
        self.line_string_xyz.reserve(capacity.line_string_xyz());
        self.polygon_xyz.reserve(capacity.polygon_xyz());
        self.mpoint_xyz.reserve(capacity.mpoint_xyz());
        self.mline_string_xyz.reserve(capacity.mline_string_xyz());
        self.mpolygon_xyz.reserve(capacity.mpolygon_xyz());
        self.gc_xyz.reserve(capacity.gc_xyz());
    }

    /// Reserves the minimum capacity for at least `additional` more Geometries.
    ///
    /// Unlike [`reserve`], this will not deliberately over-allocate to speculatively avoid
    /// frequent allocations. After calling `reserve_exact`, capacity will be greater than or equal
    /// to `self.len() + additional`. Does nothing if the capacity is already sufficient.
    ///
    /// Note that the allocator may give the collection more space than it
    /// requests. Therefore, capacity can not be relied upon to be precisely
    /// minimal. Prefer [`reserve`] if future insertions are expected.
    ///
    /// [`reserve`]: Self::reserve
    pub fn reserve_exact(&mut self, capacity: GeometryCapacity) {
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
        self.gc_xy.reserve_exact(capacity.gc_xy());

        self.point_xyz.reserve_exact(capacity.point_xyz());
        self.line_string_xyz
            .reserve_exact(capacity.line_string_xyz());
        self.polygon_xyz.reserve_exact(capacity.polygon_xyz());
        self.mpoint_xyz.reserve_exact(capacity.mpoint_xyz());
        self.mline_string_xyz
            .reserve_exact(capacity.mline_string_xyz());
        self.mpolygon_xyz.reserve_exact(capacity.mpolygon_xyz());
        self.gc_xyz.reserve_exact(capacity.gc_xyz());
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

    /// Consume the builder and convert to an immutable [`GeometryArray`]
    pub fn finish(self) -> GeometryArray {
        self.into()
    }

    /// Creates a new builder with a capacity inferred by the provided iterator.
    pub fn with_capacity_from_iter(
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait + 'a)>>,
    ) -> Result<Self> {
        Self::with_capacity_and_options_from_iter(
            geoms,
            CoordType::Interleaved,
            Default::default(),
            DEFAULT_PREFER_MULTI,
        )
    }

    /// Creates a new builder with the provided options and a capacity inferred by the provided
    /// iterator.
    pub fn with_capacity_and_options_from_iter(
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait + 'a)>>,
        coord_type: CoordType,
        metadata: Arc<Metadata>,
        prefer_multi: bool,
    ) -> Result<Self> {
        let counter = GeometryCapacity::from_geometries(geoms, prefer_multi)?;
        Ok(Self::with_capacity_and_options(
            counter,
            coord_type,
            metadata,
            prefer_multi,
        ))
    }

    /// Reserve more space in the underlying buffers with the capacity inferred from the provided
    /// geometries.
    pub fn reserve_from_iter(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait + 'a)>>,
        prefer_multi: bool,
    ) -> Result<()> {
        let counter = GeometryCapacity::from_geometries(geoms, prefer_multi)?;
        self.reserve(counter);
        Ok(())
    }

    /// Reserve more space in the underlying buffers with the capacity inferred from the provided
    /// geometries.
    pub fn reserve_exact_from_iter(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait + 'a)>>,
        prefer_multi: bool,
    ) -> Result<()> {
        let counter = GeometryCapacity::from_geometries(geoms, prefer_multi)?;
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
                        // Flush deferred nulls
                        (0..self.deferred_nulls).for_each(|_| self.mpoint_xy.push_null());
                        self.deferred_nulls = 0;

                        self.mpoint_xy.push_point(Some(point))?;
                    }
                    Dimensions::Xyz | Dimensions::Unknown(3) => {
                        // Flush deferred nulls
                        (0..self.deferred_nulls).for_each(|_| self.mpoint_xyz.push_null());
                        self.deferred_nulls = 0;

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
                        // Flush deferred nulls
                        (0..self.deferred_nulls).for_each(|_| self.point_xy.push_null());
                        self.deferred_nulls = 0;

                        self.point_xy.push_point(Some(point));
                    }
                    Dimensions::Xyz | Dimensions::Unknown(3) => {
                        // Flush deferred nulls
                        (0..self.deferred_nulls).for_each(|_| self.point_xyz.push_null());
                        self.deferred_nulls = 0;

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
    fn add_point_type(&mut self, dim: Dimension) {
        match dim {
            Dimension::XY => {
                self.offsets.push(self.point_xy.len().try_into().unwrap());
                self.types.push(1)
            }
            Dimension::XYZ => {
                self.offsets.push(self.point_xyz.len().try_into().unwrap());
                self.types.push(11)
            }
            _ => todo!("Handle M and ZM dimensions"),
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
                        // Flush deferred nulls
                        (0..self.deferred_nulls).for_each(|_| self.mline_string_xy.push_null());
                        self.deferred_nulls = 0;

                        self.mline_string_xy.push_line_string(Some(line_string))?;
                    }
                    Dimensions::Xyz | Dimensions::Unknown(3) => {
                        // Flush deferred nulls
                        (0..self.deferred_nulls).for_each(|_| self.mline_string_xyz.push_null());
                        self.deferred_nulls = 0;

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
                        // Flush deferred nulls
                        (0..self.deferred_nulls).for_each(|_| self.line_string_xy.push_null());
                        self.deferred_nulls = 0;

                        self.line_string_xy.push_line_string(Some(line_string))?;
                    }
                    Dimensions::Xyz | Dimensions::Unknown(3) => {
                        // Flush deferred nulls
                        (0..self.deferred_nulls).for_each(|_| self.line_string_xyz.push_null());
                        self.deferred_nulls = 0;

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
    fn add_line_string_type(&mut self, dim: Dimension) {
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
            _ => todo!("Handle M and ZM dimensions"),
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
                        // Flush deferred nulls
                        (0..self.deferred_nulls).for_each(|_| self.mpolygon_xy.push_null());
                        self.deferred_nulls = 0;

                        self.mpolygon_xy.push_polygon(Some(polygon))?;
                    }
                    Dimensions::Xyz | Dimensions::Unknown(3) => {
                        // Flush deferred nulls
                        (0..self.deferred_nulls).for_each(|_| self.mpolygon_xyz.push_null());
                        self.deferred_nulls = 0;

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
                        // Flush deferred nulls
                        (0..self.deferred_nulls).for_each(|_| self.polygon_xy.push_null());
                        self.deferred_nulls = 0;

                        self.polygon_xy.push_polygon(Some(polygon))?;
                    }
                    Dimensions::Xyz | Dimensions::Unknown(3) => {
                        // Flush deferred nulls
                        (0..self.deferred_nulls).for_each(|_| self.polygon_xyz.push_null());
                        self.deferred_nulls = 0;

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
    fn add_polygon_type(&mut self, dim: Dimension) {
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
            _ => todo!("Handle M and ZM dimensions"),
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
                    // Flush deferred nulls
                    (0..self.deferred_nulls).for_each(|_| self.mpoint_xy.push_null());
                    self.deferred_nulls = 0;

                    self.mpoint_xy.push_multi_point(Some(multi_point))?;
                }
                Dimensions::Xyz | Dimensions::Unknown(3) => {
                    // Flush deferred nulls
                    (0..self.deferred_nulls).for_each(|_| self.mpoint_xyz.push_null());
                    self.deferred_nulls = 0;

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
    fn add_multi_point_type(&mut self, dim: Dimension) {
        match dim {
            Dimension::XY => {
                self.offsets.push(self.mpoint_xy.len().try_into().unwrap());
                self.types.push(4)
            }
            Dimension::XYZ => {
                self.offsets.push(self.mpoint_xyz.len().try_into().unwrap());
                self.types.push(14)
            }
            _ => todo!("Handle M and ZM dimensions"),
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
                    // Flush deferred nulls
                    (0..self.deferred_nulls).for_each(|_| self.mline_string_xy.push_null());
                    self.deferred_nulls = 0;

                    self.mline_string_xy
                        .push_multi_line_string(Some(multi_line_string))?;
                }
                Dimensions::Xyz | Dimensions::Unknown(3) => {
                    // Flush deferred nulls
                    (0..self.deferred_nulls).for_each(|_| self.mline_string_xyz.push_null());
                    self.deferred_nulls = 0;

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
    fn add_multi_line_string_type(&mut self, dim: Dimension) {
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
            _ => todo!("Handle M and ZM dimensions"),
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
                    // Flush deferred nulls
                    (0..self.deferred_nulls).for_each(|_| self.mpolygon_xy.push_null());
                    self.deferred_nulls = 0;

                    self.mpolygon_xy.push_multi_polygon(Some(multi_polygon))?;
                }
                Dimensions::Xyz | Dimensions::Unknown(3) => {
                    // Flush deferred nulls
                    (0..self.deferred_nulls).for_each(|_| self.mpolygon_xyz.push_null());
                    self.deferred_nulls = 0;

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
    fn add_multi_polygon_type(&mut self, dim: Dimension) {
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
            _ => todo!("Handle M and ZM dimensions"),
        }
    }

    /// Add a new geometry to this builder
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
                        self.push_geometry_collection(Some(gc))?
                    }
                }
                Rect(_) | Triangle(_) | Line(_) => todo!(),
            };
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Add a new GeometryCollection to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    pub fn push_geometry_collection(
        &mut self,
        value: Option<&impl GeometryCollectionTrait<T = f64>>,
    ) -> Result<()> {
        if let Some(gc) = value {
            self.add_geometry_collection_type(gc.dim().try_into().unwrap());
            match gc.dim() {
                Dimensions::Xy | Dimensions::Unknown(2) => {
                    // Flush deferred nulls
                    (0..self.deferred_nulls).for_each(|_| self.gc_xy.push_null());
                    self.deferred_nulls = 0;

                    self.gc_xy.push_geometry_collection(Some(gc))?;
                }
                Dimensions::Xyz | Dimensions::Unknown(3) => {
                    // Flush deferred nulls
                    (0..self.deferred_nulls).for_each(|_| self.gc_xyz.push_null());
                    self.deferred_nulls = 0;

                    self.gc_xyz.push_geometry_collection(Some(gc))?;
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
    fn add_geometry_collection_type(&mut self, dim: Dimension) {
        match dim {
            Dimension::XY => {
                self.offsets.push(self.gc_xy.len().try_into().unwrap());
                self.types.push(7)
            }
            Dimension::XYZ => {
                self.offsets.push(self.gc_xyz.len().try_into().unwrap());
                self.types.push(17)
            }
            _ => todo!("Handle M and ZM dimensions"),
        }
    }

    /// Push a null to this builder
    ///
    /// Nulls will be pushed to one of the underlying non-empty arrays, to simplify downcasting.
    #[inline]
    pub fn push_null(&mut self) {
        if !self.point_xy.is_empty() {
            self.point_xy.push_null();
        } else if !self.line_string_xy.is_empty() {
            self.line_string_xy.push_null();
        } else if !self.polygon_xy.is_empty() {
            self.polygon_xy.push_null();
        } else if !self.mpoint_xy.is_empty() {
            self.mpoint_xy.push_null();
        } else if !self.mline_string_xy.is_empty() {
            self.mline_string_xy.push_null();
        } else if !self.mpolygon_xy.is_empty() {
            self.mpolygon_xy.push_null();
        } else if !self.point_xyz.is_empty() {
            self.point_xyz.push_null();
        } else if !self.line_string_xyz.is_empty() {
            self.line_string_xyz.push_null();
        } else if !self.polygon_xyz.is_empty() {
            self.polygon_xyz.push_null();
        } else if !self.mpoint_xyz.is_empty() {
            self.mpoint_xyz.push_null();
        } else if !self.mline_string_xyz.is_empty() {
            self.mline_string_xyz.push_null();
        } else if !self.mpolygon_xyz.is_empty() {
            self.mpolygon_xyz.push_null();
        } else {
            self.deferred_nulls += 1;
        }
    }

    /// Extend this builder with the given geometries
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
        coord_type: CoordType,
        metadata: Arc<Metadata>,
        prefer_multi: bool,
    ) -> Result<Self> {
        let mut array = Self::with_capacity_and_options_from_iter(
            geoms.iter().map(Some),
            coord_type,
            metadata,
            prefer_multi,
        )?;
        array.extend_from_iter(geoms.iter().map(Some));
        Ok(array)
    }

    /// Create this builder from a slice of nullable Geometries.
    pub fn from_nullable_geometries(
        geoms: &[Option<impl GeometryTrait<T = f64>>],
        coord_type: CoordType,
        metadata: Arc<Metadata>,
        prefer_multi: bool,
    ) -> Result<Self> {
        let mut array = Self::with_capacity_and_options_from_iter(
            geoms.iter().map(|x| x.as_ref()),
            coord_type,
            metadata,
            prefer_multi,
        )?;
        array.extend_from_iter(geoms.iter().map(|x| x.as_ref()));
        Ok(array)
    }

    pub(crate) fn from_wkb<W: OffsetSizeTrait>(
        wkb_objects: &[Option<WKB<'_, W>>],
        coord_type: CoordType,
        metadata: Arc<Metadata>,
        prefer_multi: bool,
    ) -> Result<Self> {
        let wkb_objects2 = wkb_objects
            .iter()
            .map(|maybe_wkb| maybe_wkb.as_ref().map(|wkb| wkb.parse()).transpose())
            .collect::<Result<Vec<_>>>()?;
        Self::from_nullable_geometries(&wkb_objects2, coord_type, metadata, prefer_multi)
    }
}

impl Default for GeometryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl IntoArrow for GeometryBuilder {
    type ArrowArray = UnionArray;

    fn into_arrow(self) -> Self::ArrowArray {
        todo!()
    }
}

impl From<GeometryBuilder> for GeometryArray {
    fn from(other: GeometryBuilder) -> Self {
        Self::new(
            other.types.into(),
            other.offsets.into(),
            Some(other.point_xy.into()),
            Some(other.line_string_xy.into()),
            Some(other.polygon_xy.into()),
            Some(other.mpoint_xy.into()),
            Some(other.mline_string_xy.into()),
            Some(other.mpolygon_xy.into()),
            Some(other.gc_xy.into()),
            Some(other.point_xyz.into()),
            Some(other.line_string_xyz.into()),
            Some(other.polygon_xyz.into()),
            Some(other.mpoint_xyz.into()),
            Some(other.mline_string_xyz.into()),
            Some(other.mpolygon_xyz.into()),
            Some(other.gc_xyz.into()),
            other.metadata,
        )
    }
}

impl<G: GeometryTrait<T = f64>> TryFrom<&[G]> for GeometryBuilder {
    type Error = GeoArrowError;

    fn try_from(geoms: &[G]) -> Result<Self> {
        Self::from_geometries(geoms, CoordType::Interleaved, Default::default(), true)
    }
}

impl<G: GeometryTrait<T = f64>> TryFrom<Vec<Option<G>>> for GeometryBuilder {
    type Error = GeoArrowError;

    fn try_from(geoms: Vec<Option<G>>) -> Result<Self> {
        Self::from_nullable_geometries(&geoms, CoordType::Interleaved, Default::default(), true)
    }
}

impl<O: OffsetSizeTrait> TryFrom<WKBArray<O>> for GeometryBuilder {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> std::result::Result<Self, Self::Error> {
        assert_eq!(
            value.nulls().map_or(0, |validity| validity.null_count()),
            0,
            "Parsing a WKBArray with null elements not supported",
        );

        let metadata = value.metadata();
        let wkb_objects: Vec<Option<WKB<'_, O>>> = value.iter().collect();
        Self::from_wkb(&wkb_objects, CoordType::Interleaved, metadata, true)
    }
}

impl GeometryArrayBuilder for GeometryBuilder {
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
        metadata: Arc<Metadata>,
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

    fn set_metadata(&mut self, metadata: Arc<Metadata>) {
        self.metadata = metadata;
    }

    fn metadata(&self) -> Arc<Metadata> {
        self.metadata.clone()
    }
}
