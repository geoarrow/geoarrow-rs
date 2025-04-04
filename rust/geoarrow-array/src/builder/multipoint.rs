use std::sync::Arc;

use arrow_array::{ArrayRef, GenericListArray, OffsetSizeTrait};
use arrow_buffer::NullBufferBuilder;
use geo_traits::{CoordTrait, GeometryTrait, GeometryType, MultiPointTrait, PointTrait};
use geoarrow_schema::{CoordType, Dimension, Metadata};

use crate::capacity::MultiPointCapacity;
// use super::array::check;
use crate::array::{MultiPointArray, WKBArray};
use crate::builder::{
    CoordBufferBuilder, InterleavedCoordBufferBuilder, LineStringBuilder,
    SeparatedCoordBufferBuilder,
};
use crate::error::{GeoArrowError, Result};
use crate::offset_builder::OffsetsBuilder;
use crate::scalar::WKB;
use crate::trait_::{ArrayAccessor, GeometryArrayBuilder, IntoArrow};

/// The GeoArrow equivalent to `Vec<Option<MultiPoint>>`: a mutable collection of MultiPoints.
///
/// Converting an [`MultiPointBuilder`] into a [`MultiPointArray`] is `O(1)`.
#[derive(Debug)]
pub struct MultiPointBuilder {
    metadata: Arc<Metadata>,

    coords: CoordBufferBuilder,

    geom_offsets: OffsetsBuilder<i32>,

    /// Validity is only defined at the geometry level
    validity: NullBufferBuilder,
}

impl MultiPointBuilder {
    /// Creates a new empty [`MultiPointBuilder`].
    pub fn new(dim: Dimension) -> Self {
        Self::new_with_options(dim, CoordType::default_interleaved(), Default::default())
    }

    /// Creates a new [`MultiPointBuilder`] with options
    pub fn new_with_options(
        dim: Dimension,
        coord_type: CoordType,
        metadata: Arc<Metadata>,
    ) -> Self {
        Self::with_capacity_and_options(dim, Default::default(), coord_type, metadata)
    }

    /// Creates a new [`MultiPointBuilder`] with a capacity.
    pub fn with_capacity(dim: Dimension, capacity: MultiPointCapacity) -> Self {
        Self::with_capacity_and_options(
            dim,
            capacity,
            CoordType::default_interleaved(),
            Default::default(),
        )
    }

    /// Creates a new [`MultiPointBuilder`] with capacity and options
    pub fn with_capacity_and_options(
        dim: Dimension,
        capacity: MultiPointCapacity,
        coord_type: CoordType,
        metadata: Arc<Metadata>,
    ) -> Self {
        let coords = match coord_type {
            CoordType::Interleaved => CoordBufferBuilder::Interleaved(
                InterleavedCoordBufferBuilder::with_capacity(capacity.coord_capacity, dim),
            ),
            CoordType::Separated => CoordBufferBuilder::Separated(
                SeparatedCoordBufferBuilder::with_capacity(capacity.coord_capacity, dim),
            ),
        };
        Self {
            coords,
            geom_offsets: OffsetsBuilder::with_capacity(capacity.geom_capacity),
            validity: NullBufferBuilder::new(capacity.geom_capacity),
            metadata,
        }
    }

    /// Reserves capacity for at least `additional` more MultiPoints.
    ///
    /// The collection may reserve more space to speculatively avoid frequent reallocations. After
    /// calling `reserve`, capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, capacity: MultiPointCapacity) {
        self.coords.reserve(capacity.coord_capacity);
        self.geom_offsets.reserve(capacity.geom_capacity);
    }

    /// Reserves the minimum capacity for at least `additional` more MultiPoints.
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
    pub fn reserve_exact(&mut self, capacity: MultiPointCapacity) {
        self.coords.reserve_exact(capacity.coord_capacity);
        self.geom_offsets.reserve_exact(capacity.geom_capacity);
    }

    /// The canonical method to create a [`MultiPointBuilder`] out of its internal components.
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Errors
    ///
    /// This function errors iff:
    ///
    /// - if the validity is not `None` and its length is different from the number of geometries
    /// - if the largest geometry offset does not match the number of coordinates
    pub fn try_new(
        coords: CoordBufferBuilder,
        geom_offsets: OffsetsBuilder<i32>,
        validity: NullBufferBuilder,
        metadata: Arc<Metadata>,
    ) -> Result<Self> {
        // check(
        //     &coords.clone().into(),
        //     validity.as_ref().map(|x| x.len()),
        //     &geom_offsets.clone().into(),
        // )?;
        Ok(Self {
            coords,
            geom_offsets,
            validity,
            metadata,
        })
    }

    /// Extract the low-level APIs from the [`MultiPointBuilder`].
    pub fn into_inner(self) -> (CoordBufferBuilder, OffsetsBuilder<i32>, NullBufferBuilder) {
        (self.coords, self.geom_offsets, self.validity)
    }

    /// Consume the builder and convert to an immutable [`MultiPointArray`]
    pub fn finish(self) -> MultiPointArray {
        self.into()
    }

    /// Creates a new builder with a capacity inferred by the provided iterator.
    pub fn with_capacity_from_iter<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl MultiPointTrait + 'a)>>,
        dim: Dimension,
    ) -> Self {
        Self::with_capacity_and_options_from_iter(
            geoms,
            dim,
            CoordType::default_interleaved(),
            Default::default(),
        )
    }

    /// Creates a new builder with the provided options and a capacity inferred by the provided
    /// iterator.
    pub fn with_capacity_and_options_from_iter<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl MultiPointTrait + 'a)>>,
        dim: Dimension,
        coord_type: CoordType,
        metadata: Arc<Metadata>,
    ) -> Self {
        let counter = MultiPointCapacity::from_multi_points(geoms);
        Self::with_capacity_and_options(dim, counter, coord_type, metadata)
    }

    /// Reserve more space in the underlying buffers with the capacity inferred from the provided
    /// geometries.
    pub fn reserve_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl MultiPointTrait + 'a)>>,
    ) {
        let counter = MultiPointCapacity::from_multi_points(geoms);
        self.reserve(counter)
    }

    /// Reserve more space in the underlying buffers with the capacity inferred from the provided
    /// geometries.
    pub fn reserve_exact_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl MultiPointTrait + 'a)>>,
    ) {
        let counter = MultiPointCapacity::from_multi_points(geoms);
        self.reserve_exact(counter)
    }

    /// Extend this builder with the given geometries
    pub fn extend_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl MultiPointTrait<T = f64> + 'a)>>,
    ) {
        geoms
            .into_iter()
            .try_for_each(|maybe_multi_point| self.push_multi_point(maybe_multi_point))
            .unwrap();
    }

    /// Extend this builder with the given geometries
    pub fn extend_from_geometry_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait<T = f64> + 'a)>>,
    ) -> Result<()> {
        geoms.into_iter().try_for_each(|g| self.push_geometry(g))?;
        Ok(())
    }

    /// Add a new Point to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    pub fn push_point(&mut self, value: Option<&impl PointTrait<T = f64>>) -> Result<()> {
        if let Some(point) = value {
            self.coords.push_point(point);
            self.try_push_length(1)?;
        } else {
            self.push_null();
        }

        Ok(())
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
            let num_points = multi_point.num_points();
            for point in multi_point.points() {
                self.coords.push_point(&point);
            }
            self.try_push_length(num_points)?;
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Add a new geometry to this builder
    ///
    /// This will error if the geometry type is not Point or MultiPoint.
    #[inline]
    pub fn push_geometry(&mut self, value: Option<&impl GeometryTrait<T = f64>>) -> Result<()> {
        if let Some(value) = value {
            match value.as_type() {
                GeometryType::Point(g) => self.push_point(Some(g))?,
                GeometryType::MultiPoint(g) => self.push_multi_point(Some(g))?,
                _ => return Err(GeoArrowError::General("Incorrect type".to_string())),
            }
        } else {
            self.push_null();
        };
        Ok(())
    }

    /// Push a raw coordinate to the underlying coordinate array.
    ///
    /// # Safety
    ///
    /// This is marked as unsafe because care must be taken to ensure that pushing raw coordinates
    /// to the array upholds the necessary invariants of the array.
    #[inline]
    pub unsafe fn push_coord(&mut self, coord: &impl CoordTrait<T = f64>) -> Result<()> {
        self.coords.try_push_coord(coord)
    }

    fn calculate_added_length(&self) -> Result<usize> {
        let total_length = self.coords.len();
        let offset = *self.geom_offsets.last() as usize;
        total_length
            .checked_sub(offset)
            .ok_or(GeoArrowError::Overflow)
    }

    /// Needs to be called when a valid value was extended to this array.
    /// This is a relatively low level function, prefer `try_push` when you can.
    #[inline]
    pub fn try_push_valid(&mut self) -> Result<()> {
        let length = self.calculate_added_length()?;
        self.try_push_length(length)
    }

    /// Needs to be called when a valid value was extended to this array.
    /// This is a relatively low level function, prefer `try_push` when you can.
    #[inline]
    pub(crate) fn try_push_length(&mut self, geom_offsets_length: usize) -> Result<()> {
        self.geom_offsets.try_push_usize(geom_offsets_length)?;
        self.validity.append(true);
        Ok(())
    }

    #[inline]
    pub(crate) fn push_null(&mut self) {
        self.geom_offsets.extend_constant(1);
        self.validity.append(false);
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_multi_points(
        geoms: &[impl MultiPointTrait<T = f64>],
        dim: Dimension,
        coord_type: CoordType,
        metadata: Arc<Metadata>,
    ) -> Self {
        let mut array = Self::with_capacity_and_options_from_iter(
            geoms.iter().map(Some),
            dim,
            coord_type,
            metadata,
        );
        array.extend_from_iter(geoms.iter().map(Some));
        array
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_nullable_multi_points(
        geoms: &[Option<impl MultiPointTrait<T = f64>>],
        dim: Dimension,
        coord_type: CoordType,
        metadata: Arc<Metadata>,
    ) -> Self {
        let mut array = Self::with_capacity_and_options_from_iter(
            geoms.iter().map(|x| x.as_ref()),
            dim,
            coord_type,
            metadata,
        );
        array.extend_from_iter(geoms.iter().map(|x| x.as_ref()));
        array
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_nullable_geometries(
        geoms: &[Option<impl GeometryTrait<T = f64>>],
        dim: Dimension,
        coord_type: CoordType,
        metadata: Arc<Metadata>,
    ) -> Result<Self> {
        let capacity = MultiPointCapacity::from_geometries(geoms.iter().map(|x| x.as_ref()))?;
        let mut array = Self::with_capacity_and_options(dim, capacity, coord_type, metadata);
        array.extend_from_geometry_iter(geoms.iter().map(|x| x.as_ref()))?;
        Ok(array)
    }
    pub(crate) fn from_wkb<W: OffsetSizeTrait>(
        wkb_objects: &[Option<WKB<'_, W>>],
        dim: Dimension,
        coord_type: CoordType,
        metadata: Arc<Metadata>,
    ) -> Result<Self> {
        let wkb_objects2 = wkb_objects
            .iter()
            .map(|maybe_wkb| maybe_wkb.as_ref().map(|wkb| wkb.parse()).transpose())
            .collect::<Result<Vec<_>>>()?;
        Self::from_nullable_geometries(&wkb_objects2, dim, coord_type, metadata)
    }
}

impl Default for MultiPointBuilder {
    fn default() -> Self {
        Self::new(Dimension::XY)
    }
}

impl GeometryArrayBuilder for MultiPointBuilder {
    fn new(dim: Dimension) -> Self {
        Self::new(dim)
    }

    fn with_geom_capacity_and_options(
        dim: Dimension,
        geom_capacity: usize,
        coord_type: CoordType,
        metadata: Arc<Metadata>,
    ) -> Self {
        let capacity = MultiPointCapacity::new(0, geom_capacity);
        Self::with_capacity_and_options(dim, capacity, coord_type, metadata)
    }

    fn push_geometry(&mut self, value: Option<&impl GeometryTrait<T = f64>>) -> Result<()> {
        self.push_geometry(value)
    }

    fn finish(self) -> Arc<dyn crate::NativeArray> {
        Arc::new(self.finish())
    }

    fn len(&self) -> usize {
        self.coords.len()
    }

    fn nulls(&self) -> &NullBufferBuilder {
        &self.validity
    }

    fn into_array_ref(self) -> ArrayRef {
        Arc::new(self.into_arrow())
    }

    fn coord_type(&self) -> CoordType {
        self.coords.coord_type()
    }

    fn set_metadata(&mut self, metadata: Arc<Metadata>) {
        self.metadata = metadata;
    }

    fn metadata(&self) -> Arc<Metadata> {
        self.metadata.clone()
    }
}

impl IntoArrow for MultiPointBuilder {
    type ArrowArray = GenericListArray<i32>;

    fn into_arrow(self) -> Self::ArrowArray {
        let arr: MultiPointArray = self.into();
        arr.into_arrow()
    }
}

impl From<MultiPointBuilder> for MultiPointArray {
    fn from(mut other: MultiPointBuilder) -> Self {
        let validity = other.validity.finish();

        // TODO: impl shrink_to_fit for all mutable -> * impls
        // other.coords.shrink_to_fit();
        other.geom_offsets.shrink_to_fit();

        Self::new(
            other.coords.into(),
            other.geom_offsets.into(),
            validity,
            other.metadata,
        )
    }
}

impl From<MultiPointBuilder> for GenericListArray<i32> {
    fn from(arr: MultiPointBuilder) -> Self {
        arr.into_arrow()
    }
}

impl<G: MultiPointTrait<T = f64>> From<(&[G], Dimension)> for MultiPointBuilder {
    fn from((geoms, dim): (&[G], Dimension)) -> Self {
        Self::from_multi_points(
            geoms,
            dim,
            CoordType::default_interleaved(),
            Default::default(),
        )
    }
}

impl<G: MultiPointTrait<T = f64>> From<(Vec<Option<G>>, Dimension)> for MultiPointBuilder {
    fn from((geoms, dim): (Vec<Option<G>>, Dimension)) -> Self {
        Self::from_nullable_multi_points(
            &geoms,
            dim,
            CoordType::default_interleaved(),
            Default::default(),
        )
    }
}

impl<O: OffsetSizeTrait> TryFrom<(WKBArray<O>, Dimension)> for MultiPointBuilder {
    type Error = GeoArrowError;

    fn try_from((value, dim): (WKBArray<O>, Dimension)) -> Result<Self> {
        let metadata = value.data_type.metadata().clone();
        let wkb_objects: Vec<Option<WKB<'_, O>>> = value.iter().collect();
        Self::from_wkb(
            &wkb_objects,
            dim,
            CoordType::default_interleaved(),
            metadata,
        )
    }
}

/// LineString and MultiPoint have the same layout, so enable conversions between the two to change
/// the semantic type
impl From<MultiPointBuilder> for LineStringBuilder {
    fn from(value: MultiPointBuilder) -> Self {
        Self::try_new(
            value.coords,
            value.geom_offsets,
            value.validity,
            value.metadata,
        )
        .unwrap()
    }
}
