use core::f64;
use std::sync::Arc;

use crate::array::metadata::ArrayMetadata;
// use super::array::check;
use crate::array::{
    CoordBufferBuilder, CoordType, InterleavedCoordBufferBuilder, PointArray,
    SeparatedCoordBufferBuilder, WKBArray,
};
use crate::datatypes::Dimension;
use crate::error::{GeoArrowError, Result};
use crate::scalar::WKB;
use crate::trait_::{ArrayAccessor, GeometryArrayBuilder, IntoArrow};
use arrow_array::{ArrayRef, OffsetSizeTrait};
use arrow_buffer::NullBufferBuilder;
use geo_traits::{CoordTrait, GeometryTrait, GeometryType, MultiPointTrait, PointTrait};

/// The GeoArrow equivalent to `Vec<Option<Point>>`: a mutable collection of Points.
///
/// Converting an [`PointBuilder`] into a [`PointArray`] is `O(1)`.
#[derive(Debug)]
pub struct PointBuilder {
    metadata: Arc<ArrayMetadata>,
    pub(crate) coords: CoordBufferBuilder,
    pub(crate) validity: NullBufferBuilder,
}

impl PointBuilder {
    /// Creates a new empty [`PointBuilder`].
    pub fn new(dim: Dimension) -> Self {
        Self::new_with_options(dim, Default::default(), Default::default())
    }

    /// Creates a new empty [`PointBuilder`] with the provided options.
    pub fn new_with_options(
        dim: Dimension,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        Self::with_capacity_and_options(dim, 0, coord_type, metadata)
    }

    /// Creates a new [`PointBuilder`] with a capacity.
    pub fn with_capacity(dim: Dimension, capacity: usize) -> Self {
        Self::with_capacity_and_options(dim, capacity, Default::default(), Default::default())
    }

    /// Creates a new empty [`PointBuilder`] with the provided capacity and options.
    pub fn with_capacity_and_options(
        dim: Dimension,
        capacity: usize,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        let coords = match coord_type {
            CoordType::Interleaved => CoordBufferBuilder::Interleaved(
                InterleavedCoordBufferBuilder::with_capacity(capacity, dim),
            ),
            CoordType::Separated => CoordBufferBuilder::Separated(
                SeparatedCoordBufferBuilder::with_capacity(capacity, dim),
            ),
        };
        Self {
            coords,
            validity: NullBufferBuilder::new(capacity),
            metadata,
        }
    }

    /// Reserves capacity for at least `additional` more points to be inserted
    /// in the given `Vec<T>`. The collection may reserve more space to
    /// speculatively avoid frequent reallocations. After calling `reserve`,
    /// capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, additional: usize) {
        self.coords.reserve(additional);
    }

    /// Reserves the minimum capacity for at least `additional` more points.
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
    pub fn reserve_exact(&mut self, additional: usize) {
        self.coords.reserve_exact(additional);
    }

    /// The canonical method to create a [`PointBuilder`] out of its internal components.
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Errors
    ///
    /// This function errors iff:
    ///
    /// - The validity is not `None` and its length is different from the number of geometries
    pub fn try_new(
        coords: CoordBufferBuilder,
        validity: NullBufferBuilder,
        metadata: Arc<ArrayMetadata>,
    ) -> Result<Self> {
        // check(&coords.clone().into(), validity.as_ref().map(|x| x.len()))?;
        Ok(Self {
            coords,
            validity,
            metadata,
        })
    }

    /// Extract the low-level APIs from the [`PointBuilder`].
    pub fn into_inner(self) -> (CoordBufferBuilder, NullBufferBuilder) {
        (self.coords, self.validity)
    }

    /// Consume the builder and convert to an immutable [`PointArray`]
    pub fn finish(self) -> PointArray {
        self.into()
    }

    /// Add a new coord to the end of this array, where the coord is a non-empty point
    ///
    /// ## Panics
    ///
    /// - If the added coordinate does not have the same dimension as the point array.
    #[inline]
    pub fn push_coord(&mut self, value: Option<&impl CoordTrait<T = f64>>) {
        self.try_push_coord(value).unwrap()
    }

    /// Add a new point to the end of this array.
    ///
    /// ## Panics
    ///
    /// - If the added point does not have the same dimension as the point array.
    #[inline]
    pub fn push_point(&mut self, value: Option<&impl PointTrait<T = f64>>) {
        self.try_push_point(value).unwrap()
    }

    /// Add a new coord to the end of this array, where the coord is a non-empty point
    ///
    /// ## Errors
    ///
    /// - If the added coordinate does not have the same dimension as the point array.
    #[inline]
    pub fn try_push_coord(&mut self, value: Option<&impl CoordTrait<T = f64>>) -> Result<()> {
        if let Some(value) = value {
            self.coords.try_push_coord(value)?;
            self.validity.append(true);
        } else {
            self.push_null()
        };
        Ok(())
    }

    /// Add a new point to the end of this array.
    ///
    /// ## Errors
    ///
    /// - If the added point does not have the same dimension as the point array.
    #[inline]
    pub fn try_push_point(&mut self, value: Option<&impl PointTrait<T = f64>>) -> Result<()> {
        if let Some(value) = value {
            self.coords.try_push_point(value)?;
            self.validity.append(true);
        } else {
            self.push_null()
        };
        Ok(())
    }

    /// Add a valid but empty point to the end of this array.
    #[inline]
    pub fn push_empty(&mut self) {
        self.coords.push_nan_coord();
        self.validity.append_non_null();
    }

    /// Add a new null value to the end of this array.
    #[inline]
    pub fn push_null(&mut self) {
        self.coords.push_nan_coord();
        self.validity.append_null();
    }

    /// Add a new geometry to this builder
    ///
    /// This will error if the geometry type is not Point or a MultiPoint with length 1.
    #[inline]
    pub fn push_geometry(&mut self, value: Option<&impl GeometryTrait<T = f64>>) -> Result<()> {
        if let Some(value) = value {
            match value.as_type() {
                GeometryType::Point(p) => self.push_point(Some(p)),
                GeometryType::MultiPoint(mp) => {
                    if mp.num_points() == 1 {
                        self.push_point(Some(&mp.point(0).unwrap()))
                    } else {
                        return Err(GeoArrowError::General("Incorrect type".to_string()));
                    }
                }
                _ => return Err(GeoArrowError::General("Incorrect type".to_string())),
            }
        } else {
            self.push_null()
        };
        Ok(())
    }

    /// Extend this builder with the given geometries
    pub fn extend_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl PointTrait<T = f64> + 'a)>>,
    ) {
        geoms
            .into_iter()
            .for_each(|maybe_polygon| self.push_point(maybe_polygon));
    }

    /// Extend this builder with the given geometries
    pub fn extend_from_geometry_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait<T = f64> + 'a)>>,
    ) -> Result<()> {
        geoms.into_iter().try_for_each(|g| self.push_geometry(g))?;
        Ok(())
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_points<'a>(
        geoms: impl ExactSizeIterator<Item = &'a (impl PointTrait<T = f64> + 'a)>,
        dim: Dimension,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        let mut mutable_array =
            Self::with_capacity_and_options(dim, geoms.len(), coord_type, metadata);
        geoms
            .into_iter()
            .for_each(|maybe_point| mutable_array.push_point(Some(maybe_point)));
        mutable_array
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_nullable_points<'a>(
        geoms: impl ExactSizeIterator<Item = Option<&'a (impl PointTrait<T = f64> + 'a)>>,
        dim: Dimension,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        let mut mutable_array =
            Self::with_capacity_and_options(dim, geoms.len(), coord_type, metadata);
        geoms
            .into_iter()
            .for_each(|maybe_point| mutable_array.push_point(maybe_point));
        mutable_array
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_nullable_geometries(
        geoms: &[Option<impl GeometryTrait<T = f64>>],
        dim: Dimension,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
    ) -> Result<Self> {
        let capacity = geoms.len();
        let mut array = Self::with_capacity_and_options(dim, capacity, coord_type, metadata);
        array.extend_from_geometry_iter(geoms.iter().map(|x| x.as_ref()))?;
        Ok(array)
    }

    pub(crate) fn from_wkb<O: OffsetSizeTrait>(
        wkb_objects: &[Option<WKB<'_, O>>],
        dim: Dimension,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
    ) -> Result<Self> {
        let wkb_objects2 = wkb_objects
            .iter()
            .map(|maybe_wkb| maybe_wkb.as_ref().map(|wkb| wkb.parse()).transpose())
            .collect::<Result<Vec<_>>>()?;
        Self::from_nullable_geometries(&wkb_objects2, dim, coord_type, metadata)
    }
}

impl GeometryArrayBuilder for PointBuilder {
    fn new(dim: Dimension) -> Self {
        Self::new(dim)
    }

    fn with_geom_capacity_and_options(
        dim: Dimension,
        geom_capacity: usize,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        Self::with_capacity_and_options(dim, geom_capacity, coord_type, metadata)
    }

    fn set_metadata(&mut self, metadata: Arc<ArrayMetadata>) {
        self.metadata = metadata;
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
        self.into_arrow()
    }

    fn coord_type(&self) -> CoordType {
        self.coords.coord_type()
    }

    fn metadata(&self) -> Arc<ArrayMetadata> {
        self.metadata.clone()
    }
}

impl Default for PointBuilder {
    fn default() -> Self {
        Self::new(Dimension::XY)
    }
}

impl IntoArrow for PointBuilder {
    type ArrowArray = ArrayRef;

    fn into_arrow(self) -> Self::ArrowArray {
        let point_array: PointArray = self.into();
        point_array.into_arrow()
    }
}

impl From<PointBuilder> for PointArray {
    fn from(mut other: PointBuilder) -> Self {
        let validity = other.validity.finish();
        Self::new(other.coords.into(), validity, other.metadata)
    }
}

impl From<PointBuilder> for ArrayRef {
    fn from(arr: PointBuilder) -> Self {
        arr.into_array_ref()
    }
}

impl<G: PointTrait<T = f64>> From<(&[G], Dimension)> for PointBuilder {
    fn from((value, dim): (&[G], Dimension)) -> Self {
        PointBuilder::from_points(value.iter(), dim, Default::default(), Default::default())
    }
}

impl<G: PointTrait<T = f64>> From<(Vec<Option<G>>, Dimension)> for PointBuilder {
    fn from((geoms, dim): (Vec<Option<G>>, Dimension)) -> Self {
        PointBuilder::from_nullable_points(
            geoms.iter().map(|x| x.as_ref()),
            dim,
            Default::default(),
            Default::default(),
        )
    }
}

impl<O: OffsetSizeTrait> TryFrom<(WKBArray<O>, Dimension)> for PointBuilder {
    type Error = GeoArrowError;

    fn try_from((value, dim): (WKBArray<O>, Dimension)) -> Result<Self> {
        let metadata = value.metadata.clone();
        let wkb_objects: Vec<Option<WKB<'_, O>>> = value.iter().collect();
        Self::from_wkb(&wkb_objects, dim, Default::default(), metadata)
    }
}
