use core::f64;
use std::sync::Arc;

use crate::array::metadata::ArrayMetadata;
// use super::array::check;
use crate::array::{
    CoordBufferBuilder, CoordType, InterleavedCoordBufferBuilder, PointArray,
    SeparatedCoordBufferBuilder, WKBArray,
};
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::{CoordTrait, GeometryTrait, GeometryType, MultiPointTrait, PointTrait};
use crate::io::wkb::reader::WKBPoint;
use crate::scalar::WKB;
use crate::trait_::{ArrayAccessor, GeometryArrayBuilder, IntoArrow};
use arrow_array::{Array, OffsetSizeTrait};
use arrow_buffer::NullBufferBuilder;

/// The GeoArrow equivalent to `Vec<Option<Point>>`: a mutable collection of Points.
///
/// Converting an [`PointBuilder`] into a [`PointArray`] is `O(1)`.
#[derive(Debug)]
pub struct PointBuilder<const D: usize> {
    metadata: Arc<ArrayMetadata>,
    pub coords: CoordBufferBuilder<D>,
    pub validity: NullBufferBuilder,
}

impl<const D: usize> PointBuilder<D> {
    /// Creates a new empty [`PointBuilder`].
    pub fn new() -> Self {
        Self::new_with_options(Default::default(), Default::default())
    }

    pub fn new_with_options(coord_type: CoordType, metadata: Arc<ArrayMetadata>) -> Self {
        Self::with_capacity_and_options(0, coord_type, metadata)
    }

    /// Creates a new [`PointBuilder`] with a capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_options(capacity, Default::default(), Default::default())
    }

    /// Creates a new [`PointBuilder`] with a capacity.
    pub fn with_capacity_and_options(
        capacity: usize,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        let coords = match coord_type {
            CoordType::Interleaved => CoordBufferBuilder::Interleaved(
                InterleavedCoordBufferBuilder::with_capacity(capacity),
            ),
            CoordType::Separated => {
                CoordBufferBuilder::Separated(SeparatedCoordBufferBuilder::with_capacity(capacity))
            }
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

    /// Reserves the minimum capacity for at least `additional` more points to
    /// be inserted in the given `Vec<T>`. Unlike [`reserve`], this will not
    /// deliberately over-allocate to speculatively avoid frequent allocations.
    /// After calling `reserve_exact`, capacity will be greater than or equal to
    /// `self.len() + additional`. Does nothing if the capacity is already
    /// sufficient.
    ///
    /// Note that the allocator may give the collection more space than it
    /// requests. Therefore, capacity can not be relied upon to be precisely
    /// minimal. Prefer [`reserve`] if future insertions are expected.
    ///
    /// [`reserve`]: Vec::reserve
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
        coords: CoordBufferBuilder<D>,
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
    pub fn into_inner(self) -> (CoordBufferBuilder<D>, NullBufferBuilder) {
        (self.coords, self.validity)
    }

    pub fn finish(self) -> PointArray<D> {
        self.into()
    }

    /// Add a new coord to the end of this array, where the coord is a non-empty point
    #[inline]
    pub fn push_coord(&mut self, value: Option<&impl CoordTrait<T = f64>>) {
        if let Some(value) = value {
            self.coords.push_coord(value);
            self.validity.append(true);
        } else {
            self.push_null()
        }
    }

    /// Add a new point to the end of this array.
    #[inline]
    pub fn push_point(&mut self, value: Option<&impl PointTrait<T = f64>>) {
        if let Some(value) = value {
            self.coords.push_point(value);
            self.validity.append(true);
        } else {
            self.push_null()
        }
    }

    /// Add a valid but empty point to the end of this array.
    #[inline]
    pub fn push_empty(&mut self) {
        self.coords.push(core::array::from_fn(|_| f64::NAN));
        self.validity.append_non_null();
    }

    /// Add a new null value to the end of this array.
    #[inline]
    pub fn push_null(&mut self) {
        self.coords.push(core::array::from_fn(|_| 0.));
        self.validity.append_null();
    }

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

    pub fn extend_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl PointTrait<T = f64> + 'a)>>,
    ) {
        geoms
            .into_iter()
            .for_each(|maybe_polygon| self.push_point(maybe_polygon));
    }

    pub fn from_points<'a>(
        geoms: impl ExactSizeIterator<Item = &'a (impl PointTrait<T = f64> + 'a)>,
        coord_type: Option<CoordType>,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        let mut mutable_array =
            Self::with_capacity_and_options(geoms.len(), coord_type.unwrap_or_default(), metadata);
        geoms
            .into_iter()
            .for_each(|maybe_point| mutable_array.push_point(Some(maybe_point)));
        mutable_array
    }

    pub fn from_nullable_points<'a>(
        geoms: impl ExactSizeIterator<Item = Option<&'a (impl PointTrait<T = f64> + 'a)>>,
        coord_type: Option<CoordType>,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        let mut mutable_array =
            Self::with_capacity_and_options(geoms.len(), coord_type.unwrap_or_default(), metadata);
        geoms
            .into_iter()
            .for_each(|maybe_point| mutable_array.push_point(maybe_point));
        mutable_array
    }

    pub(crate) fn from_wkb<O: OffsetSizeTrait>(
        wkb_objects: &[Option<WKB<'_, O>>],
        coord_type: Option<CoordType>,
        metadata: Arc<ArrayMetadata>,
    ) -> Result<Self> {
        let wkb_objects2: Vec<Option<WKBPoint>> = wkb_objects
            .iter()
            .map(|maybe_wkb| {
                maybe_wkb
                    .as_ref()
                    .map(|wkb| wkb.to_wkb_object().into_point())
            })
            .collect();
        Ok(Self::from_nullable_points(
            wkb_objects2.iter().map(|x| x.as_ref()),
            coord_type,
            metadata,
        ))
    }
}

impl<const D: usize> GeometryArrayBuilder for PointBuilder<D> {
    fn new() -> Self {
        Self::new()
    }

    fn with_geom_capacity_and_options(
        geom_capacity: usize,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        Self::with_capacity_and_options(geom_capacity, coord_type, metadata)
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

    fn into_array_ref(self) -> Arc<dyn Array> {
        self.into_arrow()
    }

    fn coord_type(&self) -> CoordType {
        self.coords.coord_type()
    }

    fn metadata(&self) -> Arc<ArrayMetadata> {
        self.metadata.clone()
    }
}

impl<const D: usize> Default for PointBuilder<D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const D: usize> IntoArrow for PointBuilder<D> {
    type ArrowArray = Arc<dyn Array>;

    fn into_arrow(self) -> Self::ArrowArray {
        let point_array: PointArray<D> = self.into();
        point_array.into_arrow()
    }
}

impl<const D: usize> From<PointBuilder<D>> for PointArray<D> {
    fn from(mut other: PointBuilder<D>) -> Self {
        let validity = other.validity.finish();
        Self::new(other.coords.into(), validity, other.metadata)
    }
}

impl<const D: usize> From<PointBuilder<D>> for Arc<dyn Array> {
    fn from(arr: PointBuilder<D>) -> Self {
        arr.into_array_ref()
    }
}

impl<const D: usize, G: PointTrait<T = f64>> From<&[G]> for PointBuilder<D> {
    fn from(value: &[G]) -> Self {
        PointBuilder::from_points(value.iter(), Default::default(), Default::default())
    }
}

impl<const D: usize, G: PointTrait<T = f64>> From<Vec<Option<G>>> for PointBuilder<D> {
    fn from(geoms: Vec<Option<G>>) -> Self {
        PointBuilder::from_nullable_points(
            geoms.iter().map(|x| x.as_ref()),
            Default::default(),
            Default::default(),
        )
    }
}

impl<const D: usize, O: OffsetSizeTrait> TryFrom<WKBArray<O>> for PointBuilder<D> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self> {
        let metadata = value.metadata.clone();
        let wkb_objects: Vec<Option<WKB<'_, O>>> = value.iter().collect();
        Self::from_wkb(&wkb_objects, Default::default(), metadata)
    }
}
