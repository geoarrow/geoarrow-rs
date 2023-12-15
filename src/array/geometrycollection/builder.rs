use std::sync::Arc;

use arrow_array::{Array, GenericListArray, OffsetSizeTrait};
use arrow_buffer::NullBufferBuilder;

use crate::array::geometrycollection::GeometryCollectionCapacity;
use crate::array::offset_builder::OffsetsBuilder;
use crate::array::{CoordType, GeometryCollectionArray, MixedGeometryBuilder};
use crate::error::Result;
use crate::geo_traits::GeometryCollectionTrait;
use crate::trait_::{GeometryArrayBuilder, IntoArrow};

#[derive(Debug)]
pub struct GeometryCollectionBuilder<O: OffsetSizeTrait> {
    pub(crate) geoms: MixedGeometryBuilder<O>,

    pub(crate) geom_offsets: OffsetsBuilder<O>,

    pub(crate) validity: NullBufferBuilder,
}

impl<'a, O: OffsetSizeTrait> GeometryCollectionBuilder<O> {
    /// Creates a new empty [`GeometryCollectionBuilder`].
    pub fn new() -> Self {
        Self::new_with_options(Default::default())
    }

    pub fn new_with_options(coord_type: CoordType) -> Self {
        Self::with_capacity_and_options(Default::default(), coord_type)
    }

    pub fn with_capacity(capacity: GeometryCollectionCapacity) -> Self {
        Self::with_capacity_and_options(capacity, Default::default())
    }

    pub fn with_capacity_and_options(
        capacity: GeometryCollectionCapacity,
        coord_type: CoordType,
    ) -> Self {
        Self {
            geoms: MixedGeometryBuilder::with_capacity_and_options(
                capacity.mixed_capacity,
                coord_type,
            ),
            geom_offsets: OffsetsBuilder::with_capacity(capacity.geom_capacity),
            validity: NullBufferBuilder::new(capacity.geom_capacity),
        }
    }

    pub fn with_capacity_from_iter(
        geoms: impl Iterator<Item = Option<&'a (impl GeometryCollectionTrait + 'a)>>,
    ) -> Self {
        Self::with_capacity_and_options_from_iter(geoms, Default::default())
    }

    pub fn with_capacity_and_options_from_iter(
        geoms: impl Iterator<Item = Option<&'a (impl GeometryCollectionTrait + 'a)>>,
        coord_type: CoordType,
    ) -> Self {
        let counter = GeometryCollectionCapacity::from_geometry_collections(geoms);
        Self::with_capacity_and_options(counter, coord_type)
    }

    pub fn reserve_from_iter(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl GeometryCollectionTrait + 'a)>>,
    ) {
        let counter = GeometryCollectionCapacity::from_geometry_collections(geoms);
        self.reserve(counter)
    }

    pub fn reserve_exact_from_iter(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl GeometryCollectionTrait + 'a)>>,
    ) {
        let counter = GeometryCollectionCapacity::from_geometry_collections(geoms);
        self.reserve_exact(counter)
    }

    /// Reserves capacity for at least `additional` more LineStrings to be inserted
    /// in the given `Vec<T>`. The collection may reserve more space to
    /// speculatively avoid frequent reallocations. After calling `reserve`,
    /// capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, additional: GeometryCollectionCapacity) {
        self.geoms.reserve(additional.mixed_capacity);
        self.geom_offsets.reserve(additional.geom_capacity);
    }

    /// Reserves the minimum capacity for at least `additional` more LineStrings to
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
    pub fn reserve_exact(&mut self, additional: GeometryCollectionCapacity) {
        self.geoms.reserve_exact(additional.mixed_capacity);
        self.geom_offsets.reserve_exact(additional.geom_capacity);
    }

    /// Extract the low-level APIs from the [`LineStringBuilder`].
    pub fn into_inner(
        self,
    ) -> (
        MixedGeometryBuilder<O>,
        OffsetsBuilder<O>,
        NullBufferBuilder,
    ) {
        (self.geoms, self.geom_offsets, self.validity)
    }

    pub fn push_geometry_collection(
        &mut self,
        value: Option<&impl GeometryCollectionTrait<T = f64>>,
        prefer_multi: bool,
    ) -> Result<()> {
        if let Some(gc) = value {
            let num_geoms = gc.num_geometries();
            for g_idx in 0..num_geoms {
                let g = gc.geometry(g_idx).unwrap();
                self.geoms.push_geometry(Some(&g), prefer_multi)?;
            }
            self.try_push_length(num_geoms)?;
        } else {
            self.push_null();
        }
        Ok(())
    }

    pub fn extend_from_iter(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl GeometryCollectionTrait<T = f64> + 'a)>>,
        prefer_multi: bool,
    ) {
        geoms
            .into_iter()
            .try_for_each(|maybe_gc| self.push_geometry_collection(maybe_gc, prefer_multi))
            .unwrap();
    }

    #[inline]
    pub fn try_push_length(&mut self, geom_offsets_length: usize) -> Result<()> {
        self.geom_offsets.try_push_usize(geom_offsets_length)?;
        self.validity.append(true);
        Ok(())
    }

    #[inline]
    pub(crate) fn push_null(&mut self) {
        self.geom_offsets.extend_constant(1);
        self.validity.append(false);
    }

    pub fn from_geometry_collections(
        geoms: &[impl GeometryCollectionTrait<T = f64>],
        coord_type: Option<CoordType>,
        prefer_multi: bool,
    ) -> Self {
        let mut array = Self::with_capacity_and_options_from_iter(
            geoms.iter().map(Some),
            coord_type.unwrap_or_default(),
        );
        array.extend_from_iter(geoms.iter().map(Some), prefer_multi);
        array
    }

    pub fn from_nullable_geometry_collections(
        geoms: &[Option<impl GeometryCollectionTrait<T = f64>>],
        coord_type: Option<CoordType>,
        prefer_multi: bool,
    ) -> Self {
        let mut array = Self::with_capacity_and_options_from_iter(
            geoms.iter().map(|x| x.as_ref()),
            coord_type.unwrap_or_default(),
        );
        array.extend_from_iter(geoms.iter().map(|x| x.as_ref()), prefer_multi);
        array
    }

    pub fn finish(self) -> GeometryCollectionArray<O> {
        self.into()
    }
}

impl<O: OffsetSizeTrait> GeometryArrayBuilder for GeometryCollectionBuilder<O> {
    fn len(&self) -> usize {
        self.geom_offsets.len_proxy()
    }

    fn validity(&self) -> &NullBufferBuilder {
        &self.validity
    }

    fn into_array_ref(self) -> Arc<dyn Array> {
        Arc::new(self.into_arrow())
    }
}

impl<O: OffsetSizeTrait> IntoArrow for GeometryCollectionBuilder<O> {
    type ArrowArray = GenericListArray<O>;

    fn into_arrow(self) -> Self::ArrowArray {
        let linestring_arr: GeometryCollectionArray<O> = self.into();
        linestring_arr.into_arrow()
    }
}

impl<O: OffsetSizeTrait> Default for GeometryCollectionBuilder<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: OffsetSizeTrait> From<GeometryCollectionBuilder<O>> for GeometryCollectionArray<O> {
    fn from(other: GeometryCollectionBuilder<O>) -> Self {
        let validity = other.validity.finish_cloned();
        Self::new(other.geoms.into(), other.geom_offsets.into(), validity)
    }
}

impl<O: OffsetSizeTrait> From<GeometryCollectionBuilder<O>> for GenericListArray<O> {
    fn from(arr: GeometryCollectionBuilder<O>) -> Self {
        arr.into_arrow()
    }
}

impl<O: OffsetSizeTrait, G: GeometryCollectionTrait<T = f64>> From<&[G]>
    for GeometryCollectionBuilder<O>
{
    fn from(geoms: &[G]) -> Self {
        Self::from_geometry_collections(geoms, Default::default(), true)
    }
}

impl<O: OffsetSizeTrait, G: GeometryCollectionTrait<T = f64>> From<Vec<Option<G>>>
    for GeometryCollectionBuilder<O>
{
    fn from(geoms: Vec<Option<G>>) -> Self {
        Self::from_nullable_geometry_collections(&geoms, Default::default(), true)
    }
}

impl<O: OffsetSizeTrait, G: GeometryCollectionTrait<T = f64>> From<bumpalo::collections::Vec<'_, G>>
    for GeometryCollectionBuilder<O>
{
    fn from(geoms: bumpalo::collections::Vec<'_, G>) -> Self {
        Self::from_geometry_collections(&geoms, Default::default(), true)
    }
}

impl<O: OffsetSizeTrait, G: GeometryCollectionTrait<T = f64>>
    From<bumpalo::collections::Vec<'_, Option<G>>> for GeometryCollectionBuilder<O>
{
    fn from(geoms: bumpalo::collections::Vec<'_, Option<G>>) -> Self {
        Self::from_nullable_geometry_collections(&geoms, Default::default(), true)
    }
}
