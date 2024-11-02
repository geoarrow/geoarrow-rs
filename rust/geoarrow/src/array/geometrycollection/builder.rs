use std::sync::Arc;

use arrow_array::{Array, GenericListArray, OffsetSizeTrait};
use arrow_buffer::NullBufferBuilder;

use crate::array::geometrycollection::GeometryCollectionCapacity;
use crate::array::metadata::ArrayMetadata;
use crate::array::mixed::builder::DEFAULT_PREFER_MULTI;
use crate::array::offset_builder::OffsetsBuilder;
use crate::array::{CoordType, GeometryCollectionArray, MixedGeometryBuilder, WKBArray};
use crate::error::{GeoArrowError, Result};
use crate::io::wkb::reader::WKBGeometry;
use crate::scalar::WKB;
use crate::trait_::{ArrayAccessor, GeometryArrayBuilder, IntoArrow};
use geo_traits::{
    GeometryCollectionTrait, GeometryTrait, LineStringTrait, MultiLineStringTrait, MultiPointTrait,
    MultiPolygonTrait, PointTrait, PolygonTrait,
};

/// The GeoArrow equivalent to `Vec<Option<GeometryCollection>>`: a mutable collection of
/// GeometryCollections.
///
/// Converting an [`GeometryCollectionBuilder`] into a [`GeometryCollectionArray`] is `O(1)`.
#[derive(Debug)]
pub struct GeometryCollectionBuilder<const D: usize> {
    metadata: Arc<ArrayMetadata>,

    pub(crate) geoms: MixedGeometryBuilder<D>,

    pub(crate) geom_offsets: OffsetsBuilder<i32>,

    pub(crate) validity: NullBufferBuilder,
}

impl<'a, const D: usize> GeometryCollectionBuilder<D> {
    /// Creates a new empty [`GeometryCollectionBuilder`].
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

    pub fn with_capacity(capacity: GeometryCollectionCapacity) -> Self {
        Self::with_capacity_and_options(
            capacity,
            Default::default(),
            Default::default(),
            DEFAULT_PREFER_MULTI,
        )
    }

    pub fn with_capacity_and_options(
        capacity: GeometryCollectionCapacity,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
        prefer_multi: bool,
    ) -> Self {
        // Should we be storing array metadata on child arrays?
        Self {
            geoms: MixedGeometryBuilder::with_capacity_and_options(
                capacity.mixed_capacity,
                coord_type,
                metadata.clone(),
                prefer_multi,
            ),
            geom_offsets: OffsetsBuilder::with_capacity(capacity.geom_capacity),
            validity: NullBufferBuilder::new(capacity.geom_capacity),
            metadata,
        }
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

    /// Extract the low-level APIs from the [`GeometryCollectionBuilder`].
    pub fn into_inner(
        self,
    ) -> (
        MixedGeometryBuilder<D>,
        OffsetsBuilder<i32>,
        NullBufferBuilder,
    ) {
        (self.geoms, self.geom_offsets, self.validity)
    }

    pub fn finish(self) -> GeometryCollectionArray<D> {
        self.into()
    }

    pub fn with_capacity_from_iter(
        geoms: impl Iterator<Item = Option<&'a (impl GeometryCollectionTrait + 'a)>>,
    ) -> Result<Self> {
        Self::with_capacity_and_options_from_iter(
            geoms,
            Default::default(),
            Default::default(),
            DEFAULT_PREFER_MULTI,
        )
    }

    pub fn with_capacity_and_options_from_iter(
        geoms: impl Iterator<Item = Option<&'a (impl GeometryCollectionTrait + 'a)>>,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
        prefer_multi: bool,
    ) -> Result<Self> {
        let counter = GeometryCollectionCapacity::from_geometry_collections(geoms)?;
        Ok(Self::with_capacity_and_options(
            counter,
            coord_type,
            metadata,
            prefer_multi,
        ))
    }

    pub fn reserve_from_iter(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl GeometryCollectionTrait + 'a)>>,
    ) -> Result<()> {
        let counter = GeometryCollectionCapacity::from_geometry_collections(geoms)?;
        self.reserve(counter);
        Ok(())
    }

    pub fn reserve_exact_from_iter(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl GeometryCollectionTrait + 'a)>>,
    ) -> Result<()> {
        let counter = GeometryCollectionCapacity::from_geometry_collections(geoms)?;
        self.reserve_exact(counter);
        Ok(())
    }

    /// Push a Point onto the end of this builder
    #[inline]
    pub fn push_point(&mut self, value: Option<&impl PointTrait<T = f64>>) -> Result<()> {
        self.geoms.push_point(value)?;
        self.geom_offsets.try_push_usize(1)?;
        self.validity.append(value.is_some());
        Ok(())
    }

    /// Push a LineString onto the end of this builder
    #[inline]
    pub fn push_line_string(
        &mut self,
        value: Option<&impl LineStringTrait<T = f64>>,
    ) -> Result<()> {
        self.geoms.push_line_string(value)?;
        self.geom_offsets.try_push_usize(1)?;
        self.validity.append(value.is_some());
        Ok(())
    }

    /// Push a Polygon onto the end of this builder
    #[inline]
    pub fn push_polygon(&mut self, value: Option<&impl PolygonTrait<T = f64>>) -> Result<()> {
        self.geoms.push_polygon(value)?;
        self.geom_offsets.try_push_usize(1)?;
        self.validity.append(value.is_some());
        Ok(())
    }

    /// Push a MultiPoint onto the end of this builder
    #[inline]
    pub fn push_multi_point(
        &mut self,
        value: Option<&impl MultiPointTrait<T = f64>>,
    ) -> Result<()> {
        self.geoms.push_multi_point(value)?;
        self.geom_offsets.try_push_usize(1)?;
        self.validity.append(value.is_some());
        Ok(())
    }

    /// Push a MultiLineString onto the end of this builder
    #[inline]
    pub fn push_multi_line_string(
        &mut self,
        value: Option<&impl MultiLineStringTrait<T = f64>>,
    ) -> Result<()> {
        self.geoms.push_multi_line_string(value)?;
        self.geom_offsets.try_push_usize(1)?;
        self.validity.append(value.is_some());
        Ok(())
    }

    /// Push a MultiPolygon onto the end of this builder
    #[inline]
    pub fn push_multi_polygon(
        &mut self,
        value: Option<&impl MultiPolygonTrait<T = f64>>,
    ) -> Result<()> {
        self.geoms.push_multi_polygon(value)?;
        self.geom_offsets.try_push_usize(1)?;
        self.validity.append(value.is_some());
        Ok(())
    }

    /// Push a Geometry onto the end of this builder
    #[inline]
    pub fn push_geometry(&mut self, value: Option<&impl GeometryTrait<T = f64>>) -> Result<()> {
        use geo_traits::GeometryType::*;

        if let Some(g) = value {
            match g.as_type() {
                Point(p) => self.push_point(Some(p))?,
                LineString(p) => {
                    self.push_line_string(Some(p))?;
                }
                Polygon(p) => self.push_polygon(Some(p))?,
                MultiPoint(p) => self.push_multi_point(Some(p))?,
                MultiLineString(p) => self.push_multi_line_string(Some(p))?,
                MultiPolygon(p) => self.push_multi_polygon(Some(p))?,
                GeometryCollection(p) => self.push_geometry_collection(Some(p))?,
                Rect(_) | Triangle(_) | Line(_) => todo!(),
            }
        } else {
            self.push_null();
        };
        Ok(())
    }

    /// Push a GeometryCollection onto the end of this builder
    #[inline]
    pub fn push_geometry_collection(
        &mut self,
        value: Option<&impl GeometryCollectionTrait<T = f64>>,
    ) -> Result<()> {
        if let Some(gc) = value {
            let num_geoms = gc.num_geometries();
            for g in gc.geometries() {
                self.geoms.push_geometry(Some(&g))?;
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
    ) {
        geoms
            .into_iter()
            .try_for_each(|maybe_gc| self.push_geometry_collection(maybe_gc))
            .unwrap();
    }

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

    pub fn from_geometry_collections(
        geoms: &[impl GeometryCollectionTrait<T = f64>],
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

    pub fn from_nullable_geometry_collections(
        geoms: &[Option<impl GeometryCollectionTrait<T = f64>>],
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

    pub fn from_geometries(
        geoms: &[impl GeometryTrait<T = f64>],
        coord_type: Option<CoordType>,
        metadata: Arc<ArrayMetadata>,
        prefer_multi: bool,
    ) -> Result<Self> {
        let capacity = GeometryCollectionCapacity::from_geometries(geoms.iter().map(Some))?;
        let mut array = Self::with_capacity_and_options(
            capacity,
            coord_type.unwrap_or_default(),
            metadata,
            prefer_multi,
        );
        for geom in geoms {
            array.push_geometry(Some(geom))?;
        }
        Ok(array)
    }

    pub fn from_nullable_geometries(
        geoms: &[Option<impl GeometryTrait<T = f64>>],
        coord_type: Option<CoordType>,
        metadata: Arc<ArrayMetadata>,
        prefer_multi: bool,
    ) -> Result<Self> {
        let capacity =
            GeometryCollectionCapacity::from_geometries(geoms.iter().map(|x| x.as_ref()))?;
        let mut array = Self::with_capacity_and_options(
            capacity,
            coord_type.unwrap_or_default(),
            metadata,
            prefer_multi,
        );
        for geom in geoms {
            array.push_geometry(geom.as_ref())?;
        }
        Ok(array)
    }

    pub(crate) fn from_wkb<W: OffsetSizeTrait>(
        wkb_objects: &[Option<WKB<'_, W>>],
        coord_type: Option<CoordType>,
        metadata: Arc<ArrayMetadata>,
        prefer_multi: bool,
    ) -> Result<Self> {
        let wkb_objects2: Vec<Option<WKBGeometry>> = wkb_objects
            .iter()
            .map(|maybe_wkb| maybe_wkb.as_ref().map(|wkb| wkb.to_wkb_object()))
            .collect();
        Self::from_nullable_geometries(&wkb_objects2, coord_type, metadata, prefer_multi)
    }
}

impl<const D: usize> GeometryArrayBuilder for GeometryCollectionBuilder<D> {
    fn new() -> Self {
        Self::new()
    }

    fn with_geom_capacity_and_options(
        geom_capacity: usize,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        let capacity = GeometryCollectionCapacity::new(Default::default(), geom_capacity);
        Self::with_capacity_and_options(capacity, coord_type, metadata, DEFAULT_PREFER_MULTI)
    }

    fn push_geometry(&mut self, value: Option<&impl GeometryTrait<T = f64>>) -> Result<()> {
        self.push_geometry(value)
    }

    fn finish(self) -> Arc<dyn crate::NativeArray> {
        Arc::new(self.finish())
    }

    fn len(&self) -> usize {
        self.geom_offsets.len_proxy()
    }

    fn nulls(&self) -> &NullBufferBuilder {
        &self.validity
    }

    fn into_array_ref(self) -> Arc<dyn Array> {
        Arc::new(self.into_arrow())
    }

    fn coord_type(&self) -> CoordType {
        self.geoms.coord_type()
    }

    fn set_metadata(&mut self, metadata: Arc<ArrayMetadata>) {
        self.metadata = metadata;
    }

    fn metadata(&self) -> Arc<ArrayMetadata> {
        self.metadata.clone()
    }
}

impl<const D: usize> IntoArrow for GeometryCollectionBuilder<D> {
    type ArrowArray = GenericListArray<i32>;

    fn into_arrow(self) -> Self::ArrowArray {
        let linestring_arr: GeometryCollectionArray<D> = self.into();
        linestring_arr.into_arrow()
    }
}

impl<const D: usize> Default for GeometryCollectionBuilder<D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const D: usize> From<GeometryCollectionBuilder<D>> for GeometryCollectionArray<D> {
    fn from(mut other: GeometryCollectionBuilder<D>) -> Self {
        let validity = other.validity.finish();
        Self::new(
            other.geoms.into(),
            other.geom_offsets.into(),
            validity,
            other.metadata,
        )
    }
}

impl<const D: usize> From<GeometryCollectionBuilder<D>> for GenericListArray<i32> {
    fn from(arr: GeometryCollectionBuilder<D>) -> Self {
        arr.into_arrow()
    }
}

impl<G: GeometryCollectionTrait<T = f64>> From<&[G]> for GeometryCollectionBuilder<2> {
    fn from(geoms: &[G]) -> Self {
        Self::from_geometry_collections(geoms, Default::default(), Default::default(), true)
            .unwrap()
    }
}

impl<G: GeometryCollectionTrait<T = f64>> From<Vec<Option<G>>> for GeometryCollectionBuilder<2> {
    fn from(geoms: Vec<Option<G>>) -> Self {
        Self::from_nullable_geometry_collections(
            &geoms,
            Default::default(),
            Default::default(),
            true,
        )
        .unwrap()
    }
}

impl<O: OffsetSizeTrait> TryFrom<WKBArray<O>> for GeometryCollectionBuilder<2> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self> {
        let metadata = value.metadata.clone();
        let wkb_objects: Vec<Option<WKB<'_, O>>> = value.iter().collect();
        Self::from_wkb(&wkb_objects, Default::default(), metadata, true)
    }
}