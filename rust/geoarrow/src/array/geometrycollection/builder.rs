use std::sync::Arc;

use arrow_array::{ArrayRef, GenericListArray, OffsetSizeTrait};
use arrow_buffer::NullBufferBuilder;

use crate::array::geometrycollection::GeometryCollectionCapacity;
use crate::array::metadata::ArrayMetadata;
use crate::array::mixed::builder::DEFAULT_PREFER_MULTI;
use crate::array::offset_builder::OffsetsBuilder;
use crate::array::{CoordType, GeometryCollectionArray, MixedGeometryBuilder, WKBArray};
use geoarrow_schema::Dimension;
use crate::error::{GeoArrowError, Result};
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
pub struct GeometryCollectionBuilder {
    metadata: Arc<Metadata>,

    pub(crate) geoms: MixedGeometryBuilder,

    pub(crate) geom_offsets: OffsetsBuilder<i32>,

    pub(crate) validity: NullBufferBuilder,
}

impl<'a> GeometryCollectionBuilder {
    /// Creates a new empty [`GeometryCollectionBuilder`].
    pub fn new(dim: Dimension) -> Self {
        Self::new_with_options(
            dim,
            Default::default(),
            Default::default(),
            DEFAULT_PREFER_MULTI,
        )
    }

    /// Creates a new empty [`GeometryCollectionBuilder`] with the provided options.
    pub fn new_with_options(
        dim: Dimension,
        coord_type: CoordType,
        metadata: Arc<Metadata>,
        prefer_multi: bool,
    ) -> Self {
        Self::with_capacity_and_options(dim, Default::default(), coord_type, metadata, prefer_multi)
    }

    /// Creates a new empty [`GeometryCollectionBuilder`] with the provided capacity.
    pub fn with_capacity(dim: Dimension, capacity: GeometryCollectionCapacity) -> Self {
        Self::with_capacity_and_options(
            dim,
            capacity,
            Default::default(),
            Default::default(),
            DEFAULT_PREFER_MULTI,
        )
    }

    /// Creates a new empty [`GeometryCollectionBuilder`] with the provided capacity and options.
    pub fn with_capacity_and_options(
        dim: Dimension,
        capacity: GeometryCollectionCapacity,
        coord_type: CoordType,
        metadata: Arc<Metadata>,
        prefer_multi: bool,
    ) -> Self {
        Self {
            geoms: MixedGeometryBuilder::with_capacity_and_options(
                dim,
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

    /// Reserves capacity for at least `additional` more GeometryCollections.
    ///
    /// The collection may reserve more space to speculatively avoid frequent reallocations. After
    /// calling `reserve`, capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, additional: GeometryCollectionCapacity) {
        self.geoms.reserve(additional.mixed_capacity);
        self.geom_offsets.reserve(additional.geom_capacity);
    }

    /// Reserves the minimum capacity for at least `additional` more GeometryCollections.
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
    pub fn reserve_exact(&mut self, additional: GeometryCollectionCapacity) {
        self.geoms.reserve_exact(additional.mixed_capacity);
        self.geom_offsets.reserve_exact(additional.geom_capacity);
    }

    /// Consume the builder and convert to an immutable [`GeometryCollectionArray`]
    pub fn finish(self) -> GeometryCollectionArray {
        self.into()
    }

    /// Creates a new [`GeometryCollectionBuilder`] with a capacity inferred by the provided
    /// iterator.
    pub fn with_capacity_from_iter(
        geoms: impl Iterator<Item = Option<&'a (impl GeometryCollectionTrait + 'a)>>,
        dim: Dimension,
    ) -> Result<Self> {
        Self::with_capacity_and_options_from_iter(
            geoms,
            dim,
            Default::default(),
            Default::default(),
            DEFAULT_PREFER_MULTI,
        )
    }

    /// Creates a new [`GeometryCollectionBuilder`] with the provided options and a capacity
    /// inferred by the provided iterator.
    pub fn with_capacity_and_options_from_iter(
        geoms: impl Iterator<Item = Option<&'a (impl GeometryCollectionTrait + 'a)>>,
        dim: Dimension,
        coord_type: CoordType,
        metadata: Arc<Metadata>,
        prefer_multi: bool,
    ) -> Result<Self> {
        let counter = GeometryCollectionCapacity::from_geometry_collections(geoms)?;
        Ok(Self::with_capacity_and_options(
            dim,
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
        geoms: impl Iterator<Item = Option<&'a (impl GeometryCollectionTrait + 'a)>>,
    ) -> Result<()> {
        let counter = GeometryCollectionCapacity::from_geometry_collections(geoms)?;
        self.reserve(counter);
        Ok(())
    }

    /// Reserve more space in the underlying buffers with the capacity inferred from the provided
    /// geometries.
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

    /// Extend this builder with the given geometries
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

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_geometry_collections(
        geoms: &[impl GeometryCollectionTrait<T = f64>],
        dim: Dimension,
        coord_type: CoordType,
        metadata: Arc<Metadata>,
        prefer_multi: bool,
    ) -> Result<Self> {
        let mut array = Self::with_capacity_and_options_from_iter(
            geoms.iter().map(Some),
            dim,
            coord_type,
            metadata,
            prefer_multi,
        )?;
        array.extend_from_iter(geoms.iter().map(Some));
        Ok(array)
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_nullable_geometry_collections(
        geoms: &[Option<impl GeometryCollectionTrait<T = f64>>],
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

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_geometries(
        geoms: &[impl GeometryTrait<T = f64>],
        dim: Dimension,
        coord_type: CoordType,
        metadata: Arc<Metadata>,
        prefer_multi: bool,
    ) -> Result<Self> {
        let capacity = GeometryCollectionCapacity::from_geometries(geoms.iter().map(Some))?;
        let mut array =
            Self::with_capacity_and_options(dim, capacity, coord_type, metadata, prefer_multi);
        for geom in geoms {
            array.push_geometry(Some(geom))?;
        }
        Ok(array)
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_nullable_geometries(
        geoms: &[Option<impl GeometryTrait<T = f64>>],
        dim: Dimension,
        coord_type: CoordType,
        metadata: Arc<Metadata>,
        prefer_multi: bool,
    ) -> Result<Self> {
        let capacity =
            GeometryCollectionCapacity::from_geometries(geoms.iter().map(|x| x.as_ref()))?;
        let mut array =
            Self::with_capacity_and_options(dim, capacity, coord_type, metadata, prefer_multi);
        for geom in geoms {
            array.push_geometry(geom.as_ref())?;
        }
        Ok(array)
    }

    pub(crate) fn from_wkb<W: OffsetSizeTrait>(
        wkb_objects: &[Option<WKB<'_, W>>],
        dim: Dimension,
        coord_type: CoordType,
        metadata: Arc<Metadata>,
        prefer_multi: bool,
    ) -> Result<Self> {
        let wkb_objects2 = wkb_objects
            .iter()
            .map(|maybe_wkb| maybe_wkb.as_ref().map(|wkb| wkb.parse()).transpose())
            .collect::<Result<Vec<_>>>()?;
        Self::from_nullable_geometries(&wkb_objects2, dim, coord_type, metadata, prefer_multi)
    }
}

impl GeometryArrayBuilder for GeometryCollectionBuilder {
    fn new(dim: Dimension) -> Self {
        Self::new(dim)
    }

    fn with_geom_capacity_and_options(
        dim: Dimension,
        geom_capacity: usize,
        coord_type: CoordType,
        metadata: Arc<Metadata>,
    ) -> Self {
        let capacity = GeometryCollectionCapacity::new(Default::default(), geom_capacity);
        Self::with_capacity_and_options(dim, capacity, coord_type, metadata, DEFAULT_PREFER_MULTI)
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

    fn into_array_ref(self) -> ArrayRef {
        Arc::new(self.into_arrow())
    }

    fn coord_type(&self) -> CoordType {
        self.geoms.coord_type()
    }

    fn set_metadata(&mut self, metadata: Arc<Metadata>) {
        self.metadata = metadata;
    }

    fn metadata(&self) -> Arc<Metadata> {
        self.metadata.clone()
    }
}

impl IntoArrow for GeometryCollectionBuilder {
    type ArrowArray = GenericListArray<i32>;

    fn into_arrow(self) -> Self::ArrowArray {
        let linestring_arr: GeometryCollectionArray = self.into();
        linestring_arr.into_arrow()
    }
}

impl Default for GeometryCollectionBuilder {
    fn default() -> Self {
        Self::new(Dimension::XY)
    }
}

impl From<GeometryCollectionBuilder> for GeometryCollectionArray {
    fn from(mut other: GeometryCollectionBuilder) -> Self {
        let validity = other.validity.finish();
        Self::new(
            other.geoms.into(),
            other.geom_offsets.into(),
            validity,
            other.metadata,
        )
    }
}

impl From<GeometryCollectionBuilder> for GenericListArray<i32> {
    fn from(arr: GeometryCollectionBuilder) -> Self {
        arr.into_arrow()
    }
}

impl<G: GeometryCollectionTrait<T = f64>> From<(&[G], Dimension)> for GeometryCollectionBuilder {
    fn from((geoms, dim): (&[G], Dimension)) -> Self {
        Self::from_geometry_collections(geoms, dim, Default::default(), Default::default(), true)
            .unwrap()
    }
}

impl<G: GeometryCollectionTrait<T = f64>> From<(Vec<Option<G>>, Dimension)>
    for GeometryCollectionBuilder
{
    fn from((geoms, dim): (Vec<Option<G>>, Dimension)) -> Self {
        Self::from_nullable_geometry_collections(
            &geoms,
            dim,
            Default::default(),
            Default::default(),
            true,
        )
        .unwrap()
    }
}

impl<O: OffsetSizeTrait> TryFrom<(WKBArray<O>, Dimension)> for GeometryCollectionBuilder {
    type Error = GeoArrowError;

    fn try_from((value, dim): (WKBArray<O>, Dimension)) -> Result<Self> {
        let metadata = value.metadata.clone();
        let wkb_objects: Vec<Option<WKB<'_, O>>> = value.iter().collect();
        Self::from_wkb(&wkb_objects, dim, Default::default(), metadata, true)
    }
}
