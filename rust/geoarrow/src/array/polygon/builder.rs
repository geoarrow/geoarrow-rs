use std::sync::Arc;

use arrow_array::{ArrayRef, GenericListArray, OffsetSizeTrait};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use geo_traits::{
    CoordTrait, GeometryTrait, GeometryType, LineStringTrait, MultiPolygonTrait, PolygonTrait,
    RectTrait,
};
use geoarrow_schema::Dimension;

use crate::array::offset_builder::OffsetsBuilder;
use crate::array::polygon::PolygonCapacity;
use crate::array::{
    CoordBufferBuilder, InterleavedCoordBufferBuilder, MultiLineStringBuilder, PolygonArray,
    SeparatedCoordBufferBuilder, WKBArray,
};
use crate::error::{GeoArrowError, Result};
use crate::scalar::WKB;
use crate::trait_::{ArrayAccessor, GeometryArrayBuilder, IntoArrow};

pub type MutablePolygonParts = (
    CoordBufferBuilder,
    OffsetsBuilder<i32>,
    OffsetsBuilder<i32>,
    NullBufferBuilder,
);

/// The GeoArrow equivalent to `Vec<Option<Polygon>>`: a mutable collection of Polygons.
///
/// Converting an [`PolygonBuilder`] into a [`PolygonArray`] is `O(1)`.
#[derive(Debug)]
pub struct PolygonBuilder {
    metadata: Arc<Metadata>,

    pub(crate) coords: CoordBufferBuilder,

    /// OffsetsBuilder into the ring array where each geometry starts
    pub(crate) geom_offsets: OffsetsBuilder<i32>,

    /// OffsetsBuilder into the coordinate array where each ring starts
    pub(crate) ring_offsets: OffsetsBuilder<i32>,

    /// Validity is only defined at the geometry level
    pub(crate) validity: NullBufferBuilder,
}

impl PolygonBuilder {
    /// Creates a new empty [`PolygonBuilder`].
    pub fn new(dim: Dimension) -> Self {
        Self::new_with_options(dim, Default::default(), Default::default())
    }

    /// Creates a new empty [`PolygonBuilder`] with the provided options.
    pub fn new_with_options(
        dim: Dimension,
        coord_type: CoordType,
        metadata: Arc<Metadata>,
    ) -> Self {
        Self::with_capacity_and_options(dim, Default::default(), coord_type, metadata)
    }

    /// Creates a new [`PolygonBuilder`] with given capacity and no validity.
    pub fn with_capacity(dim: Dimension, capacity: PolygonCapacity) -> Self {
        Self::with_capacity_and_options(dim, capacity, Default::default(), Default::default())
    }

    /// Creates a new empty [`PolygonBuilder`] with the provided capacity and options.
    pub fn with_capacity_and_options(
        dim: Dimension,
        capacity: PolygonCapacity,
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
            ring_offsets: OffsetsBuilder::with_capacity(capacity.ring_capacity),
            validity: NullBufferBuilder::new(capacity.geom_capacity),
            metadata,
        }
    }

    /// Reserves capacity for at least `additional` more Polygons.
    ///
    /// The collection may reserve more space to speculatively avoid frequent reallocations. After
    /// calling `reserve`, capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, capacity: PolygonCapacity) {
        self.coords.reserve(capacity.coord_capacity);
        self.ring_offsets.reserve(capacity.ring_capacity);
        self.geom_offsets.reserve(capacity.geom_capacity);
    }

    /// Reserves the minimum capacity for at least `additional` more Polygons.
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
    pub fn reserve_exact(&mut self, capacity: PolygonCapacity) {
        self.coords.reserve_exact(capacity.coord_capacity);
        self.ring_offsets.reserve_exact(capacity.ring_capacity);
        self.geom_offsets.reserve_exact(capacity.geom_capacity);
    }

    /// Reserve more space in the underlying buffers with the capacity inferred from the provided
    /// geometries.
    pub fn reserve_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl PolygonTrait + 'a)>>,
    ) {
        let counter = PolygonCapacity::from_polygons(geoms);
        self.reserve(counter)
    }

    /// Reserve more space in the underlying buffers with the capacity inferred from the provided
    /// geometries.
    pub fn reserve_exact_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl PolygonTrait + 'a)>>,
    ) {
        let counter = PolygonCapacity::from_polygons(geoms);
        self.reserve_exact(counter)
    }

    /// The canonical method to create a [`PolygonBuilder`] out of its internal components.
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Errors
    ///
    /// - if the validity is not `None` and its length is different from the number of geometries
    /// - if the largest ring offset does not match the number of coordinates
    /// - if the largest geometry offset does not match the size of ring offsets
    pub fn try_new(
        coords: CoordBufferBuilder,
        geom_offsets: OffsetsBuilder<i32>,
        ring_offsets: OffsetsBuilder<i32>,
        validity: NullBufferBuilder,
        metadata: Arc<Metadata>,
    ) -> Result<Self> {
        // check(
        //     &coords.clone().into(),
        //     &geom_offsets.clone().into(),
        //     &ring_offsets.clone().into(),
        //     validity.as_ref().map(|x| x.len()),
        // )?;
        Ok(Self {
            coords,
            geom_offsets,
            ring_offsets,
            validity,
            metadata,
        })
    }

    /// Extract the low-level APIs from the [`PolygonBuilder`].
    pub fn into_inner(self) -> MutablePolygonParts {
        (
            self.coords,
            self.geom_offsets,
            self.ring_offsets,
            self.validity,
        )
    }

    /// Push a raw offset to the underlying geometry offsets buffer.
    ///
    /// # Safety
    ///
    /// This is marked as unsafe because care must be taken to ensure that pushing raw offsets
    /// upholds the necessary invariants of the array.
    #[inline]
    pub unsafe fn try_push_geom_offset(&mut self, offsets_length: usize) -> Result<()> {
        self.geom_offsets.try_push_usize(offsets_length)?;
        self.validity.append(true);
        Ok(())
    }

    /// Push a raw offset to the underlying ring offsets buffer.
    ///
    /// # Safety
    ///
    /// This is marked as unsafe because care must be taken to ensure that pushing raw offsets
    /// upholds the necessary invariants of the array.
    #[inline]
    pub unsafe fn try_push_ring_offset(&mut self, offsets_length: usize) -> Result<()> {
        self.ring_offsets.try_push_usize(offsets_length)?;
        Ok(())
    }

    /// Consume the builder and convert to an immutable [`PolygonArray`]
    pub fn finish(self) -> PolygonArray {
        self.into()
    }

    /// Creates a new builder with a capacity inferred by the provided iterator.
    pub fn with_capacity_from_iter<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl PolygonTrait + 'a)>>,
        dim: Dimension,
    ) -> Self {
        Self::with_capacity_and_options_from_iter(
            geoms,
            dim,
            Default::default(),
            Default::default(),
        )
    }

    /// Creates a new builder with the provided options and a capacity inferred by the provided
    /// iterator.
    pub fn with_capacity_and_options_from_iter<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl PolygonTrait + 'a)>>,
        dim: Dimension,
        coord_type: CoordType,
        metadata: Arc<Metadata>,
    ) -> Self {
        let counter = PolygonCapacity::from_polygons(geoms);
        Self::with_capacity_and_options(dim, counter, coord_type, metadata)
    }

    /// Add a new Polygon to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    pub fn push_polygon(&mut self, value: Option<&impl PolygonTrait<T = f64>>) -> Result<()> {
        if let Some(polygon) = value {
            let exterior_ring = polygon.exterior();
            if exterior_ring.is_none() {
                self.push_empty();
                return Ok(());
            }

            // - Get exterior ring
            // - Add exterior ring's # of coords self.ring_offsets
            // - Push ring's coords to self.coords
            let ext_ring = polygon.exterior().unwrap();
            self.ring_offsets.try_push_usize(ext_ring.num_coords())?;
            for coord in ext_ring.coords() {
                self.coords.push_coord(&coord);
            }

            // Total number of rings in this polygon
            let num_interiors = polygon.num_interiors();
            self.geom_offsets.try_push_usize(num_interiors + 1)?;

            // For each interior ring:
            // - Get ring
            // - Add ring's # of coords to self.ring_offsets
            // - Push ring's coords to self.coords
            for int_ring in polygon.interiors() {
                self.ring_offsets.try_push_usize(int_ring.num_coords())?;
                for coord in int_ring.coords() {
                    self.coords.push_coord(&coord);
                }
            }

            self.validity.append(true);
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Add a new Rect to this builder
    #[inline]
    pub fn push_rect(&mut self, value: Option<&impl RectTrait<T = f64>>) -> Result<()> {
        if let Some(rect) = value {
            // Only one ring
            self.geom_offsets.try_push_usize(1)?;
            // ring has 5 coords
            self.ring_offsets.try_push_usize(5)?;

            let lower = rect.min();
            let upper = rect.max();

            // Ref below because I always forget the ordering
            // https://github.com/georust/geo/blob/76ad2a358bd079e9d47b1229af89608744d2635b/geo-types/src/geometry/rect.rs#L217-L225

            self.coords.push_coord(&geo::Coord {
                x: lower.x(),
                y: lower.y(),
            });
            self.coords.push_coord(&geo::Coord {
                x: lower.x(),
                y: upper.y(),
            });
            self.coords.push_coord(&geo::Coord {
                x: upper.x(),
                y: upper.y(),
            });
            self.coords.push_coord(&geo::Coord {
                x: upper.x(),
                y: lower.y(),
            });
            self.coords.push_coord(&geo::Coord {
                x: lower.x(),
                y: lower.y(),
            });
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Add a new geometry to this builder
    ///
    /// This will error if the geometry type is not Polygon, a MultiPolygon of length 1, or Rect.
    #[inline]
    pub fn push_geometry(&mut self, value: Option<&impl GeometryTrait<T = f64>>) -> Result<()> {
        if let Some(value) = value {
            match value.as_type() {
                GeometryType::Polygon(g) => self.push_polygon(Some(g))?,
                GeometryType::MultiPolygon(mp) => {
                    if mp.num_polygons() == 1 {
                        self.push_polygon(Some(&mp.polygon(0).unwrap()))?
                    } else {
                        return Err(GeoArrowError::General("Incorrect type".to_string()));
                    }
                }
                GeometryType::Rect(g) => self.push_rect(Some(g))?,
                _ => return Err(GeoArrowError::General("Incorrect type".to_string())),
            }
        } else {
            self.push_null();
        };
        Ok(())
    }

    /// Extend this builder with the given geometries
    pub fn extend_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl PolygonTrait<T = f64> + 'a)>>,
    ) {
        geoms
            .into_iter()
            .try_for_each(|maybe_polygon| self.push_polygon(maybe_polygon))
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

    /// Push a raw coordinate to the underlying coordinate array.
    ///
    /// # Safety
    ///
    /// This is marked as unsafe because care must be taken to ensure that pushing raw coordinates
    /// to the array upholds the necessary invariants of the array.
    #[inline]
    pub unsafe fn push_coord(&mut self, coord: &impl CoordTrait<T = f64>) -> Result<()> {
        self.coords.push_coord(coord);
        Ok(())
    }

    #[inline]
    pub(crate) fn push_empty(&mut self) {
        self.geom_offsets.try_push_usize(0).unwrap();
        self.validity.append(true);
    }

    #[inline]
    pub(crate) fn push_null(&mut self) {
        // NOTE! Only the geom_offsets array needs to get extended, because the next geometry will
        // point to the same ring array location
        self.geom_offsets.extend_constant(1);
        self.validity.append(false);
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_polygons(
        geoms: &[impl PolygonTrait<T = f64>],
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
    pub fn from_nullable_polygons(
        geoms: &[Option<impl PolygonTrait<T = f64>>],
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
        let capacity = PolygonCapacity::from_geometries(geoms.iter().map(|x| x.as_ref()))?;
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

impl Default for PolygonBuilder {
    fn default() -> Self {
        Self::new(Dimension::XY)
    }
}

impl GeometryArrayBuilder for PolygonBuilder {
    fn new(dim: Dimension) -> Self {
        Self::new(dim)
    }

    fn with_geom_capacity_and_options(
        dim: Dimension,
        geom_capacity: usize,
        coord_type: CoordType,
        metadata: Arc<Metadata>,
    ) -> Self {
        let capacity = PolygonCapacity::new(0, 0, geom_capacity);
        Self::with_capacity_and_options(dim, capacity, coord_type, metadata)
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
        self.coords.coord_type()
    }

    fn set_metadata(&mut self, metadata: Arc<Metadata>) {
        self.metadata = metadata;
    }

    fn metadata(&self) -> Arc<Metadata> {
        self.metadata.clone()
    }
}

impl IntoArrow for PolygonBuilder {
    type ArrowArray = GenericListArray<i32>;

    fn into_arrow(self) -> Self::ArrowArray {
        let polygon_array: PolygonArray = self.into();
        polygon_array.into_arrow()
    }
}

impl From<PolygonBuilder> for PolygonArray {
    fn from(mut other: PolygonBuilder) -> Self {
        let validity = other.validity.finish();

        let geom_offsets: OffsetBuffer<i32> = other.geom_offsets.into();
        let ring_offsets: OffsetBuffer<i32> = other.ring_offsets.into();

        Self::new(
            other.coords.into(),
            geom_offsets,
            ring_offsets,
            validity,
            other.metadata,
        )
    }
}

impl<G: PolygonTrait<T = f64>> From<(&[G], Dimension)> for PolygonBuilder {
    fn from((geoms, dim): (&[G], Dimension)) -> Self {
        Self::from_polygons(geoms, dim, Default::default(), Default::default())
    }
}

impl<G: PolygonTrait<T = f64>> From<(Vec<Option<G>>, Dimension)> for PolygonBuilder {
    fn from((geoms, dim): (Vec<Option<G>>, Dimension)) -> Self {
        Self::from_nullable_polygons(&geoms, dim, Default::default(), Default::default())
    }
}

impl<O: OffsetSizeTrait> TryFrom<(WKBArray<O>, Dimension)> for PolygonBuilder {
    type Error = GeoArrowError;

    fn try_from((value, dim): (WKBArray<O>, Dimension)) -> Result<Self> {
        let metadata = value.metadata.clone();
        let wkb_objects: Vec<Option<WKB<'_, O>>> = value.iter().collect();
        Self::from_wkb(&wkb_objects, dim, Default::default(), metadata)
    }
}

/// Polygon and MultiLineString have the same layout, so enable conversions between the two to
/// change the semantic type
impl From<PolygonBuilder> for MultiLineStringBuilder {
    fn from(value: PolygonBuilder) -> Self {
        Self::try_new(
            value.coords,
            value.geom_offsets,
            value.ring_offsets,
            value.validity,
            value.metadata,
        )
        .unwrap()
    }
}
