use std::sync::Arc;

use crate::array::metadata::ArrayMetadata;
// use super::array::check;
use crate::array::offset_builder::OffsetsBuilder;
use crate::array::polygon::PolygonCapacity;
use crate::array::{
    CoordBufferBuilder, CoordType, InterleavedCoordBufferBuilder, MultiLineStringBuilder,
    PolygonArray, SeparatedCoordBufferBuilder, WKBArray,
};
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::{
    CoordTrait, GeometryTrait, GeometryType, LineStringTrait, MultiPolygonTrait, PolygonTrait,
    RectTrait,
};
use crate::io::wkb::reader::WKBPolygon;
use crate::scalar::WKB;
use crate::trait_::{GeometryArrayAccessor, GeometryArrayBuilder, IntoArrow};
use arrow_array::{Array, GenericListArray, OffsetSizeTrait};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};

pub type MutablePolygonParts<O> = (
    CoordBufferBuilder,
    OffsetsBuilder<O>,
    OffsetsBuilder<O>,
    NullBufferBuilder,
);

/// The GeoArrow equivalent to `Vec<Option<Polygon>>`: a mutable collection of Polygons.
///
/// Converting an [`PolygonBuilder`] into a [`PolygonArray`] is `O(1)`.
#[derive(Debug)]
pub struct PolygonBuilder<O: OffsetSizeTrait> {
    metadata: Arc<ArrayMetadata>,

    pub(crate) coords: CoordBufferBuilder,

    /// OffsetsBuilder into the ring array where each geometry starts
    pub(crate) geom_offsets: OffsetsBuilder<O>,

    /// OffsetsBuilder into the coordinate array where each ring starts
    pub(crate) ring_offsets: OffsetsBuilder<O>,

    /// Validity is only defined at the geometry level
    pub(crate) validity: NullBufferBuilder,
}

impl<O: OffsetSizeTrait> PolygonBuilder<O> {
    /// Creates a new empty [`PolygonBuilder`].
    pub fn new() -> Self {
        Self::new_with_options(Default::default(), Default::default())
    }

    pub fn new_with_options(coord_type: CoordType, metadata: Arc<ArrayMetadata>) -> Self {
        Self::with_capacity_and_options(Default::default(), coord_type, metadata)
    }

    /// Creates a new [`PolygonBuilder`] with given capacity and no validity.
    pub fn with_capacity(capacity: PolygonCapacity) -> Self {
        Self::with_capacity_and_options(capacity, Default::default(), Default::default())
    }

    pub fn with_capacity_and_options(
        capacity: PolygonCapacity,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        let coords = match coord_type {
            CoordType::Interleaved => CoordBufferBuilder::Interleaved(
                InterleavedCoordBufferBuilder::with_capacity(capacity.coord_capacity),
            ),
            CoordType::Separated => CoordBufferBuilder::Separated(
                SeparatedCoordBufferBuilder::with_capacity(capacity.coord_capacity),
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

    pub fn with_capacity_from_iter<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl PolygonTrait + 'a)>>,
    ) -> Self {
        Self::with_capacity_and_options_from_iter(geoms, Default::default(), Default::default())
    }

    pub fn with_capacity_and_options_from_iter<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl PolygonTrait + 'a)>>,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        let counter = PolygonCapacity::from_polygons(geoms);
        Self::with_capacity_and_options(counter, coord_type, metadata)
    }

    /// Reserves capacity for at least `additional` more LineStrings to be inserted
    /// in the given `Vec<T>`. The collection may reserve more space to
    /// speculatively avoid frequent reallocations. After calling `reserve`,
    /// capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, capacity: PolygonCapacity) {
        self.coords.reserve(capacity.coord_capacity);
        self.ring_offsets.reserve(capacity.ring_capacity);
        self.geom_offsets.reserve(capacity.geom_capacity);
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
    pub fn reserve_exact(&mut self, capacity: PolygonCapacity) {
        self.coords.reserve_exact(capacity.coord_capacity);
        self.ring_offsets.reserve_exact(capacity.ring_capacity);
        self.geom_offsets.reserve_exact(capacity.geom_capacity);
    }

    pub fn reserve_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl PolygonTrait + 'a)>>,
    ) {
        let counter = PolygonCapacity::from_polygons(geoms);
        self.reserve(counter)
    }

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
        geom_offsets: OffsetsBuilder<O>,
        ring_offsets: OffsetsBuilder<O>,
        validity: NullBufferBuilder,
        metadata: Arc<ArrayMetadata>,
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
    pub fn into_inner(self) -> MutablePolygonParts<O> {
        (
            self.coords,
            self.geom_offsets,
            self.ring_offsets,
            self.validity,
        )
    }

    pub fn into_array_ref(self) -> Arc<dyn Array> {
        Arc::new(self.into_arrow())
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

    #[inline]
    pub fn push_rect(&mut self, value: Option<&impl RectTrait<T = f64>>) -> Result<()> {
        if let Some(rect) = value {
            // Only one ring
            self.geom_offsets.try_push_usize(1)?;
            // ring has 5 coords
            self.ring_offsets.try_push_usize(5)?;

            let lower = rect.lower();
            let upper = rect.upper();

            // Ref below because I always forget the ordering
            // https://github.com/georust/geo/blob/76ad2a358bd079e9d47b1229af89608744d2635b/geo-types/src/geometry/rect.rs#L217-L225

            self.coords.push_xy(lower.x(), lower.y());
            self.coords.push_xy(lower.x(), upper.y());
            self.coords.push_xy(upper.x(), upper.y());
            self.coords.push_xy(upper.x(), lower.y());
            self.coords.push_xy(lower.x(), lower.y());
        } else {
            self.push_null();
        }
        Ok(())
    }

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

    pub fn extend_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl PolygonTrait<T = f64> + 'a)>>,
    ) {
        geoms
            .into_iter()
            .try_for_each(|maybe_polygon| self.push_polygon(maybe_polygon))
            .unwrap();
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

    /// Push a raw coordinate to the underlying coordinate array.
    ///
    /// # Safety
    ///
    /// This is marked as unsafe because care must be taken to ensure that pushing raw coordinates
    /// to the array upholds the necessary invariants of the array.
    #[inline]
    pub unsafe fn push_xy(&mut self, x: f64, y: f64) -> Result<()> {
        self.coords.push_xy(x, y);
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

    pub fn from_polygons(
        geoms: &[impl PolygonTrait<T = f64>],
        coord_type: Option<CoordType>,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        let mut array = Self::with_capacity_and_options_from_iter(
            geoms.iter().map(Some),
            coord_type.unwrap_or_default(),
            metadata,
        );
        array.extend_from_iter(geoms.iter().map(Some));
        array
    }

    pub fn from_nullable_polygons(
        geoms: &[Option<impl PolygonTrait<T = f64>>],
        coord_type: Option<CoordType>,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        let mut array = Self::with_capacity_and_options_from_iter(
            geoms.iter().map(|x| x.as_ref()),
            coord_type.unwrap_or_default(),
            metadata,
        );
        array.extend_from_iter(geoms.iter().map(|x| x.as_ref()));
        array
    }

    pub(crate) fn from_wkb<W: OffsetSizeTrait>(
        wkb_objects: &[Option<WKB<'_, W>>],
        coord_type: Option<CoordType>,
        metadata: Arc<ArrayMetadata>,
    ) -> Result<Self> {
        let wkb_objects2: Vec<Option<WKBPolygon>> = wkb_objects
            .iter()
            .map(|maybe_wkb| {
                maybe_wkb
                    .as_ref()
                    .map(|wkb| wkb.to_wkb_object().into_polygon())
            })
            .collect();
        Ok(Self::from_nullable_polygons(
            &wkb_objects2,
            coord_type,
            metadata,
        ))
    }

    pub fn finish(self) -> PolygonArray<O> {
        self.into()
    }
}

impl<O: OffsetSizeTrait> Default for PolygonBuilder<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: OffsetSizeTrait> GeometryArrayBuilder for PolygonBuilder<O> {
    fn new() -> Self {
        Self::new()
    }

    fn with_geom_capacity_and_options(
        geom_capacity: usize,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        let capacity = PolygonCapacity::new(0, 0, geom_capacity);
        Self::with_capacity_and_options(capacity, coord_type, metadata)
    }

    fn finish(self) -> Arc<dyn crate::GeometryArrayTrait> {
        Arc::new(self.finish())
    }

    fn len(&self) -> usize {
        self.geom_offsets.len_proxy()
    }

    fn validity(&self) -> &NullBufferBuilder {
        &self.validity
    }

    fn into_array_ref(self) -> Arc<dyn Array> {
        Arc::new(self.into_arrow())
    }

    fn coord_type(&self) -> CoordType {
        self.coords.coord_type()
    }

    fn set_metadata(&mut self, metadata: Arc<ArrayMetadata>) {
        self.metadata = metadata;
    }

    fn metadata(&self) -> Arc<ArrayMetadata> {
        self.metadata.clone()
    }
}

impl<O: OffsetSizeTrait> IntoArrow for PolygonBuilder<O> {
    type ArrowArray = GenericListArray<O>;

    fn into_arrow(self) -> Self::ArrowArray {
        let polygon_array: PolygonArray<O> = self.into();
        polygon_array.into_arrow()
    }
}

impl<O: OffsetSizeTrait> From<PolygonBuilder<O>> for PolygonArray<O> {
    fn from(other: PolygonBuilder<O>) -> Self {
        let validity = other.validity.finish_cloned();

        let geom_offsets: OffsetBuffer<O> = other.geom_offsets.into();
        let ring_offsets: OffsetBuffer<O> = other.ring_offsets.into();

        Self::new(
            other.coords.into(),
            geom_offsets,
            ring_offsets,
            validity,
            other.metadata,
        )
    }
}

impl<O: OffsetSizeTrait, G: PolygonTrait<T = f64>> From<&[G]> for PolygonBuilder<O> {
    fn from(geoms: &[G]) -> Self {
        Self::from_polygons(geoms, Default::default(), Default::default())
    }
}

impl<O: OffsetSizeTrait, G: PolygonTrait<T = f64>> From<Vec<Option<G>>> for PolygonBuilder<O> {
    fn from(geoms: Vec<Option<G>>) -> Self {
        Self::from_nullable_polygons(&geoms, Default::default(), Default::default())
    }
}

impl<O: OffsetSizeTrait, G: PolygonTrait<T = f64>> From<bumpalo::collections::Vec<'_, G>>
    for PolygonBuilder<O>
{
    fn from(geoms: bumpalo::collections::Vec<'_, G>) -> Self {
        Self::from_polygons(&geoms, Default::default(), Default::default())
    }
}
impl<O: OffsetSizeTrait, G: PolygonTrait<T = f64>> From<bumpalo::collections::Vec<'_, Option<G>>>
    for PolygonBuilder<O>
{
    fn from(geoms: bumpalo::collections::Vec<'_, Option<G>>) -> Self {
        Self::from_nullable_polygons(&geoms, Default::default(), Default::default())
    }
}

impl<O: OffsetSizeTrait> TryFrom<WKBArray<O>> for PolygonBuilder<O> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self> {
        let metadata = value.metadata.clone();
        let wkb_objects: Vec<Option<WKB<'_, O>>> = value.iter().collect();
        Self::from_wkb(&wkb_objects, Default::default(), metadata)
    }
}

/// Polygon and MultiLineString have the same layout, so enable conversions between the two to
/// change the semantic type
impl<O: OffsetSizeTrait> From<PolygonBuilder<O>> for MultiLineStringBuilder<O> {
    fn from(value: PolygonBuilder<O>) -> Self {
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
