use arrow_array::OffsetSizeTrait;
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use geo_traits::{
    CoordTrait, GeometryTrait, GeometryType, LineStringTrait, MultiPolygonTrait, PolygonTrait,
};
use geoarrow_schema::{CoordType, MultiPolygonType};

use crate::capacity::MultiPolygonCapacity;
// use super::array::check;
use crate::array::{MultiPolygonArray, WkbArray};
use crate::builder::{
    CoordBufferBuilder, InterleavedCoordBufferBuilder, OffsetsBuilder, SeparatedCoordBufferBuilder,
};
use crate::error::{GeoArrowError, Result};
use crate::trait_::{ArrayAccessor, GeometryArrayBuilder};

/// The GeoArrow equivalent to `Vec<Option<MultiPolygon>>`: a mutable collection of MultiPolygons.
///
/// Converting an [`MultiPolygonBuilder`] into a [`MultiPolygonArray`] is `O(1)`.
#[derive(Debug)]
pub struct MultiPolygonBuilder {
    data_type: MultiPolygonType,

    pub(crate) coords: CoordBufferBuilder,

    /// OffsetsBuilder into the polygon array where each geometry starts
    pub(crate) geom_offsets: OffsetsBuilder<i32>,

    /// OffsetsBuilder into the ring array where each polygon starts
    pub(crate) polygon_offsets: OffsetsBuilder<i32>,

    /// OffsetsBuilder into the coordinate array where each ring starts
    pub(crate) ring_offsets: OffsetsBuilder<i32>,

    /// Validity is only defined at the geometry level
    pub(crate) validity: NullBufferBuilder,
}

impl MultiPolygonBuilder {
    /// Creates a new empty [`MultiPolygonBuilder`].
    pub fn new(typ: MultiPolygonType) -> Self {
        Self::with_capacity(typ, Default::default())
    }

    /// Creates a new [`MultiPolygonBuilder`] with a capacity.
    pub fn with_capacity(typ: MultiPolygonType, capacity: MultiPolygonCapacity) -> Self {
        let coords = match typ.coord_type() {
            CoordType::Interleaved => {
                CoordBufferBuilder::Interleaved(InterleavedCoordBufferBuilder::with_capacity(
                    capacity.coord_capacity,
                    typ.dimension(),
                ))
            }
            CoordType::Separated => {
                CoordBufferBuilder::Separated(SeparatedCoordBufferBuilder::with_capacity(
                    capacity.coord_capacity,
                    typ.dimension(),
                ))
            }
        };

        Self {
            coords,
            geom_offsets: OffsetsBuilder::with_capacity(capacity.geom_capacity),
            polygon_offsets: OffsetsBuilder::with_capacity(capacity.polygon_capacity),
            ring_offsets: OffsetsBuilder::with_capacity(capacity.ring_capacity),
            validity: NullBufferBuilder::new(capacity.geom_capacity),
            data_type: typ,
        }
    }

    /// Reserves capacity for at least `additional` more MultiPolygons.
    ///
    /// The collection may reserve more space to speculatively avoid frequent reallocations. After
    /// calling `reserve`, capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, additional: MultiPolygonCapacity) {
        self.coords.reserve(additional.coord_capacity);
        self.ring_offsets.reserve(additional.ring_capacity);
        self.polygon_offsets.reserve(additional.polygon_capacity);
        self.geom_offsets.reserve(additional.geom_capacity);
    }

    /// Reserves the minimum capacity for at least `additional` more MultiPolygons.
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
    pub fn reserve_exact(&mut self, additional: MultiPolygonCapacity) {
        self.coords.reserve_exact(additional.coord_capacity);
        self.ring_offsets.reserve_exact(additional.ring_capacity);
        self.polygon_offsets
            .reserve_exact(additional.polygon_capacity);
        self.geom_offsets.reserve_exact(additional.geom_capacity);
    }

    /// Consume the builder and convert to an immutable [`MultiPolygonArray`]
    pub fn finish(mut self) -> MultiPolygonArray {
        let validity = self.validity.finish();

        let geom_offsets: OffsetBuffer<i32> = self.geom_offsets.into();
        let polygon_offsets: OffsetBuffer<i32> = self.polygon_offsets.into();
        let ring_offsets: OffsetBuffer<i32> = self.ring_offsets.into();

        MultiPolygonArray::new(
            self.coords.into(),
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            validity,
            self.data_type.metadata().clone(),
        )
    }

    /// Creates a new builder with a capacity inferred by the provided iterator.
    pub fn with_capacity_from_iter<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl MultiPolygonTrait + 'a)>>,
        typ: MultiPolygonType,
    ) -> Self {
        let capacity = MultiPolygonCapacity::from_multi_polygons(geoms);
        Self::with_capacity(typ, capacity)
    }

    /// Reserve more space in the underlying buffers with the capacity inferred from the provided
    /// geometries.
    pub fn reserve_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl MultiPolygonTrait + 'a)>>,
    ) {
        let capacity = MultiPolygonCapacity::from_multi_polygons(geoms);
        self.reserve(capacity)
    }

    /// Reserve more space in the underlying buffers with the capacity inferred from the provided
    /// geometries.
    pub fn reserve_exact_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl MultiPolygonTrait + 'a)>>,
    ) {
        let capacity = MultiPolygonCapacity::from_multi_polygons(geoms);
        self.reserve_exact(capacity)
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

            // Total number of polygons in this MultiPolygon
            let num_polygons = 1;
            self.geom_offsets.try_push_usize(num_polygons).unwrap();

            // TODO: support empty polygons
            let ext_ring = polygon.exterior().unwrap();
            for coord in ext_ring.coords() {
                self.coords.push_coord(&coord);
            }

            // Total number of rings in this Multipolygon
            self.polygon_offsets
                .try_push_usize(polygon.num_interiors() + 1)
                .unwrap();

            // Number of coords for each ring
            self.ring_offsets
                .try_push_usize(ext_ring.num_coords())
                .unwrap();

            for int_ring in polygon.interiors() {
                self.ring_offsets
                    .try_push_usize(int_ring.num_coords())
                    .unwrap();

                for coord in int_ring.coords() {
                    self.coords.push_coord(&coord);
                }
            }
        } else {
            self.push_null();
        };
        Ok(())
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
            // Total number of polygons in this MultiPolygon
            let num_polygons = multi_polygon.num_polygons();
            unsafe { self.try_push_geom_offset(num_polygons)? }

            // Iterate over polygons
            for polygon in multi_polygon.polygons() {
                // Here we unwrap the exterior ring because a polygon inside a multi polygon should
                // never be empty.
                let ext_ring = polygon.exterior().unwrap();
                for coord in ext_ring.coords() {
                    self.coords.push_coord(&coord);
                }

                // Total number of rings in this Multipolygon
                self.polygon_offsets
                    .try_push_usize(polygon.num_interiors() + 1)
                    .unwrap();

                // Number of coords for each ring
                self.ring_offsets
                    .try_push_usize(ext_ring.num_coords())
                    .unwrap();

                for int_ring in polygon.interiors() {
                    self.ring_offsets
                        .try_push_usize(int_ring.num_coords())
                        .unwrap();

                    for coord in int_ring.coords() {
                        self.coords.push_coord(&coord);
                    }
                }
            }
        } else {
            self.push_null();
        };
        Ok(())
    }

    /// Add a new geometry to this builder
    ///
    /// This will error if the geometry type is not Polygon or MultiPolygon.
    #[inline]
    pub fn push_geometry(&mut self, value: Option<&impl GeometryTrait<T = f64>>) -> Result<()> {
        if let Some(value) = value {
            match value.as_type() {
                GeometryType::Polygon(g) => self.push_polygon(Some(g))?,
                GeometryType::MultiPolygon(g) => self.push_multi_polygon(Some(g))?,
                // TODO: support rect
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
        geoms: impl Iterator<Item = Option<&'a (impl MultiPolygonTrait<T = f64> + 'a)>>,
    ) {
        geoms
            .into_iter()
            .try_for_each(|maybe_multi_polygon| self.push_multi_polygon(maybe_multi_polygon))
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

    /// Push a raw offset to the underlying polygon offsets buffer.
    ///
    /// # Safety
    ///
    /// This is marked as unsafe because care must be taken to ensure that pushing raw offsets
    /// upholds the necessary invariants of the array.
    #[inline]
    pub unsafe fn try_push_polygon_offset(&mut self, offsets_length: usize) -> Result<()> {
        self.polygon_offsets.try_push_usize(offsets_length)?;
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
        // point to the same polygon array location
        // Note that we don't use self.try_push_geom_offset because that sets validity to true
        self.geom_offsets.extend_constant(1);
        self.validity.append(false);
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_multi_polygons(
        geoms: &[impl MultiPolygonTrait<T = f64>],
        typ: MultiPolygonType,
    ) -> Self {
        let mut array = Self::with_capacity_from_iter(geoms.iter().map(Some), typ);
        array.extend_from_iter(geoms.iter().map(Some));
        array
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_nullable_multi_polygons(
        geoms: &[Option<impl MultiPolygonTrait<T = f64>>],
        typ: MultiPolygonType,
    ) -> Self {
        let mut array = Self::with_capacity_from_iter(geoms.iter().map(|x| x.as_ref()), typ);
        array.extend_from_iter(geoms.iter().map(|x| x.as_ref()));
        array
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_nullable_geometries(
        geoms: &[Option<impl GeometryTrait<T = f64>>],
        typ: MultiPolygonType,
    ) -> Result<Self> {
        let capacity = MultiPolygonCapacity::from_geometries(geoms.iter().map(|x| x.as_ref()))?;
        let mut array = Self::with_capacity(typ, capacity);
        array.extend_from_geometry_iter(geoms.iter().map(|x| x.as_ref()))?;
        Ok(array)
    }
}

impl<O: OffsetSizeTrait> TryFrom<(WkbArray<O>, MultiPolygonType)> for MultiPolygonBuilder {
    type Error = GeoArrowError;

    fn try_from((value, typ): (WkbArray<O>, MultiPolygonType)) -> Result<Self> {
        let wkb_objects = value
            .iter()
            .map(|x| x.transpose())
            .collect::<Result<Vec<_>>>()?;
        Self::from_nullable_geometries(&wkb_objects, typ)
    }
}

impl GeometryArrayBuilder for MultiPolygonBuilder {
    fn len(&self) -> usize {
        self.geom_offsets.len_proxy()
    }

    fn push_null(&mut self) {
        self.push_null();
    }
}
