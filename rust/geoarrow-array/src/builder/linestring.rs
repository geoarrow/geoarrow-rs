use std::convert::From;
use std::sync::Arc;

use arrow_array::OffsetSizeTrait;
use arrow_buffer::NullBufferBuilder;
use geo_traits::{CoordTrait, GeometryTrait, GeometryType, LineStringTrait, MultiLineStringTrait};
use geoarrow_schema::{CoordType, Dimension, Metadata};

use crate::array::{LineStringArray, WKBArray};
use crate::builder::{
    CoordBufferBuilder, InterleavedCoordBufferBuilder, MultiPointBuilder, OffsetsBuilder,
    SeparatedCoordBufferBuilder,
};
use crate::capacity::LineStringCapacity;
use crate::error::{GeoArrowError, Result};
use crate::scalar::WKB;
use crate::trait_::ArrayAccessor;

/// The GeoArrow equivalent to `Vec<Option<LineString>>`: a mutable collection of LineStrings.
///
/// Converting an [`LineStringBuilder`] into a [`LineStringArray`] is `O(1)`.
#[derive(Debug)]
pub struct LineStringBuilder {
    metadata: Arc<Metadata>,

    pub(crate) coords: CoordBufferBuilder,

    /// Offsets into the coordinate array where each geometry starts
    pub(crate) geom_offsets: OffsetsBuilder<i32>,

    /// Validity is only defined at the geometry level
    pub(crate) validity: NullBufferBuilder,
}

impl LineStringBuilder {
    /// Creates a new empty [`LineStringBuilder`].
    pub fn new(dim: Dimension) -> Self {
        Self::new_with_options(dim, CoordType::Interleaved, Default::default())
    }

    /// Creates a new empty [`LineStringBuilder`] with the provided options.
    pub fn new_with_options(
        dim: Dimension,
        coord_type: CoordType,
        metadata: Arc<Metadata>,
    ) -> Self {
        Self::with_capacity_and_options(dim, Default::default(), coord_type, metadata)
    }

    /// Creates a new [`LineStringBuilder`] with a capacity.
    pub fn with_capacity(dim: Dimension, capacity: LineStringCapacity) -> Self {
        Self::with_capacity_and_options(dim, capacity, CoordType::Interleaved, Default::default())
    }

    /// Creates a new empty [`LineStringBuilder`] with the provided capacity and options.
    pub fn with_capacity_and_options(
        dim: Dimension,
        capacity: LineStringCapacity,
        coord_type: CoordType,
        metadata: Arc<Metadata>,
    ) -> Self {
        let coords = match coord_type {
            CoordType::Interleaved => CoordBufferBuilder::Interleaved(
                InterleavedCoordBufferBuilder::with_capacity(capacity.coord_capacity(), dim),
            ),
            CoordType::Separated => CoordBufferBuilder::Separated(
                SeparatedCoordBufferBuilder::with_capacity(capacity.coord_capacity(), dim),
            ),
        };
        Self {
            coords,
            geom_offsets: OffsetsBuilder::with_capacity(capacity.geom_capacity()),
            validity: NullBufferBuilder::new(capacity.geom_capacity()),
            metadata,
        }
    }

    /// Reserves capacity for at least `additional` more LineStrings.
    ///
    /// The collection may reserve more space to speculatively avoid frequent reallocations. After
    /// calling `reserve`, capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, additional: LineStringCapacity) {
        self.coords.reserve(additional.coord_capacity());
        self.geom_offsets.reserve(additional.geom_capacity());
    }

    /// Reserves the minimum capacity for at least `additional` more LineStrings.
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
    pub fn reserve_exact(&mut self, additional: LineStringCapacity) {
        self.coords.reserve_exact(additional.coord_capacity());
        self.geom_offsets.reserve_exact(additional.geom_capacity());
    }

    /// The canonical method to create a [`LineStringBuilder`] out of its internal components.
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

    /// Extract the low-level APIs from the [`LineStringBuilder`].
    pub fn into_inner(self) -> (CoordBufferBuilder, OffsetsBuilder<i32>, NullBufferBuilder) {
        (self.coords, self.geom_offsets, self.validity)
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

    /// Consume the builder and convert to an immutable [`LineStringArray`]
    pub fn finish(self) -> LineStringArray {
        self.into()
    }

    /// Creates a new builder with a capacity inferred by the provided iterator.
    pub fn with_capacity_from_iter<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl LineStringTrait + 'a)>>,
        dim: Dimension,
    ) -> Self {
        Self::with_capacity_and_options_from_iter(
            geoms,
            dim,
            CoordType::Interleaved,
            Default::default(),
        )
    }

    /// Creates a new builder with the provided options and a capacity inferred by the provided
    /// iterator.
    pub fn with_capacity_and_options_from_iter<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl LineStringTrait + 'a)>>,
        dim: Dimension,
        coord_type: CoordType,
        metadata: Arc<Metadata>,
    ) -> Self {
        let counter = LineStringCapacity::from_line_strings(geoms);
        Self::with_capacity_and_options(dim, counter, coord_type, metadata)
    }

    /// Reserve more space in the underlying buffers with the capacity inferred from the provided
    /// geometries.
    pub fn reserve_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl LineStringTrait + 'a)>>,
    ) {
        let counter = LineStringCapacity::from_line_strings(geoms);
        self.reserve(counter)
    }

    /// Reserve more space in the underlying buffers with the capacity inferred from the provided
    /// geometries.
    pub fn reserve_exact_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl LineStringTrait + 'a)>>,
    ) {
        let counter = LineStringCapacity::from_line_strings(geoms);
        self.reserve_exact(counter)
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_line_strings(
        geoms: &[impl LineStringTrait<T = f64>],
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
    pub fn from_nullable_line_strings(
        geoms: &[Option<impl LineStringTrait<T = f64>>],
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

    /// Add a new LineString to the end of this array.
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
            let num_coords = line_string.num_coords();
            for coord in line_string.coords() {
                self.coords.try_push_coord(&coord)?;
            }
            self.try_push_length(num_coords)?;
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Extend this builder with the given geometries
    pub fn extend_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl LineStringTrait<T = f64> + 'a)>>,
    ) {
        geoms
            .into_iter()
            .try_for_each(|maybe_multi_point| self.push_line_string(maybe_multi_point))
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
        self.coords.try_push_coord(coord)
    }

    /// Add a new geometry to this builder
    ///
    /// This will error if the geometry type is not LineString or a MultiLineString with length 1.
    #[inline]
    pub fn push_geometry(&mut self, value: Option<&impl GeometryTrait<T = f64>>) -> Result<()> {
        if let Some(value) = value {
            match value.as_type() {
                GeometryType::LineString(g) => self.push_line_string(Some(g))?,
                GeometryType::MultiLineString(ml) => {
                    if ml.num_line_strings() == 1 {
                        self.push_line_string(Some(&ml.line_string(0).unwrap()))?
                    } else {
                        return Err(GeoArrowError::General("Incorrect type".to_string()));
                    }
                }
                _ => return Err(GeoArrowError::General("Incorrect type".to_string())),
            }
        } else {
            self.push_null();
        };
        Ok(())
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_nullable_geometries(
        geoms: &[Option<impl GeometryTrait<T = f64>>],
        dim: Dimension,
        coord_type: CoordType,
        metadata: Arc<Metadata>,
    ) -> Result<Self> {
        let capacity = LineStringCapacity::from_geometries(geoms.iter().map(|x| x.as_ref()))?;
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

impl From<LineStringBuilder> for LineStringArray {
    fn from(mut other: LineStringBuilder) -> Self {
        let validity = other.validity.finish();
        Self::new(
            other.coords.into(),
            other.geom_offsets.into(),
            validity,
            other.metadata,
        )
    }
}

impl<G: LineStringTrait<T = f64>> From<(&[G], Dimension)> for LineStringBuilder {
    fn from((geoms, dim): (&[G], Dimension)) -> Self {
        Self::from_line_strings(geoms, dim, CoordType::Interleaved, Default::default())
    }
}

impl<G: LineStringTrait<T = f64>> From<(Vec<Option<G>>, Dimension)> for LineStringBuilder {
    fn from((geoms, dim): (Vec<Option<G>>, Dimension)) -> Self {
        Self::from_nullable_line_strings(&geoms, dim, CoordType::Interleaved, Default::default())
    }
}

impl<O: OffsetSizeTrait> TryFrom<(WKBArray<O>, Dimension)> for LineStringBuilder {
    type Error = GeoArrowError;

    fn try_from((value, dim): (WKBArray<O>, Dimension)) -> Result<Self> {
        let metadata = value.data_type.metadata().clone();
        let wkb_objects: Vec<Option<WKB<'_, O>>> = value.iter().collect();
        Self::from_wkb(&wkb_objects, dim, CoordType::Interleaved, metadata)
    }
}

/// LineString and MultiPoint have the same layout, so enable conversions between the two to change
/// the semantic type
impl From<LineStringBuilder> for MultiPointBuilder {
    fn from(value: LineStringBuilder) -> Self {
        Self::try_new(
            value.coords,
            value.geom_offsets,
            value.validity,
            value.metadata,
        )
        .unwrap()
    }
}
