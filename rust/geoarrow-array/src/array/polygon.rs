use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, OffsetSizeTrait};
use arrow_array::{ArrayRef, GenericListArray};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use geoarrow_schema::{CoordType, Metadata, PolygonType};

use crate::array::{CoordBuffer, RectArray, WKBArray};
use crate::builder::PolygonBuilder;
use crate::capacity::PolygonCapacity;
use crate::datatypes::GeoArrowType;
use crate::eq::offset_buffer_eq;
use crate::error::{GeoArrowError, Result};
use crate::scalar::Polygon;
use crate::trait_::{ArrayAccessor, GeoArrowArray, IntoArrow};
use crate::util::{offsets_buffer_i64_to_i32, OffsetBufferUtils};

/// An immutable array of Polygon geometries using GeoArrow's in-memory representation.
///
/// All polygons must have the same dimension.
///
/// This is semantically equivalent to `Vec<Option<Polygon>>` due to the internal validity bitmap.
#[derive(Debug, Clone)]
// #[derive(Debug, Clone, PartialEq)]
pub struct PolygonArray {
    pub(crate) data_type: PolygonType,

    pub(crate) coords: CoordBuffer,

    /// Offsets into the ring array where each geometry starts
    pub(crate) geom_offsets: OffsetBuffer<i32>,

    /// Offsets into the coordinate array where each ring starts
    pub(crate) ring_offsets: OffsetBuffer<i32>,

    /// Validity bitmap
    pub(crate) validity: Option<NullBuffer>,
}

pub(super) fn check(
    coords: &CoordBuffer,
    geom_offsets: &OffsetBuffer<i32>,
    ring_offsets: &OffsetBuffer<i32>,
    validity_len: Option<usize>,
) -> Result<()> {
    if validity_len.is_some_and(|len| len != geom_offsets.len_proxy()) {
        return Err(GeoArrowError::General(
            "validity mask length must match the number of values".to_string(),
        ));
    }

    if *ring_offsets.last() as usize != coords.len() {
        return Err(GeoArrowError::General(
            "largest ring offset must match coords length".to_string(),
        ));
    }

    if *geom_offsets.last() as usize != ring_offsets.len_proxy() {
        return Err(GeoArrowError::General(
            "largest geometry offset must match ring offsets length".to_string(),
        ));
    }

    Ok(())
}

impl PolygonArray {
    /// Create a new PolygonArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Panics
    ///
    /// - if the validity is not `None` and its length is different from the number of geometries
    /// - if the largest ring offset does not match the number of coordinates
    /// - if the largest geometry offset does not match the size of ring offsets
    pub fn new(
        coords: CoordBuffer,
        geom_offsets: OffsetBuffer<i32>,
        ring_offsets: OffsetBuffer<i32>,
        validity: Option<NullBuffer>,
        metadata: Arc<Metadata>,
    ) -> Self {
        Self::try_new(coords, geom_offsets, ring_offsets, validity, metadata).unwrap()
    }

    /// Create a new PolygonArray from parts
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
        coords: CoordBuffer,
        geom_offsets: OffsetBuffer<i32>,
        ring_offsets: OffsetBuffer<i32>,
        validity: Option<NullBuffer>,
        metadata: Arc<Metadata>,
    ) -> Result<Self> {
        check(
            &coords,
            &geom_offsets,
            &ring_offsets,
            validity.as_ref().map(|v| v.len()),
        )?;
        Ok(Self {
            data_type: PolygonType::new(coords.coord_type(), coords.dim(), metadata),
            coords,
            geom_offsets,
            ring_offsets,
            validity,
        })
    }

    fn vertices_field(&self) -> Arc<Field> {
        Field::new("vertices", self.coords.storage_type(), false).into()
    }

    fn rings_field(&self) -> Arc<Field> {
        let name = "rings";
        Field::new_list(name, self.vertices_field(), false).into()
    }

    /// Access the underlying coordinate buffer
    pub fn coords(&self) -> &CoordBuffer {
        &self.coords
    }

    /// Access the underlying geometry offsets buffer
    pub fn geom_offsets(&self) -> &OffsetBuffer<i32> {
        &self.geom_offsets
    }

    /// Access the underlying ring offsets buffer
    pub fn ring_offsets(&self) -> &OffsetBuffer<i32> {
        &self.ring_offsets
    }

    /// The lengths of each buffer contained in this array.
    pub fn buffer_lengths(&self) -> PolygonCapacity {
        PolygonCapacity::new(
            *self.ring_offsets.last() as usize,
            *self.geom_offsets.last() as usize,
            self.len(),
        )
    }

    /// The number of bytes occupied by this array.
    pub fn num_bytes(&self) -> usize {
        let validity_len = self.nulls().map(|v| v.buffer().len()).unwrap_or(0);
        validity_len + self.buffer_lengths().num_bytes()
    }

    /// Slices this [`PolygonArray`] in place.
    /// # Panic
    /// This function panics iff `offset + length > self.len()`.
    #[inline]
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        // Note: we **only** slice the geom_offsets and not any actual data or other offsets.
        // Otherwise the offsets would be in the wrong location.
        Self {
            data_type: self.data_type.clone(),
            coords: self.coords.clone(),
            geom_offsets: self.geom_offsets.slice(offset, length),
            ring_offsets: self.ring_offsets.clone(),
            validity: self.validity.as_ref().map(|v| v.slice(offset, length)),
        }
    }
}

impl GeoArrowArray for PolygonArray {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn into_array_ref(self) -> ArrayRef {
        Arc::new(self.into_arrow())
    }

    fn to_array_ref(&self) -> ArrayRef {
        self.clone().into_array_ref()
    }

    /// Returns the number of geometries in this array
    #[inline]
    fn len(&self) -> usize {
        self.geom_offsets.len_proxy()
    }

    /// Returns the optional validity.
    #[inline]
    fn nulls(&self) -> Option<&NullBuffer> {
        self.validity.as_ref()
    }

    fn data_type(&self) -> GeoArrowType {
        GeoArrowType::Polygon(self.data_type.clone())
    }

    fn slice(&self, offset: usize, length: usize) -> Arc<dyn GeoArrowArray> {
        Arc::new(self.slice(offset, length))
    }
}

impl<'a> ArrayAccessor<'a> for PolygonArray {
    type Item = Polygon<'a>;

    unsafe fn value_unchecked(&'a self, index: usize) -> Result<Self::Item> {
        Ok(Polygon::new(
            &self.coords,
            &self.geom_offsets,
            &self.ring_offsets,
            index,
        ))
    }
}

impl IntoArrow for PolygonArray {
    type ArrowArray = GenericListArray<i32>;
    type ExtensionType = PolygonType;

    fn into_arrow(self) -> Self::ArrowArray {
        let vertices_field = self.vertices_field();
        let rings_field = self.rings_field();
        let validity = self.validity;
        let coord_array = self.coords.into();
        let ring_array = Arc::new(GenericListArray::new(
            vertices_field,
            self.ring_offsets,
            coord_array,
            None,
        ));
        GenericListArray::new(rings_field, self.geom_offsets, ring_array, validity)
    }

    fn ext_type(&self) -> &Self::ExtensionType {
        &self.data_type
    }
}

impl TryFrom<(&GenericListArray<i32>, PolygonType)> for PolygonArray {
    type Error = GeoArrowError;

    fn try_from((geom_array, typ): (&GenericListArray<i32>, PolygonType)) -> Result<Self> {
        let geom_offsets = geom_array.offsets();
        let validity = geom_array.nulls();

        let rings_dyn_array = geom_array.values();
        let rings_array = rings_dyn_array.as_list::<i32>();

        let ring_offsets = rings_array.offsets();
        let coords = CoordBuffer::from_arrow(rings_array.values().as_ref(), typ.dimension())?;

        Ok(Self::new(
            coords,
            geom_offsets.clone(),
            ring_offsets.clone(),
            validity.cloned(),
            typ.metadata().clone(),
        ))
    }
}

impl TryFrom<(&GenericListArray<i64>, PolygonType)> for PolygonArray {
    type Error = GeoArrowError;

    fn try_from((geom_array, typ): (&GenericListArray<i64>, PolygonType)) -> Result<Self> {
        let geom_offsets = offsets_buffer_i64_to_i32(geom_array.offsets())?;
        let validity = geom_array.nulls();

        let rings_dyn_array = geom_array.values();
        let rings_array = rings_dyn_array.as_list::<i64>();

        let ring_offsets = offsets_buffer_i64_to_i32(rings_array.offsets())?;
        let coords = CoordBuffer::from_arrow(rings_array.values().as_ref(), typ.dimension())?;

        Ok(Self::new(
            coords,
            geom_offsets,
            ring_offsets,
            validity.cloned(),
            typ.metadata().clone(),
        ))
    }
}
impl TryFrom<(&dyn Array, PolygonType)> for PolygonArray {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&dyn Array, PolygonType)) -> Result<Self> {
        match value.data_type() {
            DataType::List(_) => (value.as_list::<i32>(), typ).try_into(),
            DataType::LargeList(_) => (value.as_list::<i64>(), typ).try_into(),
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl TryFrom<(&dyn Array, &Field)> for PolygonArray {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let typ = field.try_extension_type::<PolygonType>()?;
        (arr, typ).try_into()
    }
}

impl<O: OffsetSizeTrait> TryFrom<(WKBArray<O>, PolygonType)> for PolygonArray {
    type Error = GeoArrowError;

    fn try_from(value: (WKBArray<O>, PolygonType)) -> Result<Self> {
        let mut_arr: PolygonBuilder = value.try_into()?;
        Ok(mut_arr.finish())
    }
}

impl From<RectArray> for PolygonArray {
    fn from(value: RectArray) -> Self {
        let polygon_type = PolygonType::new(
            CoordType::Separated,
            value.data_type.dimension(),
            value.data_type.metadata().clone(),
        );

        // The number of output geoms is the same as the input
        let geom_capacity = value.len();

        // Each output polygon is a simple polygon with only one ring
        let ring_capacity = geom_capacity;

        // Each output polygon has exactly 5 coordinates
        // Don't reserve capacity for null entries
        let coord_capacity = (value.len() - value.null_count()) * 5;

        let capacity = PolygonCapacity::new(coord_capacity, ring_capacity, geom_capacity);
        let mut output_array = PolygonBuilder::with_capacity(polygon_type, capacity);

        value.iter().for_each(|maybe_g| {
            output_array
                .push_rect(maybe_g.transpose().unwrap().as_ref())
                .unwrap()
        });

        output_array.finish()
    }
}

impl PartialEq for PolygonArray {
    fn eq(&self, other: &Self) -> bool {
        if self.validity != other.validity {
            return false;
        }

        if !offset_buffer_eq(&self.geom_offsets, &other.geom_offsets) {
            return false;
        }

        if !offset_buffer_eq(&self.ring_offsets, &other.ring_offsets) {
            return false;
        }

        if self.coords != other.coords {
            return false;
        }

        true
    }
}
