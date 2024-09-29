use std::sync::Arc;

use crate::algorithm::native::downcast::can_downcast_multi;
use crate::algorithm::native::eq::offset_buffer_eq;
use crate::array::metadata::ArrayMetadata;
use crate::array::polygon::PolygonCapacity;
use crate::array::util::OffsetBufferUtils;
use crate::array::{CoordBuffer, CoordType, GeometryCollectionArray, MixedGeometryArray, MultiLineStringArray, MultiPolygonArray, RectArray, WKBArray};
use crate::datatypes::NativeType;
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::PolygonTrait;
use crate::scalar::Polygon;
use crate::trait_::{ArrayAccessor, GeometryArraySelfMethods, IntoArrow};
use crate::util::{owned_slice_offsets, owned_slice_validity};
use crate::{ArrayBase, NativeArray};
use arrow_array::{Array, OffsetSizeTrait};
use arrow_array::{GenericListArray, LargeListArray, ListArray};

use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};

use super::PolygonBuilder;

/// An immutable array of Polygon geometries using GeoArrow's in-memory representation.
///
/// This is semantically equivalent to `Vec<Option<Polygon>>` due to the internal validity bitmap.
#[derive(Debug, Clone)]
// #[derive(Debug, Clone, PartialEq)]
pub struct PolygonArray<const D: usize> {
    // Always NativeType::Polygon or NativeType::LargePolygon
    data_type: NativeType,

    pub(crate) metadata: Arc<ArrayMetadata>,

    pub(crate) coords: CoordBuffer<D>,

    /// Offsets into the ring array where each geometry starts
    pub(crate) geom_offsets: OffsetBuffer<i32>,

    /// Offsets into the coordinate array where each ring starts
    pub(crate) ring_offsets: OffsetBuffer<i32>,

    /// Validity bitmap
    pub(crate) validity: Option<NullBuffer>,
}

pub(super) fn check<const D: usize>(coords: &CoordBuffer<D>, geom_offsets: &OffsetBuffer<i32>, ring_offsets: &OffsetBuffer<i32>, validity_len: Option<usize>) -> Result<()> {
    if validity_len.map_or(false, |len| len != geom_offsets.len_proxy()) {
        return Err(GeoArrowError::General("validity mask length must match the number of values".to_string()));
    }

    if ring_offsets.last().to_usize().unwrap() != coords.len() {
        return Err(GeoArrowError::General("largest ring offset must match coords length".to_string()));
    }

    if geom_offsets.last().to_usize().unwrap() != ring_offsets.len_proxy() {
        return Err(GeoArrowError::General("largest geometry offset must match ring offsets length".to_string()));
    }

    Ok(())
}

impl<const D: usize> PolygonArray<D> {
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
    pub fn new(coords: CoordBuffer<D>, geom_offsets: OffsetBuffer<i32>, ring_offsets: OffsetBuffer<i32>, validity: Option<NullBuffer>, metadata: Arc<ArrayMetadata>) -> Self {
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
    pub fn try_new(coords: CoordBuffer<D>, geom_offsets: OffsetBuffer<i32>, ring_offsets: OffsetBuffer<i32>, validity: Option<NullBuffer>, metadata: Arc<ArrayMetadata>) -> Result<Self> {
        check(&coords, &geom_offsets, &ring_offsets, validity.as_ref().map(|v| v.len()))?;

        let coord_type = coords.coord_type();
        let data_type = NativeType::Polygon(coord_type, D.try_into()?);

        Ok(Self { data_type, coords, geom_offsets, ring_offsets, validity, metadata })
    }

    fn vertices_field(&self) -> Arc<Field> {
        Field::new("vertices", self.coords.storage_type(), false).into()
    }

    fn rings_field(&self) -> Arc<Field> {
        let name = "rings";
        Field::new_list(name, self.vertices_field(), false).into()
    }

    pub fn coords(&self) -> &CoordBuffer<D> {
        &self.coords
    }

    pub fn geom_offsets(&self) -> &OffsetBuffer<i32> {
        &self.geom_offsets
    }

    pub fn ring_offsets(&self) -> &OffsetBuffer<i32> {
        &self.ring_offsets
    }

    /// The lengths of each buffer contained in this array.
    pub fn buffer_lengths(&self) -> PolygonCapacity {
        PolygonCapacity::new(self.ring_offsets.last().to_usize().unwrap(), self.geom_offsets.last().to_usize().unwrap(), self.len())
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
        assert!(offset + length <= self.len(), "offset + length may not exceed length of array");
        // Note: we **only** slice the geom_offsets and not any actual data or other offsets.
        // Otherwise the offsets would be in the wrong location.
        Self {
            data_type: self.data_type,
            coords: self.coords.clone(),
            geom_offsets: self.geom_offsets.slice(offset, length),
            ring_offsets: self.ring_offsets.clone(),
            validity: self.validity.as_ref().map(|v| v.slice(offset, length)),
            metadata: self.metadata.clone(),
        }
    }

    pub fn owned_slice(&self, offset: usize, length: usize) -> Self {
        assert!(offset + length <= self.len(), "offset + length may not exceed length of array");
        assert!(length >= 1, "length must be at least 1");

        // Find the start and end of the ring offsets
        let (start_ring_idx, _) = self.geom_offsets.start_end(offset);
        let (_, end_ring_idx) = self.geom_offsets.start_end(offset + length - 1);

        // Find the start and end of the coord buffer
        let (start_coord_idx, _) = self.ring_offsets.start_end(start_ring_idx);
        let (_, end_coord_idx) = self.ring_offsets.start_end(end_ring_idx - 1);

        // Slice the geom_offsets
        let geom_offsets = owned_slice_offsets(&self.geom_offsets, offset, length);
        let ring_offsets = owned_slice_offsets(&self.ring_offsets, start_ring_idx, end_ring_idx - start_ring_idx);
        let coords = self.coords.owned_slice(start_coord_idx, end_coord_idx - start_coord_idx);

        let validity = owned_slice_validity(self.nulls(), offset, length);

        Self::new(coords, geom_offsets, ring_offsets, validity, self.metadata.clone())
    }

    pub fn to_coord_type(&self, coord_type: CoordType) -> Self {
        self.clone().into_coord_type(coord_type)
    }

    pub fn into_coord_type(self, coord_type: CoordType) -> Self {
        Self::new(self.coords.into_coord_type(coord_type), self.geom_offsets, self.ring_offsets, self.validity, self.metadata)
    }
}

impl<const D: usize> ArrayBase for PolygonArray<D> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn storage_type(&self) -> DataType {
        self.data_type.to_data_type()
    }

    fn extension_field(&self) -> Arc<Field> {
        self.data_type.to_field_with_metadata("geometry", true, &self.metadata).into()
    }

    fn extension_name(&self) -> &str {
        self.data_type.extension_name()
    }

    fn into_array_ref(self) -> Arc<dyn Array> {
        Arc::new(self.into_arrow())
    }

    fn to_array_ref(&self) -> arrow_array::ArrayRef {
        self.clone().into_array_ref()
    }

    fn metadata(&self) -> Arc<ArrayMetadata> {
        self.metadata.clone()
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
}

impl<const D: usize> NativeArray for PolygonArray<D> {
    fn data_type(&self) -> NativeType {
        self.data_type
    }

    fn coord_type(&self) -> CoordType {
        self.coords.coord_type()
    }

    fn to_coord_type(&self, coord_type: CoordType) -> Arc<dyn NativeArray> {
        Arc::new(self.clone().into_coord_type(coord_type))
    }

    fn with_metadata(&self, metadata: Arc<ArrayMetadata>) -> crate::trait_::NativeArrayRef {
        let mut arr = self.clone();
        arr.metadata = metadata;
        Arc::new(arr)
    }

    fn as_ref(&self) -> &dyn NativeArray {
        self
    }

    fn slice(&self, offset: usize, length: usize) -> Arc<dyn NativeArray> {
        Arc::new(self.slice(offset, length))
    }

    fn owned_slice(&self, offset: usize, length: usize) -> Arc<dyn NativeArray> {
        Arc::new(self.owned_slice(offset, length))
    }
}

impl<const D: usize> GeometryArraySelfMethods<D> for PolygonArray<D> {
    fn with_coords(self, coords: CoordBuffer<D>) -> Self {
        assert_eq!(coords.len(), self.coords.len());
        Self::new(coords, self.geom_offsets, self.ring_offsets, self.validity, self.metadata)
    }

    fn into_coord_type(self, coord_type: CoordType) -> Self {
        Self::new(self.coords.into_coord_type(coord_type), self.geom_offsets, self.ring_offsets, self.validity, self.metadata)
    }
}

// Implement geometry accessors
impl<'a, const D: usize> ArrayAccessor<'a> for PolygonArray<D> {
    type Item = Polygon<'a, D>;
    type ItemGeo = geo::Polygon;

    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        Polygon::new(&self.coords, &self.geom_offsets, &self.ring_offsets, index)
    }
}

impl<const D: usize> IntoArrow for PolygonArray<D> {
    type ArrowArray = GenericListArray<i32>;

    fn into_arrow(self) -> Self::ArrowArray {
        let vertices_field = self.vertices_field();
        let rings_field = self.rings_field();
        let validity = self.validity;
        let coord_array = self.coords.into_arrow();
        let ring_array = Arc::new(GenericListArray::new(vertices_field, self.ring_offsets, coord_array, None));
        GenericListArray::new(rings_field, self.geom_offsets, ring_array, validity)
    }
}

impl<O: OffsetSizeTrait, const D: usize> TryFrom<&GenericListArray<O>> for PolygonArray<D> {
    type Error = GeoArrowError;

    fn try_from(geom_array: &GenericListArray<O>) -> Result<Self> {
        let geom_offsets = geom_array.offsets();
        let validity = geom_array.nulls();

        let rings_dyn_array = geom_array.values();
        let rings_array = rings_dyn_array.as_any().downcast_ref::<GenericListArray<O>>().unwrap();

        let ring_offsets = rings_array.offsets();
        let coords: CoordBuffer<D> = rings_array.values().as_ref().try_into()?;

        Ok(Self::new(coords, geom_offsets.clone(), ring_offsets.clone(), validity.cloned(), Default::default()))
    }
}

impl<const D: usize> TryFrom<&dyn Array> for PolygonArray<D> {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self> {
        match value.data_type() {
            DataType::List(_) => {
                let downcasted = value.as_any().downcast_ref::<ListArray>().unwrap();
                downcasted.try_into()
            }
            DataType::LargeList(_) => {
                let downcasted = value.as_any().downcast_ref::<LargeListArray>().unwrap();
                let geom_array: PolygonArray<i64, D> = downcasted.try_into()?;
                geom_array.try_into()
            }
            _ => Err(GeoArrowError::General(format!("Unexpected type: {:?}", value.data_type()))),
        }
    }
}

impl<const D: usize> TryFrom<(&dyn Array, &Field)> for PolygonArray<D> {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let mut arr: Self = arr.try_into()?;
        arr.metadata = Arc::new(ArrayMetadata::try_from(field)?);
        Ok(arr)
    }
}

impl<G: PolygonTrait<T = f64>, const D: usize> From<Vec<Option<G>>> for PolygonArray<D> {
    fn from(other: Vec<Option<G>>) -> Self {
        let mut_arr: PolygonBuilder<D> = other.into();
        mut_arr.into()
    }
}

impl<G: PolygonTrait<T = f64>, const D: usize> From<&[G]> for PolygonArray<D> {
    fn from(other: &[G]) -> Self {
        let mut_arr: PolygonBuilder<D> = other.into();
        mut_arr.into()
    }
}

impl<O: OffsetSizeTrait, const D: usize> TryFrom<WKBArray<O>> for PolygonArray<D> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self> {
        let mut_arr: PolygonBuilder<D> = value.try_into()?;
        Ok(mut_arr.into())
    }
}

/// Polygon and MultiLineString have the same layout, so enable conversions between the two to
/// change the semantic type
impl<const D: usize> From<PolygonArray<D>> for MultiLineStringArray<D> {
    fn from(value: PolygonArray<D>) -> Self {
        Self::new(value.coords, value.geom_offsets, value.ring_offsets, value.validity, value.metadata)
    }
}

impl From<RectArray<2>> for PolygonArray<2> {
    fn from(value: RectArray<2>) -> Self {
        // The number of output geoms is the same as the input
        let geom_capacity = value.len();

        // Each output polygon is a simple polygon with only one ring
        let ring_capacity = geom_capacity;

        // Each output polygon has exactly 5 coordinates
        // Don't reserve capacity for null entries
        let coord_capacity = (value.len() - value.null_count()) * 5;

        let capacity = PolygonCapacity::new(coord_capacity, ring_capacity, geom_capacity);
        let mut output_array = PolygonBuilder::with_capacity(capacity);

        value.iter_geo().for_each(|maybe_g| output_array.push_polygon(maybe_g.map(|geom| geom.to_polygon()).as_ref()).unwrap());

        output_array.into()
    }
}

/// Default to an empty array
impl<const D: usize> Default for PolygonArray<D> {
    fn default() -> Self {
        PolygonBuilder::default().into()
    }
}

impl<const D: usize> PartialEq for PolygonArray<D> {
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

impl<const D: usize> TryFrom<MultiPolygonArray<D>> for PolygonArray<D> {
    type Error = GeoArrowError;

    fn try_from(value: MultiPolygonArray<D>) -> Result<Self> {
        if !can_downcast_multi(&value.geom_offsets) {
            return Err(GeoArrowError::General("Unable to cast".to_string()));
        }

        Ok(PolygonArray::new(value.coords, value.polygon_offsets, value.ring_offsets, value.validity, value.metadata))
    }
}

impl<const D: usize> TryFrom<MixedGeometryArray<D>> for PolygonArray<D> {
    type Error = GeoArrowError;

    fn try_from(value: MixedGeometryArray<D>) -> Result<Self> {
        if value.has_points() || value.has_line_strings() || value.has_multi_points() || value.has_multi_line_strings() {
            return Err(GeoArrowError::General("Unable to cast".to_string()));
        }

        if value.has_only_polygons() {
            return Ok(value.polygons);
        }

        if value.has_only_multi_polygons() {
            return value.multi_polygons.try_into();
        }

        let mut capacity = value.polygons.buffer_lengths();
        let buffer_lengths = value.multi_polygons.buffer_lengths();
        capacity.coord_capacity += buffer_lengths.coord_capacity;
        capacity.ring_capacity += buffer_lengths.ring_capacity;
        capacity.geom_capacity += buffer_lengths.polygon_capacity;

        let mut builder = PolygonBuilder::<D>::with_capacity_and_options(capacity, value.coord_type(), value.metadata());
        value.iter().try_for_each(|x| builder.push_geometry(x.as_ref()))?;
        Ok(builder.finish())
    }
}

impl<const D: usize> TryFrom<GeometryCollectionArray<D>> for PolygonArray<D> {
    type Error = GeoArrowError;

    fn try_from(value: GeometryCollectionArray<D>) -> Result<Self> {
        MixedGeometryArray::try_from(value)?.try_into()
    }
}

#[cfg(test)]
mod test {
    use crate::test::geoarrow_data::{example_polygon_interleaved, example_polygon_separated, example_polygon_wkb};
    use crate::test::polygon::{p0, p1};

    use super::*;

    #[test]
    fn geo_roundtrip_accurate() {
        let arr: PolygonArray<2> = vec![p0(), p1()].as_slice().into();
        assert_eq!(arr.value_as_geo(0), p0());
        assert_eq!(arr.value_as_geo(1), p1());
    }

    #[test]
    fn geo_roundtrip_accurate_option_vec() {
        let arr: PolygonArray<2> = vec![Some(p0()), Some(p1()), None].into();
        assert_eq!(arr.get_as_geo(0), Some(p0()));
        assert_eq!(arr.get_as_geo(1), Some(p1()));
        assert_eq!(arr.get_as_geo(2), None);
    }

    #[test]
    fn slice() {
        let arr: PolygonArray<2> = vec![p0(), p1()].as_slice().into();
        let sliced = arr.slice(1, 1);

        assert_eq!(sliced.len(), 1);
        assert_eq!(sliced.get_as_geo(0), Some(p1()));

        // // Offset is 1 because it's sliced on another backing buffer
        // assert_eq!(*arr.geom_offsets.first(), 1);
    }

    #[test]
    fn owned_slice() {
        let arr: PolygonArray<2> = vec![p0(), p1()].as_slice().into();
        let sliced = arr.owned_slice(1, 1);

        // assert!(
        //     !sliced.geom_offsets.buffer().is_sliced(),
        //     "underlying offsets should not be sliced"
        // );
        assert_eq!(arr.len(), 2);
        assert_eq!(sliced.len(), 1);
        assert_eq!(sliced.get_as_geo(0), Some(p1()));

        // // Offset is 0 because it's copied to an owned buffer
        // assert_eq!(*sliced.geom_offsets.first(), 0);
        // assert_eq!(*sliced.ring_offsets.first(), 0);
    }

    #[test]
    fn parse_wkb_geoarrow_interleaved_example() {
        let geom_arr = example_polygon_interleaved();

        let wkb_arr = example_polygon_wkb();
        let parsed_geom_arr: PolygonArray<2> = wkb_arr.try_into().unwrap();

        assert_eq!(geom_arr, parsed_geom_arr);
    }

    #[test]
    fn parse_wkb_geoarrow_separated_example() {
        // TODO: support checking equality of interleaved vs separated coords
        let geom_arr = example_polygon_separated().into_coord_type(CoordType::Interleaved);

        let wkb_arr = example_polygon_wkb();
        let parsed_geom_arr: PolygonArray<2> = wkb_arr.try_into().unwrap();

        assert_eq!(geom_arr, parsed_geom_arr);
    }
}
