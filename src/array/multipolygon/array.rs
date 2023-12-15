use std::collections::HashMap;
use std::sync::Arc;

use crate::algorithm::native::eq::offset_buffer_eq;
use crate::array::multipolygon::MultiPolygonCapacity;
use crate::array::offset_builder::OffsetsBuilder;
use crate::array::util::{offsets_buffer_i32_to_i64, offsets_buffer_i64_to_i32, OffsetBufferUtils};
use crate::array::zip_validity::ZipValidity;
use crate::array::{CoordBuffer, CoordType, PolygonArray, WKBArray};
use crate::datatypes::GeoDataType;
use crate::error::GeoArrowError;
use crate::geo_traits::MultiPolygonTrait;
use crate::scalar::MultiPolygon;
use crate::trait_::{GeometryArrayAccessor, GeometryArraySelfMethods, IntoArrow};
use crate::util::{owned_slice_offsets, owned_slice_validity};
use crate::GeometryArrayTrait;
use arrow_array::{Array, GenericListArray, LargeListArray, ListArray, OffsetSizeTrait};
use arrow_buffer::bit_iterator::BitIterator;
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};

use super::MultiPolygonBuilder;

/// An immutable array of MultiPolygon geometries using GeoArrow's in-memory representation.
///
/// This is semantically equivalent to `Vec<Option<MultiPolygon>>` due to the internal validity
/// bitmap.
#[derive(Debug, Clone)]
// #[derive(Debug, Clone, PartialEq)]
pub struct MultiPolygonArray<O: OffsetSizeTrait> {
    // Always GeoDataType::MultiPolygon or GeoDataType::LargeMultiPolygon
    data_type: GeoDataType,

    pub coords: CoordBuffer,

    /// Offsets into the polygon array where each geometry starts
    pub geom_offsets: OffsetBuffer<O>,

    /// Offsets into the ring array where each polygon starts
    pub polygon_offsets: OffsetBuffer<O>,

    /// Offsets into the coordinate array where each ring starts
    pub ring_offsets: OffsetBuffer<O>,

    /// Validity bitmap
    pub validity: Option<NullBuffer>,
}

pub(super) fn check<O: OffsetSizeTrait>(
    coords: &CoordBuffer,
    geom_offsets: &OffsetBuffer<O>,
    polygon_offsets: &OffsetBuffer<O>,
    ring_offsets: &OffsetBuffer<O>,
    validity_len: Option<usize>,
) -> Result<(), GeoArrowError> {
    if validity_len.map_or(false, |len| len != geom_offsets.len_proxy()) {
        return Err(GeoArrowError::General(
            "validity mask length must match the number of values".to_string(),
        ));
    }
    if ring_offsets.last().to_usize().unwrap() != coords.len() {
        return Err(GeoArrowError::General(
            "largest ring offset must match coords length".to_string(),
        ));
    }

    if polygon_offsets.last().to_usize().unwrap() != ring_offsets.len_proxy() {
        return Err(GeoArrowError::General(
            "largest polygon offset must match ring offsets length".to_string(),
        ));
    }

    if geom_offsets.last().to_usize().unwrap() != polygon_offsets.len_proxy() {
        return Err(GeoArrowError::General(
            "largest geometry offset must match polygon offsets length".to_string(),
        ));
    }

    Ok(())
}

impl<O: OffsetSizeTrait> MultiPolygonArray<O> {
    /// Create a new MultiPolygonArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Panics
    ///
    /// - if the validity is not `None` and its length is different from the number of geometries
    /// - if the largest ring offset does not match the number of coordinates
    /// - if the largest polygon offset does not match the size of ring offsets
    /// - if the largest geometry offset does not match the size of polygon offsets
    pub fn new(
        coords: CoordBuffer,
        geom_offsets: OffsetBuffer<O>,
        polygon_offsets: OffsetBuffer<O>,
        ring_offsets: OffsetBuffer<O>,
        validity: Option<NullBuffer>,
    ) -> Self {
        Self::try_new(
            coords,
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            validity,
        )
        .unwrap()
    }

    /// Create a new MultiPolygonArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Errors
    ///
    /// - if the validity is not `None` and its length is different from the number of geometries
    /// - if the largest ring offset does not match the number of coordinates
    /// - if the largest polygon offset does not match the size of ring offsets
    /// - if the largest geometry offset does not match the size of polygon offsets
    pub fn try_new(
        coords: CoordBuffer,
        geom_offsets: OffsetBuffer<O>,
        polygon_offsets: OffsetBuffer<O>,
        ring_offsets: OffsetBuffer<O>,
        validity: Option<NullBuffer>,
    ) -> Result<Self, GeoArrowError> {
        check(
            &coords,
            &geom_offsets,
            &polygon_offsets,
            &ring_offsets,
            validity.as_ref().map(|v| v.len()),
        )?;

        let coord_type = coords.coord_type();
        let data_type = match O::IS_LARGE {
            true => GeoDataType::LargeMultiPolygon(coord_type),
            false => GeoDataType::MultiPolygon(coord_type),
        };

        Ok(Self {
            data_type,
            coords,
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            validity,
        })
    }

    fn vertices_field(&self) -> Arc<Field> {
        Field::new("vertices", self.coords.storage_type(), false).into()
    }

    fn rings_field(&self) -> Arc<Field> {
        let name = "rings";
        match O::IS_LARGE {
            true => Field::new_large_list(name, self.vertices_field(), false).into(),
            false => Field::new_list(name, self.vertices_field(), false).into(),
        }
    }

    fn polygons_field(&self) -> Arc<Field> {
        let name = "polygons";
        match O::IS_LARGE {
            true => Field::new_large_list(name, self.rings_field(), false).into(),
            false => Field::new_list(name, self.rings_field(), false).into(),
        }
    }

    fn outer_type(&self) -> DataType {
        match O::IS_LARGE {
            true => DataType::LargeList(self.polygons_field()),
            false => DataType::List(self.polygons_field()),
        }
    }

    pub fn buffer_lengths(&self) -> MultiPolygonCapacity {
        MultiPolygonCapacity::new(
            self.ring_offsets.last().to_usize().unwrap(),
            self.polygon_offsets.last().to_usize().unwrap(),
            self.geom_offsets.last().to_usize().unwrap(),
            self.len(),
        )
    }
}

impl<O: OffsetSizeTrait> GeometryArrayTrait for MultiPolygonArray<O> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> &GeoDataType {
        &self.data_type
    }

    fn storage_type(&self) -> DataType {
        self.outer_type()
    }

    fn extension_field(&self) -> Arc<Field> {
        let mut metadata = HashMap::new();
        metadata.insert(
            "ARROW:extension:name".to_string(),
            self.extension_name().to_string(),
        );
        Arc::new(Field::new("geometry", self.storage_type(), true).with_metadata(metadata))
    }

    fn extension_name(&self) -> &str {
        "geoarrow.multipolygon"
    }

    fn into_array_ref(self) -> Arc<dyn Array> {
        Arc::new(self.into_arrow())
    }

    fn coord_type(&self) -> CoordType {
        self.coords.coord_type()
    }

    /// Returns the number of geometries in this array
    #[inline]
    fn len(&self) -> usize {
        self.geom_offsets.len_proxy()
    }

    /// Returns the optional validity.
    #[inline]
    fn validity(&self) -> Option<&NullBuffer> {
        self.validity.as_ref()
    }
}

impl<O: OffsetSizeTrait> GeometryArraySelfMethods for MultiPolygonArray<O> {
    fn with_coords(self, coords: CoordBuffer) -> Self {
        assert_eq!(coords.len(), self.coords.len());
        Self::new(
            coords,
            self.geom_offsets,
            self.polygon_offsets,
            self.ring_offsets,
            self.validity,
        )
    }

    fn into_coord_type(self, coord_type: CoordType) -> Self {
        Self::new(
            self.coords.into_coord_type(coord_type),
            self.geom_offsets,
            self.polygon_offsets,
            self.ring_offsets,
            self.validity,
        )
    }

    /// Slices this [`MultiPolygonArray`] in place.
    /// # Panic
    /// This function panics iff `offset + length > self.len()`.
    #[inline]
    fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        // Note: we **only** slice the geom_offsets and not any actual data. Otherwise the offsets
        // would be in the wrong location.
        Self {
            data_type: self.data_type.clone(),
            coords: self.coords.clone(),
            geom_offsets: self.geom_offsets.slice(offset, length),
            polygon_offsets: self.polygon_offsets.clone(),
            ring_offsets: self.ring_offsets.clone(),
            validity: self.validity.as_ref().map(|v| v.slice(offset, length)),
        }
    }

    fn owned_slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        assert!(length >= 1, "length must be at least 1");

        // Find the start and end of the polygon offsets
        let (start_polygon_idx, _) = self.geom_offsets.start_end(offset);
        let (_, end_polygon_idx) = self.geom_offsets.start_end(offset + length - 1);

        // Find the start and end of the ring offsets
        let (start_ring_idx, _) = self.polygon_offsets.start_end(start_polygon_idx);
        let (_, end_ring_idx) = self.polygon_offsets.start_end(end_polygon_idx - 1);

        // Find the start and end of the coord buffer
        let (start_coord_idx, _) = self.ring_offsets.start_end(start_ring_idx);
        let (_, end_coord_idx) = self.ring_offsets.start_end(end_ring_idx - 1);

        // Slice the geom_offsets
        let geom_offsets = owned_slice_offsets(&self.geom_offsets, offset, length);
        let polygon_offsets = owned_slice_offsets(
            &self.polygon_offsets,
            start_polygon_idx,
            end_polygon_idx - start_polygon_idx,
        );
        let ring_offsets = owned_slice_offsets(
            &self.ring_offsets,
            start_ring_idx,
            end_ring_idx - start_ring_idx,
        );
        let coords = self
            .coords
            .owned_slice(start_coord_idx, end_coord_idx - start_coord_idx);

        let validity = owned_slice_validity(self.nulls(), offset, length);

        Self::new(
            coords,
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            validity,
        )
    }
}

// Implement geometry accessors
impl<'a, O: OffsetSizeTrait> GeometryArrayAccessor<'a> for MultiPolygonArray<O> {
    type Item = MultiPolygon<'a, O>;
    type ItemGeo = geo::MultiPolygon;

    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        MultiPolygon::new_borrowed(
            &self.coords,
            &self.geom_offsets,
            &self.polygon_offsets,
            &self.ring_offsets,
            index,
        )
    }
}

impl<O: OffsetSizeTrait> IntoArrow for MultiPolygonArray<O> {
    type ArrowArray = GenericListArray<O>;

    fn into_arrow(self) -> Self::ArrowArray {
        let vertices_field = self.vertices_field();
        let rings_field = self.rings_field();
        let polygons_field = self.polygons_field();

        let validity = self.validity;
        let coord_array = self.coords.into_arrow();
        let ring_array = Arc::new(GenericListArray::new(
            vertices_field,
            self.ring_offsets,
            coord_array,
            None,
        ));
        let polygons_array = Arc::new(GenericListArray::new(
            rings_field,
            self.polygon_offsets,
            ring_array,
            None,
        ));
        GenericListArray::new(polygons_field, self.geom_offsets, polygons_array, validity)
    }
}
impl<O: OffsetSizeTrait> MultiPolygonArray<O> {
    /// Iterator over geo Geometry objects, not looking at validity
    pub fn iter_geo_values(&self) -> impl Iterator<Item = geo::MultiPolygon> + '_ {
        (0..self.len()).map(|i| self.value_as_geo(i))
    }

    /// Iterator over geo Geometry objects, taking into account validity
    pub fn iter_geo(
        &self,
    ) -> ZipValidity<geo::MultiPolygon, impl Iterator<Item = geo::MultiPolygon> + '_, BitIterator>
    {
        ZipValidity::new_with_validity(self.iter_geo_values(), self.nulls())
    }

    /// Returns the value at slot `i` as a GEOS geometry.
    #[cfg(feature = "geos")]
    pub fn value_as_geos(&self, i: usize) -> geos::Geometry {
        self.value(i).try_into().unwrap()
    }

    /// Gets the value at slot `i` as a GEOS geometry, additionally checking the validity bitmap
    #[cfg(feature = "geos")]
    pub fn get_as_geos(&self, i: usize) -> Option<geos::Geometry> {
        if self.is_null(i) {
            return None;
        }

        Some(self.value_as_geos(i))
    }

    /// Iterator over GEOS geometry objects
    #[cfg(feature = "geos")]
    pub fn iter_geos_values(&self) -> impl Iterator<Item = geos::Geometry> + '_ {
        (0..self.len()).map(|i| self.value_as_geos(i))
    }

    /// Iterator over GEOS geometry objects, taking validity into account
    #[cfg(feature = "geos")]
    pub fn iter_geos(
        &self,
    ) -> ZipValidity<geos::Geometry, impl Iterator<Item = geos::Geometry> + '_, BitIterator> {
        ZipValidity::new_with_validity(self.iter_geos_values(), self.nulls())
    }
}

impl<O: OffsetSizeTrait> TryFrom<&GenericListArray<O>> for MultiPolygonArray<O> {
    type Error = GeoArrowError;

    fn try_from(geom_array: &GenericListArray<O>) -> Result<Self, Self::Error> {
        let geom_offsets = geom_array.offsets();
        let validity = geom_array.nulls();

        let polygons_dyn_array = geom_array.values();
        let polygons_array = polygons_dyn_array
            .as_any()
            .downcast_ref::<GenericListArray<O>>()
            .unwrap();

        let polygon_offsets = polygons_array.offsets();
        let rings_dyn_array = polygons_array.values();
        let rings_array = rings_dyn_array
            .as_any()
            .downcast_ref::<GenericListArray<O>>()
            .unwrap();

        let ring_offsets = rings_array.offsets();
        let coords: CoordBuffer = rings_array.values().as_ref().try_into()?;

        Ok(Self::new(
            coords,
            geom_offsets.clone(),
            polygon_offsets.clone(),
            ring_offsets.clone(),
            validity.cloned(),
        ))
    }
}

impl TryFrom<&dyn Array> for MultiPolygonArray<i32> {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self, Self::Error> {
        match value.data_type() {
            DataType::List(_) => {
                let downcasted = value.as_any().downcast_ref::<ListArray>().unwrap();
                downcasted.try_into()
            }
            DataType::LargeList(_) => {
                let downcasted = value.as_any().downcast_ref::<LargeListArray>().unwrap();
                let geom_array: MultiPolygonArray<i64> = downcasted.try_into()?;
                geom_array.try_into()
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}
impl TryFrom<&dyn Array> for MultiPolygonArray<i64> {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self, Self::Error> {
        match value.data_type() {
            DataType::List(_) => {
                let downcasted = value.as_any().downcast_ref::<ListArray>().unwrap();
                let geom_array: MultiPolygonArray<i32> = downcasted.try_into()?;
                Ok(geom_array.into())
            }
            DataType::LargeList(_) => {
                let downcasted = value.as_any().downcast_ref::<LargeListArray>().unwrap();
                downcasted.try_into()
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl<O: OffsetSizeTrait, G: MultiPolygonTrait<T = f64>> From<Vec<Option<G>>>
    for MultiPolygonArray<O>
{
    fn from(other: Vec<Option<G>>) -> Self {
        let mut_arr: MultiPolygonBuilder<O> = other.into();
        mut_arr.into()
    }
}

impl<O: OffsetSizeTrait, G: MultiPolygonTrait<T = f64>> From<&[G]> for MultiPolygonArray<O> {
    fn from(other: &[G]) -> Self {
        let mut_arr: MultiPolygonBuilder<O> = other.into();
        mut_arr.into()
    }
}

impl<O: OffsetSizeTrait, G: MultiPolygonTrait<T = f64>>
    From<bumpalo::collections::Vec<'_, Option<G>>> for MultiPolygonArray<O>
{
    fn from(other: bumpalo::collections::Vec<'_, Option<G>>) -> Self {
        let mut_arr: MultiPolygonBuilder<O> = other.into();
        mut_arr.into()
    }
}

impl<O: OffsetSizeTrait, G: MultiPolygonTrait<T = f64>> From<bumpalo::collections::Vec<'_, G>>
    for MultiPolygonArray<O>
{
    fn from(other: bumpalo::collections::Vec<'_, G>) -> Self {
        let mut_arr: MultiPolygonBuilder<O> = other.into();
        mut_arr.into()
    }
}

impl<O: OffsetSizeTrait> TryFrom<WKBArray<O>> for MultiPolygonArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self, Self::Error> {
        let mut_arr: MultiPolygonBuilder<O> = value.try_into()?;
        Ok(mut_arr.into())
    }
}

impl<O: OffsetSizeTrait> TryFrom<PolygonArray<O>> for MultiPolygonArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: PolygonArray<O>) -> Result<Self, Self::Error> {
        let geom_length = value.len();

        let coords = value.coords;
        let ring_offsets = value.ring_offsets;
        let polygon_offsets = value.geom_offsets;
        let validity = value.validity;

        // Create offsets that are all of length 1
        let mut geom_offsets = OffsetsBuilder::with_capacity(geom_length);
        for _ in 0..coords.len() {
            geom_offsets.try_push_usize(1)?;
        }

        Ok(Self::new(
            coords,
            geom_offsets.into(),
            polygon_offsets,
            ring_offsets,
            validity,
        ))
    }
}

impl From<MultiPolygonArray<i32>> for MultiPolygonArray<i64> {
    fn from(value: MultiPolygonArray<i32>) -> Self {
        Self::new(
            value.coords,
            offsets_buffer_i32_to_i64(&value.geom_offsets),
            offsets_buffer_i32_to_i64(&value.polygon_offsets),
            offsets_buffer_i32_to_i64(&value.ring_offsets),
            value.validity,
        )
    }
}

impl TryFrom<MultiPolygonArray<i64>> for MultiPolygonArray<i32> {
    type Error = GeoArrowError;

    fn try_from(value: MultiPolygonArray<i64>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            value.coords,
            offsets_buffer_i64_to_i32(&value.geom_offsets)?,
            offsets_buffer_i64_to_i32(&value.polygon_offsets)?,
            offsets_buffer_i64_to_i32(&value.ring_offsets)?,
            value.validity,
        ))
    }
}

/// Default to an empty array
impl<O: OffsetSizeTrait> Default for MultiPolygonArray<O> {
    fn default() -> Self {
        MultiPolygonBuilder::default().into()
    }
}

impl<O: OffsetSizeTrait> PartialEq for MultiPolygonArray<O> {
    fn eq(&self, other: &Self) -> bool {
        if self.validity != other.validity {
            return false;
        }

        if !offset_buffer_eq(&self.geom_offsets, &other.geom_offsets) {
            return false;
        }

        if !offset_buffer_eq(&self.polygon_offsets, &other.polygon_offsets) {
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::geoarrow_data::{
        example_multipolygon_interleaved, example_multipolygon_separated, example_multipolygon_wkb,
    };
    use crate::test::multipolygon::{mp0, mp1};

    #[test]
    fn geo_roundtrip_accurate() {
        let arr: MultiPolygonArray<i64> = vec![mp0(), mp1()].as_slice().into();
        assert_eq!(arr.value_as_geo(0), mp0());
        assert_eq!(arr.value_as_geo(1), mp1());
    }

    #[test]
    fn geo_roundtrip_accurate_option_vec() {
        let arr: MultiPolygonArray<i64> = vec![Some(mp0()), Some(mp1()), None].into();
        assert_eq!(arr.get_as_geo(0), Some(mp0()));
        assert_eq!(arr.get_as_geo(1), Some(mp1()));
        assert_eq!(arr.get_as_geo(2), None);
    }

    #[test]
    fn slice() {
        let arr: MultiPolygonArray<i64> = vec![mp0(), mp1()].as_slice().into();
        let sliced = arr.slice(1, 1);
        assert_eq!(sliced.len(), 1);
        assert_eq!(sliced.get_as_geo(0), Some(mp1()));
    }

    #[test]
    fn owned_slice() {
        let arr: MultiPolygonArray<i64> = vec![mp0(), mp1()].as_slice().into();
        let sliced = arr.owned_slice(1, 1);

        // assert!(
        //     !sliced.geom_offsets.buffer().is_sliced(),
        //     "underlying offsets should not be sliced"
        // );
        assert_eq!(arr.len(), 2);
        assert_eq!(sliced.len(), 1);
        assert_eq!(sliced.get_as_geo(0), Some(mp1()));

        // // Offset is 0 because it's copied to an owned buffer
        // assert_eq!(*sliced.geom_offsets.first(), 0);
        // assert_eq!(*sliced.ring_offsets.first(), 0);
    }

    #[test]
    fn parse_wkb_geoarrow_interleaved_example() {
        let geom_arr = example_multipolygon_interleaved();

        let wkb_arr = example_multipolygon_wkb();
        let parsed_geom_arr: MultiPolygonArray<i64> = wkb_arr.try_into().unwrap();

        assert_eq!(geom_arr, parsed_geom_arr);
    }

    #[test]
    fn parse_wkb_geoarrow_separated_example() {
        // TODO: support checking equality of interleaved vs separated coords
        let geom_arr = example_multipolygon_separated().into_coord_type(CoordType::Interleaved);

        let wkb_arr = example_multipolygon_wkb();
        let parsed_geom_arr: MultiPolygonArray<i64> = wkb_arr.try_into().unwrap();

        assert_eq!(geom_arr, parsed_geom_arr);
    }
}
