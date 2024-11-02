use std::sync::Arc;

use crate::algorithm::native::eq::offset_buffer_eq;
use crate::array::metadata::ArrayMetadata;
use crate::array::multipolygon::MultiPolygonCapacity;
use crate::array::util::{offsets_buffer_i64_to_i32, OffsetBufferUtils};
use crate::array::{
    CoordBuffer, CoordType, GeometryCollectionArray, MixedGeometryArray, PolygonArray, WKBArray,
};
use crate::datatypes::NativeType;
use crate::error::{GeoArrowError, Result};
use crate::scalar::{Geometry, MultiPolygon};
use crate::trait_::{ArrayAccessor, GeometryArraySelfMethods, IntoArrow, NativeGeometryAccessor};
use crate::util::{owned_slice_offsets, owned_slice_validity};
use crate::{ArrayBase, NativeArray};
use arrow::array::AsArray;
use arrow_array::{Array, GenericListArray, OffsetSizeTrait};
use geo_traits::MultiPolygonTrait;

use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};

use super::MultiPolygonBuilder;

/// An immutable array of MultiPolygon geometries using GeoArrow's in-memory representation.
///
/// This is semantically equivalent to `Vec<Option<MultiPolygon>>` due to the internal validity
/// bitmap.
#[derive(Debug, Clone)]
pub struct MultiPolygonArray<const D: usize> {
    // Always NativeType::MultiPolygon or NativeType::LargeMultiPolygon
    data_type: NativeType,

    pub(crate) metadata: Arc<ArrayMetadata>,

    pub(crate) coords: CoordBuffer,

    /// Offsets into the polygon array where each geometry starts
    pub(crate) geom_offsets: OffsetBuffer<i32>,

    /// Offsets into the ring array where each polygon starts
    pub(crate) polygon_offsets: OffsetBuffer<i32>,

    /// Offsets into the coordinate array where each ring starts
    pub(crate) ring_offsets: OffsetBuffer<i32>,

    /// Validity bitmap
    pub(crate) validity: Option<NullBuffer>,
}

pub(super) fn check(
    coords: &CoordBuffer,
    geom_offsets: &OffsetBuffer<i32>,
    polygon_offsets: &OffsetBuffer<i32>,
    ring_offsets: &OffsetBuffer<i32>,
    validity_len: Option<usize>,
) -> Result<()> {
    if validity_len.map_or(false, |len| len != geom_offsets.len_proxy()) {
        return Err(GeoArrowError::General(
            "validity mask length must match the number of values".to_string(),
        ));
    }
    if *ring_offsets.last() as usize != coords.len() {
        return Err(GeoArrowError::General(
            "largest ring offset must match coords length".to_string(),
        ));
    }

    if *polygon_offsets.last() as usize != ring_offsets.len_proxy() {
        return Err(GeoArrowError::General(
            "largest polygon offset must match ring offsets length".to_string(),
        ));
    }

    if *geom_offsets.last() as usize != polygon_offsets.len_proxy() {
        return Err(GeoArrowError::General(
            "largest geometry offset must match polygon offsets length".to_string(),
        ));
    }

    Ok(())
}

impl<const D: usize> MultiPolygonArray<D> {
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
        geom_offsets: OffsetBuffer<i32>,
        polygon_offsets: OffsetBuffer<i32>,
        ring_offsets: OffsetBuffer<i32>,
        validity: Option<NullBuffer>,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        Self::try_new(
            coords,
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            validity,
            metadata,
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
        geom_offsets: OffsetBuffer<i32>,
        polygon_offsets: OffsetBuffer<i32>,
        ring_offsets: OffsetBuffer<i32>,
        validity: Option<NullBuffer>,
        metadata: Arc<ArrayMetadata>,
    ) -> Result<Self> {
        check(
            &coords,
            &geom_offsets,
            &polygon_offsets,
            &ring_offsets,
            validity.as_ref().map(|v| v.len()),
        )?;

        let coord_type = coords.coord_type();
        let data_type = NativeType::MultiPolygon(coord_type, D.try_into()?);

        Ok(Self {
            data_type,
            coords,
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            validity,
            metadata,
        })
    }

    fn vertices_field(&self) -> Arc<Field> {
        Field::new("vertices", self.coords.storage_type(), false).into()
    }

    fn rings_field(&self) -> Arc<Field> {
        let name = "rings";
        Field::new_list(name, self.vertices_field(), false).into()
    }

    fn polygons_field(&self) -> Arc<Field> {
        let name = "polygons";
        Field::new_list(name, self.rings_field(), false).into()
    }

    pub fn coords(&self) -> &CoordBuffer {
        &self.coords
    }

    pub fn into_inner(
        self,
    ) -> (
        CoordBuffer,
        OffsetBuffer<i32>,
        OffsetBuffer<i32>,
        OffsetBuffer<i32>,
    ) {
        (
            self.coords,
            self.geom_offsets,
            self.polygon_offsets,
            self.ring_offsets,
        )
    }

    pub fn geom_offsets(&self) -> &OffsetBuffer<i32> {
        &self.geom_offsets
    }

    pub fn polygon_offsets(&self) -> &OffsetBuffer<i32> {
        &self.polygon_offsets
    }

    pub fn ring_offsets(&self) -> &OffsetBuffer<i32> {
        &self.ring_offsets
    }

    /// The lengths of each buffer contained in this array.
    pub fn buffer_lengths(&self) -> MultiPolygonCapacity {
        MultiPolygonCapacity::new(
            *self.ring_offsets.last() as usize,
            *self.polygon_offsets.last() as usize,
            *self.geom_offsets.last() as usize,
            self.len(),
        )
    }

    /// The number of bytes occupied by this array.
    pub fn num_bytes(&self) -> usize {
        let validity_len = self.nulls().map(|v| v.buffer().len()).unwrap_or(0);
        validity_len + self.buffer_lengths().num_bytes()
    }

    /// Slices this [`MultiPolygonArray`] in place.
    /// # Panic
    /// This function panics iff `offset + length > self.len()`.
    #[inline]
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        // Note: we **only** slice the geom_offsets and not any actual data. Otherwise the offsets
        // would be in the wrong location.
        Self {
            data_type: self.data_type,
            coords: self.coords.clone(),
            geom_offsets: self.geom_offsets.slice(offset, length),
            polygon_offsets: self.polygon_offsets.clone(),
            ring_offsets: self.ring_offsets.clone(),
            validity: self.validity.as_ref().map(|v| v.slice(offset, length)),
            metadata: self.metadata(),
        }
    }

    pub fn owned_slice(&self, offset: usize, length: usize) -> Self {
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
            self.metadata(),
        )
    }

    pub fn to_coord_type(&self, coord_type: CoordType) -> Self {
        self.clone().into_coord_type(coord_type)
    }

    pub fn into_coord_type(self, coord_type: CoordType) -> Self {
        Self::new(
            self.coords.into_coord_type(coord_type),
            self.geom_offsets,
            self.polygon_offsets,
            self.ring_offsets,
            self.validity,
            self.metadata,
        )
    }
}

impl<const D: usize> ArrayBase for MultiPolygonArray<D> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn storage_type(&self) -> DataType {
        self.data_type.to_data_type()
    }

    fn extension_field(&self) -> Arc<Field> {
        self.data_type
            .to_field_with_metadata("geometry", true, &self.metadata)
            .into()
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

impl<const D: usize> NativeArray for MultiPolygonArray<D> {
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

impl<const D: usize> GeometryArraySelfMethods<D> for MultiPolygonArray<D> {
    fn with_coords(self, coords: CoordBuffer) -> Self {
        assert_eq!(coords.len(), self.coords.len());
        Self::new(
            coords,
            self.geom_offsets,
            self.polygon_offsets,
            self.ring_offsets,
            self.validity,
            self.metadata,
        )
    }

    fn into_coord_type(self, coord_type: CoordType) -> Self {
        Self::new(
            self.coords.into_coord_type(coord_type),
            self.geom_offsets,
            self.polygon_offsets,
            self.ring_offsets,
            self.validity,
            self.metadata,
        )
    }
}

impl<const D: usize> NativeGeometryAccessor<D> for MultiPolygonArray<D> {
    unsafe fn value_as_geometry_unchecked(&self, index: usize) -> crate::scalar::Geometry<D> {
        Geometry::MultiPolygon(MultiPolygon::new(
            &self.coords,
            &self.geom_offsets,
            &self.polygon_offsets,
            &self.ring_offsets,
            index,
        ))
    }
}

#[cfg(feature = "geos")]
impl<'a, const D: usize> crate::trait_::NativeGEOSGeometryAccessor<'a> for MultiPolygonArray<D> {
    unsafe fn value_as_geometry_unchecked(
        &'a self,
        index: usize,
    ) -> std::result::Result<geos::Geometry, geos::Error> {
        let geom = MultiPolygon::<D>::new(
            &self.coords,
            &self.geom_offsets,
            &self.polygon_offsets,
            &self.ring_offsets,
            index,
        );
        (&geom).try_into()
    }
}

impl<'a, const D: usize> ArrayAccessor<'a> for MultiPolygonArray<D> {
    type Item = MultiPolygon<'a, D>;
    type ItemGeo = geo::MultiPolygon;

    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        MultiPolygon::new(
            &self.coords,
            &self.geom_offsets,
            &self.polygon_offsets,
            &self.ring_offsets,
            index,
        )
    }
}

impl<const D: usize> IntoArrow for MultiPolygonArray<D> {
    type ArrowArray = GenericListArray<i32>;

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

impl<const D: usize> TryFrom<&GenericListArray<i32>> for MultiPolygonArray<D> {
    type Error = GeoArrowError;

    fn try_from(geom_array: &GenericListArray<i32>) -> Result<Self> {
        let geom_offsets = geom_array.offsets();
        let validity = geom_array.nulls();

        let polygons_dyn_array = geom_array.values();
        let polygons_array = polygons_dyn_array.as_list::<i32>();

        let polygon_offsets = polygons_array.offsets();
        let rings_dyn_array = polygons_array.values();
        let rings_array = rings_dyn_array.as_list::<i32>();

        let ring_offsets = rings_array.offsets();
        let coords: CoordBuffer = rings_array.values().as_ref().try_into()?;

        Ok(Self::new(
            coords,
            geom_offsets.clone(),
            polygon_offsets.clone(),
            ring_offsets.clone(),
            validity.cloned(),
            Default::default(),
        ))
    }
}

impl<const D: usize> TryFrom<&GenericListArray<i64>> for MultiPolygonArray<D> {
    type Error = GeoArrowError;

    fn try_from(geom_array: &GenericListArray<i64>) -> Result<Self> {
        let geom_offsets = offsets_buffer_i64_to_i32(geom_array.offsets())?;
        let validity = geom_array.nulls();

        let polygons_dyn_array = geom_array.values();
        let polygons_array = polygons_dyn_array.as_list::<i64>();

        let polygon_offsets = offsets_buffer_i64_to_i32(polygons_array.offsets())?;
        let rings_dyn_array = polygons_array.values();
        let rings_array = rings_dyn_array.as_list::<i64>();

        let ring_offsets = offsets_buffer_i64_to_i32(rings_array.offsets())?;
        let coords: CoordBuffer = rings_array.values().as_ref().try_into()?;

        Ok(Self::new(
            coords,
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            validity.cloned(),
            Default::default(),
        ))
    }
}

impl<const D: usize> TryFrom<&dyn Array> for MultiPolygonArray<D> {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self> {
        match value.data_type() {
            DataType::List(_) => {
                let downcasted = value.as_list::<i32>();
                downcasted.try_into()
            }
            DataType::LargeList(_) => {
                let downcasted = value.as_list::<i64>();
                downcasted.try_into()
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl<const D: usize> TryFrom<(&dyn Array, &Field)> for MultiPolygonArray<D> {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let mut arr: Self = arr.try_into()?;
        arr.metadata = Arc::new(ArrayMetadata::try_from(field)?);
        Ok(arr)
    }
}

impl<G: MultiPolygonTrait<T = f64>, const D: usize> From<Vec<Option<G>>> for MultiPolygonArray<D> {
    fn from(other: Vec<Option<G>>) -> Self {
        let mut_arr: MultiPolygonBuilder<D> = other.into();
        mut_arr.into()
    }
}

impl<G: MultiPolygonTrait<T = f64>, const D: usize> From<&[G]> for MultiPolygonArray<D> {
    fn from(other: &[G]) -> Self {
        let mut_arr: MultiPolygonBuilder<D> = other.into();
        mut_arr.into()
    }
}

impl<O: OffsetSizeTrait, const D: usize> TryFrom<WKBArray<O>> for MultiPolygonArray<D> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self> {
        let mut_arr: MultiPolygonBuilder<D> = value.try_into()?;
        Ok(mut_arr.into())
    }
}

impl<const D: usize> From<PolygonArray<D>> for MultiPolygonArray<D> {
    fn from(value: PolygonArray<D>) -> Self {
        let coords = value.coords;
        let geom_offsets = OffsetBuffer::from_lengths(vec![1; coords.len()]);
        let ring_offsets = value.ring_offsets;
        let polygon_offsets = value.geom_offsets;
        let validity = value.validity;
        Self::new(
            coords,
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            validity,
            value.metadata,
        )
    }
}

/// Default to an empty array
impl<const D: usize> Default for MultiPolygonArray<D> {
    fn default() -> Self {
        MultiPolygonBuilder::default().into()
    }
}

impl<const D: usize> PartialEq for MultiPolygonArray<D> {
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

impl<const D: usize> TryFrom<MixedGeometryArray<D>> for MultiPolygonArray<D> {
    type Error = GeoArrowError;

    fn try_from(value: MixedGeometryArray<D>) -> Result<Self> {
        if value.has_points()
            || value.has_line_strings()
            || value.has_multi_points()
            || value.has_multi_line_strings()
        {
            return Err(GeoArrowError::General("Unable to cast".to_string()));
        }

        if value.has_only_polygons() {
            return Ok(value.polygons.into());
        }

        if value.has_only_multi_polygons() {
            return Ok(value.multi_polygons);
        }

        let mut capacity = value.multi_polygons.buffer_lengths();
        capacity += value.polygons.buffer_lengths();

        let mut builder = MultiPolygonBuilder::<D>::with_capacity_and_options(
            capacity,
            value.coord_type(),
            value.metadata(),
        );
        value
            .iter()
            .try_for_each(|x| builder.push_geometry(x.as_ref()))?;
        Ok(builder.finish())
    }
}

impl<const D: usize> TryFrom<GeometryCollectionArray<D>> for MultiPolygonArray<D> {
    type Error = GeoArrowError;

    fn try_from(value: GeometryCollectionArray<D>) -> Result<Self> {
        MixedGeometryArray::try_from(value)?.try_into()
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
        let arr: MultiPolygonArray<2> = vec![mp0(), mp1()].as_slice().into();
        assert_eq!(arr.value_as_geo(0), mp0());
        assert_eq!(arr.value_as_geo(1), mp1());
    }

    #[test]
    fn geo_roundtrip_accurate_option_vec() {
        let arr: MultiPolygonArray<2> = vec![Some(mp0()), Some(mp1()), None].into();
        assert_eq!(arr.get_as_geo(0), Some(mp0()));
        assert_eq!(arr.get_as_geo(1), Some(mp1()));
        assert_eq!(arr.get_as_geo(2), None);
    }

    #[test]
    fn slice() {
        let arr: MultiPolygonArray<2> = vec![mp0(), mp1()].as_slice().into();
        let sliced = arr.slice(1, 1);
        assert_eq!(sliced.len(), 1);
        assert_eq!(sliced.get_as_geo(0), Some(mp1()));
    }

    #[test]
    fn owned_slice() {
        let arr: MultiPolygonArray<2> = vec![mp0(), mp1()].as_slice().into();
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
        let parsed_geom_arr: MultiPolygonArray<2> = wkb_arr.try_into().unwrap();

        assert_eq!(geom_arr, parsed_geom_arr);
    }

    #[test]
    fn parse_wkb_geoarrow_separated_example() {
        // TODO: support checking equality of interleaved vs separated coords
        let geom_arr = example_multipolygon_separated().into_coord_type(CoordType::Interleaved);

        let wkb_arr = example_multipolygon_wkb();
        let parsed_geom_arr: MultiPolygonArray<2> = wkb_arr.try_into().unwrap();

        assert_eq!(geom_arr, parsed_geom_arr);
    }
}
