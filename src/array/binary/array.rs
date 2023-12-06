use std::collections::HashMap;
use std::sync::Arc;

use crate::array::mixed::MixedCapacity;
use crate::array::util::{offsets_buffer_i32_to_i64, offsets_buffer_i64_to_i32};
use crate::array::zip_validity::ZipValidity;
use crate::array::{
    CoordType, LineStringBuilder, MixedGeometryBuilder, MultiLineStringBuilder, MultiPointBuilder,
    MultiPolygonBuilder, PointBuilder, PolygonBuilder, WKBBuilder,
};
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::io::wkb::reader::geometry::WKBGeometry;
use crate::io::wkb::reader::r#type::infer_geometry_type;
use crate::scalar::WKB;
// use crate::util::{owned_slice_offsets, owned_slice_validity};
use crate::trait_::{GeometryArrayAccessor, GeometryArraySelfMethods, IntoArrow};
use crate::GeometryArrayTrait;
use arrow_array::OffsetSizeTrait;
use arrow_array::{Array, BinaryArray, GenericBinaryArray, LargeBinaryArray};
use arrow_buffer::bit_iterator::BitIterator;
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field};

/// An immutable array of WKB geometries using GeoArrow's in-memory representation.
///
/// This is semantically equivalent to `Vec<Option<WKB>>` due to the internal validity bitmap.
///
/// This array _can_ be used directly for operations, but that will incur costly encoding to and
/// from WKB on every operation. Instead, you usually want to use the WKBArray only for
/// serialization purposes (e.g. to and from [GeoParquet](https://geoparquet.org/)) but convert to
/// strongly-typed arrays (such as the [`PointArray`][crate::array::PointArray]) for computations.
#[derive(Debug, Clone, PartialEq)]
// TODO: convert to named struct
pub struct WKBArray<O: OffsetSizeTrait>(GenericBinaryArray<O>, GeoDataType);

// Implement geometry accessors
impl<O: OffsetSizeTrait> WKBArray<O> {
    /// Create a new WKBArray from a BinaryArray
    pub fn new(arr: GenericBinaryArray<O>) -> Self {
        let data_type = match O::IS_LARGE {
            true => GeoDataType::LargeWKB,
            false => GeoDataType::WKB,
        };

        Self(arr, data_type)
    }

    /// Returns true if the array is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Infer the minimal GeoDataType that this WKBArray can be casted to.
    pub fn infer_geo_data_type(
        &self,
        large_type: bool,
        coord_type: CoordType,
    ) -> Result<GeoDataType> {
        infer_geometry_type(self.iter().flatten(), large_type, coord_type)
    }

    pub fn parse_to_geoarrow(
        &self,
        large_type: bool,
        coord_type: CoordType,
    ) -> Result<Arc<dyn GeometryArrayTrait>> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = self.iter().collect();
        let wkb_objects2: Vec<Option<WKBGeometry>> = wkb_objects
            .iter()
            .map(|maybe_wkb| maybe_wkb.as_ref().map(|wkb| wkb.to_wkb_object()))
            .collect();

        let capacity = MixedCapacity::from_owned_geometries(wkb_objects2.into_iter());
        if capacity.point_compatible() {
            let mut builder =
                PointBuilder::with_capacity_and_options(capacity.point_capacity(), coord_type);
            let wkb_points: Vec<Option<_>> = wkb_objects
                .iter()
                .map(|maybe_wkb| {
                    maybe_wkb
                        .as_ref()
                        .map(|wkb| wkb.to_wkb_object().into_point())
                })
                .collect();
            builder.extend_from_iter(wkb_points.iter().map(|x| x.as_ref()));
            Ok(Arc::new(builder.finish()))
        } else if capacity.line_string_compatible() {
            let wkb_line_strings: Vec<Option<_>> = wkb_objects
                .iter()
                .map(|maybe_wkb| {
                    maybe_wkb
                        .as_ref()
                        .map(|wkb| wkb.to_wkb_object().into_line_string())
                })
                .collect();
            if large_type {
                let mut builder = LineStringBuilder::<i32>::with_capacity_and_options(
                    capacity.line_string_capacity(),
                    coord_type,
                );
                builder.extend_from_iter(wkb_line_strings.iter().map(|x| x.as_ref()));
                Ok(Arc::new(builder.finish()))
            } else {
                let mut builder = LineStringBuilder::<i64>::with_capacity_and_options(
                    capacity.line_string_capacity(),
                    coord_type,
                );
                builder.extend_from_iter(wkb_line_strings.iter().map(|x| x.as_ref()));
                Ok(Arc::new(builder.finish()))
            }
        } else if capacity.polygon_compatible() {
            let wkb_polygons: Vec<Option<_>> = wkb_objects
                .iter()
                .map(|maybe_wkb| {
                    maybe_wkb
                        .as_ref()
                        .map(|wkb| wkb.to_wkb_object().into_polygon())
                })
                .collect();
            if large_type {
                let mut builder = PolygonBuilder::<i32>::with_capacity_and_options(
                    capacity.polygon_capacity(),
                    coord_type,
                );
                builder.extend_from_iter(wkb_polygons.iter().map(|x| x.as_ref()));
                Ok(Arc::new(builder.finish()))
            } else {
                let mut builder = PolygonBuilder::<i64>::with_capacity_and_options(
                    capacity.polygon_capacity(),
                    coord_type,
                );
                builder.extend_from_iter(wkb_polygons.iter().map(|x| x.as_ref()));
                Ok(Arc::new(builder.finish()))
            }
        } else if capacity.multi_point_compatible() {
            let wkb_multi_points: Vec<Option<_>> = wkb_objects
                .iter()
                .map(|maybe_wkb| {
                    maybe_wkb
                        .as_ref()
                        .map(|wkb| wkb.to_wkb_object().into_maybe_multi_point())
                })
                .collect();

            // Have to add point and multi point capacity together
            let mut multi_point_capacity = capacity.multi_point_capacity();
            multi_point_capacity.add_point_capacity(capacity.point_capacity());

            if large_type {
                let mut builder = MultiPointBuilder::<i32>::with_capacity_and_options(
                    multi_point_capacity,
                    coord_type,
                );
                builder.extend_from_iter(wkb_multi_points.iter().map(|x| x.as_ref()));
                Ok(Arc::new(builder.finish()))
            } else {
                let mut builder = MultiPointBuilder::<i64>::with_capacity_and_options(
                    multi_point_capacity,
                    coord_type,
                );
                builder.extend_from_iter(wkb_multi_points.iter().map(|x| x.as_ref()));
                Ok(Arc::new(builder.finish()))
            }
        } else if capacity.multi_line_string_compatible() {
            let wkb_multi_line_strings: Vec<Option<_>> = wkb_objects
                .iter()
                .map(|maybe_wkb| {
                    maybe_wkb
                        .as_ref()
                        .map(|wkb| wkb.to_wkb_object().into_maybe_multi_line_string())
                })
                .collect();

            // Have to add line string and multi line string capacity together
            let mut multi_line_string_capacity = capacity.multi_line_string_capacity();
            multi_line_string_capacity.add_line_string_capacity(capacity.line_string_capacity());

            if large_type {
                let mut builder = MultiLineStringBuilder::<i32>::with_capacity_and_options(
                    multi_line_string_capacity,
                    coord_type,
                );
                builder.extend_from_iter(wkb_multi_line_strings.iter().map(|x| x.as_ref()));
                Ok(Arc::new(builder.finish()))
            } else {
                let mut builder = MultiLineStringBuilder::<i64>::with_capacity_and_options(
                    multi_line_string_capacity,
                    coord_type,
                );
                builder.extend_from_iter(wkb_multi_line_strings.iter().map(|x| x.as_ref()));
                Ok(Arc::new(builder.finish()))
            }
        } else if capacity.multi_polygon_compatible() {
            let wkb_multi_polygons: Vec<Option<_>> = wkb_objects
                .iter()
                .map(|maybe_wkb| {
                    maybe_wkb
                        .as_ref()
                        .map(|wkb| wkb.to_wkb_object().into_maybe_multi_polygon())
                })
                .collect();

            // Have to add line string and multi line string capacity together
            let mut multi_polygon_capacity = capacity.multi_polygon_capacity();
            multi_polygon_capacity.add_polygon_capacity(capacity.polygon_capacity());

            if large_type {
                let mut builder = MultiPolygonBuilder::<i32>::with_capacity_and_options(
                    multi_polygon_capacity,
                    coord_type,
                );
                builder.extend_from_iter(wkb_multi_polygons.iter().map(|x| x.as_ref()));
                Ok(Arc::new(builder.finish()))
            } else {
                let mut builder = MultiPolygonBuilder::<i64>::with_capacity_and_options(
                    multi_polygon_capacity,
                    coord_type,
                );
                builder.extend_from_iter(wkb_multi_polygons.iter().map(|x| x.as_ref()));
                Ok(Arc::new(builder.finish()))
            }
        } else {
            let wkb_geometry: Vec<Option<_>> = wkb_objects
                .iter()
                .map(|maybe_wkb| maybe_wkb.as_ref().map(|wkb| wkb.to_wkb_object()))
                .collect();

            #[allow(clippy::collapsible_else_if)]
            if large_type {
                let mut builder =
                    MixedGeometryBuilder::<i32>::with_capacity_and_options(capacity, coord_type);
                builder.extend_from_iter(wkb_geometry.iter().map(|x| x.as_ref()));
                Ok(Arc::new(builder.finish()))
            } else {
                let mut builder =
                    MixedGeometryBuilder::<i64>::with_capacity_and_options(capacity, coord_type);
                builder.extend_from_iter(wkb_geometry.iter().map(|x| x.as_ref()));
                Ok(Arc::new(builder.finish()))
            }
        }
    }

    // pub fn with_validity(&self, validity: Option<NullBuffer>) -> Self {
    //     WKBArray::new(self.0.clone().with_validity(validity))
    // }
}

impl<O: OffsetSizeTrait> GeometryArrayTrait for WKBArray<O> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> &GeoDataType {
        &self.1
    }

    fn storage_type(&self) -> DataType {
        self.0.data_type().clone()
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
        "geoarrow.wkb"
    }

    fn into_array_ref(self) -> Arc<dyn Array> {
        // Recreate a BinaryArray so that we can force it to have geoarrow.wkb extension type
        Arc::new(self.into_arrow())
    }

    fn coord_type(&self) -> CoordType {
        CoordType::Interleaved
    }

    /// Returns the number of geometries in this array
    #[inline]
    fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns the optional validity.
    fn validity(&self) -> Option<&NullBuffer> {
        self.0.nulls()
    }
}

impl<O: OffsetSizeTrait> GeometryArraySelfMethods for WKBArray<O> {
    fn with_coords(self, _coords: crate::array::CoordBuffer) -> Self {
        unimplemented!()
    }

    fn into_coord_type(self, _coord_type: CoordType) -> Self {
        self
    }

    /// Slices this [`WKBArray`] in place.
    /// # Panic
    /// This function panics iff `offset + length > self.len()`.
    #[inline]
    fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        Self(self.0.slice(offset, length), self.1.clone())
    }

    fn owned_slice(&self, _offset: usize, _length: usize) -> Self {
        todo!()
        // assert!(
        //     offset + length <= self.len(),
        //     "offset + length may not exceed length of array"
        // );
        // assert!(length >= 1, "length must be at least 1");

        // // Find the start and end of the ring offsets
        // let (start_idx, _) = self.0.offsets().start_end(offset);
        // let (_, end_idx) = self.0.offsets().start_end(offset + length - 1);

        // let new_offsets = owned_slice_offsets(self.0.offsets(), offset, length);

        // let mut values = self.0.slice(start_idx, end_idx - start_idx);

        // let validity = owned_slice_validity(self.0.nulls(), offset, length);

        // Self::new(GenericBinaryArray::new(
        //     new_offsets,
        //     values.as_slice().to_vec().into(),
        //     validity,
        // ))
    }
}

impl<'a, O: OffsetSizeTrait> GeometryArrayAccessor<'a> for WKBArray<O> {
    type Item = WKB<'a, O>;
    type ItemGeo = geo::Geometry;

    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        WKB::new_borrowed(&self.0, index)
    }
}

impl<O: OffsetSizeTrait> IntoArrow for WKBArray<O> {
    type ArrowArray = GenericBinaryArray<O>;

    fn into_arrow(self) -> Self::ArrowArray {
        GenericBinaryArray::new(
            self.0.offsets().clone(),
            self.0.values().clone(),
            self.0.nulls().cloned(),
        )
    }
}

impl<O: OffsetSizeTrait> WKBArray<O> {
    /// Returns the value at slot `i` as a GEOS geometry.
    #[cfg(feature = "geos")]
    pub fn value_as_geos(&self, i: usize) -> geos::Geometry {
        let buf = self.0.value(i);
        geos::Geometry::new_from_wkb(buf).expect("Unable to parse WKB")
    }

    /// Gets the value at slot `i` as a GEOS geometry, additionally checking the validity bitmap
    #[cfg(feature = "geos")]
    pub fn get_as_geos(&self, i: usize) -> Option<geos::Geometry> {
        if self.is_null(i) {
            return None;
        }

        let buf = self.0.value(i);
        Some(geos::Geometry::new_from_wkb(buf).expect("Unable to parse WKB"))
    }

    /// Iterator over geo Geometry objects, not looking at validity
    pub fn iter_geo_values(&self) -> impl Iterator<Item = geo::Geometry> + '_ {
        (0..self.len()).map(|i| self.value_as_geo(i))
    }

    /// Iterator over geo Geometry objects, taking into account validity
    pub fn iter_geo(
        &self,
    ) -> ZipValidity<geo::Geometry, impl Iterator<Item = geo::Geometry> + '_, BitIterator> {
        ZipValidity::new_with_validity(self.iter_geo_values(), self.nulls())
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

impl<O: OffsetSizeTrait> From<GenericBinaryArray<O>> for WKBArray<O> {
    fn from(value: GenericBinaryArray<O>) -> Self {
        Self::new(value)
    }
}

impl TryFrom<&dyn Array> for WKBArray<i32> {
    type Error = GeoArrowError;
    fn try_from(value: &dyn Array) -> Result<Self> {
        match value.data_type() {
            DataType::Binary => {
                let downcasted = value.as_any().downcast_ref::<BinaryArray>().unwrap();
                Ok(downcasted.clone().into())
            }
            DataType::LargeBinary => {
                let downcasted = value.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                let geom_array: WKBArray<i64> = downcasted.clone().into();
                geom_array.try_into()
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl TryFrom<&dyn Array> for WKBArray<i64> {
    type Error = GeoArrowError;
    fn try_from(value: &dyn Array) -> Result<Self> {
        match value.data_type() {
            DataType::Binary => {
                let downcasted = value.as_any().downcast_ref::<BinaryArray>().unwrap();
                let geom_array: WKBArray<i32> = downcasted.clone().into();
                Ok(geom_array.into())
            }
            DataType::LargeBinary => {
                let downcasted = value.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                Ok(downcasted.clone().into())
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl From<WKBArray<i32>> for WKBArray<i64> {
    fn from(value: WKBArray<i32>) -> Self {
        let binary_array = value.0;
        let (offsets, values, nulls) = binary_array.into_parts();
        Self::new(LargeBinaryArray::new(
            offsets_buffer_i32_to_i64(&offsets),
            values,
            nulls,
        ))
    }
}

impl TryFrom<WKBArray<i64>> for WKBArray<i32> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<i64>) -> Result<Self> {
        let binary_array = value.0;
        let (offsets, values, nulls) = binary_array.into_parts();
        Ok(Self::new(BinaryArray::new(
            offsets_buffer_i64_to_i32(&offsets)?,
            values,
            nulls,
        )))
    }
}

// impl TryFrom<&BinaryArray<i64>> for WKBArray {
//     type Error = GeoArrowError;

//     fn try_from(value: &BinaryArray<i64>) -> Result<Self, Self::Error> {
//         Ok(Self::new(value.clone()))
//     }
// }

// impl TryFrom<&dyn Array> for WKBArray {
//     type Error = GeoArrowError;

//     fn try_from(value: &dyn Array) -> Result<Self, Self::Error> {
//         match value.data_type() {
//             DataType::Binary => {
//                 let downcasted = value.as_any().downcast_ref::<BinaryArray<i32>>().unwrap();
//                 downcasted.try_into()
//             }
//             DataType::LargeBinary => {
//                 let downcasted = value.as_any().downcast_ref::<BinaryArray<i64>>().unwrap();
//                 downcasted.try_into()
//             }
//             _ => Err(GeoArrowError::General(format!(
//                 "Unexpected type: {:?}",
//                 value.data_type()
//             ))),
//         }
//     }
// }

impl<O: OffsetSizeTrait> From<Vec<Option<geo::Geometry>>> for WKBArray<O> {
    fn from(other: Vec<Option<geo::Geometry>>) -> Self {
        let mut_arr: WKBBuilder<O> = other.into();
        mut_arr.into()
    }
}

impl<O: OffsetSizeTrait> From<bumpalo::collections::Vec<'_, Option<geo::Geometry>>>
    for WKBArray<O>
{
    fn from(other: bumpalo::collections::Vec<'_, Option<geo::Geometry>>) -> Self {
        let mut_arr: WKBBuilder<O> = other.into();
        mut_arr.into()
    }
}
