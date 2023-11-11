use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, OffsetSizeTrait, UnionArray};
use arrow_buffer::{NullBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, UnionFields, UnionMode};

use crate::array::mixed::mutable::MutableMixedGeometryArray;
use crate::array::{
    CoordType, LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray,
    PointArray, PolygonArray,
};
use crate::datatypes::GeoDataType;
use crate::error::GeoArrowError;
use crate::scalar::Geometry;
use crate::trait_::GeoArrayAccessor;
use crate::GeometryArrayTrait;

/// # Invariants
///
/// - All arrays must have the same dimension
/// - All arrays must have the same coordinate layout (interleaved or separated)
#[derive(Debug, Clone)]
// #[derive(Debug, Clone, PartialEq)]
pub struct MixedGeometryArray<O: OffsetSizeTrait> {
    // Always GeoDataType::Mixed or GeoDataType::LargeMixed
    data_type: GeoDataType,

    // Invariant: every item in `types` is `> 0 && < fields.len()`
    types: ScalarBuffer<i8>,

    // Invariant: `offsets.len() == types.len()`
    offsets: ScalarBuffer<i32>,

    /// A lookup table for which child array is used
    ///
    /// To read a value:
    /// ``rs
    /// let child_index = self.types[i];
    /// let offset = self.offsets[i] as usize;
    /// let geometry_type = self.map[child_index as usize];
    /// ``
    /// then match on the geometry_type to access the underlying array.
    ///
    /// Note that we include an ordering so that exporting this array to Arrow is O(1). If we used
    /// another ordering like always Point, LineString, etc. then we'd either have to always export
    /// all arrays (including some zero-length arrays) or have to reorder the `types` buffer when
    /// exporting.
    ///
    /// The default ordering is:
    /// - 0: PointArray
    /// - 1: LineStringArray
    /// - 2: PolygonArray
    /// - 3: MultiPointArray
    /// - 4: MultiLineStringArray
    /// - 5: MultiPolygonArray
    ///
    /// But the ordering can be different if coming from an external source.
    // TODO: change this to a wrapper type that contains this array of 6?
    // Then that wrapper type can also take a default ordering.
    map: [Option<GeometryType>; 6],

    points: PointArray,
    line_strings: LineStringArray<O>,
    polygons: PolygonArray<O>,
    multi_points: MultiPointArray<O>,
    multi_line_strings: MultiLineStringArray<O>,
    multi_polygons: MultiPolygonArray<O>,

    /// An offset used for slicing into this array. The offset will be 0 if the array has not been
    /// sliced.
    ///
    /// In order to slice this array efficiently (and zero-cost) we can't slice the underlying
    /// fields directly. If this were always a _sparse_ union array, we could! We could then always
    /// slice from offset to length of each underlying array. But we're under the assumption that
    /// most or all of the time we have a dense union array, where the `offsets` buffer is defined.
    /// In that case, to know how to slice each underlying array, we'd have to walk the `types` and
    /// `offsets` arrays (in O(N) time) to figure out how to slice the underlying arrays.
    ///
    /// Instead, we store the slice offset.
    ///
    /// Note that this offset is only for slicing into the **fields**, i.e. the geometry arrays.
    /// The `types` and `offsets` arrays are sliced as usual.
    ///
    /// TODO: when exporting this array, export to arrow2 and then slice from scratch because we
    /// can't set the `offset` in a UnionArray constructor
    slice_offset: usize,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GeometryType {
    Point = 0,
    LineString = 1,
    Polygon = 2,
    MultiPoint = 3,
    MultiLineString = 4,
    MultiPolygon = 5,
}

impl GeometryType {
    pub fn default_ordering(&self) -> i8 {
        match self {
            GeometryType::Point => 0,
            GeometryType::LineString => 1,
            GeometryType::Polygon => 2,
            GeometryType::MultiPoint => 3,
            GeometryType::MultiLineString => 4,
            GeometryType::MultiPolygon => 5,
        }
    }
}

impl From<&String> for GeometryType {
    fn from(value: &String) -> Self {
        match value.as_str() {
            "geoarrow.point" => GeometryType::Point,
            "geoarrow.linestring" => GeometryType::LineString,
            "geoarrow.polygon" => GeometryType::Polygon,
            "geoarrow.multipoint" => GeometryType::MultiPoint,
            "geoarrow.multilinestring" => GeometryType::MultiLineString,
            "geoarrow.multipolygon" => GeometryType::MultiPolygon,
            _ => panic!(),
        }
    }
}

impl<O: OffsetSizeTrait> MixedGeometryArray<O> {
    /// Create a new MixedGeometryArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Panics
    ///
    /// - if the validity is not `None` and its length is different from the number of geometries
    /// - if the largest geometry offset does not match the number of coordinates
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        types: ScalarBuffer<i8>,
        offsets: ScalarBuffer<i32>,
        points: PointArray,
        line_strings: LineStringArray<O>,
        polygons: PolygonArray<O>,
        multi_points: MultiPointArray<O>,
        multi_line_strings: MultiLineStringArray<O>,
        multi_polygons: MultiPolygonArray<O>,
    ) -> Self {
        let default_ordering = [
            Some(GeometryType::Point),
            Some(GeometryType::LineString),
            Some(GeometryType::Polygon),
            Some(GeometryType::MultiPoint),
            Some(GeometryType::MultiLineString),
            Some(GeometryType::MultiPolygon),
        ];

        // let coord_type = coords.coord_type();
        // TODO: use correct coord type
        let coord_type = CoordType::Interleaved;
        let data_type = match O::IS_LARGE {
            true => GeoDataType::LargeMixed(coord_type),
            false => GeoDataType::Mixed(coord_type),
        };

        Self {
            data_type,
            types,
            offsets,
            map: default_ordering,
            points,
            line_strings,
            polygons,
            multi_points,
            multi_line_strings,
            multi_polygons,
            slice_offset: 0,
        }
    }
}

impl<'a, O: OffsetSizeTrait> GeometryArrayTrait<'a> for MixedGeometryArray<O> {
    type ArrowArray = UnionArray;

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> &GeoDataType {
        &self.data_type
    }

    fn storage_type(&self) -> DataType {
        let mut fields: Vec<Arc<Field>> = vec![];
        let mut type_ids = vec![];

        if self.points.len() > 0 {
            fields.push(self.points.extension_field());
            type_ids.push(0);
        }
        if self.line_strings.len() > 0 {
            fields.push(self.line_strings.extension_field());
            type_ids.push(1);
        }
        if self.polygons.len() > 0 {
            fields.push(self.polygons.extension_field());
            type_ids.push(2);
        }
        if self.multi_points.len() > 0 {
            fields.push(self.multi_points.extension_field());
            type_ids.push(3);
        }
        if self.multi_line_strings.len() > 0 {
            fields.push(self.multi_line_strings.extension_field());
            type_ids.push(4);
        }
        if self.multi_polygons.len() > 0 {
            fields.push(self.multi_polygons.extension_field());
            type_ids.push(5);
        }

        let union_fields = UnionFields::new(type_ids, fields);
        DataType::Union(union_fields, UnionMode::Dense)
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
        "geoarrow.mixed"
    }

    fn into_arrow(self) -> Self::ArrowArray {
        let _extension_field = self.extension_field();
        let mut fields = vec![];

        if self.points.len() > 0 {
            fields.push(self.points.into_array_ref());
        }
        if self.line_strings.len() > 0 {
            fields.push(self.line_strings.into_array_ref());
        }
        if self.polygons.len() > 0 {
            fields.push(self.polygons.into_array_ref());
        }
        if self.multi_points.len() > 0 {
            fields.push(self.multi_points.into_array_ref());
        }
        if self.multi_line_strings.len() > 0 {
            fields.push(self.multi_line_strings.into_array_ref());
        }
        if self.multi_polygons.len() > 0 {
            fields.push(self.multi_polygons.into_array_ref());
        }

        todo!()
        // UnionArray::new(extension_type, self.types, fields, Some(self.offsets))
    }

    fn into_array_ref(self) -> Arc<dyn Array> {
        Arc::new(self.into_arrow())
    }

    fn with_coords(self, _coords: crate::array::CoordBuffer) -> Self {
        todo!();
    }

    fn coord_type(&self) -> crate::array::CoordType {
        todo!();
    }

    fn into_coord_type(self, _coord_type: crate::array::CoordType) -> Self {
        todo!();
    }

    /// Returns the number of geometries in this array
    #[inline]
    fn len(&self) -> usize {
        // Note that `types` is sliced as usual, and thus always has the correct length.
        self.types.len()
    }

    /// Returns the optional validity.
    #[inline]
    fn validity(&self) -> Option<&NullBuffer> {
        None
    }

    /// Slices this [`MixedGeometryArray`] in place.
    ///
    /// # Implementation
    ///
    /// This operation is `O(F)` where `F` is the number of fields.
    ///
    /// # Panic
    ///
    /// This function panics iff `offset + length >= self.len()`.
    #[inline]
    fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        Self {
            data_type: self.data_type.clone(),
            types: self.types.slice(offset, length),
            offsets: self.offsets.slice(offset, length),
            map: self.map,
            points: self.points.clone(),
            line_strings: self.line_strings.clone(),
            polygons: self.polygons.clone(),
            multi_points: self.multi_points.clone(),
            multi_line_strings: self.multi_line_strings.clone(),
            multi_polygons: self.multi_polygons.clone(),
            slice_offset: self.slice_offset + offset,
        }
    }

    fn owned_slice(&self, _offset: usize, _length: usize) -> Self {
        todo!()
    }

    fn to_boxed(&self) -> Box<Self> {
        Box::new(self.clone())
    }
}

impl<'a, O: OffsetSizeTrait> GeoArrayAccessor<'a> for MixedGeometryArray<O> {
    type Item = Geometry<'a, O>;
    type ItemGeo = geo::Geometry;

    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        dbg!(&self.types);
        let child_index = self.types[index];
        dbg!(child_index);
        let offset = self.offsets[index] as usize;
        dbg!(offset);
        dbg!(&self.map);
        let geometry_type = self.map[child_index as usize].unwrap();

        match geometry_type {
            GeometryType::Point => Geometry::Point(GeoArrayAccessor::value(&self.points, offset)),
            GeometryType::LineString => {
                Geometry::LineString(GeoArrayAccessor::value(&self.line_strings, offset))
            }
            GeometryType::Polygon => {
                Geometry::Polygon(GeoArrayAccessor::value(&self.polygons, offset))
            }
            GeometryType::MultiPoint => {
                Geometry::MultiPoint(GeoArrayAccessor::value(&self.multi_points, offset))
            }
            GeometryType::MultiLineString => {
                Geometry::MultiLineString(GeoArrayAccessor::value(&self.multi_line_strings, offset))
            }
            GeometryType::MultiPolygon => {
                Geometry::MultiPolygon(GeoArrayAccessor::value(&self.multi_polygons, offset))
            }
        }
    }
}

// Implement geometry accessors
impl<O: OffsetSizeTrait> MixedGeometryArray<O> {
    /// Iterator over geo Geometry objects, not looking at validity
    pub fn iter_geo_values(&self) -> impl Iterator<Item = geo::Geometry> + '_ {
        (0..self.len()).map(|i| self.value_as_geo(i))
    }

    /// Iterator over geo Geometry objects, taking into account validity
    pub fn iter_geo(&self) -> impl Iterator<Item = Option<geo::Geometry>> + '_ {
        (0..self.len()).map(|i| self.get_as_geo(i))
    }

    /// Returns the value at slot `i` as a GEOS geometry.
    #[cfg(feature = "geos")]
    pub fn value_as_geos(&self, i: usize) -> geos::Geometry {
        GeoArrayAccessor::value(self, i).try_into().unwrap()
    }

    /// Gets the value at slot `i` as a GEOS geometry, additionally checking the validity bitmap
    #[cfg(feature = "geos")]
    pub fn get_as_geos(&self, i: usize) -> Option<geos::Geometry> {
        self.get(i).map(|geom| geom.try_into().unwrap())
    }

    /// Iterator over GEOS geometry objects
    #[cfg(feature = "geos")]
    pub fn iter_geos_values(&self) -> impl Iterator<Item = geos::Geometry> + '_ {
        (0..self.len()).map(|i| self.value_as_geos(i))
    }

    /// Iterator over GEOS geometry objects, taking validity into account
    #[cfg(feature = "geos")]
    pub fn iter_geos(&self) -> impl Iterator<Item = Option<geos::Geometry>> + '_ {
        (0..self.len()).map(|i| self.get_as_geos(i))
    }
}

impl TryFrom<&UnionArray> for MixedGeometryArray<i32> {
    type Error = GeoArrowError;

    fn try_from(_value: &UnionArray) -> std::result::Result<Self, Self::Error> {
        todo!()
        // let types = value.types().clone();
        // let offsets = value.offsets().unwrap().clone();
        // let child_arrays = value.fields();

        // // Need to construct the mapping from the logical ordering to the physical ordering
        // let map = match value.data_type() {
        //     DataType::Union(fields, _mode) => {
        //         let mut map: [Option<GeometryType>; 6] = [None, None, None, None, None, None];
        //         assert!(ids.len() < 6);
        //         for (pos, &id) in ids.iter().enumerate() {
        //             let geom_type: GeometryType = match fields[pos].data_type() {
        //                 DataType::Extension(ext_name, _, _) => (ext_name).into(),
        //                 _ => panic!(),
        //             };

        //             // Set this geometry type in the lookup table
        //             // So when you see `type: 3`, then you look up index `map[3]`, which gives you
        //             // a geometry type. Then that geometry type is looked up in the primitive
        //             // arrays.
        //             map[id as usize] = Some(geom_type);
        //         }

        //         map
        //     }
        //     DataType::Union(_, None, _) => {
        //         // return default ordering
        //         [
        //             Some(GeometryType::Point),
        //             Some(GeometryType::LineString),
        //             Some(GeometryType::Polygon),
        //             Some(GeometryType::MultiPoint),
        //             Some(GeometryType::MultiLineString),
        //             Some(GeometryType::MultiPolygon),
        //         ]
        //     }
        //     _ => panic!(),
        // };

        // let mut points: Option<PointArray> = None;
        // let mut line_strings: Option<LineStringArray<i32>> = None;
        // let mut polygons: Option<PolygonArray<i32>> = None;
        // let mut multi_points: Option<MultiPointArray<i32>> = None;
        // let mut multi_line_strings: Option<MultiLineStringArray<i32>> = None;
        // let mut multi_polygons: Option<MultiPolygonArray<i32>> = None;

        // for field in child_arrays {
        //     let geometry_array: GeometryArray<i32> = field.as_ref().try_into().unwrap();
        //     match geometry_array {
        //         GeometryArray::Point(arr) => {
        //             points = Some(arr);
        //         }
        //         GeometryArray::LineString(arr) => {
        //             line_strings = Some(arr);
        //         }
        //         GeometryArray::Polygon(arr) => {
        //             polygons = Some(arr);
        //         }
        //         GeometryArray::MultiPoint(arr) => {
        //             multi_points = Some(arr);
        //         }
        //         GeometryArray::MultiLineString(arr) => {
        //             multi_line_strings = Some(arr);
        //         }
        //         GeometryArray::MultiPolygon(arr) => {
        //             multi_polygons = Some(arr);
        //         }
        //         _ => todo!(),
        //     }
        // }

        // Ok(Self {
        //     types,
        //     offsets,
        //     map,
        //     points: points.unwrap_or_default(),
        //     line_strings: line_strings.unwrap_or_default(),
        //     polygons: polygons.unwrap_or_default(),
        //     multi_points: multi_points.unwrap_or_default(),
        //     multi_line_strings: multi_line_strings.unwrap_or_default(),
        //     multi_polygons: multi_polygons.unwrap_or_default(),
        //     slice_offset: 0,
        // })
    }
}

impl TryFrom<&UnionArray> for MixedGeometryArray<i64> {
    type Error = GeoArrowError;

    fn try_from(_value: &UnionArray) -> std::result::Result<Self, Self::Error> {
        todo!()
        // let types = value.types().clone();
        // let offsets = value.offsets().unwrap().clone();
        // let child_arrays = value.fields();

        // // Need to construct the mapping from the logical ordering to the physical ordering
        // let map = match value.data_type() {
        //     DataType::Union(fields, Some(ids), _mode) => {
        //         let mut map: [Option<GeometryType>; 6] = [None, None, None, None, None, None];
        //         assert!(ids.len() < 6);
        //         for (pos, &id) in ids.iter().enumerate() {
        //             let geom_type: GeometryType = match fields[pos].data_type() {
        //                 DataType::Extension(ext_name, _, _) => (ext_name).into(),
        //                 _ => panic!(),
        //             };

        //             // Set this geometry type in the lookup table
        //             // So when you see `type: 3`, then you look up index `map[3]`, which gives you
        //             // a geometry type. Then that geometry type is looked up in the primitive
        //             // arrays.
        //             map[id as usize] = Some(geom_type);
        //         }

        //         map
        //     }
        //     DataType::Union(_, None, _) => {
        //         // return default ordering
        //         [
        //             Some(GeometryType::Point),
        //             Some(GeometryType::LineString),
        //             Some(GeometryType::Polygon),
        //             Some(GeometryType::MultiPoint),
        //             Some(GeometryType::MultiLineString),
        //             Some(GeometryType::MultiPolygon),
        //         ]
        //     }
        //     _ => panic!(),
        // };

        // let mut points: Option<PointArray> = None;
        // let mut line_strings: Option<LineStringArray<i64>> = None;
        // let mut polygons: Option<PolygonArray<i64>> = None;
        // let mut multi_points: Option<MultiPointArray<i64>> = None;
        // let mut multi_line_strings: Option<MultiLineStringArray<i64>> = None;
        // let mut multi_polygons: Option<MultiPolygonArray<i64>> = None;

        // for field in child_arrays {
        //     let geometry_array: GeometryArray<i64> = field.as_ref().try_into().unwrap();
        //     match geometry_array {
        //         GeometryArray::Point(arr) => {
        //             points = Some(arr);
        //         }
        //         GeometryArray::LineString(arr) => {
        //             line_strings = Some(arr);
        //         }
        //         GeometryArray::Polygon(arr) => {
        //             polygons = Some(arr);
        //         }
        //         GeometryArray::MultiPoint(arr) => {
        //             multi_points = Some(arr);
        //         }
        //         GeometryArray::MultiLineString(arr) => {
        //             multi_line_strings = Some(arr);
        //         }
        //         GeometryArray::MultiPolygon(arr) => {
        //             multi_polygons = Some(arr);
        //         }
        //         _ => todo!(),
        //     }
        // }

        // Ok(Self {
        //     types,
        //     offsets,
        //     map,
        //     points: points.unwrap_or_default(),
        //     line_strings: line_strings.unwrap_or_default(),
        //     polygons: polygons.unwrap_or_default(),
        //     multi_points: multi_points.unwrap_or_default(),
        //     multi_line_strings: multi_line_strings.unwrap_or_default(),
        //     multi_polygons: multi_polygons.unwrap_or_default(),
        //     slice_offset: 0,
        // })
    }
}

impl<O: OffsetSizeTrait> TryFrom<Vec<geo::Geometry>> for MixedGeometryArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<geo::Geometry>) -> std::result::Result<Self, Self::Error> {
        let mut_arr: MutableMixedGeometryArray<O> = value.try_into()?;
        Ok(mut_arr.into())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::array::MixedGeometryArray;
    use crate::test::{linestring, multilinestring, multipoint, multipolygon, point, polygon};

    #[test]
    fn geo_roundtrip_accurate_points() {
        let geoms: Vec<geo::Geometry> = vec![
            geo::Geometry::Point(point::p0()),
            geo::Geometry::Point(point::p1()),
            geo::Geometry::Point(point::p2()),
        ];
        let arr: MixedGeometryArray<i32> = geoms.try_into().unwrap();

        assert_eq!(
            arr.value_as_geo(0),
            geo::Geometry::MultiPoint(geo::MultiPoint(vec![point::p0()]))
        );
        assert_eq!(
            arr.value_as_geo(1),
            geo::Geometry::MultiPoint(geo::MultiPoint(vec![point::p1()]))
        );
        assert_eq!(
            arr.value_as_geo(2),
            geo::Geometry::MultiPoint(geo::MultiPoint(vec![point::p2()]))
        );
    }

    #[test]
    fn geo_roundtrip_accurate_all() {
        let geoms: Vec<geo::Geometry> = vec![
            geo::Geometry::Point(point::p0()),
            geo::Geometry::LineString(linestring::ls0()),
            geo::Geometry::Polygon(polygon::p0()),
            geo::Geometry::MultiPoint(multipoint::mp0()),
            geo::Geometry::MultiLineString(multilinestring::ml0()),
            geo::Geometry::MultiPolygon(multipolygon::mp0()),
        ];
        let arr: MixedGeometryArray<i32> = geoms.clone().try_into().unwrap();

        assert_eq!(
            arr.value_as_geo(0),
            geo::Geometry::MultiPoint(geo::MultiPoint(vec![point::p0()]))
        );
        assert_eq!(
            arr.value_as_geo(1),
            geo::Geometry::MultiLineString(geo::MultiLineString(vec![linestring::ls0()]))
        );
        assert_eq!(
            arr.value_as_geo(2),
            geo::Geometry::MultiPolygon(geo::MultiPolygon(vec![polygon::p0()]))
        );
        assert_eq!(arr.value_as_geo(3), geoms[3]);
        assert_eq!(arr.value_as_geo(4), geoms[4]);
        assert_eq!(arr.value_as_geo(5), geoms[5]);
    }

    #[ignore = "Something wrong in arrow-rs transition"]
    #[test]
    fn arrow2_roundtrip() {
        let geoms: Vec<geo::Geometry> = vec![
            geo::Geometry::Point(point::p0()),
            geo::Geometry::LineString(linestring::ls0()),
            geo::Geometry::Polygon(polygon::p0()),
            geo::Geometry::MultiPoint(multipoint::mp0()),
            geo::Geometry::MultiLineString(multilinestring::ml0()),
            geo::Geometry::MultiPolygon(multipolygon::mp0()),
        ];
        let arr: MixedGeometryArray<i32> = geoms.clone().try_into().unwrap();

        // Round trip to/from arrow2
        let arrow_array = arr.into_arrow();
        let round_trip_arr: MixedGeometryArray<i32> = (&arrow_array).try_into().unwrap();

        assert_eq!(
            round_trip_arr.value_as_geo(0),
            geo::Geometry::MultiPoint(geo::MultiPoint(vec![point::p0()]))
        );
        assert_eq!(
            round_trip_arr.value_as_geo(1),
            geo::Geometry::MultiLineString(geo::MultiLineString(vec![linestring::ls0()]))
        );
        assert_eq!(
            round_trip_arr.value_as_geo(2),
            geo::Geometry::MultiPolygon(geo::MultiPolygon(vec![polygon::p0()]))
        );
        assert_eq!(round_trip_arr.value_as_geo(3), geoms[3]);
        assert_eq!(round_trip_arr.value_as_geo(4), geoms[4]);
        assert_eq!(round_trip_arr.value_as_geo(5), geoms[5]);
    }
}
