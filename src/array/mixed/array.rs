use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{Array, OffsetSizeTrait, UnionArray};
use arrow_buffer::{NullBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, UnionFields, UnionMode};

use crate::array::mixed::builder::MixedGeometryBuilder;
use crate::array::{
    LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray, PointArray,
    PolygonArray,
};
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::GeometryTrait;
use crate::scalar::Geometry;
use crate::trait_::{GeometryArrayAccessor, GeometryArraySelfMethods, IntoArrow};
use crate::GeometryArrayTrait;

/// # Invariants
///
/// - All arrays must have the same dimension
/// - All arrays must have the same coordinate layout (interleaved or separated)
#[derive(Debug, Clone)]
// #[derive(Debug, Clone, PartialEq)]
pub struct MixedGeometryArray<O: OffsetSizeTrait> {
    /// Always GeoDataType::Mixed or GeoDataType::LargeMixed
    data_type: GeoDataType,

    /// Invariant: every item in `type_ids` is `> 0 && < fields.len()` if `type_ids` are not provided. If `type_ids` exist in the GeoDataType, then every item in `type_ids` is `> 0 && `
    type_ids: ScalarBuffer<i8>,

    /// Invariant: `offsets.len() == type_ids.len()`
    offsets: ScalarBuffer<i32>,

    /// A lookup table for which child array is used
    ///
    /// To read a value:
    /// ``rs
    /// let child_index = self.type_ids[i];
    /// let offset = self.offsets[i] as usize;
    /// let geometry_type = self.map[child_index as usize];
    /// ``
    /// then match on the geometry_type to access the underlying array.
    ///
    /// Note that we include an ordering so that exporting this array to Arrow is O(1). If we used
    /// another ordering like always Point, LineString, etc. then we'd either have to always export
    /// all arrays (including some zero-length arrays) or have to reorder the `type_ids` buffer when
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

    points: Option<PointArray>,
    line_strings: Option<LineStringArray<O>>,
    polygons: Option<PolygonArray<O>>,
    multi_points: Option<MultiPointArray<O>>,
    multi_line_strings: Option<MultiLineStringArray<O>>,
    multi_polygons: Option<MultiPolygonArray<O>>,

    /// An offset used for slicing into this array. The offset will be 0 if the array has not been
    /// sliced.
    ///
    /// In order to slice this array efficiently (and zero-cost) we can't slice the underlying
    /// fields directly. If this were always a _sparse_ union array, we could! We could then always
    /// slice from offset to length of each underlying array. But we're under the assumption that
    /// most or all of the time we have a dense union array, where the `offsets` buffer is defined.
    /// In that case, to know how to slice each underlying array, we'd have to walk the `type_ids`
    /// and `offsets` arrays (in O(N) time) to figure out how to slice the underlying arrays.
    ///
    /// Instead, we store the slice offset.
    ///
    /// Note that this offset is only for slicing into the **fields**, i.e. the geometry arrays.
    /// The `type_ids` and `offsets` arrays are sliced as usual.
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
        type_ids: ScalarBuffer<i8>,
        offsets: ScalarBuffer<i32>,
        points: Option<PointArray>,
        line_strings: Option<LineStringArray<O>>,
        polygons: Option<PolygonArray<O>>,
        multi_points: Option<MultiPointArray<O>>,
        multi_line_strings: Option<MultiLineStringArray<O>>,
        multi_polygons: Option<MultiPolygonArray<O>>,
    ) -> Self {
        let default_ordering = [
            Some(GeometryType::Point),
            Some(GeometryType::LineString),
            Some(GeometryType::Polygon),
            Some(GeometryType::MultiPoint),
            Some(GeometryType::MultiLineString),
            Some(GeometryType::MultiPolygon),
        ];

        let mut coord_types = HashSet::new();
        if let Some(ref points) = points {
            coord_types.insert(points.coord_type());
        }
        if let Some(ref line_strings) = line_strings {
            coord_types.insert(line_strings.coord_type());
        }
        if let Some(ref polygons) = polygons {
            coord_types.insert(polygons.coord_type());
        }
        if let Some(ref multi_points) = multi_points {
            coord_types.insert(multi_points.coord_type());
        }
        if let Some(ref multi_line_strings) = multi_line_strings {
            coord_types.insert(multi_line_strings.coord_type());
        }
        if let Some(ref multi_polygons) = multi_polygons {
            coord_types.insert(multi_polygons.coord_type());
        }
        assert_eq!(coord_types.len(), 1);
        let coord_type = coord_types.into_iter().next().unwrap();
        let data_type = match O::IS_LARGE {
            true => GeoDataType::LargeMixed(coord_type),
            false => GeoDataType::Mixed(coord_type),
        };

        Self {
            data_type,
            type_ids,
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

impl<O: OffsetSizeTrait> GeometryArrayTrait for MixedGeometryArray<O> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> &GeoDataType {
        &self.data_type
    }

    fn storage_type(&self) -> DataType {
        let mut fields: Vec<Arc<Field>> = vec![];
        let mut type_ids = vec![];

        if let Some(ref points) = self.points {
            fields.push(points.extension_field());
            type_ids.push(0);
        }
        if let Some(ref line_strings) = self.line_strings {
            fields.push(line_strings.extension_field());
            type_ids.push(1);
        }
        if let Some(ref polygons) = self.polygons {
            fields.push(polygons.extension_field());
            type_ids.push(2);
        }
        if let Some(ref multi_points) = self.multi_points {
            fields.push(multi_points.extension_field());
            type_ids.push(3);
        }
        if let Some(ref multi_line_strings) = self.multi_line_strings {
            fields.push(multi_line_strings.extension_field());
            type_ids.push(4);
        }
        if let Some(ref multi_polygons) = self.multi_polygons {
            fields.push(multi_polygons.extension_field());
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

    fn into_array_ref(self) -> Arc<dyn Array> {
        Arc::new(self.into_arrow())
    }

    fn coord_type(&self) -> crate::array::CoordType {
        todo!();
    }

    /// Returns the number of geometries in this array
    #[inline]
    fn len(&self) -> usize {
        // Note that `type_ids` is sliced as usual, and thus always has the correct length.
        self.type_ids.len()
    }

    /// Returns the optional validity.
    #[inline]
    fn validity(&self) -> Option<&NullBuffer> {
        None
    }
}

impl<O: OffsetSizeTrait> GeometryArraySelfMethods for MixedGeometryArray<O> {
    fn with_coords(self, _coords: crate::array::CoordBuffer) -> Self {
        todo!();
    }

    fn into_coord_type(self, _coord_type: crate::array::CoordType) -> Self {
        todo!();
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
            type_ids: self.type_ids.slice(offset, length),
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
}

impl<'a, O: OffsetSizeTrait> GeometryArrayAccessor<'a> for MixedGeometryArray<O> {
    type Item = Geometry<'a, O>;
    type ItemGeo = geo::Geometry;

    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        let child_index = self.type_ids[index];
        let offset = self.offsets[index] as usize;
        let geometry_type = self.map[child_index as usize].unwrap();

        match geometry_type {
            GeometryType::Point => Geometry::Point(self.points.as_ref().unwrap().value(offset)),
            GeometryType::LineString => {
                Geometry::LineString(self.line_strings.as_ref().unwrap().value(offset))
            }
            GeometryType::Polygon => {
                Geometry::Polygon(self.polygons.as_ref().unwrap().value(offset))
            }
            GeometryType::MultiPoint => {
                Geometry::MultiPoint(self.multi_points.as_ref().unwrap().value(offset))
            }
            GeometryType::MultiLineString => {
                Geometry::MultiLineString(self.multi_line_strings.as_ref().unwrap().value(offset))
            }
            GeometryType::MultiPolygon => {
                Geometry::MultiPolygon(self.multi_polygons.as_ref().unwrap().value(offset))
            }
        }
    }
}

impl<O: OffsetSizeTrait> IntoArrow for MixedGeometryArray<O> {
    type ArrowArray = UnionArray;

    fn into_arrow(self) -> Self::ArrowArray {
        let mut field_type_ids = vec![];
        let mut child_arrays = vec![];

        if let Some(ref points) = self.points {
            field_type_ids.push(0);
            child_arrays.push((
                points.extension_field().as_ref().clone(),
                points.clone().into_array_ref(),
            ));
        }
        if let Some(ref line_strings) = self.line_strings {
            field_type_ids.push(1);
            child_arrays.push((
                line_strings.extension_field().as_ref().clone(),
                line_strings.clone().into_array_ref(),
            ));
        }
        if let Some(ref polygons) = self.polygons {
            field_type_ids.push(2);
            child_arrays.push((
                polygons.extension_field().as_ref().clone(),
                polygons.clone().into_array_ref(),
            ));
        }
        if let Some(ref multi_points) = self.multi_points {
            field_type_ids.push(3);
            child_arrays.push((
                multi_points.extension_field().as_ref().clone(),
                multi_points.clone().into_array_ref(),
            ));
        }
        if let Some(ref multi_line_strings) = self.multi_line_strings {
            field_type_ids.push(4);
            child_arrays.push((
                multi_line_strings.extension_field().as_ref().clone(),
                multi_line_strings.clone().into_array_ref(),
            ));
        }
        if let Some(ref multi_polygons) = self.multi_polygons {
            field_type_ids.push(5);
            child_arrays.push((
                multi_polygons.extension_field().as_ref().clone(),
                multi_polygons.clone().into_array_ref(),
            ));
        }

        UnionArray::try_new(
            &field_type_ids,
            self.type_ids.into_inner(),
            Some(self.offsets.into_inner()),
            child_arrays,
        )
        .unwrap()
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
        self.value(i).try_into().unwrap()
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

    fn try_from(value: &UnionArray) -> std::result::Result<Self, Self::Error> {
        let mut points: Option<PointArray> = None;
        let mut line_strings: Option<LineStringArray<i32>> = None;
        let mut polygons: Option<PolygonArray<i32>> = None;
        let mut multi_points: Option<MultiPointArray<i32>> = None;
        let mut multi_line_strings: Option<MultiLineStringArray<i32>> = None;
        let mut multi_polygons: Option<MultiPolygonArray<i32>> = None;
        match value.data_type() {
            DataType::Union(fields, mode) => {
                if !matches!(mode, UnionMode::Dense) {
                    return Err(GeoArrowError::General("Expected dense union".to_string()));
                }

                for (type_id, field) in fields.iter() {
                    if let Some(extension_name) = field.metadata().get("ARROW:extension:name") {
                        match extension_name.as_str() {
                            "geoarrow.point" => {
                                points = Some(value.child(type_id).as_ref().try_into().unwrap());
                            }
                            "geoarrow.linestring" => {
                                line_strings =
                                    Some(value.child(type_id).as_ref().try_into().unwrap());
                            }
                            "geoarrow.polygon" => {
                                polygons = Some(value.child(type_id).as_ref().try_into().unwrap());
                            }
                            "geoarrow.multipoint" => {
                                multi_points =
                                    Some(value.child(type_id).as_ref().try_into().unwrap());
                            }
                            "geoarrow.multilinestring" => {
                                multi_line_strings =
                                    Some(value.child(type_id).as_ref().try_into().unwrap());
                            }
                            "geoarrow.multipolygon" => {
                                multi_polygons =
                                    Some(value.child(type_id).as_ref().try_into().unwrap());
                            }
                            _ => {
                                return Err(GeoArrowError::General(format!(
                                    "Unexpected geoarrow type {}",
                                    extension_name
                                )))
                            }
                        }
                    };
                }
            }
            _ => panic!("expected union type"),
        };

        let type_ids = value.type_ids().clone();
        // This is after checking for dense union
        let offsets = value.offsets().unwrap().clone();

        Ok(Self::new(
            type_ids,
            offsets,
            points,
            line_strings,
            polygons,
            multi_points,
            multi_line_strings,
            multi_polygons,
        ))
    }
}

impl TryFrom<&UnionArray> for MixedGeometryArray<i64> {
    type Error = GeoArrowError;

    fn try_from(value: &UnionArray) -> std::result::Result<Self, Self::Error> {
        let mut points: Option<PointArray> = None;
        let mut line_strings: Option<LineStringArray<i64>> = None;
        let mut polygons: Option<PolygonArray<i64>> = None;
        let mut multi_points: Option<MultiPointArray<i64>> = None;
        let mut multi_line_strings: Option<MultiLineStringArray<i64>> = None;
        let mut multi_polygons: Option<MultiPolygonArray<i64>> = None;
        match value.data_type() {
            DataType::Union(fields, mode) => {
                if !matches!(mode, UnionMode::Dense) {
                    return Err(GeoArrowError::General("Expected dense union".to_string()));
                }

                for (type_id, field) in fields.iter() {
                    if let Some(extension_name) = field.metadata().get("ARROW:extension:name") {
                        match extension_name.as_str() {
                            "geoarrow.point" => {
                                points = Some(value.child(type_id).as_ref().try_into().unwrap());
                            }
                            "geoarrow.linestring" => {
                                line_strings =
                                    Some(value.child(type_id).as_ref().try_into().unwrap());
                            }
                            "geoarrow.polygon" => {
                                polygons = Some(value.child(type_id).as_ref().try_into().unwrap());
                            }
                            "geoarrow.multipoint" => {
                                multi_points =
                                    Some(value.child(type_id).as_ref().try_into().unwrap());
                            }
                            "geoarrow.multilinestring" => {
                                multi_line_strings =
                                    Some(value.child(type_id).as_ref().try_into().unwrap());
                            }
                            "geoarrow.multipolygon" => {
                                multi_polygons =
                                    Some(value.child(type_id).as_ref().try_into().unwrap());
                            }
                            _ => {
                                return Err(GeoArrowError::General(format!(
                                    "Unexpected geoarrow type {}",
                                    extension_name
                                )))
                            }
                        }
                    };
                }
            }
            _ => panic!("expected union type"),
        };

        let type_ids = value.type_ids().clone();
        // This is after checking for dense union
        let offsets = value.offsets().unwrap().clone();

        Ok(Self::new(
            type_ids,
            offsets,
            points,
            line_strings,
            polygons,
            multi_points,
            multi_line_strings,
            multi_polygons,
        ))
    }
}

impl<O: OffsetSizeTrait, G: GeometryTrait<T = f64>> TryFrom<&[G]> for MixedGeometryArray<O> {
    type Error = GeoArrowError;

    fn try_from(geoms: &[G]) -> Result<Self> {
        let mut_arr: MixedGeometryBuilder<O> = geoms.try_into()?;
        Ok(mut_arr.into())
    }
}

impl<O: OffsetSizeTrait, G: GeometryTrait<T = f64>> TryFrom<&[Option<G>]>
    for MixedGeometryArray<O>
{
    type Error = GeoArrowError;

    fn try_from(geoms: &[Option<G>]) -> Result<Self> {
        let mut_arr: MixedGeometryBuilder<O> = geoms.try_into()?;
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
        let arr: MixedGeometryArray<i32> = geoms.as_slice().try_into().unwrap();

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
        let arr: MixedGeometryArray<i32> = geoms.as_slice().try_into().unwrap();

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

    #[test]
    fn arrow_roundtrip() {
        let geoms: Vec<geo::Geometry> = vec![
            geo::Geometry::Point(point::p0()),
            geo::Geometry::LineString(linestring::ls0()),
            geo::Geometry::Polygon(polygon::p0()),
            geo::Geometry::MultiPoint(multipoint::mp0()),
            geo::Geometry::MultiLineString(multilinestring::ml0()),
            geo::Geometry::MultiPolygon(multipolygon::mp0()),
        ];
        let arr: MixedGeometryArray<i32> = geoms.as_slice().try_into().unwrap();

        // Round trip to/from arrow-rs
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

    #[test]
    fn arrow_roundtrip_not_all_types() {
        let geoms: Vec<geo::Geometry> = vec![
            geo::Geometry::MultiPoint(multipoint::mp0()),
            geo::Geometry::MultiLineString(multilinestring::ml0()),
            geo::Geometry::MultiPolygon(multipolygon::mp0()),
        ];
        let arr: MixedGeometryArray<i32> = geoms.as_slice().try_into().unwrap();

        // Round trip to/from arrow-rs
        let arrow_array = arr.into_arrow();
        let round_trip_arr: MixedGeometryArray<i32> = (&arrow_array).try_into().unwrap();

        assert_eq!(round_trip_arr.value_as_geo(0), geoms[0]);
        assert_eq!(round_trip_arr.value_as_geo(1), geoms[1]);
        assert_eq!(round_trip_arr.value_as_geo(2), geoms[2]);
    }

    #[test]
    fn arrow_roundtrip_not_all_types2() {
        let geoms: Vec<geo::Geometry> = vec![
            geo::Geometry::MultiPoint(multipoint::mp0()),
            geo::Geometry::MultiPolygon(multipolygon::mp0()),
        ];
        let arr: MixedGeometryArray<i32> = geoms.as_slice().try_into().unwrap();

        // Round trip to/from arrow-rs
        let arrow_array = arr.into_arrow();
        let round_trip_arr: MixedGeometryArray<i32> = (&arrow_array).try_into().unwrap();

        assert_eq!(round_trip_arr.value_as_geo(0), geoms[0]);
        assert_eq!(round_trip_arr.value_as_geo(1), geoms[1]);
    }
}
