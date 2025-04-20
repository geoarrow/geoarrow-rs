use std::collections::HashSet;

use geo_traits::{
    GeometryCollectionTrait, GeometryTrait, MultiLineStringTrait, MultiPointTrait,
    MultiPolygonTrait,
};
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_array::error::{GeoArrowError, Result};
use geoarrow_array::{ArrayAccessor, GeoArrowArray, GeoArrowType};
use geoarrow_schema::Dimension;

/// Infer a common native geometry type, if any
///
/// None means that there is no common type to downcast to, and can be left as GeometryType or a
/// serialized type.
pub fn infer_downcast_type<'a>(
    arrays: impl Iterator<Item = &'a dyn GeoArrowArray>,
) -> Result<Option<(NativeType, Dimension)>> {
    let mut type_ids = HashSet::new();
    for array in arrays {
        let type_id = get_type_ids(array)?;
        type_ids.extend(type_id);
    }

    if type_ids.is_empty() {
        return Err(GeoArrowError::General(
            "Cannot infer type from empty sequence of arrays".to_string(),
        ));
    }

    infer_from_native_type_and_dimension(type_ids)
}

fn get_type_ids(array: &dyn GeoArrowArray) -> Result<HashSet<NativeTypeAndDimension>> {
    use GeoArrowType::*;
    let type_ids: HashSet<NativeTypeAndDimension> = match array.data_type() {
        Point(typ) => [NativeTypeAndDimension::new(
            NativeType::Point,
            typ.dimension(),
        )]
        .into_iter()
        .collect(),
        LineString(typ) => [NativeTypeAndDimension::new(
            NativeType::LineString,
            typ.dimension(),
        )]
        .into_iter()
        .collect(),
        Polygon(typ) => [NativeTypeAndDimension::new(
            NativeType::Polygon,
            typ.dimension(),
        )]
        .into_iter()
        .collect(),
        MultiPoint(typ) => {
            let dim = typ.dimension();
            let array = array.as_multi_point();
            array
                .iter()
                .flatten()
                .map(|multi_point| {
                    let geom_type = if multi_point?.num_points() >= 2 {
                        NativeTypeAndDimension::new(NativeType::MultiPoint, dim)
                    } else {
                        NativeTypeAndDimension::new(NativeType::Point, dim)
                    };
                    Ok::<_, GeoArrowError>(geom_type)
                })
                .collect::<Result<HashSet<NativeTypeAndDimension>>>()?
        }
        MultiLineString(typ) => {
            let dim = typ.dimension();
            let array = array.as_multi_line_string();
            array
                .iter()
                .flatten()
                .map(|multi_line_string| {
                    let geom_type = if multi_line_string?.num_line_strings() >= 2 {
                        NativeTypeAndDimension::new(NativeType::MultiLineString, dim)
                    } else {
                        NativeTypeAndDimension::new(NativeType::LineString, dim)
                    };
                    Ok::<_, GeoArrowError>(geom_type)
                })
                .collect::<Result<HashSet<NativeTypeAndDimension>>>()?
        }
        MultiPolygon(typ) => {
            let dim = typ.dimension();
            let array = array.as_multi_polygon();
            array
                .iter()
                .flatten()
                .map(|multi_polygon| {
                    let geom_type = if multi_polygon?.num_polygons() >= 2 {
                        NativeTypeAndDimension::new(NativeType::MultiPolygon, dim)
                    } else {
                        NativeTypeAndDimension::new(NativeType::Polygon, dim)
                    };
                    Ok::<_, GeoArrowError>(geom_type)
                })
                .collect::<Result<HashSet<NativeTypeAndDimension>>>()?
        }
        GeometryCollection(typ) => {
            let dim = typ.dimension();
            let array = array.as_geometry_collection();
            array
                .iter()
                .flatten()
                .map(|geometry_collection| {
                    let geometry_collection = geometry_collection?;
                    let geom_type = if geometry_collection.num_geometries() == 1 {
                        let geom_type = NativeType::from_geometry_trait(
                            &geometry_collection.geometry(0).unwrap(),
                        );
                        NativeTypeAndDimension::new(geom_type, dim)
                    } else {
                        NativeTypeAndDimension::new(NativeType::GeometryCollection, dim)
                    };
                    Ok::<_, GeoArrowError>(geom_type)
                })
                .collect::<Result<HashSet<NativeTypeAndDimension>>>()?
        }
        Rect(typ) => [NativeTypeAndDimension::new(
            NativeType::Rect,
            typ.dimension(),
        )]
        .into_iter()
        .collect(),
        Geometry(_) => array
            .as_geometry()
            .type_ids()
            .iter()
            .map(|type_id| NativeTypeAndDimension::from_type_id(*type_id))
            .collect(),
        Wkb(_) => {
            let wkb_scalars = array
                .as_wkb::<i32>()
                .iter()
                .flatten()
                .collect::<Result<Vec<_>>>()?;
            wkb_scalars
                .iter()
                .map(|wkb| {
                    let dim = wkb.dim().try_into()?;
                    let geom_type = NativeType::from_geometry_trait(wkb);
                    Ok(NativeTypeAndDimension::new(geom_type, dim))
                })
                .collect::<Result<HashSet<NativeTypeAndDimension>>>()?
        }
        LargeWkb(_) => {
            let wkb_scalars = array
                .as_wkb::<i64>()
                .iter()
                .flatten()
                .collect::<Result<Vec<_>>>()?;
            wkb_scalars
                .iter()
                .map(|wkb| {
                    let dim = wkb.dim().try_into()?;
                    let geom_type = NativeType::from_geometry_trait(wkb);
                    Ok(NativeTypeAndDimension::new(geom_type, dim))
                })
                .collect::<Result<HashSet<NativeTypeAndDimension>>>()?
        }
        Wkt(_) => array
            .as_wkt::<i32>()
            .inner()
            .iter()
            .flatten()
            .map(|s| {
                let (wkt_type, wkt_dim) = wkt::infer_type(s).map_err(GeoArrowError::Cast)?;
                let geom_type = NativeTypeAndDimension::new(wkt_type.into(), wkt_dim.into());
                Ok(geom_type)
            })
            .collect::<Result<HashSet<NativeTypeAndDimension>>>()?,
        LargeWkt(_) => array
            .as_wkt::<i64>()
            .inner()
            .iter()
            .flatten()
            .map(|s| {
                let (wkt_type, wkt_dim) = wkt::infer_type(s).map_err(GeoArrowError::Cast)?;
                let geom_type = NativeTypeAndDimension::new(wkt_type.into(), wkt_dim.into());
                Ok(geom_type)
            })
            .collect::<Result<HashSet<NativeTypeAndDimension>>>()?,
    };
    Ok(type_ids)
}

fn infer_from_native_type_and_dimension(
    type_ids: HashSet<NativeTypeAndDimension>,
) -> Result<Option<(NativeType, Dimension)>> {
    // Easy, if there's only one type, return that
    if type_ids.len() == 1 {
        let type_id = type_ids.into_iter().next().unwrap();
        return Ok(Some((type_id.geometry_type, type_id.dim)));
    }

    // If there are multiple dimensions, we can't cast to a single type
    let (dims, native_types): (HashSet<_>, HashSet<_>) = type_ids
        .iter()
        .map(|type_id| (type_id.dim, type_id.geometry_type))
        .unzip();
    if dims.len() > 1 {
        return Ok(None);
    }
    let dim = dims.into_iter().next().unwrap();

    if native_types.len() == 2 {
        if native_types.contains(&NativeType::Point)
            && native_types.contains(&NativeType::MultiPoint)
        {
            return Ok(Some((NativeType::MultiPoint, dim)));
        }

        if native_types.contains(&NativeType::LineString)
            && native_types.contains(&NativeType::MultiLineString)
        {
            return Ok(Some((NativeType::MultiLineString, dim)));
        }

        if native_types.contains(&NativeType::Polygon)
            && native_types.contains(&NativeType::MultiPolygon)
        {
            return Ok(Some((NativeType::MultiPolygon, dim)));
        }
    }

    Ok(None)
}

/// An enum representing the different native GeoArrow geometry types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NativeType {
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
    GeometryCollection,
    Rect,
}

impl NativeType {
    fn from_geometry_trait(geometry: &impl GeometryTrait) -> Self {
        match geometry.as_type() {
            geo_traits::GeometryType::Point(_) => Self::Point,
            geo_traits::GeometryType::LineString(_) => Self::LineString,
            geo_traits::GeometryType::Polygon(_) => Self::Polygon,
            geo_traits::GeometryType::MultiPoint(_) => Self::MultiPoint,
            geo_traits::GeometryType::MultiLineString(_) => Self::MultiLineString,
            geo_traits::GeometryType::MultiPolygon(_) => Self::MultiPolygon,
            geo_traits::GeometryType::GeometryCollection(_) => Self::GeometryCollection,
            _ => panic!("Unsupported geometry type"),
        }
    }
}

impl From<wkt::types::GeometryType> for NativeType {
    fn from(value: wkt::types::GeometryType) -> Self {
        match value {
            wkt::types::GeometryType::Point => Self::Point,
            wkt::types::GeometryType::LineString => Self::LineString,
            wkt::types::GeometryType::Polygon => Self::Polygon,
            wkt::types::GeometryType::MultiPoint => Self::MultiPoint,
            wkt::types::GeometryType::MultiLineString => Self::MultiLineString,
            wkt::types::GeometryType::MultiPolygon => Self::MultiPolygon,
            wkt::types::GeometryType::GeometryCollection => Self::GeometryCollection,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct NativeTypeAndDimension {
    geometry_type: NativeType,
    dim: Dimension,
}

impl NativeTypeAndDimension {
    fn new(geometry_type: NativeType, dim: Dimension) -> Self {
        Self { geometry_type, dim }
    }

    fn from_type_id(type_id: i8) -> Self {
        let dim = match type_id / 10 {
            0 => Dimension::XY,
            1 => Dimension::XYZ,
            2 => Dimension::XYM,
            3 => Dimension::XYZM,
            _ => panic!("unsupported type_id: {type_id}"),
        };
        let geometry_type = match type_id % 10 {
            1 => NativeType::Point,
            2 => NativeType::LineString,
            3 => NativeType::Polygon,
            4 => NativeType::MultiPoint,
            5 => NativeType::MultiLineString,
            6 => NativeType::MultiPolygon,
            7 => NativeType::GeometryCollection,
            _ => panic!("unsupported type id"),
        };
        Self { geometry_type, dim }
    }
}

impl From<(NativeType, Dimension)> for NativeTypeAndDimension {
    fn from(value: (NativeType, Dimension)) -> Self {
        Self::new(value.0, value.1)
    }
}

#[cfg(test)]
mod test {
    use geoarrow_array::cast::{to_wkb, to_wkt};
    use geoarrow_array::test;
    use geoarrow_schema::CoordType;

    use super::*;

    #[test]
    fn infer_get_type_ids_point() {
        // Point
        for dim in [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ] {
            let array = test::point::array(CoordType::Interleaved, dim);
            assert_eq!(
                get_type_ids(&array).unwrap(),
                HashSet::from_iter([NativeTypeAndDimension::new(NativeType::Point, dim)])
            );
        }
    }

    #[test]
    fn infer_get_type_ids_linestring() {
        // LineString
        for dim in [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ] {
            let array = test::linestring::array(CoordType::Interleaved, dim);
            assert_eq!(
                get_type_ids(&array).unwrap(),
                HashSet::from_iter([NativeTypeAndDimension::new(NativeType::LineString, dim)])
            );
        }
    }

    #[test]
    fn infer_get_type_ids_polygon() {
        // Polygon
        for dim in [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ] {
            let array = test::polygon::array(CoordType::Interleaved, dim);
            assert_eq!(
                get_type_ids(&array).unwrap(),
                HashSet::from_iter([NativeTypeAndDimension::new(NativeType::Polygon, dim)])
            );
        }
    }

    #[test]
    fn infer_get_type_ids_multipoint() {
        // MultiPoint
        for dim in [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ] {
            let array = test::multipoint::array(CoordType::Interleaved, dim);
            assert_eq!(
                get_type_ids(&array).unwrap(),
                HashSet::from_iter([
                    NativeTypeAndDimension::new(NativeType::Point, dim),
                    NativeTypeAndDimension::new(NativeType::MultiPoint, dim),
                ])
            );
        }
    }

    #[test]
    fn infer_get_type_ids_multilinestring() {
        // MultiLineString
        for dim in [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ] {
            let array = test::multilinestring::array(CoordType::Interleaved, dim);
            assert_eq!(
                get_type_ids(&array).unwrap(),
                HashSet::from_iter([
                    NativeTypeAndDimension::new(NativeType::LineString, dim),
                    NativeTypeAndDimension::new(NativeType::MultiLineString, dim),
                ])
            );
        }
    }

    #[test]
    fn infer_get_type_ids_multipolygon() {
        // MultiPolygon
        for dim in [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ] {
            let array = test::multipolygon::array(CoordType::Interleaved, dim);
            assert_eq!(
                get_type_ids(&array).unwrap(),
                HashSet::from_iter([
                    NativeTypeAndDimension::new(NativeType::Polygon, dim),
                    NativeTypeAndDimension::new(NativeType::MultiPolygon, dim),
                ])
            );
        }
    }

    #[test]
    fn infer_get_type_ids_geometrycollection() {
        // GeometryCollection
        for dim in [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ] {
            let array = test::geometrycollection::array(CoordType::Interleaved, dim, false);
            assert_eq!(
                get_type_ids(&array).unwrap(),
                HashSet::from_iter([
                    NativeTypeAndDimension::new(NativeType::Point, dim),
                    NativeTypeAndDimension::new(NativeType::LineString, dim),
                    NativeTypeAndDimension::new(NativeType::Polygon, dim),
                    NativeTypeAndDimension::new(NativeType::MultiPoint, dim),
                    NativeTypeAndDimension::new(NativeType::MultiLineString, dim),
                    NativeTypeAndDimension::new(NativeType::MultiPolygon, dim),
                    NativeTypeAndDimension::new(NativeType::GeometryCollection, dim),
                ])
            );
        }
    }

    #[test]
    fn infer_get_type_ids_geometry_wkb_wkt() {
        let array = test::geometry::array(CoordType::Interleaved, false);
        let wkb_array = to_wkb::<i32>(&array).unwrap();
        let large_wkb_array = to_wkb::<i64>(&array).unwrap();
        let wkt_array = to_wkt::<i32>(&array).unwrap();
        let large_wkt_array = to_wkt::<i64>(&array).unwrap();

        let mut expected_types = HashSet::new();
        for dim in [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ] {
            expected_types.insert(NativeTypeAndDimension::new(NativeType::Point, dim));
            expected_types.insert(NativeTypeAndDimension::new(NativeType::LineString, dim));
            expected_types.insert(NativeTypeAndDimension::new(NativeType::Polygon, dim));
            expected_types.insert(NativeTypeAndDimension::new(NativeType::MultiPoint, dim));
            expected_types.insert(NativeTypeAndDimension::new(
                NativeType::MultiLineString,
                dim,
            ));
            expected_types.insert(NativeTypeAndDimension::new(NativeType::MultiPolygon, dim));
            expected_types.insert(NativeTypeAndDimension::new(
                NativeType::GeometryCollection,
                dim,
            ));
        }

        assert_eq!(get_type_ids(&array).unwrap(), expected_types);
        assert_eq!(get_type_ids(&wkb_array).unwrap(), expected_types);
        assert_eq!(get_type_ids(&large_wkb_array).unwrap(), expected_types);
        assert_eq!(get_type_ids(&wkt_array).unwrap(), expected_types);
        assert_eq!(get_type_ids(&large_wkt_array).unwrap(), expected_types);
    }

    #[test]
    fn infer_from_one_type() {
        let input_type = NativeTypeAndDimension::new(NativeType::Point, Dimension::XY);
        let type_ids = [input_type].into_iter().collect::<HashSet<_>>();
        let resolved_type = infer_from_native_type_and_dimension(type_ids)
            .unwrap()
            .unwrap();
        assert_eq!(input_type, resolved_type.into());
    }

    #[test]
    fn cant_infer_from_two_dims() {
        let input_types = [
            NativeTypeAndDimension::new(NativeType::Point, Dimension::XY),
            NativeTypeAndDimension::new(NativeType::Point, Dimension::XYZ),
        ];
        let type_ids = input_types.into_iter().collect::<HashSet<_>>();
        assert!(
            infer_from_native_type_and_dimension(type_ids)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn infer_point_multi_point() {
        let input_types = [
            NativeTypeAndDimension::new(NativeType::Point, Dimension::XYZ),
            NativeTypeAndDimension::new(NativeType::MultiPoint, Dimension::XYZ),
        ];
        let type_ids = input_types.into_iter().collect::<HashSet<_>>();
        let resolved_type = infer_from_native_type_and_dimension(type_ids)
            .unwrap()
            .unwrap();
        assert_eq!(
            NativeTypeAndDimension::new(NativeType::MultiPoint, Dimension::XYZ),
            resolved_type.into()
        );
    }

    #[test]
    fn infer_linestring_multilinestring() {
        let input_types = [
            NativeTypeAndDimension::new(NativeType::LineString, Dimension::XYM),
            NativeTypeAndDimension::new(NativeType::MultiLineString, Dimension::XYM),
        ];
        let type_ids = input_types.into_iter().collect::<HashSet<_>>();
        let resolved_type = infer_from_native_type_and_dimension(type_ids)
            .unwrap()
            .unwrap();
        assert_eq!(
            NativeTypeAndDimension::new(NativeType::MultiLineString, Dimension::XYM),
            resolved_type.into()
        );
    }

    #[test]
    fn infer_polygon_multipolygon() {
        let input_types = [
            NativeTypeAndDimension::new(NativeType::Polygon, Dimension::XYZM),
            NativeTypeAndDimension::new(NativeType::MultiPolygon, Dimension::XYZM),
        ];
        let type_ids = input_types.into_iter().collect::<HashSet<_>>();
        let resolved_type = infer_from_native_type_and_dimension(type_ids)
            .unwrap()
            .unwrap();
        assert_eq!(
            NativeTypeAndDimension::new(NativeType::MultiPolygon, Dimension::XYZM),
            resolved_type.into()
        );
    }

    #[test]
    fn unable_to_infer() {
        let input_types = [
            NativeTypeAndDimension::new(NativeType::Point, Dimension::XY),
            NativeTypeAndDimension::new(NativeType::LineString, Dimension::XY),
        ];
        let type_ids = input_types.into_iter().collect::<HashSet<_>>();
        assert!(
            infer_from_native_type_and_dimension(type_ids)
                .unwrap()
                .is_none()
        );
    }
}
