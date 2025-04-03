use std::str::FromStr;
use std::sync::Arc;

use arrow_array::OffsetSizeTrait;
use geoarrow_schema::{CoordType, Metadata};

use crate::array::{GeometryArray, GeometryBuilder, WKTArray};
use crate::error::{GeoArrowError, Result};
use crate::{ArrayBase, NativeArray};

/// Parse a WKT array into a native GeoArrow array.
///
/// Currently, a [GeometryArray] is always returned. This may change in the future with the
/// addition of a `downcast` parameter, which would automatically downcast the result.
pub fn read_wkt<O: OffsetSizeTrait>(
    arr: &WKTArray<O>,
    coord_type: CoordType,
    prefer_multi: bool,
) -> Result<Arc<dyn NativeArray>> {
    let array_metadata = arr.metadata();
    let parsed = from_str_iter(arr.array.iter(), coord_type, array_metadata, prefer_multi)?;
    Ok(Arc::new(parsed))
}

fn from_str_iter<'a>(
    iter: impl Iterator<Item = Option<&'a str>>,
    coord_type: CoordType,
    metadata: Arc<Metadata>,
    prefer_multi: bool,
) -> Result<GeometryArray> {
    let mut builder = GeometryBuilder::new_with_options(coord_type, metadata, prefer_multi);
    for wkt_str in iter {
        if let Some(s) = wkt_str {
            let wkt = wkt::Wkt::<f64>::from_str(s).map_err(GeoArrowError::WktStrError)?;
            builder.push_geometry(Some(&wkt))?;
        } else {
            builder.push_null();
        }
    }
    Ok(builder.finish())
}

#[cfg(test)]
mod test {
    use crate::array::AsNativeArray;
    use crate::trait_::ArrayAccessor;
    use arrow_array::builder::StringBuilder;

    use super::*;

    #[test]
    fn test_read_wkt() {
        let wkt_geoms = [
            "POINT (30 10)",
            "LINESTRING (30 10, 10 30, 40 40)",
            "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))",
        ];
        let mut builder = StringBuilder::new();
        wkt_geoms.iter().for_each(|s| builder.append_value(s));
        let arr = WKTArray::new(builder.finish(), Default::default());

        let parsed = read_wkt(&arr, Default::default(), false).unwrap();
        let parsed_ref = parsed.as_ref();
        let geom_arr = parsed_ref.as_geometry();

        assert_eq!(
            geom_arr.value_as_geo(0),
            geo::Geometry::Point(geo::point!( x: 30.0, y: 10.0 ))
        );
        // let geo_point = geo::Point::try_from(geom_arr.value(0).to_geo().unwrap()).unwrap();
        // assert_eq!(geo_point.x(), 30.0);
        // assert_eq!(geo_point.y(), 10.0);
    }

    // #[test]
    // fn test_read_wkt_downcast_from_multi() {
    //     let wkt_geoms = ["POINT (30 10)", "POINT (20 5)", "POINT (3 10)"];
    //     let mut builder = StringBuilder::new();
    //     wkt_geoms.iter().for_each(|s| builder.append_value(s));
    //     let arr = builder.finish();
    //     // dbg!(arr);
    //     let geom_arr = MixedGeometryArray::from_wkt(
    //         &arr,
    //         Default::default(),
    //         Dimension::XY,
    //         Default::default(),
    //         true,
    //     )
    //     .unwrap();
    //     let geom_arr = geom_arr.downcast(true);
    //     assert!(matches!(
    //         geom_arr.data_type(),
    //         NativeType::Point(_, Dimension::XY)
    //     ));
    // }
}
