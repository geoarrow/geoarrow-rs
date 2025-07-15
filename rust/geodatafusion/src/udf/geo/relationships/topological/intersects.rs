use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_array::BooleanArray;
use arrow_array::builder::BooleanBuilder;
use arrow_schema::{DataType, Field};
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use geo::{PreparedGeometry, Relate};
use geo_traits::to_geo::ToGeoGeometry;
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowResult;

use crate::error::GeoDataFusionResult;

#[derive(Debug)]
pub struct Intersects {
    signature: Signature,
}

impl Intersects {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl Default for Intersects {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Intersects {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_intersects"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut arrays = args.args.into_iter();
        Ok(intersects_impl(
            arrays.next().unwrap(),
            &args.arg_fields[0],
            arrays.next().unwrap(),
            &args.arg_fields[1],
        )?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns true if two geometries intersect. Geometries intersect if they have any point in common.",
                "ST_Intersects(geomA, geomB)",
            )
            .with_argument("geomA", "geometry")
            .with_argument("geomB", "geometry")
            .build()
        }))
    }
}

fn intersects_impl(
    left: ColumnarValue,
    left_field: &Field,
    right: ColumnarValue,
    right_field: &Field,
) -> GeoDataFusionResult<ColumnarValue> {
    match (left, right) {
        (ColumnarValue::Scalar(left_scalar), ColumnarValue::Scalar(right_scalar)) => {
            let mut arrays =
                ColumnarValue::values_to_arrays(&[left_scalar.into(), right_scalar.into()])?
                    .into_iter();
            let left_array = ColumnarValue::Array(arrays.next().unwrap());
            let right_array = ColumnarValue::Array(arrays.next().unwrap());
            intersects_impl(left_array, left_field, right_array, right_field)
        }
        (ColumnarValue::Array(left_arr), ColumnarValue::Array(right_arr)) => {
            let left_arr = from_arrow_array(&left_arr, left_field)?;
            let right_arr = from_arrow_array(&right_arr, right_field)?;
            let result = geoarrow_geo::intersects(&left_arr, &right_arr)?;
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        (ColumnarValue::Scalar(left_scalar), ColumnarValue::Array(right_array)) => {
            let left_scalar_array = ColumnarValue::values_to_arrays(&[left_scalar.into()])?;
            let left_geo_array = from_arrow_array(&left_scalar_array[0], left_field)?;
            let left_geo_scalar = to_geo_scalar(left_geo_array.as_ref())?
                .expect("Null geometries not currently supported");

            let left_prepared_geometry = PreparedGeometry::from(left_geo_scalar);

            let right_geo_array = from_arrow_array(&right_array, right_field)?;
            let result = intersects_prepared_geometry(&right_geo_array, &left_prepared_geometry)?;
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        // Reflexive
        (ColumnarValue::Array(left_array), ColumnarValue::Scalar(right_scalar)) => intersects_impl(
            right_scalar.into(),
            right_field,
            left_array.into(),
            left_field,
        ),
    }
}

/// Convert a length-1 GeoArrowArray to a geo::Geometry scalar.
fn to_geo_scalar(arr: &dyn GeoArrowArray) -> GeoArrowResult<Option<geo::Geometry>> {
    downcast_geoarrow_array!(arr, _to_geo_scalar_impl)
}

fn _to_geo_scalar_impl<'a>(
    arr: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Option<geo::Geometry>> {
    if let Some(geom) = arr.iter().next().unwrap() {
        let geom = geom?;
        Ok(geom.try_to_geometry())
    } else {
        Ok(None)
    }
}

fn intersects_prepared_geometry(
    array: &dyn GeoArrowArray,
    prepared: &PreparedGeometry<geo::Geometry>,
) -> GeoDataFusionResult<BooleanArray> {
    downcast_geoarrow_array!(array, _intersects_prepared_geometry_impl, prepared)
}

fn _intersects_prepared_geometry_impl<'a>(
    arr: &'a impl GeoArrowArrayAccessor<'a>,
    prepared: &PreparedGeometry<geo::Geometry>,
) -> GeoDataFusionResult<BooleanArray> {
    let mut builder = BooleanBuilder::with_capacity(arr.len());

    for item in arr.iter() {
        if let Some(geom) = item {
            let geo_geom = geom?.to_geometry();
            builder.append_value(geo_geom.relate(prepared).is_intersects());
        } else {
            builder.append_null();
        }
    }

    Ok(builder.finish())
}

#[cfg(test)]
mod test {
    use arrow_array::RecordBatch;
    use arrow_array::cast::AsArray;
    use arrow_schema::Schema;
    use datafusion::prelude::SessionContext;
    use geoarrow_array::builder::PointBuilder;
    use geoarrow_schema::{Dimension, PointType};

    use super::*;
    use crate::udf::native::io::GeomFromText;

    #[tokio::test]
    async fn test_intersects_scalars_true() {
        let ctx = SessionContext::new();

        ctx.register_udf(Intersects::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_Intersects(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('LINESTRING ( 0 0, 0 2 )'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        assert!(col.as_boolean().value(0));
    }

    #[tokio::test]
    async fn test_intersects_scalars_false() {
        let ctx = SessionContext::new();

        ctx.register_udf(Intersects::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_Intersects(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('LINESTRING ( 2 0, 0 2 )'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        assert!(!col.as_boolean().value(0));
    }

    #[tokio::test]
    async fn test_intersects_array_scalar() {
        let ctx = SessionContext::new();

        let point1 = wkt::wkt! { POINT(0.0 0.0) };
        let point2 = wkt::wkt! { POINT(2.0 2.0) };
        let point_arr = PointBuilder::from_points(
            [point1, point2].iter(),
            PointType::new(Dimension::XY, Default::default()),
        )
        .finish();

        let schema = Schema::new([Arc::new(point_arr.data_type().to_field("geometry", true))]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![point_arr.to_array_ref()]).unwrap();

        ctx.register_batch("t", batch).unwrap();

        ctx.register_udf(Intersects::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_Intersects(ST_GeomFromText('LINESTRING ( 0 0, 0 2 )'), geometry) FROM t;")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        assert!(col.as_boolean().value(0));
        assert!(!col.as_boolean().value(1));
    }
}
