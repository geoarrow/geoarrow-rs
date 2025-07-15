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
use geo::relate::IntersectionMatrix;
use geo::{PreparedGeometry, Relate};
use geo_traits::to_geo::ToGeoGeometry;
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowResult;

use crate::error::GeoDataFusionResult;

macro_rules! impl_relate_udf {
    ($struct_name:ident, $udf_name:expr, $documentation_name:ident, $callback:expr, $doc_text:expr, $doc_example:expr) => {
        #[derive(Debug)]
        pub struct $struct_name {
            signature: Signature,
        }

        impl $struct_name {
            pub fn new() -> Self {
                Self {
                    signature: Signature::any(2, Volatility::Immutable),
                }
            }
        }

        impl Default for $struct_name {
            fn default() -> Self {
                Self::new()
            }
        }

        static $documentation_name: OnceLock<Documentation> = OnceLock::new();

        impl ScalarUDFImpl for $struct_name {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn name(&self) -> &str {
                $udf_name
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Ok(DataType::Boolean)
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                let mut arrays = args.args.into_iter();
                Ok(relate_impl(
                    arrays.next().unwrap(),
                    &args.arg_fields[0],
                    arrays.next().unwrap(),
                    &args.arg_fields[1],
                    $callback,
                )?)
            }

            fn documentation(&self) -> Option<&Documentation> {
                Some($documentation_name.get_or_init(|| {
                    Documentation::builder(DOC_SECTION_OTHER, $doc_text, $doc_example)
                        .with_argument("geomA", "geometry")
                        .with_argument("geomB", "geometry")
                        .build()
                }))
            }
        }
    };
}

impl_relate_udf!(
    Intersects,
    "st_intersects",
    INTERSECTS_DOC,
    |matrix| matrix.is_intersects(),
    "Returns true if two geometries intersect. Geometries intersect if they have any point in common.",
    "ST_Intersects(geomA, geomB)"
);
impl_relate_udf!(
    Disjoint,
    "st_disjoint",
    DISJOINT_DOC,
    |matrix| matrix.is_disjoint(),
    "Returns true if two geometries are disjoint. Geometries are disjoint if they have no point in common.",
    "ST_Disjoint(geomA, geomB)"
);
impl_relate_udf!(
    Within,
    "st_within",
    WITHIN_DOC,
    |matrix| matrix.is_within(),
    "Returns TRUE if geometry A is within geometry B. A is within B if and only if all points of A lie inside (i.e. in the interior or boundary of) B (or equivalently, no points of A lie in the exterior of B), and the interiors of A and B have at least one point in common.",
    "ST_Within(geomA, geomB)"
);
impl_relate_udf!(
    Contains,
    "st_contains",
    CONTAINS_DOC,
    |matrix| matrix.is_contains(),
    "Returns TRUE if geometry A contains geometry B. A contains B if and only if all points of B lie inside (i.e. in the interior or boundary of) A (or equivalently, no points of B lie in the exterior of A), and the interiors of A and B have at least one point in common.",
    "ST_Contains(geomA, geomB)"
);
impl_relate_udf!(
    Equals,
    "st_equals",
    EQUALS_DOC,
    |matrix| matrix.is_equal_topo(),
    "Returns true if the given geometries are \"topologically equal\". Use this for a 'better' answer than '='. Topological equality means that the geometries have the same dimension, and their point-sets occupy the same space. This means that the order of vertices may be different in topologically equal geometries.",
    "ST_Equals(geomA, geomB)"
);
impl_relate_udf!(
    CoveredBy,
    "st_coveredby",
    COVERED_BY_DOC,
    |matrix| matrix.is_coveredby(),
    "Returns true if every point in Geometry/Geography A lies inside (i.e. intersects the interior or boundary of) Geometry/Geography B. Equivalently, tests that no point of A lies outside (in the exterior of) B.",
    "ST_CoveredBy(geomA, geomB)"
);
impl_relate_udf!(
    Covers,
    "st_covers",
    COVERS_DOC,
    |matrix| matrix.is_covers(),
    "Returns true if every point in Geometry/Geography B lies inside (i.e. intersects the interior or boundary of) Geometry/Geography A. Equivalently, tests that no point of B lies outside (in the exterior of) A.",
    "ST_CoveredBy(geomA, geomB)"
);
impl_relate_udf!(
    Touches,
    "st_touches",
    TOUCHES_DOC,
    |matrix| matrix.is_touches(),
    "Returns TRUE if A and B intersect, but their interiors do not intersect. Equivalently, A and B have at least one point in common, and the common points lie in at least one boundary. For Point/Point inputs the relationship is always FALSE, since points do not have a boundary.",
    "ST_Touches(geomA, geomB)"
);

impl_relate_udf!(
    Crosses,
    "st_crosses",
    CROSSES_DOC,
    |matrix| matrix.is_crosses(),
    "Compares two geometry objects and returns true if their intersection \"spatially crosses\"; that is, the geometries have some, but not all interior points in common. The intersection of the interiors of the geometries must be non-empty and must have dimension less than the maximum dimension of the two input geometries, and the intersection of the two geometries must not equal either geometry. Otherwise, it returns false. The crosses relation is symmetric and irreflexive.",
    "ST_Crosses(geomA, geomB)"
);
impl_relate_udf!(
    Overlaps,
    "st_overlaps",
    OVERLAPS_DOC,
    |matrix| matrix.is_overlaps(),
    "Returns TRUE if geometry A and B \"spatially overlap\". Two geometries overlap if they have the same dimension, their interiors intersect in that dimension. and each has at least one point inside the other (or equivalently, neither one covers the other). The overlaps relation is symmetric and irreflexive.",
    "ST_Overlaps(geomA, geomB)"
);

fn relate_impl(
    left: ColumnarValue,
    left_field: &Field,
    right: ColumnarValue,
    right_field: &Field,
    relate_cb: impl Fn(IntersectionMatrix) -> bool,
) -> GeoDataFusionResult<ColumnarValue> {
    match (left, right) {
        (ColumnarValue::Scalar(left_scalar), ColumnarValue::Scalar(right_scalar)) => {
            let mut arrays =
                ColumnarValue::values_to_arrays(&[left_scalar.into(), right_scalar.into()])?
                    .into_iter();
            let left_array = ColumnarValue::Array(arrays.next().unwrap());
            let right_array = ColumnarValue::Array(arrays.next().unwrap());
            relate_impl(left_array, left_field, right_array, right_field, relate_cb)
        }
        (ColumnarValue::Array(left_arr), ColumnarValue::Array(right_arr)) => {
            let left_arr = from_arrow_array(&left_arr, left_field)?;
            let right_arr = from_arrow_array(&right_arr, right_field)?;
            let result = geoarrow_geo::relate_boolean(&left_arr, &right_arr, relate_cb)?;
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        (ColumnarValue::Scalar(left_scalar), ColumnarValue::Array(right_array)) => {
            let left_scalar_array = ColumnarValue::values_to_arrays(&[left_scalar.into()])?;
            let left_geo_array = from_arrow_array(&left_scalar_array[0], left_field)?;
            let left_geo_scalar = to_geo_scalar(left_geo_array.as_ref())?
                .expect("Null geometries not currently supported");

            let left_prepared_geometry = PreparedGeometry::from(left_geo_scalar);

            let right_geo_array = from_arrow_array(&right_array, right_field)?;
            let result =
                relate_prepared_geometry(&right_geo_array, &left_prepared_geometry, relate_cb)?;
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        (ColumnarValue::Array(left_array), ColumnarValue::Scalar(right_scalar)) => {
            let right_scalar_array = ColumnarValue::values_to_arrays(&[right_scalar.into()])?;
            let right_geo_array = from_arrow_array(&right_scalar_array[0], right_field)?;
            let right_geo_scalar = to_geo_scalar(right_geo_array.as_ref())?
                .expect("Null geometries not currently supported");

            let right_prepared_geometry = PreparedGeometry::from(right_geo_scalar);

            let left_geo_array = from_arrow_array(&left_array, left_field)?;
            let result =
                relate_prepared_geometry(&left_geo_array, &right_prepared_geometry, relate_cb)?;
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
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

fn relate_prepared_geometry(
    array: &dyn GeoArrowArray,
    prepared: &PreparedGeometry<geo::Geometry>,
    relate_cb: impl Fn(IntersectionMatrix) -> bool,
) -> GeoDataFusionResult<BooleanArray> {
    downcast_geoarrow_array!(array, _relate_prepared_geometry_impl, prepared, relate_cb)
}

fn _relate_prepared_geometry_impl<'a>(
    arr: &'a impl GeoArrowArrayAccessor<'a>,
    prepared: &PreparedGeometry<geo::Geometry>,
    relate_cb: impl Fn(IntersectionMatrix) -> bool,
) -> GeoDataFusionResult<BooleanArray> {
    let mut builder = BooleanBuilder::with_capacity(arr.len());

    for item in arr.iter() {
        if let Some(geom) = item {
            let geo_geom = geom?.to_geometry();
            builder.append_value(relate_cb(geo_geom.relate(prepared)));
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

    /// https://postgis.net/docs/ST_Overlaps.html
    #[tokio::test]
    async fn test_point_linestring() {
        let ctx = SessionContext::new();

        ctx.register_udf(Contains::new().into());
        ctx.register_udf(Crosses::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());
        ctx.register_udf(Intersects::new().into());
        ctx.register_udf(Overlaps::new().into());

        let sql_str = r#"
            SELECT ST_Overlaps(a,b) AS overlaps,       ST_Crosses(a,b) AS crosses,
                   ST_Intersects(a, b) AS intersects,  ST_Contains(b,a) AS b_contains_a
            FROM (SELECT
                ST_GeomFromText('POINT (100 100)') As a,
                ST_GeomFromText('LINESTRING (30 50, 40 160, 160 40, 180 160)') AS b) AS t
        "#;

        let df = ctx.sql(sql_str).await.unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        assert!(
            !batch
                .column_by_name("overlaps")
                .unwrap()
                .as_boolean()
                .value(0)
        );
        assert!(
            !batch
                .column_by_name("crosses")
                .unwrap()
                .as_boolean()
                .value(0)
        );
        assert!(
            batch
                .column_by_name("intersects")
                .unwrap()
                .as_boolean()
                .value(0)
        );
        assert!(
            batch
                .column_by_name("b_contains_a")
                .unwrap()
                .as_boolean()
                .value(0)
        );
    }

    /// https://postgis.net/docs/ST_Overlaps.html
    #[tokio::test]
    async fn test_linestring_polygon() {
        let ctx = SessionContext::new();

        ctx.register_udf(Contains::new().into());
        ctx.register_udf(Crosses::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());
        ctx.register_udf(Intersects::new().into());
        ctx.register_udf(Overlaps::new().into());

        let sql_str = r#"
            SELECT ST_Overlaps(a,b) AS overlaps,       ST_Crosses(a,b) AS crosses,
                   ST_Intersects(a, b) AS intersects,  ST_Contains(b,a) AS b_contains_a
            FROM (SELECT
                ST_GeomFromText('POLYGON ((40 170, 90 30, 180 100, 40 170))') AS a,
             ST_GeomFromText('LINESTRING(10 10, 190 190)') AS b) AS t
        "#;

        let df = ctx.sql(sql_str).await.unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        assert!(
            !batch
                .column_by_name("overlaps")
                .unwrap()
                .as_boolean()
                .value(0)
        );
        assert!(
            batch
                .column_by_name("crosses")
                .unwrap()
                .as_boolean()
                .value(0)
        );
        assert!(
            batch
                .column_by_name("intersects")
                .unwrap()
                .as_boolean()
                .value(0)
        );
        assert!(
            !batch
                .column_by_name("b_contains_a")
                .unwrap()
                .as_boolean()
                .value(0)
        );
    }

    /// https://postgis.net/docs/ST_Overlaps.html
    #[tokio::test]
    async fn test_polygon_polygon() {
        let ctx = SessionContext::new();

        ctx.register_udf(Contains::new().into());
        ctx.register_udf(Crosses::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());
        ctx.register_udf(Intersects::new().into());
        ctx.register_udf(Overlaps::new().into());

        let sql_str = r#"
            SELECT ST_Overlaps(a,b) AS overlaps,       ST_Crosses(a,b) AS crosses,
                   ST_Intersects(a, b) AS intersects,  ST_Contains(b,a) AS b_contains_a
            FROM (SELECT
                ST_GeomFromText('POLYGON ((40 170, 90 30, 180 100, 40 170))') AS a,
             ST_GeomFromText('POLYGON ((110 180, 20 60, 130 90, 110 180))') AS b) AS t
        "#;

        let df = ctx.sql(sql_str).await.unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        assert!(
            batch
                .column_by_name("overlaps")
                .unwrap()
                .as_boolean()
                .value(0)
        );
        assert!(
            !batch
                .column_by_name("crosses")
                .unwrap()
                .as_boolean()
                .value(0)
        );
        assert!(
            batch
                .column_by_name("intersects")
                .unwrap()
                .as_boolean()
                .value(0)
        );
        assert!(
            !batch
                .column_by_name("b_contains_a")
                .unwrap()
                .as_boolean()
                .value(0)
        );
    }
}
