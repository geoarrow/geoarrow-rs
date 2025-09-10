use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_array::builder::UInt8Builder;
use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::GeometryTrait;
use geoarrow_array::GeoArrowArrayAccessor;
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_schema::{Dimension, GeoArrowType};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct CoordDim {
    signature: Signature,
}

impl CoordDim {
    pub fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input(),
        }
    }
}

impl Default for CoordDim {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for CoordDim {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_coorddim"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::UInt8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(coord_dim_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return the coordinate dimension of the ST_Geometry value.",
                "ST_CoordDim(geometry)",
            )
            .with_argument("g1", "geometry")
            .build()
        }))
    }
}

fn dimension_size(dim: Dimension) -> u8 {
    match dim {
        Dimension::XY => 2,
        Dimension::XYZ => 3,
        Dimension::XYM => 3,
        Dimension::XYZM => 4,
    }
}

fn coord_dim_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(&args.args)?
        .into_iter()
        .next()
        .unwrap();
    let field = &args.arg_fields[0];
    let geo_array = from_arrow_array(&array, field)?;

    macro_rules! iter_geom {
        ($cast_function:ident) => {{
            let mut output_array = UInt8Builder::with_capacity(geo_array.len());
            for geom in geo_array.$cast_function().iter() {
                if let Some(geom) = geom {
                    output_array.append_value(geom?.dim().size().try_into().unwrap());
                } else {
                    output_array.append_null();
                }
            }
            Ok(ColumnarValue::Array(Arc::new(output_array.finish())))
        }};
        ($cast_function:ident, $param:ident) => {{
            let mut output_array = UInt8Builder::with_capacity(geo_array.len());
            for geom in geo_array.$cast_function::<$param>().iter() {
                if let Some(geom) = geom {
                    output_array.append_value(geom?.dim().size().try_into().unwrap());
                } else {
                    output_array.append_null();
                }
            }
            Ok(ColumnarValue::Array(Arc::new(output_array.finish())))
        }};
    }

    use GeoArrowType::*;
    match geo_array.data_type() {
        Point(t) => Ok(ColumnarValue::Scalar(dimension_size(t.dimension()).into())),
        LineString(t) => Ok(ColumnarValue::Scalar(dimension_size(t.dimension()).into())),
        Polygon(t) => Ok(ColumnarValue::Scalar(dimension_size(t.dimension()).into())),
        MultiPoint(t) => Ok(ColumnarValue::Scalar(dimension_size(t.dimension()).into())),
        MultiLineString(t) => Ok(ColumnarValue::Scalar(dimension_size(t.dimension()).into())),
        MultiPolygon(t) => Ok(ColumnarValue::Scalar(dimension_size(t.dimension()).into())),
        GeometryCollection(t) => Ok(ColumnarValue::Scalar(dimension_size(t.dimension()).into())),
        Rect(t) => Ok(ColumnarValue::Scalar(dimension_size(t.dimension()).into())),
        Geometry(_) => iter_geom!(as_geometry),
        Wkb(_) => iter_geom!(as_wkb, i32),
        LargeWkb(_) => iter_geom!(as_wkb, i64),
        WkbView(_) => iter_geom!(as_wkb_view),
        Wkt(_) => iter_geom!(as_wkt, i32),
        LargeWkt(_) => iter_geom!(as_wkt, i64),
        WktView(_) => iter_geom!(as_wkt_view),
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct NDims {
    signature: Signature,
}

impl NDims {
    pub fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input(),
        }
    }
}

impl Default for NDims {
    fn default() -> Self {
        Self::new()
    }
}

static NDIMS_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for NDims {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_ndims"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::UInt8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(coord_dim_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(NDIMS_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return the coordinate dimension of the geometry.",
                "ST_NDims(geometry)",
            )
            .with_argument("g1", "geometry")
            .build()
        }))
    }
}

#[cfg(test)]
mod test {
    use arrow_array::cast::AsArray;
    use arrow_array::types::UInt8Type;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::udf::native::io::GeomFromText;

    #[tokio::test]
    async fn test_coord_dim() {
        let ctx = SessionContext::new();

        ctx.register_udf(CoordDim::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_CoordDim(ST_GeomFromText('POINT(1 1)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let val = col.as_primitive::<UInt8Type>().value(0);
        assert_eq!(val, 2);
    }

    #[tokio::test]
    async fn test_ndims() {
        let ctx = SessionContext::new();

        ctx.register_udf(NDims::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_NDims(ST_GeomFromText('POINT(1 1)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let val = col.as_primitive::<UInt8Type>().value(0);
        assert_eq!(val, 2);
    }
}
