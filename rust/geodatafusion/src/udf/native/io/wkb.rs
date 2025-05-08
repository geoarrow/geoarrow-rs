use std::any::Any;
use std::sync::OnceLock;

use arrow_schema::{DataType, Field};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::cast::to_wkb;
use geoarrow_array::{GeoArrowArray, GeoArrowType};
use geoarrow_schema::WkbType;

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionError;

#[derive(Debug)]
pub(super) struct AsBinary {
    signature: Signature,
}

impl AsBinary {
    pub fn new() -> Self {
        // TODO: extend to allow specifying little/big endian
        Self {
            signature: any_single_geometry_type_input(),
        }
    }
}

static AS_BINARY_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for AsBinary {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_asbinary"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Field> {
        let field = &args.arg_fields[0];
        let data_type = GeoArrowType::try_from(field).map_err(GeoDataFusionError::GeoArrow)?;
        let wkb_type = WkbType::new(data_type.metadata().clone());
        Ok(
            Field::new(field.name(), DataType::Binary, field.is_nullable())
                .with_extension_type(wkb_type),
        )
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let array = ColumnarValue::values_to_arrays(&args.args)?
            .into_iter()
            .next()
            .unwrap();
        let field = args.arg_fields[0];
        let geo_array = from_arrow_array(&array, field).map_err(GeoDataFusionError::GeoArrow)?;
        let wkb_arr = to_wkb::<i32>(geo_array.as_ref()).map_err(GeoDataFusionError::GeoArrow)?;
        Ok(ColumnarValue::Array(wkb_arr.into_array_ref()))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(AS_BINARY_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the OGC/ISO Well-Known Binary (WKB) representation of the geometry.",
                "ST_AsBinary(geometry)",
            )
            .with_argument("g1", "geometry")
            .build()
        }))
    }
}

// #[derive(Debug)]
// pub(super) struct GeomFromWKB {
//     signature: Signature,
// }

// impl GeomFromWKB {
//     pub fn new() -> Self {
//         Self {
//             signature: Signature::exact(vec![DataType::Binary], Volatility::Immutable),
//         }
//     }
// }

// static GEOM_FROM_WKB_DOC: OnceLock<Documentation> = OnceLock::new();

// impl ScalarUDFImpl for GeomFromWKB {
//     fn as_any(&self) -> &dyn Any {
//         self
//     }

//     fn name(&self) -> &str {
//         "st_geomfromwkb"
//     }

//     fn signature(&self) -> &Signature {
//         &self.signature
//     }

//     fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
//         Ok(GEOMETRY_TYPE().into())
//     }

//     fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
//         Ok(geom_from_wkb_impl(args)?)
//     }

//     fn documentation(&self) -> Option<&Documentation> {
//         Some(GEOM_FROM_WKB_DOC.get_or_init(|| {
//             Documentation::builder(DOC_SECTION_OTHER, "Takes a well-known binary representation of a geometry and a Spatial Reference System ID (SRID) and creates an instance of the appropriate geometry type", "ST_GeomFromWKB(buffer)")
//                 .with_argument("geom", "WKB buffers")
//                 .build()
//         }))
//     }
// }

// fn geom_from_wkb_impl(args: &[ColumnarValue]) -> GeoDataFusionResult<ColumnarValue> {
//     let array = ColumnarValue::values_to_arrays(args)?
//         .into_iter()
//         .next()
//         .unwrap();
//     let wkb_arr = WkbArray::new(array.as_binary::<i32>().clone(), Default::default());
//     let native_arr = from_wkb(
//         &wkb_arr,
//         GeoArrowType::Geometry(GeometryType::new(CoordType::Separated, Default::default())),
//         false,
//     )?;
//     Ok(native_arr.to_array_ref().into())
// }

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::RecordBatch;
    use arrow_schema::Schema;
    use datafusion::prelude::SessionContext;
    use geoarrow_array::test::point;
    use geoarrow_schema::{CoordType, Crs, Dimension, Metadata};

    use super::*;

    #[tokio::test]
    async fn test_as_binary() {
        let ctx = SessionContext::new();

        let crs = Crs::from_srid("4326".to_string());
        let metadata = Arc::new(Metadata::new(crs.clone(), Default::default()));

        let geo_arr = point::array(CoordType::Separated, Dimension::XY).with_metadata(metadata);

        let arr = geo_arr.to_array_ref();
        let field = geo_arr.data_type().to_field("geometry", true);
        let schema = Schema::new([Arc::new(field)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![arr]).unwrap();

        ctx.register_batch("t", batch).unwrap();

        ctx.register_udf(AsBinary::new().into());

        let sql_df = ctx
            .sql("SELECT ST_AsBinary(geometry) FROM t;")
            .await
            .unwrap();

        let output_batches = sql_df.collect().await.unwrap();
        assert_eq!(output_batches.len(), 1);
        let output_batch = &output_batches[0];

        let output_schema = output_batch.schema();
        let output_field = output_schema.field(0);
        let output_wkb_type = output_field.try_extension_type::<WkbType>().unwrap();

        assert_eq!(&crs, output_wkb_type.metadata().crs());
    }
}
