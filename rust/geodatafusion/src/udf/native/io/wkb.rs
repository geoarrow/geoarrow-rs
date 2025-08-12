use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_schema::{DataType, Field};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::{LargeWkbArray, WkbArray, WkbViewArray, from_arrow_array};
use geoarrow_array::cast::{from_wkb, to_wkb};
use geoarrow_schema::{CoordType, GeoArrowType, GeometryType, Metadata, WkbType};

use crate::data_types::any_single_geometry_type_input;
use crate::error::{GeoDataFusionError, GeoDataFusionResult};

#[derive(Debug)]
pub struct AsBinary {
    signature: Signature,
}

impl AsBinary {
    pub fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input(),
        }
    }
}

impl Default for AsBinary {
    fn default() -> Self {
        Self::new()
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

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let input_field = &args.arg_fields[0];
        let metadata = Arc::new(Metadata::try_from(input_field.as_ref()).unwrap_or_default());
        let wkb_type = WkbType::new(metadata);
        Ok(Field::new(
            input_field.name(),
            DataType::Binary,
            input_field.is_nullable(),
        )
        .with_extension_type(wkb_type)
        .into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let array = &ColumnarValue::values_to_arrays(&args.args)?[0];
        let field = &args.arg_fields[0];
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

#[derive(Debug)]
pub struct GeomFromWKB {
    signature: Signature,
    coord_type: CoordType,
    aliases: Vec<String>,
}

impl GeomFromWKB {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![
                    DataType::Binary,
                    DataType::LargeBinary,
                    DataType::BinaryView,
                ],
                Volatility::Immutable,
            ),
            coord_type,
            aliases: vec!["st_wkbtosql".to_string()],
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
        let array = &ColumnarValue::values_to_arrays(&args.args)?[0];
        let field = &args.arg_fields[0];
        let to_type = GeoArrowType::from_arrow_field(args.return_field.as_ref())?;
        let geom_arr = match field.data_type() {
            DataType::Binary => from_wkb(
                &WkbArray::try_from((array.as_ref(), field.as_ref()))?,
                to_type,
            ),
            DataType::LargeBinary => from_wkb(
                &LargeWkbArray::try_from((array.as_ref(), field.as_ref()))?,
                to_type,
            ),
            DataType::BinaryView => from_wkb(
                &WkbViewArray::try_from((array.as_ref(), field.as_ref()))?,
                to_type,
            ),
            _ => unreachable!(),
        }?;
        Ok(ColumnarValue::Array(geom_arr.to_array_ref()))
    }
}

static GEOM_FROM_WKB_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for GeomFromWKB {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_geomfromwkb"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let input_field = &args.arg_fields[0];
        let metadata = Arc::new(Metadata::try_from(input_field.as_ref())?);
        let geom_type = GeometryType::new(metadata).with_coord_type(self.coord_type);
        Ok(geom_type
            .to_field(input_field.name(), input_field.is_nullable())
            .into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(self.invoke_with_args(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(GEOM_FROM_WKB_DOC.get_or_init(|| {
            Documentation::builder(DOC_SECTION_OTHER, "Takes a well-known binary representation of a geometry and a Spatial Reference System ID (SRID) and creates an instance of the appropriate geometry type", "ST_GeomFromWKB(buffer)")
                .with_argument("geom", "WKB buffers")
                .build()
        }))
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::RecordBatch;
    use arrow_schema::Schema;
    use datafusion::prelude::SessionContext;
    use geoarrow_array::array::GeometryArray;
    use geoarrow_array::test::point;
    use geoarrow_schema::{CoordType, Crs, Dimension, Metadata};

    use super::*;

    #[tokio::test]
    async fn test_as_binary() {
        let ctx = SessionContext::new();

        let crs = Crs::from_srid("4326".to_string());
        let metadata = Arc::new(Metadata::new(crs.clone(), Default::default()));

        let point_arr = point::array(CoordType::Separated, Dimension::XY).with_metadata(metadata);

        let arr = point_arr.to_array_ref();
        let field = point_arr.data_type().to_field("geometry", true);
        let schema = Schema::new([Arc::new(field)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![arr]).unwrap();

        ctx.register_batch("t", batch).unwrap();

        ctx.register_udf(AsBinary::new().into());
        ctx.register_udf(GeomFromWKB::new(CoordType::Separated).into());

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

        let sql_df2 = ctx
            .sql("SELECT ST_GeomFromWKB(ST_AsBinary(geometry)) FROM t;")
            .await
            .unwrap();

        let output_batches = sql_df2.collect().await.unwrap();
        assert_eq!(output_batches.len(), 1);
        let output_batch = &output_batches[0];
        let output_schema = output_batch.schema();
        let output_field = output_schema.field(0);
        let output_column = output_batch.column(0);
        let geom_arr = GeometryArray::try_from((output_column.as_ref(), output_field)).unwrap();

        assert_eq!(geom_arr, GeometryArray::from(point_arr));
    }
}
