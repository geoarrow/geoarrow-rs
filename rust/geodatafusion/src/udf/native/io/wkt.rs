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
use geoarrow_array::array::{LargeWktArray, WktArray, WktViewArray, from_arrow_array};
use geoarrow_array::cast::{from_wkt, to_wkt};
use geoarrow_schema::{CoordType, GeoArrowType, GeometryType, Metadata, WktType};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct AsText {
    signature: Signature,
}

impl AsText {
    pub fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input(),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
        let array = &ColumnarValue::values_to_arrays(&args.args)?[0];
        let field = &args.arg_fields[0];
        let geo_array = from_arrow_array(&array, field.as_ref())?;
        let wkt_arr = to_wkt::<i32>(geo_array.as_ref())?;
        Ok(ColumnarValue::Array(wkt_arr.into_array_ref()))
    }
}

impl Default for AsText {
    fn default() -> Self {
        Self::new()
    }
}

static AS_TEXT_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for AsText {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_astext"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let input_field = &args.arg_fields[0];
        let metadata = Arc::new(Metadata::try_from(input_field.as_ref())?);
        let wkb_type = WktType::new(metadata);
        Ok(Field::new(
            input_field.name(),
            DataType::Utf8,
            input_field.is_nullable(),
        )
        .with_extension_type(wkb_type)
        .into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(self.invoke_with_args(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(AS_TEXT_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the OGC Well-Known Text (WKT) representation of the geometry/geography.",
                "ST_AsText(geometry)",
            )
            .with_argument("g1", "geometry")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct GeomFromText {
    signature: Signature,
    coord_type: CoordType,
    aliases: Vec<String>,
}

impl GeomFromText {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View],
                Volatility::Immutable,
            ),
            coord_type,
            aliases: vec!["st_geometryfromtext".to_string(), "st_wkttosql".to_string()],
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
        let array = &ColumnarValue::values_to_arrays(&args.args)?[0];
        let field = &args.arg_fields[0];
        let to_type = GeoArrowType::from_arrow_field(args.return_field.as_ref())?;
        let geom_arr = match field.data_type() {
            DataType::Utf8 => from_wkt(
                &WktArray::try_from((array.as_ref(), field.as_ref()))?,
                to_type,
            ),
            DataType::LargeUtf8 => from_wkt(
                &LargeWktArray::try_from((array.as_ref(), field.as_ref()))?,
                to_type,
            ),
            DataType::Utf8View => from_wkt(
                &WktViewArray::try_from((array.as_ref(), field.as_ref()))?,
                to_type,
            ),
            _ => unreachable!(),
        }?;

        Ok(ColumnarValue::Array(geom_arr.to_array_ref()))
    }
}

impl Default for GeomFromText {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static GEOM_FROM_TEXT_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for GeomFromText {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_geomfromtext"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
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
        Some(GEOM_FROM_TEXT_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Constructs a geometry object from the OGC Well-Known text representation.",
                "ST_GeomFromText(text)",
            )
            .with_argument("g1", "geometry")
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
    use geoarrow_array::test::point;
    use geoarrow_schema::crs::Crs;
    use geoarrow_schema::{CoordType, Dimension, Metadata};

    use super::*;

    #[tokio::test]
    async fn test_as_text() {
        let ctx = SessionContext::new();

        let crs = Crs::from_srid("4326".to_string());
        let metadata = Arc::new(Metadata::new(crs.clone(), Default::default()));

        let geo_arr = point::array(CoordType::Separated, Dimension::XY).with_metadata(metadata);

        let arr = geo_arr.to_array_ref();
        let field = geo_arr.data_type().to_field("geometry", true);
        let schema = Schema::new([Arc::new(field)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![arr]).unwrap();

        ctx.register_batch("t", batch).unwrap();

        ctx.register_udf(AsText::new().into());

        let sql_df = ctx.sql("SELECT ST_AsText(geometry) FROM t;").await.unwrap();

        let output_batches = sql_df.collect().await.unwrap();
        assert_eq!(output_batches.len(), 1);
        let output_batch = &output_batches[0];

        let output_schema = output_batch.schema();
        let output_field = output_schema.field(0);
        let output_wkb_type = output_field.try_extension_type::<WktType>().unwrap();

        assert_eq!(&crs, output_wkb_type.metadata().crs());
    }

    #[tokio::test]
    async fn test_from_text() {
        let ctx = SessionContext::new();

        ctx.register_udf(GeomFromText::new(CoordType::Separated).into());

        let sql_df = ctx
            .sql(r#"SELECT ST_GeomFromText('POINT(30 10)');"#)
            .await
            .unwrap();

        sql_df.show().await.unwrap();
    }
}
