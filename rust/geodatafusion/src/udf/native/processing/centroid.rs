use std::any::Any;
use std::sync::OnceLock;

use arrow_schema::DataType;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{ColumnarValue, Documentation, ScalarUDFImpl, Signature};
use geoarrow::algorithm::geo::Centroid as _Centroid;
use geoarrow::ArrayBase;
use geoarrow_schema::CoordType;

use crate::data_types::{any_single_geometry_type_input, parse_to_native_array, POINT2D_TYPE};
use crate::error::GeoDataFusionResult;

#[derive(Debug)]
pub(super) struct Centroid {
    signature: Signature,
}

impl Centroid {
    pub fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input(),
        }
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Centroid {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_centroid"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(POINT2D_TYPE().into())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        Ok(centroid_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Computes a point which is the geometric center of mass of a geometry.",
                "ST_Centroid(geometry)",
            )
            .with_argument("g1", "geometry")
            .build()
        }))
    }
}

fn centroid_impl(args: &[ColumnarValue]) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(args)?
        .into_iter()
        .next()
        .unwrap();
    let native_array = parse_to_native_array(array)?;
    let output = native_array.as_ref().centroid()?;
    Ok(output
        .into_coord_type(CoordType::Separated)
        .into_array_ref()
        .into())
}

#[cfg(test)]
mod test {
    use arrow_array::RecordBatch;
    use arrow_schema::Schema;
    use datafusion::error::Result;
    use datafusion::prelude::SessionContext;
    use geoarrow::algorithm::native::Cast;
    use geoarrow::datatypes::NativeType;
    use geoarrow::io::flatgeobuf::{FlatGeobufReaderBuilder, FlatGeobufReaderOptions};
    use geoarrow::table::Table;
    use geoarrow_schema::GeometryType;
    use std::fs::File;
    use std::sync::Arc;

    use super::*;

    fn load_file() -> RecordBatch {
        let file = File::open("../../fixtures/flatgeobuf/countries.fgb").unwrap();
        let reader_builder = FlatGeobufReaderBuilder::open(file).unwrap();
        let options = FlatGeobufReaderOptions {
            coord_type: CoordType::Separated,
            ..Default::default()
        };
        let reader = reader_builder.read(options).unwrap();
        let table =
            Table::try_from(Box::new(reader) as Box<dyn arrow_array::RecordBatchReader>).unwrap();

        let geometry = table.geometry_column(None).unwrap();
        let geometry = geometry
            .as_ref()
            .cast(NativeType::Geometry(GeometryType::new(
                CoordType::Separated,
                Default::default(),
            )))
            .unwrap();
        let field = geometry.extension_field();
        let chunk = geometry.array_refs()[0].clone();
        RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![chunk]).unwrap()
    }

    fn create_context() -> Result<SessionContext> {
        let ctx = SessionContext::new();

        let batch = load_file();

        ctx.register_batch("t", batch).unwrap();
        Ok(ctx)
    }

    #[ignore = "Union fields length must match child arrays length"]
    #[tokio::test]
    async fn test() -> Result<()> {
        let ctx = create_context()?;
        ctx.register_udf(Centroid::new().into());

        let sql_df = ctx.sql("SELECT ST_centroid(geometry) FROM t;").await?;
        // print the results
        sql_df.show().await?;

        Ok(())
    }
}
