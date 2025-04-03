use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_schema::DataType;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{ColumnarValue, Documentation, ScalarUDFImpl, Signature};
use geoarrow::algorithm::geo::Area as _Area;

use crate::data_types::{any_single_geometry_type_input, parse_to_native_array};
use crate::error::GeoDataFusionResult;

#[derive(Debug)]
pub(super) struct Area {
    signature: Signature,
}

impl Area {
    pub fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input(),
        }
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Area {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_area"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        Ok(area_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the area of a polygonal geometry.",
                "ST_Area(geom)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

fn area_impl(args: &[ColumnarValue]) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(args)?
        .into_iter()
        .next()
        .unwrap();
    let native_array = parse_to_native_array(array)?;
    let area = native_array.as_ref().unsigned_area()?;
    Ok(ColumnarValue::Array(Arc::new(area)))
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
    use geoarrow_schema::{CoordType, GeometryType};
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
        ctx.register_udf(Area::new().into());

        let sql_df = ctx.sql("SELECT ST_Area(geometry) FROM t;").await?;
        // print the results
        sql_df.show().await?;

        Ok(())
    }
}
