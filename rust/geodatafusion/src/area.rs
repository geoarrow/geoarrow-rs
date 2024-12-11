use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::DataType;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility};
use datafusion::prelude::create_udf;
use geoarrow::algorithm::geo::Area;
use geoarrow::array::{CoordType, GeometryArray};
use geoarrow::datatypes::NativeType;

pub fn area_udf() -> ScalarUDF {
    let area = Arc::new(|args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let arg = args.into_iter().next().unwrap();
        let geom_arr = GeometryArray::try_from(arg.as_ref()).unwrap();
        let output = geom_arr.unsigned_area();
        Ok(ColumnarValue::from(Arc::new(output) as ArrayRef))
    });

    create_udf(
        "area",
        vec![NativeType::Geometry(CoordType::Separated).to_data_type()],
        DataType::Float64.into(),
        Volatility::Immutable,
        area,
    )
}

#[cfg(test)]
mod test {
    use arrow_array::RecordBatch;
    use arrow_schema::Schema;
    use datafusion::error::Result;
    use datafusion::prelude::SessionContext;
    use geoarrow::algorithm::native::Cast;
    use geoarrow::array::CoordType;
    use geoarrow::datatypes::NativeType;
    use geoarrow::io::flatgeobuf::read_flatgeobuf;
    use std::fs::File;
    use std::sync::Arc;

    use crate::area::area_udf;

    fn load_file() -> RecordBatch {
        let mut file =
            File::open("/Users/kyle/github/geoarrow/geoarrow-rs/fixtures/flatgeobuf/countries.fgb")
                .unwrap();
        let table = read_flatgeobuf(&mut file, Default::default()).unwrap();
        let geometry = table.geometry_column(None).unwrap();
        let geometry = geometry
            .as_ref()
            .cast(NativeType::Geometry(CoordType::Separated))
            .unwrap();
        let field = geometry.extension_field();
        let chunk = geometry.array_refs()[0].clone();
        RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![chunk]).unwrap()
    }

    fn create_context() -> Result<SessionContext> {
        let ctx = SessionContext::new();

        let batch = load_file();

        ctx.register_batch("table", batch).unwrap();
        Ok(ctx)
    }

    #[tokio::test]
    async fn test() -> Result<()> {
        let ctx = create_context()?;
        ctx.register_udf(area_udf());

        let sql_df = ctx.sql("SELECT area(geometry) FROM table;").await?;
        // print the results
        sql_df.show().await?;

        Ok(())
    }
}
