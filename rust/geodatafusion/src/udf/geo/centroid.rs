use std::sync::Arc;

use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility};
use datafusion::prelude::create_udf;
use geoarrow::algorithm::geo::Centroid;
use geoarrow::array::{CoordType, GeometryArray};
use geoarrow::datatypes::NativeType;
use geoarrow::ArrayBase;

use crate::error::GeoDataFusionResult;

/// ST_Centroid
///
/// - Returns the geometric center of a geometry.
pub fn centroid() -> ScalarUDF {
    create_udf(
        "st_centroid",
        vec![NativeType::Geometry(CoordType::Separated).to_data_type()],
        NativeType::Geometry(CoordType::Separated)
            .to_data_type()
            .into(),
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| Ok(_centroid(args)?)),
    )
}

fn _centroid(args: &[ColumnarValue]) -> GeoDataFusionResult<ColumnarValue> {
    let args = ColumnarValue::values_to_arrays(args)?;
    let arg = args.into_iter().next().unwrap();
    let geom_arr = GeometryArray::try_from(arg.as_ref()).unwrap();

    let point_array = geom_arr.centroid().into_coord_type(CoordType::Separated);
    Ok(GeometryArray::from(point_array).into_array_ref().into())
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

    pub use super::*;

    fn load_file() -> RecordBatch {
        let mut file = File::open("../../fixtures/flatgeobuf/countries.fgb").unwrap();
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

        ctx.register_batch("t", batch).unwrap();
        Ok(ctx)
    }

    #[tokio::test]
    async fn test() -> Result<()> {
        let ctx = create_context()?;
        ctx.register_udf(centroid());

        let sql_df = ctx.sql("SELECT ST_centroid(geometry) FROM t;").await?;
        // print the results
        sql_df.show().await?;

        Ok(())
    }
}
