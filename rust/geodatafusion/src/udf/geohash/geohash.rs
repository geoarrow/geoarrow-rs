use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_array::builder::StringViewBuilder;
use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use geo_traits::PointTrait;
use geo_traits::to_geo::ToGeoCoord;
use geoarrow_array::GeoArrowArrayAccessor;
use geoarrow_array::array::PointArray;
use geoarrow_schema::{CoordType, Dimension, PointType};

use crate::error::GeoDataFusionResult;

#[derive(Debug)]
pub struct GeoHash {
    signature: Signature,
}

impl GeoHash {
    pub fn new() -> Self {
        let valid_types = vec![
            PointType::new(Dimension::XY, Default::default())
                .with_coord_type(CoordType::Separated)
                .data_type(),
            PointType::new(Dimension::XY, Default::default())
                .with_coord_type(CoordType::Interleaved)
                .data_type(),
        ];
        Self {
            signature: Signature::uniform(1, valid_types, Volatility::Immutable),
        }
    }
}

impl Default for GeoHash {
    fn default() -> Self {
        Self::new()
    }
}

static GEOHASH_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for GeoHash {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_geohash"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(geohash_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(GEOHASH_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Computes a GeoHash representation of a geometry. A GeoHash encodes a geographic Point into a text form that is sortable and searchable based on prefixing. A shorter GeoHash is a less precise representation of a point. It can be thought of as a box that contains the point.",
                "ST_GeoHash(point)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

fn geohash_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(&args.args)?
        .into_iter()
        .next()
        .unwrap();
    let point_array = PointArray::try_from((array.as_ref(), args.arg_fields[0].as_ref()))?;
    let mut builder = StringViewBuilder::with_capacity(array.len());

    for point in point_array.iter() {
        if let Some(point) = point {
            let coord = point?.coord().unwrap();
            // TODO: make arg
            // 12 is the max length supported by rust geohash. We should document this and maybe
            // clamp numbers to 12.
            let s = geohash::encode(coord.to_coord(), 12)?;
            builder.append_value(s);
        } else {
            builder.append_null();
        }
    }

    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

#[cfg(test)]
mod test {
    use arrow_array::cast::AsArray;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::udf::native::constructors::Point;

    #[tokio::test]
    async fn test_geohash() {
        let ctx = SessionContext::new();
        ctx.register_udf(GeoHash::default().into());
        ctx.register_udf(Point::default().into());

        let df = ctx
            .sql("SELECT ST_GeoHash( ST_Point(-126,48) );")
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        let column = batches[0].column(0);
        let string_arr = column.as_string_view();

        assert_eq!(string_arr.value(0), "c0w3hf1s70w3");
    }
}
