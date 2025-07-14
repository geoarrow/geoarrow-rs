use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_schema::{DataType, Field};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::from_arrow_array;
use geoarrow_geo::centroid;
use geoarrow_schema::{CoordType, Dimension, GeoArrowType, PointType};

use crate::data_types::any_single_geometry_type_input;
use crate::error::{GeoDataFusionError, GeoDataFusionResult};

#[derive(Debug)]
pub struct Centroid {
    signature: Signature,
    coord_type: CoordType,
}

impl Centroid {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: any_single_geometry_type_input(),
            coord_type,
        }
    }
}

impl Default for Centroid {
    fn default() -> Self {
        Self::new(Default::default())
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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let input_field = &args.arg_fields[0];
        let data_type =
            GeoArrowType::try_from(input_field.as_ref()).map_err(GeoDataFusionError::from)?;
        let point_type = PointType::new(Dimension::XY, data_type.metadata().clone())
            .with_coord_type(self.coord_type);
        let field = point_type.to_field(input_field.name(), input_field.is_nullable());
        Ok(Arc::new(field))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(centroid_impl(args, self.coord_type)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Computes a point which is the geometric center of mass of a geometry.",
                "ST_Centroid(geom)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

fn centroid_impl(
    args: ScalarFunctionArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(&args.args)?
        .into_iter()
        .next()
        .unwrap();
    let field = &args.arg_fields[0];
    let geo_array = from_arrow_array(&array, field)?;
    let result = centroid(&geo_array, coord_type)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

#[cfg(test)]
mod test {
    use approx::relative_eq;
    use datafusion::prelude::SessionContext;
    use geo_traits::{CoordTrait, PointTrait};
    use geoarrow_array::GeoArrowArrayAccessor;
    use geoarrow_array::array::PointArray;

    use super::*;
    use crate::udf::native::io::GeomFromText;

    #[tokio::test]
    async fn test_centroid() {
        let ctx = SessionContext::new();

        ctx.register_udf(Centroid::default().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql(
                "select ST_Centroid(ST_GeomFromText('MULTIPOINT ( -1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6 )'));",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let geo_arr =
            PointArray::try_from((batch.column(0).as_ref(), batch.schema().field(0))).unwrap();
        let point = geo_arr.value(0).unwrap();
        assert!(relative_eq!(point.coord().unwrap().x(), 2.3076923076923075));
        assert!(relative_eq!(point.coord().unwrap().y(), 3.3076923076923075));
    }
}
