//! Point constructors

use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, Field};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion::scalar::ScalarValue;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::{PointArray, SeparatedCoordBuffer};
use geoarrow_array::builder::PointBuilder;
use geoarrow_schema::{CoordType, Crs, Dimension, Metadata, PointType};

use crate::error::GeoDataFusionResult;

#[derive(Debug)]
pub struct Point {
    signature: Signature,
    coord_type: CoordType,
}

impl Point {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Float64, DataType::Float64]),
                    TypeSignature::Exact(vec![
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Int64,
                    ]),
                ],
                Volatility::Immutable,
            ),
            coord_type,
        }
    }
}

impl Default for Point {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static POINT_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Point {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_point"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let mut typ =
            PointType::new(Dimension::XY, Default::default()).with_coord_type(self.coord_type);

        if let Some(srid) = args.scalar_arguments.get(2) {
            if let Some(ScalarValue::Int64(srid_val)) = srid {
                let crs = Crs::from_srid(srid_val.unwrap().to_string());
                typ = typ.with_metadata(Arc::new(Metadata::new(crs, None)));
            } else {
                return Err(DataFusionError::Internal(
                    "ST_Point only supports SRID as a scalar integer".to_string(),
                ));
            }
        };

        Ok(typ.to_field("", true).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args[..2])?;
        let point_arr = create_point_array(arrays, &args.return_field)?;
        Ok(point_arr.into_array_ref().into())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(POINT_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a Point with the given X and Y coordinate values.",
                "ST_Point(-71.104, 42.315) or ST_Point(-71.104, 42.315, 4326)",
            )
            .with_argument("x", "x value")
            .with_argument("y", "y value")
            .with_argument("srid", "integer SRID value")
            .with_related_udf("st_makepoint")
            .with_related_udf("st_pointz")
            .build()
        }))
    }
}

#[derive(Debug)]
pub struct PointZ {
    signature: Signature,
    coord_type: CoordType,
}

impl PointZ {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Int64,
                    ]),
                ],
                Volatility::Immutable,
            ),
            coord_type,
        }
    }
}

impl Default for PointZ {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static POINT_Z_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for PointZ {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_pointz"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let mut typ =
            PointType::new(Dimension::XYZ, Default::default()).with_coord_type(self.coord_type);

        if let Some(srid) = args.scalar_arguments.get(3) {
            if let Some(ScalarValue::Int64(srid_val)) = srid {
                let crs = Crs::from_srid(srid_val.unwrap().to_string());
                typ = typ.with_metadata(Arc::new(Metadata::new(crs, None)));
            } else {
                return Err(DataFusionError::Internal(
                    "ST_Point only supports SRID as a scalar integer".to_string(),
                ));
            }
        };

        Ok(typ.to_field("", true).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args[..3])?;
        let point_arr = create_point_array(arrays, &args.return_field)?;
        Ok(point_arr.into_array_ref().into())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(POINT_Z_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns an Point with the given X, Y and Z coordinate values, and optionally an SRID number.",
                "ST_Point(-71.104, 42.315) or ST_Point(-71.104, 42.315, 4326)",
            )
            .with_argument("x", "x value")
            .with_argument("y", "y value")
            .with_argument("z", "z value")
            .with_argument("srid", "integer SRID value")
            .with_related_udf("st_makepoint")
            .with_related_udf("st_pointz")
            .build()
        }))
    }
}

#[derive(Debug)]
pub struct PointM {
    signature: Signature,
    coord_type: CoordType,
}

impl PointM {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Int64,
                    ]),
                ],
                Volatility::Immutable,
            ),
            coord_type,
        }
    }
}

impl Default for PointM {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static POINT_M_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for PointM {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_pointm"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let mut typ =
            PointType::new(Dimension::XYM, Default::default()).with_coord_type(self.coord_type);

        if let Some(srid) = args.scalar_arguments.get(3) {
            if let Some(ScalarValue::Int64(srid_val)) = srid {
                let crs = Crs::from_srid(srid_val.unwrap().to_string());
                typ = typ.with_metadata(Arc::new(Metadata::new(crs, None)));
            } else {
                return Err(DataFusionError::Internal(
                    "ST_Point only supports SRID as a scalar integer".to_string(),
                ));
            }
        };

        Ok(typ.to_field("", true).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args[..3])?;
        let point_arr = create_point_array(arrays, &args.return_field)?;
        Ok(point_arr.into_array_ref().into())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(POINT_M_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns an Point with the given X, Y and M coordinate values, and optionally an SRID number.",
                "ST_PointM(-71.104, 42.315, 3.4) or ST_PointM(-71.104, 42.315, 3.4, 4326)",
            )
            .with_argument("x", "x value")
            .with_argument("y", "y value")
            .with_argument("m", "m value")
            .with_argument("srid", "integer SRID value")
            .with_related_udf("st_makepoint")
            .with_related_udf("st_pointz")
            .build()
        }))
    }
}

#[derive(Debug)]
pub struct PointZM {
    signature: Signature,
    coord_type: CoordType,
}

impl PointZM {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Int64,
                    ]),
                ],
                Volatility::Immutable,
            ),
            coord_type,
        }
    }
}

impl Default for PointZM {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static POINT_ZM_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for PointZM {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_pointzm"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let mut typ =
            PointType::new(Dimension::XYZM, Default::default()).with_coord_type(self.coord_type);

        if let Some(srid) = args.scalar_arguments.get(4) {
            if let Some(ScalarValue::Int64(srid_val)) = srid {
                let crs = Crs::from_srid(srid_val.unwrap().to_string());
                typ = typ.with_metadata(Arc::new(Metadata::new(crs, None)));
            } else {
                return Err(DataFusionError::Internal(
                    "ST_Point only supports SRID as a scalar integer".to_string(),
                ));
            }
        };

        Ok(typ.to_field("", true).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args[..4])?;
        let point_arr = create_point_array(arrays, &args.return_field)?;
        Ok(point_arr.into_array_ref().into())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(POINT_ZM_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns an Point with the given X, Y, Z and M coordinate values, and optionally an SRID number.",
                "ST_Point(-71.104, 42.315) or ST_Point(-71.104, 42.315, 4326)",
            )
            .with_argument("x", "x value")
            .with_argument("y", "y value")
            .with_argument("z", "z value")
            .with_argument("m", "m value")
            .with_argument("srid", "integer SRID value")
            .with_related_udf("st_makepoint")
            .with_related_udf("st_pointz")
            .build()
        }))
    }
}
#[derive(Debug)]
pub struct MakePoint {
    signature: Signature,
    coord_type: CoordType,
}

impl MakePoint {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Float64, DataType::Float64]),
                    TypeSignature::Exact(vec![
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                    ]),
                ],
                Volatility::Immutable,
            ),
            coord_type,
        }
    }
}

impl Default for MakePoint {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static MAKE_POINT_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for MakePoint {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_makepoint"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let dim = match args.arg_fields.len() {
            2 => Dimension::XY,
            3 => Dimension::XYZ,
            4 => Dimension::XYZM,
            _ => unreachable!(),
        };

        let typ = PointType::new(dim, Default::default()).with_coord_type(self.coord_type);
        Ok(typ.to_field("", true).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let point_arr = create_point_array(arrays, &args.return_field)?;
        Ok(point_arr.into_array_ref().into())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(MAKE_POINT_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Creates a 2D XY or 3D XYZ or 4D XYZM Point geometry. Use ST_MakePointM to make points with XYM coordinates",
                "ST_MakePoint(-71.104, 42.315)",
            )
            .with_argument("x", "x value")
            .with_argument("y", "y value")
            .with_argument("z", "z value")
            .with_argument("m", "m value")
            .with_related_udf("st_point")
            .with_related_udf("st_pointz")
            .with_related_udf("ST_MakePointM")
            .build()
        }))
    }
}

#[derive(Debug)]
pub struct MakePointM {
    signature: Signature,
    coord_type: CoordType,
}

impl MakePointM {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Float64, DataType::Float64, DataType::Float64],
                Volatility::Immutable,
            ),
            coord_type,
        }
    }
}

impl Default for MakePointM {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static MAKE_POINT_M_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for MakePointM {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_makepointm"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let typ =
            PointType::new(Dimension::XYM, Default::default()).with_coord_type(self.coord_type);
        Ok(typ.to_field("", true).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let point_arr = create_point_array(arrays, &args.return_field)?;
        Ok(point_arr.into_array_ref().into())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(MAKE_POINT_M_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Creates a point with X, Y and M (measure) ordinates. Use ST_MakePoint to make points with XY, XYZ, or XYZM coordinates.",
                "ST_MakePointM(-71.104, 42.315, 10)",
            )
            .with_argument("x", "x value")
            .with_argument("y", "y value")
            .with_argument("m", "m value")
            .with_related_udf("st_point")
            .with_related_udf("st_pointz")
            .with_related_udf("ST_MakePoint")
            .build()
        }))
    }
}

fn create_point_array(
    arrays: Vec<ArrayRef>,
    return_field: &Field,
) -> GeoDataFusionResult<PointArray> {
    let x = arrays[0].as_primitive::<Float64Type>();
    let y = arrays[1].as_primitive::<Float64Type>();
    let z = arrays.get(2).map(|arr| arr.as_primitive::<Float64Type>());
    let m = arrays.get(3).map(|arr| arr.as_primitive::<Float64Type>());

    let typ = return_field.extension_type::<PointType>();
    let point_arr = match typ.coord_type() {
        CoordType::Interleaved => {
            let mut builder = PointBuilder::with_capacity(typ, x.len());

            match (z, m) {
                (None, None) => {
                    for (x, y) in x.iter().zip(y.iter()) {
                        if let (Some(x), Some(y)) = (x, y) {
                            let coord = wkt::types::Coord {
                                x,
                                y,
                                z: None,
                                m: None,
                            };
                            builder.push_coord(Some(&coord));
                        } else {
                            builder.push_null();
                        }
                    }
                }
                (Some(z), None) => {
                    for ((x, y), z) in x.iter().zip(y.iter()).zip(z.iter()) {
                        if let (Some(x), Some(y), Some(z)) = (x, y, z) {
                            let coord = wkt::types::Coord {
                                x,
                                y,
                                z: Some(z),
                                m: None,
                            };
                            builder.push_coord(Some(&coord));
                        } else {
                            builder.push_null();
                        }
                    }
                }
                (None, Some(m)) => {
                    for ((x, y), m) in x.iter().zip(y.iter()).zip(m.iter()) {
                        if let (Some(x), Some(y), Some(m)) = (x, y, m) {
                            let coord = wkt::types::Coord {
                                x,
                                y,
                                z: None,
                                m: Some(m),
                            };
                            builder.push_coord(Some(&coord));
                        } else {
                            builder.push_null();
                        }
                    }
                }
                (Some(z), Some(m)) => {
                    for (((x, y), z), m) in x.iter().zip(y.iter()).zip(z.iter()).zip(m.iter()) {
                        if let (Some(x), Some(y), Some(z), Some(m)) = (x, y, z, m) {
                            let coord = wkt::types::Coord {
                                x,
                                y,
                                z: Some(z),
                                m: Some(m),
                            };
                            builder.push_coord(Some(&coord));
                        } else {
                            builder.push_null();
                        }
                    }
                }
            }

            builder.finish()
        }
        CoordType::Separated => {
            let (_, dim, metadata) = typ.into_inner();
            let mut coord_buffers = vec![x.values().clone(), y.values().clone()];
            if let Some(z) = z {
                coord_buffers.push(z.values().clone());
            }
            if let Some(m) = m {
                coord_buffers.push(m.values().clone());
            }

            let coords = SeparatedCoordBuffer::from_vec(coord_buffers, dim)?;
            PointArray::new(coords.into(), x.nulls().cloned(), metadata)
        }
    };

    Ok(point_arr)
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use approx::relative_eq;
    use arrow_array::{RecordBatch, create_array};
    use arrow_schema::Schema;
    use datafusion::prelude::SessionContext;
    use geo_traits::{CoordTrait, PointTrait};
    use geoarrow_array::GeoArrowArrayAccessor;

    use super::*;

    #[tokio::test]
    async fn test_st_point() {
        let ctx = SessionContext::new();

        ctx.register_udf(Point::new(CoordType::Separated).into());

        let sql_df = ctx
            .sql(r#"SELECT ST_Point(-71.104, 42.315);"#)
            .await
            .unwrap();

        let output_batches = sql_df.collect().await.unwrap();
        assert_eq!(output_batches.len(), 1);
        let output_batch = &output_batches[0];
        let output_schema = output_batch.schema();
        let output_field = output_schema.field(0);

        let output_column = output_batch.column(0);
        let point_arr = PointArray::try_from((output_column.as_ref(), output_field)).unwrap();

        assert_eq!(point_arr.len(), 1);
        let (x, y) = point_arr.value(0).unwrap().coord().unwrap().x_y();

        assert!(relative_eq!(x, -71.104));
        assert!(relative_eq!(y, 42.315));
    }

    #[tokio::test]
    async fn test_st_point_from_table() {
        let ctx = SessionContext::new();

        ctx.register_udf(Point::new(CoordType::Separated).into());

        let x = create_array!(Float64, [-71.104]);
        let y = create_array!(Float64, [42.315]);

        let schema = Schema::new([
            Arc::new(Field::new("x", x.data_type().clone(), true)),
            Arc::new(Field::new("y", y.data_type().clone(), true)),
        ]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![x, y]).unwrap();

        ctx.register_batch("t", batch).unwrap();

        let sql_df = ctx.sql(r#"SELECT ST_Point(x, y) from t;"#).await.unwrap();

        let output_batches = sql_df.collect().await.unwrap();
        assert_eq!(output_batches.len(), 1);
        let output_batch = &output_batches[0];
        let output_schema = output_batch.schema();
        let output_field = output_schema.field(0);

        // This succeeds
        assert_eq!(output_field.extension_type_name(), Some("geoarrow.point"));

        let output_column = output_batch.column(0);
        let point_arr = PointArray::try_from((output_column.as_ref(), output_field)).unwrap();

        assert_eq!(point_arr.len(), 1);
        let (x, y) = point_arr.value(0).unwrap().coord().unwrap().x_y();

        assert!(relative_eq!(x, -71.104));
        assert!(relative_eq!(y, 42.315));
    }

    #[tokio::test]
    async fn test_st_point_srid() {
        let ctx = SessionContext::new();

        ctx.register_udf(Point::new(CoordType::Separated).into());

        let x = create_array!(Float64, [-71.104]);
        let y = create_array!(Float64, [42.315]);

        let schema = Schema::new([
            Arc::new(Field::new("x", x.data_type().clone(), true)),
            Arc::new(Field::new("y", y.data_type().clone(), true)),
        ]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![x, y]).unwrap();

        ctx.register_batch("t", batch).unwrap();

        let sql_df = ctx
            .sql(r#"SELECT ST_Point(x, y, 4326) as geometry from t;"#)
            .await
            .unwrap();

        let output_batches = sql_df.collect().await.unwrap();
        assert_eq!(output_batches.len(), 1);
        let output_batch = &output_batches[0];
        let output_schema = output_batch.schema();
        let output_field = output_schema.field(0);
        let point_type = output_field.extension_type::<PointType>();
        assert_eq!(
            point_type.metadata().crs(),
            &Crs::from_srid("4326".to_string())
        );
    }
}
