use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_array::builder::StringBuilder;
use arrow_array::cast::AsArray;
use arrow_schema::DataType;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use geo_traits::PointTrait;
use geoarrow::ArrayBase;
use geoarrow::array::{PointArray, PointBuilder, RectBuilder};
use geoarrow::trait_::{ArrayAccessor, NativeScalar};
use geoarrow_schema::{CoordType, Dimension};

use crate::data_types::{BOX2D_TYPE, POINT2D_TYPE};
use crate::error::GeoDataFusionResult;

#[derive(Debug)]
pub(super) struct Box2DFromGeoHash {
    signature: Signature,
}

impl Box2DFromGeoHash {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

static BOX_FROM_GEOHASH_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Box2DFromGeoHash {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_box2dfromgeohash"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(BOX2D_TYPE().into())
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        Ok(box_from_geohash_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(BOX_FROM_GEOHASH_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return a BOX2D from a GeoHash string.",
                "ST_Box2dFromGeoHash(geohash)",
            )
            .with_argument("text", "geohash")
            .build()
        }))
    }
}

fn box_from_geohash_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(args)?
        .into_iter()
        .next()
        .unwrap();

    let string_array = array.as_string::<i32>();
    let mut builder =
        RectBuilder::with_capacity_and_options(Dimension::XY, array.len(), Default::default());

    for s in string_array.iter() {
        builder.push_rect(s.map(geohash::decode_bbox).transpose()?.as_ref());
    }

    Ok(builder.finish().into_array_ref().into())
}

#[derive(Debug)]
pub(super) struct PointFromGeoHash {
    signature: Signature,
}

impl PointFromGeoHash {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

static POINT_FROM_GEOHASH_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for PointFromGeoHash {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_pointfromgeohash"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(POINT2D_TYPE().into())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        Ok(point_from_geohash_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(POINT_FROM_GEOHASH_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return a point from a GeoHash string. The point represents the center point of the GeoHash.",
                "ST_PointFromGeoHash(geohash)",
            )
            .with_argument("text", "geohash")
            .build()
        }))
    }
}

fn point_from_geohash_impl(args: &[ColumnarValue]) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(args)?
        .into_iter()
        .next()
        .unwrap();

    let string_array = array.as_string::<i32>();
    let mut builder = PointBuilder::with_capacity_and_options(
        Dimension::XY,
        array.len(),
        CoordType::Separated,
        Default::default(),
    );

    for s in string_array.iter() {
        if let Some(s) = s {
            let (coord, _, _) = geohash::decode(s)?;
            builder.push_coord(Some(&coord));
        } else {
            builder.push_null();
        }
    }

    Ok(builder.finish().into_array_ref().into())
}

#[derive(Debug)]
pub(super) struct GeoHash {
    signature: Signature,
}

impl GeoHash {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![POINT2D_TYPE().into()], Volatility::Immutable),
        }
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
        Ok(DataType::Utf8)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
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

fn geohash_impl(args: &[ColumnarValue]) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(args)?
        .into_iter()
        .next()
        .unwrap();
    let point_array = PointArray::try_from((array.as_ref(), Dimension::XY))?;

    let mut builder = StringBuilder::with_capacity(array.len(), 0);

    for point in point_array.iter() {
        if let Some(point) = point {
            let coord = point.coord().unwrap();
            // TODO: make arg
            // 12 is the max length supported by rust geohash. We should document this and maybe
            // clamp numbers to 12.
            let s = geohash::encode(coord.to_geo(), 12)?;
            builder.append_value(s);
        } else {
            builder.append_null();
        }
    }

    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

#[cfg(test)]
mod test {
    use approx::relative_eq;
    use arrow_array::cast::AsArray;
    use datafusion::prelude::*;
    use geo_traits::{CoordTrait, PointTrait, RectTrait};
    use geoarrow::array::{PointArray, RectArray};
    use geoarrow::trait_::ArrayAccessor;

    use crate::data_types::{BOX2D_TYPE, POINT2D_TYPE};
    use crate::udf::native::register_native;

    use super::*;

    #[tokio::test]
    async fn test_box2d_from_geohash() {
        let ctx = SessionContext::new();
        register_native(&ctx);

        let out = ctx
            .sql("SELECT ST_Box2dFromGeoHash('ww8p1r4t8');")
            .await
            .unwrap();

        let batches = out.collect().await.unwrap();
        assert_eq!(batches.len(), 1);

        let batch = batches.into_iter().next().unwrap();
        assert_eq!(batch.columns().len(), 1);

        assert!(
            batch
                .schema()
                .field(0)
                .data_type()
                .equals_datatype(&BOX2D_TYPE().into())
        );

        let rect_array = RectArray::try_from((batch.columns()[0].as_ref(), Dimension::XY)).unwrap();
        let rect = rect_array.value(0);

        assert!(relative_eq!(rect.min().x(), 112.55836486816406));
        assert!(relative_eq!(rect.min().y(), 37.83236503601074));
        assert!(relative_eq!(rect.max().x(), 112.5584077835083));
        assert!(relative_eq!(rect.max().y(), 37.83240795135498));
    }

    #[tokio::test]
    async fn test_point_from_geohash() {
        let ctx = SessionContext::new();
        register_native(&ctx);

        let out = ctx
            .sql("SELECT ST_PointFromGeoHash('9qqj');")
            .await
            .unwrap();

        let batches = out.collect().await.unwrap();
        assert_eq!(batches.len(), 1);

        let batch = batches.into_iter().next().unwrap();
        assert_eq!(batch.columns().len(), 1);

        assert!(
            batch
                .schema()
                .field(0)
                .data_type()
                .equals_datatype(&POINT2D_TYPE().into())
        );

        let point_array =
            PointArray::try_from((batch.columns()[0].as_ref(), Dimension::XY)).unwrap();
        let point = point_array.value(0);

        assert!(relative_eq!(point.coord().unwrap().x(), -115.13671875));
        assert!(relative_eq!(point.coord().unwrap().y(), 36.123046875));
    }

    #[tokio::test]
    async fn test_geohash() {
        let ctx = SessionContext::new();
        register_native(&ctx);

        let out = ctx
            .sql("SELECT ST_GeoHash( ST_Point(-126,48) );")
            .await
            .unwrap();

        let batches = out.collect().await.unwrap();
        assert_eq!(batches.len(), 1);

        let batch = batches.into_iter().next().unwrap();
        assert_eq!(batch.columns().len(), 1);

        let arr = batch.columns()[0].as_string::<i32>();
        assert_eq!(arr.value(0), "c0w3hf1s70w3");
    }
}
