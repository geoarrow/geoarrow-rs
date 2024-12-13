use std::any::Any;
use std::sync::Arc;

use arrow::array::UnionBuilder;
use arrow::datatypes::{Float64Type, Int32Type};
use arrow_array::Array;
use arrow_schema::{DataType, Field, UnionFields, UnionMode};
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug)]
pub struct UnionExample {
    signature: Signature,
}

impl UnionExample {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            signature: Signature::nullary(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for UnionExample {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "example_union"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        let fields = UnionFields::new(
            vec![0, 1],
            vec![
                Arc::new(Field::new("a", DataType::Int32, false)),
                Arc::new(Field::new("b", DataType::Float64, false)),
            ],
        );
        Ok(DataType::Union(fields, UnionMode::Dense))
    }

    fn invoke_no_args(&self, _number_rows: usize) -> datafusion::error::Result<ColumnarValue> {
        let mut builder = UnionBuilder::new_dense();
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append::<Float64Type>("b", 3.0).unwrap();
        builder.append::<Int32Type>("a", 4).unwrap();
        let arr = builder.build().unwrap();

        assert_eq!(arr.type_id(0), 0);
        assert_eq!(arr.type_id(1), 1);
        assert_eq!(arr.type_id(2), 0);

        assert_eq!(arr.value_offset(0), 0);
        assert_eq!(arr.value_offset(1), 0);
        assert_eq!(arr.value_offset(2), 1);

        let arr = arr.slice(0, 1);

        assert!(matches!(
            arr.data_type(),
            DataType::Union(_, UnionMode::Dense)
        ));

        Ok(ColumnarValue::Array(Arc::new(arr)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use datafusion::prelude::*;

    #[tokio::test]
    async fn test() {
        let ctx = SessionContext::new();
        ctx.register_udf(UnionExample::new().into());

        let out = ctx.sql("SELECT example_union();").await.unwrap();
        // TODO: fix this error upstream
        // https://github.com/apache/datafusion/issues/13762
        out.show().await.unwrap_err();
    }
}
