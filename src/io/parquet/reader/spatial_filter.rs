use arrow::array::AsArray;
use arrow::compute::kernels::cmp::{gt_eq, lt_eq};
use arrow::datatypes::Float64Type;
use arrow_array::{Float64Array, Scalar};
use parquet::arrow::arrow_reader::{
    ArrowPredicate, ArrowPredicateFn, ArrowReaderBuilder, RowFilter,
};
use parquet::arrow::ProjectionMask;
use parquet::schema::types::{ColumnPath, SchemaDescriptor};

use crate::error::Result;

#[derive(Debug, Clone)]
pub struct ParquetBboxQuery {
    pub bbox: [f64; 4],
    pub minx_col_path: Vec<String>,
    pub miny_col_path: Vec<String>,
    pub maxx_col_path: Vec<String>,
    pub maxy_col_path: Vec<String>,
}

pub(crate) fn apply_spatial_filter<T>(
    builder: ArrowReaderBuilder<T>,
    query: ParquetBboxQuery,
) -> Result<ArrowReaderBuilder<T>> {
    let parquet_schema = builder.parquet_schema();
    let predicate = construct_predicate(parquet_schema, query)?;
    let filter = RowFilter::new(vec![predicate]);
    Ok(builder.with_row_filter(filter))
}

pub(crate) fn construct_predicate(
    parquet_schema: &SchemaDescriptor,
    query: ParquetBboxQuery,
) -> Result<Box<dyn ArrowPredicate>> {
    let column_idxs = get_bbox_columns(
        parquet_schema,
        query.minx_col_path.as_ref(),
        query.miny_col_path.as_ref(),
        query.maxx_col_path.as_ref(),
        query.maxy_col_path.as_ref(),
    )?;

    let mask = ProjectionMask::leaves(parquet_schema, column_idxs);
    let predicate = ArrowPredicateFn::new(mask, move |batch| {
        let struct_col = batch.column(0).as_struct();
        let minx_col = struct_col.column(0).as_primitive::<Float64Type>();
        let miny_col = struct_col.column(1).as_primitive::<Float64Type>();
        let maxx_col = struct_col.column(2).as_primitive::<Float64Type>();
        let maxy_col = struct_col.column(3).as_primitive::<Float64Type>();

        let minx_scalar = Scalar::new(Float64Array::from(vec![query.bbox[0]]));
        let miny_scalar = Scalar::new(Float64Array::from(vec![query.bbox[1]]));
        let maxx_scalar = Scalar::new(Float64Array::from(vec![query.bbox[2]]));
        let maxy_scalar = Scalar::new(Float64Array::from(vec![query.bbox[3]]));

        let minx_cmp = gt_eq(minx_col, &minx_scalar).unwrap();
        let miny_cmp = gt_eq(miny_col, &miny_scalar).unwrap();
        let maxx_cmp = lt_eq(maxx_col, &maxx_scalar).unwrap();
        let maxy_cmp = lt_eq(maxy_col, &maxy_scalar).unwrap();

        let first = arrow::compute::and(&minx_cmp, &miny_cmp).unwrap();
        let second = arrow::compute::and(&first, &maxx_cmp).unwrap();
        let third = arrow::compute::and(&second, &maxy_cmp).unwrap();

        Ok(third)
    });

    Ok(Box::new(predicate))
}

/// Loops through the columns in the SchemaDescriptor, looking at each's path
pub(crate) fn get_bbox_columns<S: AsRef<str>>(
    parquet_schema: &SchemaDescriptor,
    minx_col_path: &[S],
    miny_col_path: &[S],
    maxx_col_path: &[S],
    maxy_col_path: &[S],
) -> Result<[usize; 4]> {
    let mut indexes = [None; 4];
    for (column_idx, column_meta) in parquet_schema.columns().iter().enumerate() {
        // If all column paths have been found, break from loop
        if indexes.iter().all(|x| x.is_some()) {
            break;
        }

        if indexes[0].is_none() && path_equals(minx_col_path, column_meta.path()) {
            indexes[0] = Some(column_idx);
            continue;
        }

        if indexes[1].is_none() && path_equals(miny_col_path, column_meta.path()) {
            indexes[1] = Some(column_idx);
            continue;
        }

        if indexes[2].is_none() && path_equals(maxx_col_path, column_meta.path()) {
            indexes[2] = Some(column_idx);
            continue;
        }

        if indexes[3].is_none() && path_equals(maxy_col_path, column_meta.path()) {
            indexes[3] = Some(column_idx);
            continue;
        }
    }

    Ok(indexes.map(|x| x.unwrap()))
}

fn path_equals<S: AsRef<str>>(a: &[S], b: &ColumnPath) -> bool {
    if a.len() != b.parts().len() {
        return false;
    }

    for (left, right) in a.iter().zip(b.parts()) {
        if left.as_ref() != right.as_str() {
            return false;
        }
    }

    true
}
