use std::fmt::Debug;

use arrow::array::AsArray;
use arrow::compute::kernels::cmp::{gt_eq, lt_eq};
use arrow::datatypes::Float64Type;
use arrow_array::{Float64Array, Scalar};
use geo::{coord, CoordNum, Rect};
use parquet::arrow::arrow_reader::{
    ArrowPredicate, ArrowPredicateFn, ArrowReaderBuilder, RowFilter,
};
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::{ColumnChunkMetaData, RowGroupMetaData};
use parquet::file::statistics::Statistics;
use parquet::schema::types::{ColumnPath, SchemaDescriptor};

use crate::array::{RectArray, RectBuilder};
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::{CoordTrait, RectTrait};
use crate::trait_::GeometryArrayAccessor;

/// A helper for interpreting bounding box row group statistics from GeoParquet files
///
/// This is intended to be user facing
pub struct ParquetBboxPaths {
    /// The path in the Parquet schema of the column that contains the xmin
    pub minx_path: Vec<String>,

    /// The path in the Parquet schema of the column that contains the ymin
    pub miny_path: Vec<String>,

    /// The path in the Parquet schema of the column that contains the xmin
    pub maxx_path: Vec<String>,

    /// The path in the Parquet schema of the column that contains the ymax
    pub maxy_path: Vec<String>,
}

/// A helper for interpreting bounding box row group statistics from GeoParquet files
///
/// This is **not** intended to be user facing. It's an internal struct that needs access to the
/// SchemaDescriptor to create.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ParquetBboxStatistics {
    /// The index of the Parquet column that contains the xmin
    minx_col: usize,

    /// The index of the Parquet column that contains the ymin
    miny_col: usize,

    /// The index of the Parquet column that contains the xmin
    maxx_col: usize,

    /// The index of the Parquet column that contains the ymax
    maxy_col: usize,
}

impl ParquetBboxStatistics {
    /// Loops through the columns in the SchemaDescriptor, looking at each's path
    pub fn try_new(parquet_schema: &SchemaDescriptor, paths: &ParquetBboxPaths) -> Result<Self> {
        let mut minx_col: Option<usize> = None;
        let mut miny_col: Option<usize> = None;
        let mut maxx_col: Option<usize> = None;
        let mut maxy_col: Option<usize> = None;

        for (column_idx, column_meta) in parquet_schema.columns().iter().enumerate() {
            // If all column paths have been found, break from loop
            if minx_col.is_some() && miny_col.is_some() && maxx_col.is_some() && maxy_col.is_some()
            {
                break;
            }

            if minx_col.is_none() && path_equals(paths.minx_path.as_ref(), column_meta.path()) {
                minx_col = Some(column_idx);
                continue;
            }

            if miny_col.is_none() && path_equals(paths.miny_path.as_ref(), column_meta.path()) {
                miny_col = Some(column_idx);
                continue;
            }

            if maxx_col.is_none() && path_equals(paths.maxx_path.as_ref(), column_meta.path()) {
                maxx_col = Some(column_idx);
                continue;
            }

            if maxy_col.is_none() && path_equals(paths.maxy_path.as_ref(), column_meta.path()) {
                maxy_col = Some(column_idx);
                continue;
            }
        }

        if minx_col.is_none() {
            return Err(GeoArrowError::General(format!(
                "Unable to find xmin_path: {:?}",
                minx_col
            )));
        }

        if miny_col.is_none() {
            return Err(GeoArrowError::General(format!(
                "Unable to find ymin_path: {:?}",
                miny_col
            )));
        }

        if maxx_col.is_none() {
            return Err(GeoArrowError::General(format!(
                "Unable to find xmax_path: {:?}",
                maxx_col
            )));
        }

        if maxy_col.is_none() {
            return Err(GeoArrowError::General(format!(
                "Unable to find ymax_path: {:?}",
                maxy_col
            )));
        }

        Ok(Self {
            minx_col: minx_col.unwrap(),
            miny_col: miny_col.unwrap(),
            maxx_col: maxx_col.unwrap(),
            maxy_col: maxy_col.unwrap(),
        })
    }

    /// Extract the bounding box from a given row group's metadata.
    ///
    /// This uses the column statistics contained in the row group metadata.
    pub fn get_bbox(&self, rg_meta: &RowGroupMetaData) -> Result<Rect> {
        let (minx, _) = parse_statistics_f64(rg_meta.column(self.minx_col))?;
        let (miny, _) = parse_statistics_f64(rg_meta.column(self.miny_col))?;
        let (_, maxx) = parse_statistics_f64(rg_meta.column(self.maxx_col))?;
        let (_, maxy) = parse_statistics_f64(rg_meta.column(self.maxy_col))?;
        Ok(Rect::new(
            coord! { x: minx, y: miny },
            coord! {x: maxx, y: maxy},
        ))
    }

    /// Extract the bounding boxes for a sequence of row groups
    pub fn get_bboxes(&self, row_groups: &[RowGroupMetaData]) -> Result<RectArray> {
        let mut builder = RectBuilder::with_capacity(row_groups.len(), Default::default());
        for rg_meta in row_groups.iter() {
            builder.push_rect(Some(&self.get_bbox(rg_meta)?));
        }
        Ok(builder.finish())
    }
}

pub(crate) fn apply_bbox_row_groups<T>(
    builder: ArrowReaderBuilder<T>,
    bbox_cols: ParquetBboxStatistics,
    bbox_query: Rect,
) -> Result<ArrowReaderBuilder<T>> {
    let row_groups = builder.metadata().row_groups();
    let row_groups_bounds = bbox_cols.get_bboxes(row_groups)?;
    let mut intersects_row_groups_idxs = vec![];
    for (row_group_idx, row_group_bounds) in row_groups_bounds.iter_values().enumerate() {
        if rect_intersects(&row_group_bounds, &bbox_query) {
            intersects_row_groups_idxs.push(row_group_idx);
        }
    }

    Ok(builder.with_row_groups(intersects_row_groups_idxs))
}

pub(crate) fn apply_bbox_row_filter<T>(
    builder: ArrowReaderBuilder<T>,
    bbox_cols: ParquetBboxStatistics,
    bbox_query: Rect,
) -> Result<ArrowReaderBuilder<T>> {
    let parquet_schema = builder.parquet_schema();
    let predicate = construct_predicate(parquet_schema, bbox_cols, bbox_query)?;
    let filter = RowFilter::new(vec![predicate]);
    Ok(builder.with_row_filter(filter))
}

pub(crate) fn construct_predicate(
    parquet_schema: &SchemaDescriptor,
    bbox_cols: ParquetBboxStatistics,
    bbox_query: Rect,
) -> Result<Box<dyn ArrowPredicate>> {
    let mask = ProjectionMask::leaves(
        parquet_schema,
        [
            bbox_cols.minx_col,
            bbox_cols.miny_col,
            bbox_cols.maxx_col,
            bbox_cols.maxy_col,
        ],
    );

    let predicate = ArrowPredicateFn::new(mask, move |batch| {
        let struct_col = batch.column(0).as_struct();

        ///////////////////////////////////////////////
        // TODO: come back to this
        ///////////////////////////////////////////////

        let minx_col = struct_col.column(0).as_primitive::<Float64Type>();
        let maxx_col = struct_col.column(1).as_primitive::<Float64Type>();
        let miny_col = struct_col.column(2).as_primitive::<Float64Type>();
        let maxy_col = struct_col.column(3).as_primitive::<Float64Type>();

        let minx_scalar = Scalar::new(Float64Array::from(vec![bbox_query.lower().x()]));
        let miny_scalar = Scalar::new(Float64Array::from(vec![bbox_query.lower().y()]));
        let maxx_scalar = Scalar::new(Float64Array::from(vec![bbox_query.upper().x()]));
        let maxy_scalar = Scalar::new(Float64Array::from(vec![bbox_query.upper().y()]));

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

fn path_equals<T: AsRef<str>>(a: &[T], b: &ColumnPath) -> bool {
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

fn parse_statistics_f64(column_meta: &ColumnChunkMetaData) -> Result<(f64, f64)> {
    let stats = column_meta
        .statistics()
        .ok_or(GeoArrowError::General(format!(
            "No statistics for column {}",
            column_meta.column_path()
        )))?;
    match stats {
        Statistics::Double(ref typed_stats) => Ok((*typed_stats.min(), *typed_stats.max())),
        Statistics::Float(ref typed_stats) => {
            Ok((*typed_stats.min() as f64, *typed_stats.max() as f64))
        }
        st => Err(GeoArrowError::General(format!(
            "Unexpected statistics type: {:?}",
            st
        ))),
    }
}

fn rect_intersects<T: CoordNum>(a: &impl RectTrait<T = T>, b: &impl RectTrait<T = T>) -> bool {
    if a.upper().x() < b.lower().x() {
        return false;
    }

    if a.upper().y() < b.lower().y() {
        return false;
    }

    if a.lower().x() > b.upper().x() {
        return false;
    }

    if a.lower().y() > b.upper().y() {
        return false;
    }

    true
}
