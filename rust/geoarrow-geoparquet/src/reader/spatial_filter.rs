use std::collections::HashSet;
use std::fmt::Debug;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type};
use arrow_array::{Array, Float32Array, Float64Array, Scalar};
use arrow_buffer::ScalarBuffer;
use arrow_ord::cmp::{gt_eq, lt_eq};
use geo_traits::{CoordTrait, RectTrait};
use geo_types::{CoordNum, Rect, coord};
use geoarrow_array::GeoArrowArrayAccessor;
use geoarrow_array::array::{RectArray, from_arrow_array};
use geoarrow_array::builder::RectBuilder;
use geoarrow_array::error::{GeoArrowError, Result};
use geoarrow_schema::{BoxType, Dimension};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowPredicate, ArrowPredicateFn, ArrowReaderBuilder, RowFilter,
};
use parquet::file::metadata::{ColumnChunkMetaData, RowGroupMetaData};
use parquet::file::statistics::Statistics;
use parquet::schema::types::{ColumnPath, SchemaDescriptor};

use crate::metadata::GeoParquetBboxCovering;
use crate::total_bounds::bounding_rect;

/// A helper for interpreting bounding box row group statistics from GeoParquet files
///
/// This is **not** intended to be user facing. It's an internal struct that needs access to the
/// SchemaDescriptor to create.
#[derive(Debug, Clone)]
pub(crate) struct ParquetBboxStatistics<'a> {
    /// The schema path of the Parquet column that contains the xmin
    minx_col_path: &'a [String],

    /// The schema path of the Parquet column that contains the ymin
    miny_col_path: &'a [String],

    /// The schema path of the Parquet column that contains the xmin
    maxx_col_path: &'a [String],

    /// The schema path of the Parquet column that contains the ymax
    maxy_col_path: &'a [String],

    /// The index of the Parquet column that contains the xmin
    minx_col: usize,

    /// The index of the Parquet column that contains the ymin
    miny_col: usize,

    /// The index of the Parquet column that contains the xmin
    maxx_col: usize,

    /// The index of the Parquet column that contains the ymax
    maxy_col: usize,
}

impl<'a> ParquetBboxStatistics<'a> {
    /// Loops through the columns in the SchemaDescriptor, looking at each's path
    pub fn try_new(
        parquet_schema: &SchemaDescriptor,
        paths: &'a GeoParquetBboxCovering,
    ) -> Result<Self> {
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

            // NOTE: we **don't** want to `continue` out of the loop after matching one of these
            // paths because in the native encoding case the same column can be _both_ the minx and
            // maxx column paths.
            if minx_col.is_none() && path_equals(paths.xmin.as_ref(), column_meta.path()) {
                minx_col = Some(column_idx);
            }

            if miny_col.is_none() && path_equals(paths.ymin.as_ref(), column_meta.path()) {
                miny_col = Some(column_idx);
            }

            if maxx_col.is_none() && path_equals(paths.xmax.as_ref(), column_meta.path()) {
                maxx_col = Some(column_idx);
            }

            if maxy_col.is_none() && path_equals(paths.ymax.as_ref(), column_meta.path()) {
                maxy_col = Some(column_idx);
            }
        }

        if minx_col.is_none() {
            return Err(GeoArrowError::General(format!(
                "Unable to find xmin_path: {:?}",
                paths.xmin
            )));
        }

        if miny_col.is_none() {
            return Err(GeoArrowError::General(format!(
                "Unable to find ymin_path: {:?}",
                paths.ymin
            )));
        }

        if maxx_col.is_none() {
            return Err(GeoArrowError::General(format!(
                "Unable to find xmax_path: {:?}",
                paths.xmax
            )));
        }

        if maxy_col.is_none() {
            return Err(GeoArrowError::General(format!(
                "Unable to find ymax_path: {:?}",
                paths.ymax
            )));
        }

        Ok(Self {
            minx_col_path: paths.xmin.as_slice(),
            miny_col_path: paths.ymin.as_slice(),
            maxx_col_path: paths.xmax.as_slice(),
            maxy_col_path: paths.ymax.as_slice(),
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
        let mut builder = RectBuilder::with_capacity(
            BoxType::new(Dimension::XY, Default::default()),
            row_groups.len(),
        );
        for rg_meta in row_groups.iter() {
            builder.push_rect(Some(&self.get_bbox(rg_meta)?));
        }
        Ok(builder.finish())
    }
}

pub(crate) fn apply_bbox_row_groups<T>(
    builder: ArrowReaderBuilder<T>,
    bbox_cols: &ParquetBboxStatistics,
    bbox_query: Rect,
) -> Result<ArrowReaderBuilder<T>> {
    let row_groups = builder.metadata().row_groups();
    let row_groups_bounds = bbox_cols.get_bboxes(row_groups)?;
    let mut intersects_row_groups_idxs = vec![];
    for (row_group_idx, row_group_bounds) in row_groups_bounds.iter_values().enumerate() {
        if rect_intersects(&row_group_bounds?, &bbox_query) {
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

    // If the min and max columns are the same, then it's a native column
    let predicate =
        if bbox_cols.minx_col == bbox_cols.maxx_col && bbox_cols.miny_col == bbox_cols.maxy_col {
            construct_native_predicate(parquet_schema, bbox_cols, bbox_query)?
        } else {
            construct_bbox_columns_predicate(parquet_schema, bbox_cols, bbox_query)?
        };
    let filter = RowFilter::new(vec![predicate]);
    Ok(builder.with_row_filter(filter))
}

/// Upcast a Float32Array to a Float64Array
fn upcast_float_array(array: &Float32Array) -> Float64Array {
    let nulls = array.nulls().cloned();
    let values = ScalarBuffer::from_iter(array.values().iter().map(|val| *val as f64));
    Float64Array::new(values, nulls)
}

/// Construct an [ArrowPredicate] used for spatial filtering when the input is encoded as a native
/// geometry.
fn construct_native_predicate(
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
        let array = batch.column(0);
        let field = batch.schema_ref().field(0);
        let nulls = array.nulls();
        let geo_arr = from_arrow_array(array, field)?;
        let rect_arr = bounding_rect(geo_arr.as_ref())?;

        let xmin_col = Float64Array::new(rect_arr.lower().raw_buffers()[0].clone(), nulls.cloned());
        let ymin_col = Float64Array::new(rect_arr.lower().raw_buffers()[1].clone(), nulls.cloned());
        let xmax_col = Float64Array::new(rect_arr.upper().raw_buffers()[0].clone(), nulls.cloned());
        let ymax_col = Float64Array::new(rect_arr.upper().raw_buffers()[1].clone(), nulls.cloned());

        // Construct the bounding box from user input
        let minx_scalar = Scalar::new(Float64Array::from(vec![bbox_query.min().x()]));
        let miny_scalar = Scalar::new(Float64Array::from(vec![bbox_query.min().y()]));
        let maxx_scalar = Scalar::new(Float64Array::from(vec![bbox_query.max().x()]));
        let maxy_scalar = Scalar::new(Float64Array::from(vec![bbox_query.max().y()]));

        // Perform bbox comparison
        // TODO: do this in one pass instead of four?
        let minx_cmp = gt_eq(&xmax_col, &minx_scalar).unwrap();
        let miny_cmp = gt_eq(&ymax_col, &miny_scalar).unwrap();
        let maxx_cmp = lt_eq(&xmin_col, &maxx_scalar).unwrap();
        let maxy_cmp = lt_eq(&ymin_col, &maxy_scalar).unwrap();

        // AND together the results
        let first = arrow_arith::boolean::and(&minx_cmp, &miny_cmp).unwrap();
        let second = arrow_arith::boolean::and(&first, &maxx_cmp).unwrap();
        let third = arrow_arith::boolean::and(&second, &maxy_cmp).unwrap();

        Ok(third)
    });
    Ok(Box::new(predicate))
}

/// Construct an [ArrowPredicate] used for spatial filtering when the input is a struct column of 4
/// floats or doubles, as described in GeoParquet 1.1 bounding box columns.
fn construct_bbox_columns_predicate(
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

    // The GeoParquet spec allows the bounding box columns to be either Double or Float data type.
    // We need to know which type it is so that we can downcast the produced Arrow arrays to the
    // correct type.
    let mut column_types = HashSet::with_capacity(4);
    column_types.insert(parquet_schema.column(bbox_cols.minx_col).physical_type());
    column_types.insert(parquet_schema.column(bbox_cols.miny_col).physical_type());
    column_types.insert(parquet_schema.column(bbox_cols.maxx_col).physical_type());
    column_types.insert(parquet_schema.column(bbox_cols.maxy_col).physical_type());
    if column_types.len() != 1 {
        return Err(GeoArrowError::General(format!(
            "Expected one column type for GeoParquet bbox columns, got {:?}",
            column_types
        )));
    }

    let column_type = column_types.drain().next().unwrap();
    if !(matches!(column_type, parquet::basic::Type::FLOAT)
        || matches!(column_type, parquet::basic::Type::DOUBLE))
    {
        return Err(GeoArrowError::General(format!(
            "Expected column type for GeoParquet bbox column to be FLOAT or DOUBLE, got {:?}",
            column_type
        )));
    }

    // Note: the GeoParquet specification declares that these columns MUST be named xmin, ymin,
    // xmax, ymax. But the Overture data does not yet comply with this, so we follow the user
    // input.
    let minx_struct_field_name = bbox_cols.minx_col_path.last().unwrap().clone();
    let miny_struct_field_name = bbox_cols.miny_col_path.last().unwrap().clone();
    let maxx_struct_field_name = bbox_cols.maxx_col_path.last().unwrap().clone();
    let maxy_struct_field_name = bbox_cols.maxy_col_path.last().unwrap().clone();

    let predicate = ArrowPredicateFn::new(mask, move |batch| {
        let struct_col = batch.column(0).as_struct();

        let struct_fields = struct_col.fields();
        let (xmin_struct_idx, _) = struct_fields.find(&minx_struct_field_name).unwrap();
        let (ymin_struct_idx, _) = struct_fields.find(&miny_struct_field_name).unwrap();
        let (xmax_struct_idx, _) = struct_fields.find(&maxx_struct_field_name).unwrap();
        let (ymax_struct_idx, _) = struct_fields.find(&maxy_struct_field_name).unwrap();

        let (xmin_col, ymin_col, xmax_col, ymax_col) = match column_type {
            parquet::basic::Type::FLOAT => {
                let minx_col = struct_col
                    .column(xmin_struct_idx)
                    .as_primitive::<Float32Type>();
                let miny_col = struct_col
                    .column(ymin_struct_idx)
                    .as_primitive::<Float32Type>();
                let maxx_col = struct_col
                    .column(xmax_struct_idx)
                    .as_primitive::<Float32Type>();
                let maxy_col = struct_col
                    .column(ymax_struct_idx)
                    .as_primitive::<Float32Type>();

                (
                    &upcast_float_array(minx_col),
                    &upcast_float_array(miny_col),
                    &upcast_float_array(maxx_col),
                    &upcast_float_array(maxy_col),
                )
            }
            parquet::basic::Type::DOUBLE => {
                let minx_col = struct_col
                    .column(xmin_struct_idx)
                    .as_primitive::<Float64Type>();
                let miny_col = struct_col
                    .column(ymin_struct_idx)
                    .as_primitive::<Float64Type>();
                let maxx_col = struct_col
                    .column(xmax_struct_idx)
                    .as_primitive::<Float64Type>();
                let maxy_col = struct_col
                    .column(ymax_struct_idx)
                    .as_primitive::<Float64Type>();
                (minx_col, miny_col, maxx_col, maxy_col)
            }
            _ => unreachable!(),
        };

        // Construct the bounding box from user input
        let minx_scalar = Scalar::new(Float64Array::from(vec![bbox_query.min().x()]));
        let miny_scalar = Scalar::new(Float64Array::from(vec![bbox_query.min().y()]));
        let maxx_scalar = Scalar::new(Float64Array::from(vec![bbox_query.max().x()]));
        let maxy_scalar = Scalar::new(Float64Array::from(vec![bbox_query.max().y()]));

        // Perform bbox comparison
        // TODO: do this in one pass instead of four?
        let minx_cmp = gt_eq(&xmax_col, &minx_scalar).unwrap();
        let miny_cmp = gt_eq(&ymax_col, &miny_scalar).unwrap();
        let maxx_cmp = lt_eq(&xmin_col, &maxx_scalar).unwrap();
        let maxy_cmp = lt_eq(&ymin_col, &maxy_scalar).unwrap();

        // AND together the results
        let first = arrow_arith::boolean::and(&minx_cmp, &miny_cmp).unwrap();
        let second = arrow_arith::boolean::and(&first, &maxx_cmp).unwrap();
        let third = arrow_arith::boolean::and(&second, &maxy_cmp).unwrap();

        Ok(third)
    });

    Ok(Box::new(predicate))
}

/// Check whether two paths are equal
fn path_equals<T: AsRef<str> + Debug>(a: &[T], b: &ColumnPath) -> bool {
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

/// Parse Parquet statistics as f64
///
/// When statistics are stored as f32, this will upcast to f64.
fn parse_statistics_f64(column_meta: &ColumnChunkMetaData) -> Result<(f64, f64)> {
    let stats = column_meta
        .statistics()
        .ok_or(GeoArrowError::General(format!(
            "No statistics for column {}",
            column_meta.column_path()
        )))?;
    match stats {
        Statistics::Double(typed_stats) => Ok((
            *typed_stats.min_opt().unwrap(),
            *typed_stats.max_opt().unwrap(),
        )),
        Statistics::Float(typed_stats) => Ok((
            *typed_stats.min_opt().unwrap() as f64,
            *typed_stats.max_opt().unwrap() as f64,
        )),
        st => Err(GeoArrowError::General(format!(
            "Unexpected statistics type: {:?}",
            st
        ))),
    }
}

/// Check whether two [RectTrait] intersect.
fn rect_intersects<T: CoordNum>(a: &impl RectTrait<T = T>, b: &impl RectTrait<T = T>) -> bool {
    if a.max().x() < b.min().x() {
        return false;
    }

    if a.max().y() < b.min().y() {
        return false;
    }

    if a.min().x() > b.max().x() {
        return false;
    }

    if a.min().y() > b.max().y() {
        return false;
    }

    true
}
