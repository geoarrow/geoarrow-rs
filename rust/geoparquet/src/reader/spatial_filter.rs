use std::fmt::Debug;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type};
use arrow_array::{Array, Float32Array, Float64Array, Scalar};
use arrow_buffer::ScalarBuffer;
use arrow_ord::cmp::{gt_eq, lt_eq};
use arrow_schema::ArrowError;
use geo_traits::{CoordTrait, RectTrait};
use geo_types::{CoordNum, Rect, coord};
use geoarrow_array::GeoArrowArrayAccessor;
use geoarrow_array::array::{RectArray, from_arrow_array};
use geoarrow_array::builder::RectBuilder;
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use geoarrow_schema::{BoxType, Dimension, Metadata};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowPredicate, ArrowPredicateFn};
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
    pub(crate) fn try_new(
        parquet_schema: &SchemaDescriptor,
        bbox_covering: &'a GeoParquetBboxCovering,
    ) -> GeoArrowResult<Self> {
        // No structural requirements here: the statistics-only consumers (row-group pruning
        // and bounds) work with any resolvable paths, including spec-noncompliant flat
        // columns. The row-filter path checks its stricter shape itself.
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
            if minx_col.is_none() && path_equals(bbox_covering.xmin.as_ref(), column_meta.path()) {
                minx_col = Some(column_idx);
            }

            if miny_col.is_none() && path_equals(bbox_covering.ymin.as_ref(), column_meta.path()) {
                miny_col = Some(column_idx);
            }

            if maxx_col.is_none() && path_equals(bbox_covering.xmax.as_ref(), column_meta.path()) {
                maxx_col = Some(column_idx);
            }

            if maxy_col.is_none() && path_equals(bbox_covering.ymax.as_ref(), column_meta.path()) {
                maxy_col = Some(column_idx);
            }
        }

        if minx_col.is_none() {
            return Err(GeoArrowError::GeoParquet(format!(
                "Unable to find xmin_path: {:?}",
                bbox_covering.xmin
            )));
        }

        if miny_col.is_none() {
            return Err(GeoArrowError::GeoParquet(format!(
                "Unable to find ymin_path: {:?}",
                bbox_covering.ymin
            )));
        }

        if maxx_col.is_none() {
            return Err(GeoArrowError::GeoParquet(format!(
                "Unable to find xmax_path: {:?}",
                bbox_covering.xmax
            )));
        }

        if maxy_col.is_none() {
            return Err(GeoArrowError::GeoParquet(format!(
                "Unable to find ymax_path: {:?}",
                bbox_covering.ymax
            )));
        }

        Ok(Self {
            minx_col_path: bbox_covering.xmin.as_slice(),
            miny_col_path: bbox_covering.ymin.as_slice(),
            maxx_col_path: bbox_covering.xmax.as_slice(),
            maxy_col_path: bbox_covering.ymax.as_slice(),
            minx_col: minx_col.unwrap(),
            miny_col: miny_col.unwrap(),
            maxx_col: maxx_col.unwrap(),
            maxy_col: maxy_col.unwrap(),
        })
    }

    /// Extract the bounding box from a given row group's metadata.
    ///
    /// This uses the column statistics contained in the row group metadata.
    pub(crate) fn get_bbox(&self, rg_meta: &RowGroupMetaData) -> GeoArrowResult<Rect> {
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
    ///
    /// If `metadata` is provided, it will be assigned onto the generated `RectArray`.
    pub(crate) fn get_bboxes(
        &self,
        row_groups: &[RowGroupMetaData],
        metadata: Arc<Metadata>,
    ) -> GeoArrowResult<RectArray> {
        let rect_type = BoxType::new(Dimension::XY, metadata);

        let mut builder = RectBuilder::with_capacity(rect_type, row_groups.len());
        for rg_meta in row_groups.iter() {
            builder.push_rect(Some(&self.get_bbox(rg_meta)?));
        }
        Ok(builder.finish())
    }
}

pub(crate) fn bbox_row_groups(
    row_groups: &[RowGroupMetaData],
    bbox_cols: &ParquetBboxStatistics,
    bbox_query: Rect,
) -> GeoArrowResult<Vec<usize>> {
    let row_groups_bounds = bbox_cols.get_bboxes(row_groups, Default::default())?;
    let mut intersects_row_groups_idxs = vec![];
    for (row_group_idx, row_group_bounds) in row_groups_bounds.iter_values().enumerate() {
        if rect_intersects(&row_group_bounds?, &bbox_query) {
            intersects_row_groups_idxs.push(row_group_idx);
        }
    }

    Ok(intersects_row_groups_idxs)
}

pub(crate) fn bbox_arrow_predicate(
    parquet_schema: &SchemaDescriptor,
    bbox_cols: ParquetBboxStatistics,
    bbox_query: Rect,
) -> GeoArrowResult<Box<dyn ArrowPredicate>> {
    // If the min and max columns are the same, then it's a native column
    if bbox_cols.minx_col == bbox_cols.maxx_col && bbox_cols.miny_col == bbox_cols.maxy_col {
        construct_native_predicate(parquet_schema, bbox_cols, bbox_query)
    } else {
        construct_bbox_columns_predicate(parquet_schema, bbox_cols, bbox_query)
    }
    // Ok(RowFilter::new(vec![predicate]))
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
) -> GeoArrowResult<Box<dyn ArrowPredicate>> {
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
        let minx_cmp = gt_eq(&xmax_col, &minx_scalar)?;
        let miny_cmp = gt_eq(&ymax_col, &miny_scalar)?;
        let maxx_cmp = lt_eq(&xmin_col, &maxx_scalar)?;
        let maxy_cmp = lt_eq(&ymin_col, &maxy_scalar)?;

        // AND together the results
        let first = arrow_arith::boolean::and(&minx_cmp, &miny_cmp)?;
        let second = arrow_arith::boolean::and(&first, &maxx_cmp)?;
        let third = arrow_arith::boolean::and(&second, &maxy_cmp)?;

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
) -> GeoArrowResult<Box<dyn ArrowPredicate>> {
    // The predicate projects one root column and reads batch.column(0) as a struct of the
    // projected leaves, so the covering must be a GeoParquet 1.1 bounding box column: one
    // top-level struct, every path [column, field], four distinct fields.
    let root = bbox_cols.minx_col_path.first();
    if bbox_cols.miny_col_path.first() != root
        || bbox_cols.maxx_col_path.first() != root
        || bbox_cols.maxy_col_path.first() != root
    {
        return Err(GeoArrowError::GeoParquet(format!(
            "GeoParquet bbox covering paths must share the same root column, got {:?}, {:?}, {:?}, {:?}",
            bbox_cols.minx_col_path,
            bbox_cols.miny_col_path,
            bbox_cols.maxx_col_path,
            bbox_cols.maxy_col_path
        )));
    }

    for path in [
        bbox_cols.minx_col_path,
        bbox_cols.miny_col_path,
        bbox_cols.maxx_col_path,
        bbox_cols.maxy_col_path,
    ] {
        if path.len() != 2 {
            return Err(GeoArrowError::GeoParquet(format!(
                "Expected a GeoParquet bbox covering path of the form [column, field], got {path:?}",
            )));
        }
    }

    let leaf_columns = [
        bbox_cols.minx_col,
        bbox_cols.miny_col,
        bbox_cols.maxx_col,
        bbox_cols.maxy_col,
    ];

    // Legitimate leaf sharing (native encoding) routes to construct_native_predicate, so a
    // duplicate leaf here means the covering maps one field onto two roles.
    if (1..leaf_columns.len()).any(|i| leaf_columns[..i].contains(&leaf_columns[i])) {
        return Err(GeoArrowError::GeoParquet(format!(
            "GeoParquet bbox covering paths must name four distinct fields, got {:?}, {:?}, {:?}, {:?}",
            bbox_cols.minx_col_path,
            bbox_cols.miny_col_path,
            bbox_cols.maxx_col_path,
            bbox_cols.maxy_col_path
        )));
    }

    // The GeoParquet spec allows the bounding box columns to be either Double or Float data type.
    for column in leaf_columns {
        let column_type = parquet_schema.column(column).physical_type();
        if !matches!(
            column_type,
            parquet::basic::Type::FLOAT | parquet::basic::Type::DOUBLE
        ) {
            return Err(GeoArrowError::GeoParquet(format!(
                "Expected column type for GeoParquet bbox column to be FLOAT or DOUBLE, got {column_type:?}",
            )));
        }
    }

    let mask = ProjectionMask::leaves(parquet_schema, leaf_columns);

    // The projected batch is one struct column whose children are the projected leaves in
    // parquet schema order, so each role's child index is the rank of its leaf index.
    let [
        xmin_struct_idx,
        ymin_struct_idx,
        xmax_struct_idx,
        ymax_struct_idx,
    ] = leaf_columns.map(|column| leaf_columns.iter().filter(|&&other| other < column).count());

    let predicate = ArrowPredicateFn::new(mask, move |batch| {
        let struct_col = batch.column(0).as_struct_opt().ok_or_else(|| {
            ArrowError::ComputeError(
                "GeoParquet bbox covering root column is not a struct".to_string(),
            )
        })?;

        let xmin_col = bbox_child_f64(struct_col.column(xmin_struct_idx))?;
        let ymin_col = bbox_child_f64(struct_col.column(ymin_struct_idx))?;
        let xmax_col = bbox_child_f64(struct_col.column(xmax_struct_idx))?;
        let ymax_col = bbox_child_f64(struct_col.column(ymax_struct_idx))?;

        // Construct the bounding box from user input
        let minx_scalar = Scalar::new(Float64Array::from(vec![bbox_query.min().x()]));
        let miny_scalar = Scalar::new(Float64Array::from(vec![bbox_query.min().y()]));
        let maxx_scalar = Scalar::new(Float64Array::from(vec![bbox_query.max().x()]));
        let maxy_scalar = Scalar::new(Float64Array::from(vec![bbox_query.max().y()]));

        // Perform bbox comparison
        // TODO: do this in one pass instead of four?
        let minx_cmp = gt_eq(&xmax_col, &minx_scalar)?;
        let miny_cmp = gt_eq(&ymax_col, &miny_scalar)?;
        let maxx_cmp = lt_eq(&xmin_col, &maxx_scalar)?;
        let maxy_cmp = lt_eq(&ymin_col, &maxy_scalar)?;

        // AND together the results
        let first = arrow_arith::boolean::and(&minx_cmp, &miny_cmp)?;
        let second = arrow_arith::boolean::and(&first, &maxx_cmp)?;
        let third = arrow_arith::boolean::and(&second, &maxy_cmp)?;

        Ok(third)
    });

    Ok(Box::new(predicate))
}

/// Read a bbox struct child as Float64, upcasting Float32.
///
/// The decoded array type can differ from the parquet physical type (e.g. through an embedded
/// `ARROW:schema` hint), so this must not assume the construction-time check still holds.
fn bbox_child_f64(array: &Arc<dyn Array>) -> Result<Float64Array, ArrowError> {
    if let Some(float64) = array.as_primitive_opt::<Float64Type>() {
        Ok(float64.clone())
    } else if let Some(float32) = array.as_primitive_opt::<Float32Type>() {
        Ok(upcast_float_array(float32))
    } else {
        Err(ArrowError::ComputeError(format!(
            "Expected GeoParquet bbox covering field to decode as Float32 or Float64, got {:?}",
            array.data_type()
        )))
    }
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
fn parse_statistics_f64(column_meta: &ColumnChunkMetaData) -> GeoArrowResult<(f64, f64)> {
    let stats = column_meta
        .statistics()
        .ok_or(GeoArrowError::GeoParquet(format!(
            "No statistics for column {}",
            column_meta.column_path()
        )))?;
    // Statistics can carry a null count and no minimum or maximum value.
    let missing_min_max = || {
        GeoArrowError::GeoParquet(format!(
            "Statistics for column {} have no min/max values",
            column_meta.column_path()
        ))
    };
    match stats {
        Statistics::Double(typed_stats) => Ok((
            *typed_stats.min_opt().ok_or_else(missing_min_max)?,
            *typed_stats.max_opt().ok_or_else(missing_min_max)?,
        )),
        Statistics::Float(typed_stats) => Ok((
            *typed_stats.min_opt().ok_or_else(missing_min_max)? as f64,
            *typed_stats.max_opt().ok_or_else(missing_min_max)? as f64,
        )),
        st => Err(GeoArrowError::GeoParquet(format!(
            "Unexpected statistics type: {st:?}",
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::schema_descr;

    #[test]
    fn statistics_without_min_max_error() {
        let descr = schema_descr("message schema { required double x; }");
        let column_meta = ColumnChunkMetaData::builder(descr.column(0))
            .set_statistics(Statistics::double(None, None, None, Some(4), false))
            .build()
            .unwrap();
        let err = parse_statistics_f64(&column_meta).unwrap_err();
        assert!(err.to_string().contains("min/max"));
    }

    /// Nonstandard covering shapes resolve for the statistics-only consumers but are rejected
    /// by the row-filter path.
    #[test]
    fn covering_shapes_gate_the_row_filter_only() {
        let query = Rect::new(coord! { x: 0., y: 0. }, coord! { x: 1., y: 1. });
        let cases = [
            (
                "message schema { required double a; required double b; required double c; required double d; }",
                serde_json::json!({"xmin": ["a"], "ymin": ["b"], "xmax": ["c"], "ymax": ["d"]}),
                "same root column",
            ),
            (
                "message schema { required group bbox { required double xmin; required double ymin; required double xmax; required double ymax; } }",
                serde_json::json!({
                    "xmin": ["bbox", "xmin"], "ymin": ["bbox", "ymin"],
                    "xmax": ["bbox", "xmin"], "ymax": ["bbox", "ymax"]
                }),
                "distinct",
            ),
            (
                "message schema { required group props { required group bbox { required double xmin; required double ymin; required double xmax; required double ymax; } } }",
                serde_json::json!({
                    "xmin": ["props", "bbox", "xmin"], "ymin": ["props", "bbox", "ymin"],
                    "xmax": ["props", "bbox", "xmax"], "ymax": ["props", "bbox", "ymax"]
                }),
                "[column, field]",
            ),
        ];
        for (message_type, covering, expected) in cases {
            let descr = schema_descr(message_type);
            let covering: GeoParquetBboxCovering = serde_json::from_value(covering).unwrap();
            let stats = ParquetBboxStatistics::try_new(&descr, &covering).unwrap();
            let err = bbox_arrow_predicate(&descr, stats, query).err().unwrap();
            assert!(err.to_string().contains(expected), "{expected}");
        }
    }
}
