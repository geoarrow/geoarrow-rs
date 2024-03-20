use std::fmt::Debug;

use parquet::file::metadata::{ColumnChunkMetaData, RowGroupMetaData};
use parquet::file::statistics::Statistics;
use parquet::schema::types::{ColumnPath, SchemaDescriptor};

use crate::algorithm::native::bounding_rect::BoundingRect;
use crate::error::{GeoArrowError, Result};

/// A helper for accessing the bounding box of a single row group from the Parquet metadata.
///
/// Note that this is only valid for files with the exact same Parquet schema.
pub struct GeoStatistics {
    /// The index of the Parquet column that contains the xmin
    xmin_col: usize,

    /// The index of the Parquet column that contains the ymin
    ymin_col: usize,

    /// The index of the Parquet column that contains the xmin
    xmax_col: usize,

    /// The index of the Parquet column that contains the ymax
    ymax_col: usize,
}

impl GeoStatistics {
    /// Construct from a Parquet SchemaDescriptor plus the path in the Parquet schema to the xmin,
    /// ymin, etc
    pub fn from_schema<T: AsRef<str> + Debug>(
        schema: &SchemaDescriptor,
        xmin_path: &[T],
        ymin_path: &[T],
        xmax_path: &[T],
        ymax_path: &[T],
    ) -> Result<Self> {
        let column_descriptors = schema.columns();

        let mut xmin_col: Option<usize> = None;
        let mut ymin_col: Option<usize> = None;
        let mut xmax_col: Option<usize> = None;
        let mut ymax_col: Option<usize> = None;
        for (idx, column_desc) in column_descriptors.iter().enumerate() {
            let column_path = column_desc.path();

            if xmin_col.is_none() && path_matches(column_path, xmin_path) {
                xmin_col = Some(idx);
                continue;
            }

            if ymin_col.is_none() && path_matches(column_path, ymin_path) {
                ymin_col = Some(idx);
                continue;
            }

            if xmax_col.is_none() && path_matches(column_path, xmax_path) {
                xmax_col = Some(idx);
                continue;
            }

            if ymax_col.is_none() && path_matches(column_path, ymax_path) {
                ymax_col = Some(idx);
                continue;
            }
        }

        if xmin_col.is_none() {
            return Err(GeoArrowError::General(format!(
                "Unable to find xmin_path: {:?}",
                xmin_path
            )));
        }

        if ymin_col.is_none() {
            return Err(GeoArrowError::General(format!(
                "Unable to find ymin_path: {:?}",
                ymin_path
            )));
        }

        if xmax_col.is_none() {
            return Err(GeoArrowError::General(format!(
                "Unable to find xmax_path: {:?}",
                xmax_path
            )));
        }

        if ymax_col.is_none() {
            return Err(GeoArrowError::General(format!(
                "Unable to find ymax_path: {:?}",
                ymax_path
            )));
        }

        Ok(Self {
            xmin_col: xmin_col.unwrap(),
            ymin_col: ymin_col.unwrap(),
            xmax_col: xmax_col.unwrap(),
            ymax_col: ymax_col.unwrap(),
        })
    }

    /// Extract the bounding box from a given row group's metadata.
    ///
    /// This uses the column statistics contained in the row group metadata.
    pub fn get_bbox(&self, rg_meta: &RowGroupMetaData) -> Result<BoundingRect> {
        let (minx, _) = parse_statistics_f64(rg_meta.column(self.xmin_col))?;
        let (miny, _) = parse_statistics_f64(rg_meta.column(self.ymin_col))?;
        let (_, maxx) = parse_statistics_f64(rg_meta.column(self.xmax_col))?;
        let (_, maxy) = parse_statistics_f64(rg_meta.column(self.ymax_col))?;
        Ok(BoundingRect {
            minx,
            miny,
            maxx,
            maxy,
        })
    }
}

fn path_matches<T: AsRef<str>>(path: &ColumnPath, search: &[T]) -> bool {
    let parts = path.parts();
    if parts.len() != search.len() {
        return false;
    }

    for (part, expected) in parts.iter().zip(search) {
        if part.as_str() != expected.as_ref() {
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
