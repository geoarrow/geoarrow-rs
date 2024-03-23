use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::Array;
use arrow::array::UInt32Builder;
use arrow::compute::take_record_batch;
use arrow_array::RecordBatch;
use arrow_schema::SchemaBuilder;
use geo::Intersects;
use geo_index::rtree::sort::HilbertSort;
use geo_index::rtree::{OwnedRTree, RTreeBuilder, RTreeIndex};
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};

use crate::error::Result;
use crate::indexed::array::{create_indexed_array, IndexedGeometryArrayTrait};
use crate::table::GeoTable;
use crate::GeometryArrayTrait;

/// For now, only inner, intersects join
pub fn spatial_join(left: &GeoTable, right: &GeoTable) -> Result<GeoTable> {
    let left_indexed_chunks = create_indexed_chunks(&left.geometry()?.geometry_chunks())?;
    let right_indexed_chunks = create_indexed_chunks(&right.geometry()?.geometry_chunks())?;

    let left_indexed_chunks_refs = left_indexed_chunks
        .iter()
        .map(|chunk| chunk.as_ref())
        .collect::<Vec<_>>();
    let right_indexed_chunks_refs = right_indexed_chunks
        .iter()
        .map(|chunk| chunk.as_ref())
        .collect::<Vec<_>>();

    let chunk_candidates = get_chunk_candidates(
        left_indexed_chunks_refs.as_slice(),
        right_indexed_chunks_refs.as_slice(),
    );

    let new_batches = chunk_candidates
        .into_par_iter()
        .map(|(left_chunk_idx, right_chunk_idx)| {
            let left_chunk = left_indexed_chunks_refs[left_chunk_idx];
            let right_chunk = right_indexed_chunks_refs[right_chunk_idx];

            let left_batch = &left.batches()[left_chunk_idx];
            let right_batch = &right.batches()[right_chunk_idx];

            let (left_indices, right_indices) = chunk_intersects(left_chunk, right_chunk);
            let new_left_batch = take_record_batch(left_batch, &left_indices)?;
            let new_right_batch = take_record_batch(right_batch, &right_indices)?;
            Ok((new_left_batch, new_right_batch))
        })
        .collect::<Result<Vec<_>>>()?;

    assemble_table(new_batches, left, right)
}

fn create_indexed_chunks(
    chunks: &[&dyn GeometryArrayTrait],
) -> Result<Vec<Arc<dyn IndexedGeometryArrayTrait>>> {
    chunks
        .par_iter()
        .map(|chunk| create_indexed_array(*chunk))
        .collect()
}

/// Get the _chunks_ from the left and right sides whose children need to be compared with each
/// other.
///
/// That is, if the left dataset has 5 chunks and the right dataset has 10 chunks. If there is
/// _any_ spatial sorting happening, not all of the 5 left chunks will intersect with every single
/// one of the 5 right chunks. So instead of having to perform _50_ child calls (`left * right`),
/// we can perform less than that: only those whose bounding boxes actually intersect.
///
/// We use the root bounding box of each RTree to infer a "higher level" RTree, which itself we
/// search through to find these candidates.
fn get_chunk_candidates(
    left_chunks: &[&dyn IndexedGeometryArrayTrait],
    right_chunks: &[&dyn IndexedGeometryArrayTrait],
) -> Vec<(usize, usize)> {
    if left_chunks.len() == 1 && right_chunks.len() == 1 {
        return vec![(0, 0)];
    }

    let left_chunk_tree = create_tree_from_chunks(left_chunks);
    let right_chunk_tree = create_tree_from_chunks(right_chunks);

    match (left_chunk_tree, right_chunk_tree) {
        (None, None) => panic!("should be covered above"),
        (Some(left), None) => {
            let right_bbox = right_chunks[0].total_bounds();
            let left_chunk_idxs = left.search(
                right_bbox.minx,
                right_bbox.miny,
                right_bbox.maxx,
                right_bbox.maxy,
            );
            left_chunk_idxs.iter().map(|idx| (*idx, 0)).collect()
        }
        (None, Some(right)) => {
            let left_bbox = left_chunks[0].total_bounds();
            let right_chunk_idxs = right.search(
                left_bbox.minx,
                left_bbox.miny,
                left_bbox.maxx,
                left_bbox.maxy,
            );
            right_chunk_idxs.iter().map(|idx| (0, *idx)).collect()
        }
        (Some(left), Some(right)) => left
            .intersection_candidates_with_other_tree(&right)
            .collect(),
    }
}

fn create_tree_from_chunks(chunks: &[&dyn IndexedGeometryArrayTrait]) -> Option<OwnedRTree<f64>> {
    if chunks.len() == 1 {
        return None;
    }

    let mut tree = RTreeBuilder::<f64>::new(chunks.len());
    chunks.iter().for_each(|chunk| {
        let bounding_rect = chunk.total_bounds();
        tree.add(
            bounding_rect.minx,
            bounding_rect.miny,
            bounding_rect.maxx,
            bounding_rect.maxy,
        );
    });
    Some(tree.finish::<HilbertSort>())
}

/// Call [Intersects][geo::Intersects] on the pairs within the given chunks whose bounding boxes
/// intersect.
///
/// Returns the left and right indexes that pairwise intersect.
fn chunk_intersects(
    left_chunk: &dyn IndexedGeometryArrayTrait,
    right_chunk: &dyn IndexedGeometryArrayTrait,
) -> (Arc<dyn Array>, Arc<dyn Array>) {
    let left_array = left_chunk.array();
    let right_array = right_chunk.array();

    let mut left_idxs = UInt32Builder::new();
    let mut right_idxs = UInt32Builder::new();
    for (left_idx, right_idx) in left_chunk
        .index()
        .intersection_candidates_with_other_tree(right_chunk.index())
    {
        let left_geom = left_array.value_as_geo_geometry(left_idx);
        let right_geom = right_array.value_as_geo_geometry(right_idx);
        if left_geom.intersects(&right_geom) {
            left_idxs.append_value(left_idx.try_into().unwrap());
            right_idxs.append_value(right_idx.try_into().unwrap());
        }
    }

    (Arc::new(left_idxs.finish()), Arc::new(right_idxs.finish()))
}

fn assemble_table(
    new_batches: Vec<(RecordBatch, RecordBatch)>,
    prev_left_table: &GeoTable,
    prev_right_table: &GeoTable,
) -> Result<GeoTable> {
    let prev_left_schema = prev_left_table.schema();
    let prev_right_schema = prev_right_table.schema();
    let right_geom_col_idx = prev_right_table.geometry_column_index();

    let mut left_names = HashSet::with_capacity(prev_left_schema.fields().len());
    prev_left_schema.fields().iter().for_each(|field| {
        left_names.insert(field.name());
    });

    let mut overlapping_field_names = HashSet::new();
    prev_right_schema
        .fields()
        .iter()
        .enumerate()
        .for_each(|(idx, field)| {
            if idx != right_geom_col_idx {
                if left_names.contains(field.name()) {
                    overlapping_field_names.insert(field.name());
                }
            }
        });

    let mut new_schema = SchemaBuilder::with_capacity(
        prev_left_schema.fields().len() + prev_right_schema.fields.len() - 1,
    );
    prev_left_schema.fields().iter().for_each(|field| {
        if overlapping_field_names.contains(field.name()) {
            let new_field = field
                .as_ref()
                .clone()
                .with_name(format!("{}_left", field.name()));
            new_schema.push(Arc::new(new_field))
        } else {
            new_schema.push(field.clone())
        }
    });
    prev_right_schema
        .fields()
        .iter()
        .enumerate()
        .for_each(|(idx, field)| {
            if idx == right_geom_col_idx {
                ()
            } else if overlapping_field_names.contains(field.name()) {
                let new_field = field
                    .as_ref()
                    .clone()
                    .with_name(format!("{}_right", field.name()));
                new_schema.push(Arc::new(new_field))
            } else {
                new_schema.push(field.clone())
            }
        });
    let new_schema = Arc::new(new_schema.finish());

    let new_batches = new_batches
        .into_iter()
        .map(|(left_batch, right_batch)| {
            let mut left_columns = left_batch.columns().to_vec();
            let mut right_columns = right_batch.columns().to_vec();
            right_columns.remove(right_geom_col_idx);
            left_columns.extend_from_slice(&right_columns);
            RecordBatch::try_new(new_schema.clone(), left_columns).unwrap()
        })
        .collect::<Vec<_>>();

    GeoTable::try_new(
        new_schema,
        new_batches,
        prev_left_table.geometry_column_index(),
    )
}
