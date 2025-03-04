//! Note: This entire mod is a candidate to upstream into arrow-rs.

use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;

use crate::error::Result;

pub(crate) fn offsets_buffer_i32_to_i64(offsets: &OffsetBuffer<i32>) -> OffsetBuffer<i64> {
    let i64_offsets = offsets.iter().map(|x| *x as i64).collect::<Vec<_>>();
    unsafe { OffsetBuffer::new_unchecked(i64_offsets.into()) }
}

pub(crate) fn offsets_buffer_i64_to_i32(offsets: &OffsetBuffer<i64>) -> Result<OffsetBuffer<i32>> {
    // TODO: raise nicer error. Ref:
    // https://github.com/jorgecarleitao/arrow2/blob/6a4b53169a48cbd234cecde6ab6a98f84146fca2/src/offset.rs#L492
    i32::try_from(*offsets.last()).unwrap();

    let i32_offsets = offsets.iter().map(|x| *x as i32).collect::<Vec<_>>();
    Ok(unsafe { OffsetBuffer::new_unchecked(i32_offsets.into()) })
}

#[allow(dead_code)]
pub(crate) fn offsets_buffer_to_i64<O: OffsetSizeTrait>(
    offsets: &OffsetBuffer<O>,
) -> OffsetBuffer<i64> {
    let i64_offsets = offsets
        .iter()
        .map(|x| x.to_i64().unwrap())
        .collect::<Vec<_>>();
    unsafe { OffsetBuffer::new_unchecked(i64_offsets.into()) }
}

#[allow(dead_code)]
pub(crate) fn offsets_buffer_to_i32<O: OffsetSizeTrait>(
    offsets: &OffsetBuffer<O>,
) -> Result<OffsetBuffer<i32>> {
    // TODO: raise nicer error. Ref:
    // https://github.com/jorgecarleitao/arrow2/blob/6a4b53169a48cbd234cecde6ab6a98f84146fca2/src/offset.rs#L492
    i32::try_from(offsets.last().as_usize()).unwrap();

    let i32_offsets = offsets
        .iter()
        .map(|x| x.to_usize().unwrap() as i32)
        .collect::<Vec<_>>();
    Ok(unsafe { OffsetBuffer::new_unchecked(i32_offsets.into()) })
}

/// Offsets utils that I miss from arrow2
pub(crate) trait OffsetBufferUtils<O: OffsetSizeTrait> {
    /// Returns the length an array with these offsets would be.
    fn len_proxy(&self) -> usize;

    /// Returns a range (start, end) corresponding to the position `index`
    /// # Panic
    /// This function panics iff `index >= self.len_proxy()`
    fn start_end(&self, index: usize) -> (usize, usize);

    /// Returns the last offset.
    fn last(&self) -> &O;
}

impl<O: OffsetSizeTrait> OffsetBufferUtils<O> for OffsetBuffer<O> {
    /// Returns the length an array with these offsets would be.
    #[inline]
    fn len_proxy(&self) -> usize {
        self.len() - 1
    }

    /// Returns a range (start, end) corresponding to the position `index`
    ///
    /// # Panic
    ///
    /// Panics iff `index >= self.len_proxy()`
    #[inline]
    fn start_end(&self, index: usize) -> (usize, usize) {
        assert!(index < self.len_proxy());
        let start = self[index].to_usize().unwrap();
        let end = self[index + 1].to_usize().unwrap();
        (start, end)
    }

    /// Returns the last offset.
    #[inline]
    fn last(&self) -> &O {
        self.as_ref().last().unwrap()
    }
}
