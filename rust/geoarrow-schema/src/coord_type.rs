/// The permitted GeoArrow coordinate representations.
///
/// GeoArrow permits coordinate types to either be `Interleaved`, where the X and Y coordinates are
/// in a single buffer as XYXYXY or `Separated`, where the X and Y coordinates are in multiple
/// buffers as XXXX and YYYY.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CoordType {
    /// Interleaved coordinates.
    // #[default]
    Interleaved,

    /// Separated coordinates.
    Separated,
}

impl CoordType {
    /// Specify Interleaved as a "default".
    ///
    /// There are discussions ongoing about whether `CoordType` should define a default value. This
    /// exists for places where we want to use a default value of `CoordType` without currently
    /// defining `Default` on `CoordType`.
    pub fn default_interleaved() -> Self {
        Self::Interleaved
    }
}
