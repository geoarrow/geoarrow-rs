/// The permitted GeoArrow coordinate representations.
///
/// GeoArrow permits coordinate types to either be `Interleaved`, where the X and Y coordinates are
/// in a single buffer as XYXYXY or `Separated`, where the X and Y coordinates are in multiple
/// buffers as XXXX and YYYY.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CoordType {
    /// Interleaved coordinates.
    #[default]
    Interleaved,

    /// Separated coordinates.
    Separated,
}
