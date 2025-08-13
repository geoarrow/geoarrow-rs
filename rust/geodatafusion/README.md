# `geodatafusion`

Spatial extensions for [Apache DataFusion](https://datafusion.apache.org/), an extensible query engine written in Rust that uses Apache Arrow as its in-memory format.

## Functions supported

### Geometry Constructors

| Name                  | Implemented | Description                                                                                                                |
| --------------------- | ----------- | -------------------------------------------------------------------------------------------------------------------------- |
| ST_Collect            |             | Creates a GeometryCollection or Multi\* geometry from a set of geometries.                                                 |
| ST_LineFromMultiPoint |             | Creates a LineString from a MultiPoint geometry.                                                                           |
| ST_MakeEnvelope       |             | Creates a rectangular Polygon from minimum and maximum coordinates.                                                        |
| ST_MakeLine           |             | Creates a LineString from Point, MultiPoint, or LineString geometries.                                                     |
| ST_MakePoint          | ✅          | Creates a 2D, 3DZ or 4D Point.                                                                                             |
| ST_MakePointM         | ✅          | Creates a Point from X, Y and M values.                                                                                    |
| ST_MakePolygon        |             | Creates a Polygon from a shell and optional list of holes.                                                                 |
| ST_Point              | ✅          | Creates a Point with X, Y and SRID values.                                                                                 |
| ST_PointZ             | ✅          | Creates a Point with X, Y, Z and SRID values.                                                                              |
| ST_PointM             | ✅          | Creates a Point with X, Y, M and SRID values.                                                                              |
| ST_PointZM            | ✅          | Creates a Point with X, Y, Z, M and SRID values.                                                                           |
| ST_Polygon            |             | Creates a Polygon from a LineString with a specified SRID.                                                                 |
| ST_TileEnvelope       |             | Creates a rectangular Polygon in Web Mercator (SRID:3857) using the XYZ tile system.                                       |
| ST_HexagonGrid        |             | Returns a set of hexagons and cell indices that completely cover the bounds of the geometry argument.                      |
| ST_Hexagon            |             | Returns a single hexagon, using the provided edge size and cell coordinate within the hexagon grid space.                  |
| ST_SquareGrid         |             | Returns a set of grid squares and cell indices that completely cover the bounds of the geometry argument.                  |
| ST_Square             |             | Returns a single square, using the provided edge size and cell coordinate within the square grid space.                    |
| ST_Letters            |             | Returns the input letters rendered as geometry with a default start position at the origin and default text height of 100. |

### Geometry Accessors

| Name                | Implemented | Description                                                                                                               |
| ------------------- | ----------- | ------------------------------------------------------------------------------------------------------------------------- |
| GeometryType        |             | Returns the type of a geometry as text.                                                                                   |
| ST_Boundary         |             | Returns the boundary of a geometry.                                                                                       |
| ST_BoundingDiagonal |             | Returns the diagonal of a geometry's bounding box.                                                                        |
| ST_CoordDim         | ✅          | Return the coordinate dimension of a geometry.                                                                            |
| ST_Dimension        |             | Returns the topological dimension of a geometry.                                                                          |
| ST_Dump             |             | Returns a set of geometry_dump rows for the components of a geometry.                                                     |
| ST_DumpPoints       |             | Returns a set of geometry_dump rows for the coordinates in a geometry.                                                    |
| ST_DumpSegments     |             | Returns a set of geometry_dump rows for the segments in a geometry.                                                       |
| ST_DumpRings        |             | Returns a set of geometry_dump rows for the exterior and interior rings of a Polygon.                                     |
| ST_EndPoint         |             | Returns the last point of a LineString or CircularLineString.                                                             |
| ST_Envelope         | ✅          | Returns a geometry representing the bounding box of a geometry.                                                           |
| ST_ExteriorRing     |             | Returns a LineString representing the exterior ring of a Polygon.                                                         |
| ST_GeometryN        |             | Return an element of a geometry collection.                                                                               |
| ST_GeometryType     |             | Returns the SQL-MM type of a geometry as text.                                                                            |
| ST_HasArc           |             | Tests if a geometry contains a circular arc                                                                               |
| ST_InteriorRingN    |             | Returns the Nth interior ring (hole) of a Polygon.                                                                        |
| ST_IsClosed         |             | Tests if a LineStrings's start and end points are coincident. For a PolyhedralSurface tests if it is closed (volumetric). |
| ST_IsCollection     |             | Tests if a geometry is a geometry collection type.                                                                        |
| ST_IsEmpty          |             | Tests if a geometry is empty.                                                                                             |
| ST_IsPolygonCCW     |             | Tests if Polygons have exterior rings oriented counter-clockwise and interior rings oriented clockwise.                   |
| ST_IsPolygonCW      |             | Tests if Polygons have exterior rings oriented clockwise and interior rings oriented counter-clockwise.                   |
| ST_IsRing           |             | Tests if a LineString is closed and simple.                                                                               |
| ST_IsSimple         |             | Tests if a geometry has no points of self-intersection or self-tangency.                                                  |
| ST_M                | ✅          | Returns the M coordinate of a Point.                                                                                      |
| ST_MemSize          |             | Returns the amount of memory space a geometry takes.                                                                      |
| ST_NDims            | ✅          | Returns the coordinate dimension of a geometry.                                                                           |
| ST_NPoints          |             | Returns the number of points (vertices) in a geometry.                                                                    |
| ST_NRings           |             | Returns the number of rings in a polygonal geometry.                                                                      |
| ST_NumGeometries    |             | Returns the number of elements in a geometry collection.                                                                  |
| ST_NumInteriorRings |             | Returns the number of interior rings (holes) of a Polygon.                                                                |
| ST_NumInteriorRing  |             | Returns the number of interior rings (holes) of a Polygon. Aias for ST_NumInteriorRings                                   |
| ST_NumPatches       |             | Return the number of faces on a Polyhedral Surface. Will return null for non-polyhedral geometries.                       |
| ST_NumPoints        |             | Returns the number of points in a LineString or CircularString.                                                           |
| ST_PatchN           |             | Returns the Nth geometry (face) of a PolyhedralSurface.                                                                   |
| ST_PointN           |             | Returns the Nth point in the first LineString or circular LineString in a geometry.                                       |
| ST_Points           |             | Returns a MultiPoint containing the coordinates of a geometry.                                                            |
| ST_StartPoint       | ✅          | Returns the first point of a LineString.                                                                                  |
| ST_Summary          |             | Returns a text summary of the contents of a geometry.                                                                     |
| ST_X                | ✅          | Returns the X coordinate of a Point.                                                                                      |
| ST_Y                | ✅          | Returns the Y coordinate of a Point.                                                                                      |
| ST_Z                | ✅          | Returns the Z coordinate of a Point.                                                                                      |
| ST_Zmflag           |             | Returns a code indicating the ZM coordinate dimension of a geometry.                                                      |
| ST_HasZ             |             | Checks if a geometry has a Z dimension.                                                                                   |
| ST_HasM             |             | Checks if a geometry has an M (measure) dimension.                                                                        |

### Geometry Editors

| Name                             | Implemented | Description                                                                                         |
| -------------------------------- | ----------- | --------------------------------------------------------------------------------------------------- |
| ST_AddPoint                      |             | Add a point to a LineString.                                                                        |
| ST_CollectionExtract             |             | Given a geometry collection, returns a multi-geometry containing only elements of a specified type. |
| ST_CollectionHomogenize          |             | Returns the simplest representation of a geometry collection.                                       |
| ST_Scroll                        |             | Change start point of a closed LineString.                                                          |
| ST_FlipCoordinates               |             | Returns a version of a geometry with X and Y axis flipped.                                          |
| ST_Force2D                       |             | Force the geometries into a "2-dimensional mode".                                                   |
| ST_Force3D                       |             | Force the geometries into XYZ mode. This is an alias for ST_Force3DZ.                               |
| ST_Force3DZ                      |             | Force the geometries into XYZ mode.                                                                 |
| ST_Force3DM                      |             | Force the geometries into XYM mode.                                                                 |
| ST_Force4D                       |             | Force the geometries into XYZM mode.                                                                |
| ST_ForceCollection               |             | Convert the geometry into a GEOMETRYCOLLECTION.                                                     |
| ST_ForcePolygonCCW               |             | Orients all exterior rings counter-clockwise and all interior rings clockwise.                      |
| ST_ForcePolygonCW                |             | Orients all exterior rings clockwise and all interior rings counter-clockwise.                      |
| ST_ForceSFS                      |             | Force the geometries to use SFS 1.1 geometry types only.                                            |
| ST_ForceRHR                      |             | Force the orientation of the vertices in a polygon to follow the Right-Hand-Rule.                   |
| ST_LineExtend                    |             | Returns a line extended forwards and backwards by specified distances.                              |
| ST_Multi                         |             | Return the geometry as a MULTI\* geometry.                                                          |
| ST_Normalize                     |             | Return the geometry in its canonical form.                                                          |
| ST_Project                       |             | Returns a point projected from a start point by a distance and bearing (azimuth).                   |
| ST_QuantizeCoordinates           |             | Sets least significant bits of coordinates to zero                                                  |
| ST_RemovePoint                   |             | Remove a point from a linestring.                                                                   |
| ST_RemoveRepeatedPoints          |             | Returns a version of a geometry with duplicate points removed.                                      |
| ST_RemoveIrrelevantPointsForView |             | Removes points that are irrelevant for rendering a specific rectangluar view of a geometry.         |
| ST_RemoveSmallParts              |             | Removes small parts (polygon rings or linestrings) of a geometry.                                   |
| ST_Reverse                       |             | Return the geometry with vertex order reversed.                                                     |
| ST_Segmentize                    |             | Returns a modified geometry/geography having no segment longer than a given distance.               |
| ST_SetPoint                      |             | Replace point of a linestring with a given point.                                                   |
| ST_ShiftLongitude                |             | Shifts the longitude coordinates of a geometry between -180..180 and 0..360.                        |
| ST_WrapX                         |             | Wrap a geometry around an X value.                                                                  |
| ST_SnapToGrid                    |             | Snap all points of the input geometry to a regular grid.                                            |
| ST_Snap                          |             | Snap segments and vertices of input geometry to vertices of a reference geometry.                   |
| ST_SwapOrdinates                 |             | Returns a version of the given geometry with given ordinate values swapped.                         |

### Geometry Validation

| Name             | Implemented | Description                                                                                  |
| ---------------- | ----------- | -------------------------------------------------------------------------------------------- |
| ST_IsValid       | ✅          | Tests if a geometry is well-formed in 2D.                                                    |
| ST_IsValidDetail |             | Returns a valid_detail row stating if a geometry is valid or if not a reason and a location. |
| ST_IsValidReason | ✅          | Returns text stating if a geometry is valid, or a reason for invalidity.                     |
| ST_MakeValid     |             | Attempts to make an invalid geometry valid without losing vertices.                          |

### Geometry Input

#### Well-Known Text (WKT)

| Name                | Implemented | Description                                                                                                                                           |
| ------------------- | ----------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| ST_BdPolyFromText   |             | Construct a Polygon given an arbitrary collection of closed linestrings as a MultiLineString Well-Known text representation.                          |
| ST_BdMPolyFromText  |             | Construct a MultiPolygon given an arbitrary collection of closed linestrings as a MultiLineString text representation Well-Known text representation. |
| ST_GeomCollFromText |             | Makes a collection Geometry from collection WKT with the given SRID. If SRID is not given, it defaults to 0.                                          |
| ST_GeomFromEWKT     |             | Return a specified ST_Geometry value from Extended Well-Known Text representation (EWKT).                                                             |
| ST_GeometryFromText | ✅          | Return a specified ST_Geometry value from Well-Known Text representation (WKT). This is an alias name for ST_GeomFromText                             |
| ST_GeomFromText     | ✅          | Return a specified ST_Geometry value from Well-Known Text representation (WKT).                                                                       |
| ST_LineFromText     |             | Makes a Geometry from WKT representation with the given SRID. If SRID is not given, it defaults to 0.                                                 |
| ST_MLineFromText    |             | Return a specified ST_MultiLineString value from WKT representation.                                                                                  |
| ST_MPointFromText   |             | Makes a Geometry from WKT with the given SRID. If SRID is not given, it defaults to 0.                                                                |
| ST_MPolyFromText    |             | Makes a MultiPolygon Geometry from WKT with the given SRID. If SRID is not given, it defaults to 0.                                                   |
| ST_PointFromText    |             | Makes a point Geometry from WKT with the given SRID. If SRID is not given, it defaults to unknown.                                                    |
| ST_PolygonFromText  |             | Makes a Geometry from WKT with the given SRID. If SRID is not given, it defaults to 0.                                                                |
| ST_WKTToSQL         | ✅          | Return a specified ST_Geometry value from Well-Known Text representation (WKT). This is an alias name for ST_GeomFromText                             |

#### Well-Known Binary (WKB)

| Name                 | Implemented | Description                                                                                                                                   |
| -------------------- | ----------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| ST_GeomFromEWKB      |             | Return a specified ST_Geometry value from Extended Well-Known Binary representation (EWKB).                                                   |
| ST_GeomFromWKB       | ✅          | Creates a geometry instance from a Well-Known Binary geometry representation (WKB) and optional SRID.                                         |
| ST_LineFromWKB       |             | Makes a LINESTRING from WKB with the given SRID                                                                                               |
| ST_LinestringFromWKB |             | Makes a geometry from WKB with the given SRID.                                                                                                |
| ST_PointFromWKB      |             | Makes a geometry from WKB with the given SRID                                                                                                 |
| ST_WKBToSQL          | ✅          | Return a specified ST_Geometry value from Well-Known Binary representation (WKB). This is an alias name for ST_GeomFromWKB that takes no srid |

#### Other Formats

| Name                       | Implemented | Description                                                                                            |
| -------------------------- | ----------- | ------------------------------------------------------------------------------------------------------ |
| ST_Box2dFromGeoHash        | ✅          | Return a BOX2D from a GeoHash string.                                                                  |
| ST_GeomFromGeoHash         |             | Return a geometry from a GeoHash string.                                                               |
| ST_GeomFromGML             |             | Takes as input GML representation of geometry and outputs a PostGIS geometry object                    |
| ST_GeomFromGeoJSON         |             | Takes as input a geojson representation of a geometry and outputs a PostGIS geometry object            |
| ST_GeomFromKML             |             | Takes as input KML representation of geometry and outputs a PostGIS geometry object                    |
| ST_GeomFromTWKB            |             | Creates a geometry instance from a TWKB ("Tiny Well-Known Binary") geometry representation.            |
| ST_GMLToSQL                |             | Return a specified ST_Geometry value from GML representation. This is an alias name for ST_GeomFromGML |
| ST_LineFromEncodedPolyline |             | Creates a LineString from an Encoded Polyline.                                                         |
| ST_PointFromGeoHash        | ✅          | Return a point from a GeoHash string.                                                                  |
| ST_FromFlatGeobufToTable   |             | Creates a table based on the structure of FlatGeobuf data.                                             |
| ST_FromFlatGeobuf          |             | Reads FlatGeobuf data.                                                                                 |

### Geometry Output

#### Well-Known Text (WKT)

| Name      | Implemented | Description                                                                                      |
| --------- | ----------- | ------------------------------------------------------------------------------------------------ |
| ST_AsEWKT |             | Return the Well-Known Text (WKT) representation of the geometry with SRID meta data.             |
| ST_AsText | ✅          | Return the Well-Known Text (WKT) representation of the geometry/geography without SRID metadata. |

#### Well-Known Binary (WKB)

| Name         | Implemented | Description                                                                                                   |
| ------------ | ----------- | ------------------------------------------------------------------------------------------------------------- |
| ST_AsBinary  | ✅          | Return the OGC/ISO Well-Known Binary (WKB) representation of the geometry/geography without SRID meta data.   |
| ST_AsEWKB    |             | Return the Extended Well-Known Binary (EWKB) representation of the geometry with SRID meta data.              |
| ST_AsHEXEWKB |             | Returns a Geometry in HEXEWKB format (as text) using either little-endian (NDR) or big-endian (XDR) encoding. |

#### Other Formats

| Name                 | Implemented | Description                                                             |
| -------------------- | ----------- | ----------------------------------------------------------------------- |
| ST_AsEncodedPolyline |             | Returns an Encoded Polyline from a LineString geometry.                 |
| ST_AsFlatGeobuf      |             | Return a FlatGeobuf representation of a set of rows.                    |
| ST_AsGeobuf          |             | Return a Geobuf representation of a set of rows.                        |
| ST_AsGeoJSON         |             | Return a geometry or feature in GeoJSON format.                         |
| ST_AsGML             |             | Return the geometry as a GML version 2 or 3 element.                    |
| ST_AsKML             |             | Return the geometry as a KML element.                                   |
| ST_AsLatLonText      |             | Return the Degrees, Minutes, Seconds representation of the given point. |
| ST_AsMVTGeom         |             | Transforms a geometry into the coordinate space of a MVT tile.          |
| ST_AsMVT             |             | Aggregate function returning a MVT representation of a set of rows.     |
| ST_AsSVG             |             | Returns SVG path data for a geometry.                                   |
| ST_AsTWKB            |             | Returns the geometry as TWKB, aka "Tiny Well-Known Binary"              |
| ST_GeoHash           | ✅          | Return a GeoHash representation of the geometry.                        |

### Operators

### Spatial Relationships

#### Topological Relationships

| Name                     | Implemented | Description                                                                                                                             |
| ------------------------ | ----------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| ST_3DIntersects          |             | Tests if two geometries spatially intersect in 3D - only for points, linestrings, polygons, polyhedral surface (area).                  |
| ST_Contains              | ✅          | Tests if every point of B lies in A, and their interiors have a point in common.                                                        |
| ST_ContainsProperly      |             | Tests if every point of B lies in the interior of A.                                                                                    |
| ST_CoveredBy             | ✅          | Tests if every point of A lies in B.                                                                                                    |
| ST_Covers                | ✅          | Tests if every point of B lies in A.                                                                                                    |
| ST_Crosses               | ✅          | Tests if two geometries have some, but not all, interior points in common.                                                              |
| ST_Disjoint              | ✅          | Tests if two geometries have no points in common.                                                                                       |
| ST_Equals                | ✅          | Tests if two geometries include the same set of points.                                                                                 |
| ST_Intersects            | ✅          | Tests if two geometries intersect (they have at least one point in common).                                                             |
| ST_LineCrossingDirection |             | Returns a number indicating the crossing behavior of two LineStrings.                                                                   |
| ST_OrderingEquals        |             | Tests if two geometries represent the same geometry and have points in the same directional order.                                      |
| ST_Overlaps              | ✅          | Tests if two geometries have the same dimension and intersect, but each has at least one point not in the other.                        |
| ST_Relate                |             | Tests if two geometries have a topological relationship matching an Intersection Matrix pattern, or computes their Intersection Matrix. |
| ST_RelateMatch           |             | Tests if a DE-9IM Intersection Matrix matches an Intersection Matrix pattern.                                                           |
| ST_Touches               | ✅          | Tests if two geometries have at least one point in common, but their interiors do not intersect.                                        |
| ST_Within                | ✅          | Tests if every point of A lies in B, and their interiors have a point in common.                                                        |

### Measurement Functions

| Name                    | Implemented | Description                                                                                                                    |
| ----------------------- | ----------- | ------------------------------------------------------------------------------------------------------------------------------ |
| ST_Area                 | ✅          | Returns the area of a polygonal geometry.                                                                                      |
| ST_Azimuth              |             | Returns the north-based azimuth of a line between two points.                                                                  |
| ST_Angle                |             | Returns the angle between two vectors defined by 3 or 4 points, or 2 lines.                                                    |
| ST_ClosestPoint         |             | Returns the 2D point on g1 that is closest to g2. This is the first point of the shortest line from one geometry to the other. |
| ST_3DClosestPoint       |             | Returns the 3D point on g1 that is closest to g2. This is the first point of the 3D shortest line.                             |
| ST_Distance             | ✅          | Returns the distance between two geometry or geography values.                                                                 |
| ST_3DDistance           |             | Returns the 3D cartesian minimum distance (based on spatial ref) between two geometries in projected units.                    |
| ST_DistanceSphere       |             | Returns minimum distance in meters between two lon/lat geometries using a spherical earth model.                               |
| ST_DistanceSpheroid     |             | Returns the minimum distance between two lon/lat geometries using a spheroidal earth model.                                    |
| ST_FrechetDistance      |             | Returns the Fréchet distance between two geometries.                                                                           |
| ST_HausdorffDistance    |             | Returns the Hausdorff distance between two geometries.                                                                         |
| ST_Length               | ✅          | Returns the 2D length of a linear geometry.                                                                                    |
| ST_Length2D             | ✅          | Returns the 2D length of a linear geometry. Alias for ST_Length                                                                |
| ST_3DLength             |             | Returns the 3D length of a linear geometry.                                                                                    |
| ST_LengthSpheroid       |             | Returns the 2D or 3D length/perimeter of a lon/lat geometry on a spheroid.                                                     |
| ST_LongestLine          |             | Returns the 2D longest line between two geometries.                                                                            |
| ST_3DLongestLine        |             | Returns the 3D longest line between two geometries                                                                             |
| ST_MaxDistance          |             | Returns the 2D largest distance between two geometries in projected units.                                                     |
| ST_3DMaxDistance        |             | Returns the 3D cartesian maximum distance (based on spatial ref) between two geometries in projected units.                    |
| ST_MinimumClearance     |             | Returns the minimum clearance of a geometry, a measure of a geometry's robustness.                                             |
| ST_MinimumClearanceLine |             | Returns the two-point LineString spanning a geometry's minimum clearance.                                                      |
| ST_Perimeter            |             | Returns the length of the boundary of a polygonal geometry or geography.                                                       |
| ST_Perimeter2D          |             | Returns the 2D perimeter of a polygonal geometry. Alias for ST_Perimeter.                                                      |
| ST_3DPerimeter          |             | Returns the 3D perimeter of a polygonal geometry.                                                                              |
| ST_ShortestLine         |             | Returns the 2D shortest line between two geometries                                                                            |
| ST_3DShortestLine       |             | Returns the 3D shortest line between two geometries                                                                            |

### Overlay Functions

| Name             | Implemented | Description                                                                                 |
| ---------------- | ----------- | ------------------------------------------------------------------------------------------- |
| ST_ClipByBox2D   |             | Computes the portion of a geometry falling within a rectangle.                              |
| ST_Difference    |             | Computes a geometry representing the part of geometry A that does not intersect geometry B. |
| ST_Intersection  |             | Computes a geometry representing the shared portion of geometries A and B.                  |
| ST_MemUnion      |             | Aggregate function which unions geometries in a memory-efficent but slower way              |
| ST_Node          |             | Nodes a collection of lines.                                                                |
| ST_Split         |             | Returns a collection of geometries created by splitting a geometry by another geometry.     |
| ST_Subdivide     |             | Computes a rectilinear subdivision of a geometry.                                           |
| ST_SymDifference |             | Computes a geometry representing the portions of geometries A and B that do not intersect.  |
| ST_UnaryUnion    |             | Computes the union of the components of a single geometry.                                  |
| ST_Union         |             | Computes a geometry representing the point-set union of the input geometries.               |

### Geometry Processing

| Name                        | Implemented | Description                                                                                       |
| --------------------------- | ----------- | ------------------------------------------------------------------------------------------------- |
| ST_Buffer                   |             | Computes a geometry covering all points within a given distance from a geometry.                  |
| ST_BuildArea                |             | Creates a polygonal geometry formed by the linework of a geometry.                                |
| ST_Centroid                 | ✅          | Returns the geometric center of a geometry.                                                       |
| ST_ChaikinSmoothing         |             | Returns a smoothed version of a geometry, using the Chaikin algorithm                             |
| ST_ConcaveHull              | ✅          | Computes a possibly concave geometry that contains all input geometry vertices                    |
| ST_ConvexHull               | ✅          | Computes the convex hull of a geometry.                                                           |
| ST_DelaunayTriangles        |             | Returns the Delaunay triangulation of the vertices of a geometry.                                 |
| ST_FilterByM                |             | Removes vertices based on their M value                                                           |
| ST_GeneratePoints           |             | Generates a multipoint of random points contained in a Polygon or MultiPolygon.                   |
| ST_GeometricMedian          |             | Returns the geometric median of a MultiPoint.                                                     |
| ST_LineMerge                |             | Return the lines formed by sewing together a MultiLineString.                                     |
| ST_MaximumInscribedCircle   |             | Computes the largest circle contained within a geometry.                                          |
| ST_LargestEmptyCircle       |             | Computes the largest circle not overlapping a geometry.                                           |
| ST_MinimumBoundingCircle    |             | Returns the smallest circle polygon that contains a geometry.                                     |
| ST_MinimumBoundingRadius    |             | Returns the center point and radius of the smallest circle that contains a geometry.              |
| ST_OrientedEnvelope         | ✅          | Returns a minimum-area rectangle containing a geometry.                                           |
| ST_OffsetCurve              |             | Returns an offset line at a given distance and side from an input line.                           |
| ST_PointOnSurface           | ✅          | Computes a point guaranteed to lie in a polygon, or on a geometry.                                |
| ST_Polygonize               |             | Computes a collection of polygons formed from the linework of a set of geometries.                |
| ST_ReducePrecision          |             | Returns a valid geometry with points rounded to a grid tolerance.                                 |
| ST_SharedPaths              |             | Returns a collection containing paths shared by the two input linestrings/multilinestrings.       |
| ST_Simplify                 | ✅          | Returns a simplified representation of a geometry, using the Douglas-Peucker algorithm.           |
| ST_SimplifyPreserveTopology | ✅          | Returns a simplified and valid representation of a geometry, using the Douglas-Peucker algorithm. |
| ST_SimplifyPolygonHull      |             | Computes a simplifed topology-preserving outer or inner hull of a polygonal geometry.             |
| ST_SimplifyVW               | ✅          | Returns a simplified representation of a geometry, using the Visvalingam-Whyatt algorithm         |
| ST_SetEffectiveArea         |             | Sets the effective area for each vertex, using the Visvalingam-Whyatt algorithm.                  |
| ST_TriangulatePolygon       |             | Computes the constrained Delaunay triangulation of polygons                                       |
| ST_VoronoiLines             |             | Returns the boundaries of the Voronoi diagram of the vertices of a geometry.                      |
| ST_VoronoiPolygons          |             | Returns the cells of the Voronoi diagram of the vertices of a geometry.                           |

### Coverages

| Name                    | Implemented | Description                                                                          |
| ----------------------- | ----------- | ------------------------------------------------------------------------------------ |
| ST_CoverageInvalidEdges |             | Window function that finds locations where polygons fail to form a valid coverage.   |
| ST_CoverageSimplify     |             | Window function that simplifies the edges of a polygonal coverage.                   |
| ST_CoverageUnion        |             | Computes the union of a set of polygons forming a coverage by removing shared edges. |

### Affine Transformations

| Name          | Implemented | Description                                                    |
| ------------- | ----------- | -------------------------------------------------------------- |
| ST_Affine     |             | Apply a 3D affine transformation to a geometry.                |
| ST_Rotate     |             | Rotates a geometry about an origin point.                      |
| ST_RotateX    |             | Rotates a geometry about the X axis.                           |
| ST_RotateY    |             | Rotates a geometry about the Y axis.                           |
| ST_RotateZ    |             | Rotates a geometry about the Z axis.                           |
| ST_Scale      |             | Scales a geometry by given factors.                            |
| ST_Translate  |             | Translates a geometry by given offsets.                        |
| ST_TransScale |             | Translates and scales a geometry by given offsets and factors. |

### Clustering Functions

| Name                      | Implemented | Description                                                                                                         |
| ------------------------- | ----------- | ------------------------------------------------------------------------------------------------------------------- |
| ST_ClusterDBSCAN          |             | Window function that returns a cluster id for each input geometry using the DBSCAN algorithm.                       |
| ST_ClusterIntersecting    |             | Aggregate function that clusters input geometries into connected sets.                                              |
| ST_ClusterIntersectingWin |             | Window function that returns a cluster id for each input geometry, clustering input geometries into connected sets. |
| ST_ClusterKMeans          |             | Window function that returns a cluster id for each input geometry using the K-means algorithm.                      |
| ST_ClusterWithin          |             | Aggregate function that clusters geometries by separation distance.                                                 |
| ST_ClusterWithinWin       |             | Window function that returns a cluster id for each input geometry, clustering using separation distance.            |

### Bounding Box Functions

| Name               | Implemented | Description                                                              |
| ------------------ | ----------- | ------------------------------------------------------------------------ |
| Box2D              | ✅          | Returns a BOX2D representing the 2D extent of a geometry.                |
| Box3D              | ✅          | Returns a BOX3D representing the 3D extent of a geometry.                |
| ST_EstimatedExtent |             | Returns the estimated extent of a spatial table.                         |
| ST_Expand          | ✅          | Returns a bounding box expanded from another bounding box or a geometry. |
| ST_Extent          | ✅          | Aggregate function that returns the bounding box of geometries.          |
| ST_3DExtent        |             | Aggregate function that returns the 3D bounding box of geometries.       |
| ST_MakeBox2D       | ✅          | Creates a BOX2D defined by two 2D point geometries.                      |
| ST_3DMakeBox       | ✅          | Creates a BOX3D defined by two 3D point geometries.                      |
| ST_XMax            | ✅          | Returns the X maxima of a 2D or 3D bounding box or a geometry.           |
| ST_XMin            | ✅          | Returns the X minima of a 2D or 3D bounding box or a geometry.           |
| ST_YMax            | ✅          | Returns the Y maxima of a 2D or 3D bounding box or a geometry.           |
| ST_YMin            | ✅          | Returns the Y minima of a 2D or 3D bounding box or a geometry.           |
| ST_ZMax            | ✅          | Returns the Z maxima of a 2D or 3D bounding box or a geometry.           |
| ST_ZMin            | ✅          | Returns the Z minima of a 2D or 3D bounding box or a geometry.           |

### Linear Referencing

| Name                       | Implemented | Description                                                                |
| -------------------------- | ----------- | -------------------------------------------------------------------------- |
| ST_LineInterpolatePoint    |             | Returns a point interpolated along a line at a fractional location.        |
| ST_3DLineInterpolatePoint  |             | Returns a point interpolated along a 3D line at a fractional location.     |
| ST_LineInterpolatePoints   |             | Returns points interpolated along a line at a fractional interval.         |
| ST_LineLocatePoint         |             | Returns the fractional location of the closest point on a line to a point. |
| ST_LineSubstring           |             | Returns the part of a line between two fractional locations.               |
| ST_LocateAlong             |             | Returns the point(s) on a geometry that match a measure value.             |
| ST_LocateBetween           |             | Returns the portions of a geometry that match a measure range.             |
| ST_LocateBetweenElevations |             | Returns the portions of a geometry that lie in an elevation (Z) range.     |
| ST_InterpolatePoint        |             | Returns the interpolated measure of a geometry closest to a point.         |
| ST_AddMeasure              |             | Interpolates measures along a linear geometry.                             |
