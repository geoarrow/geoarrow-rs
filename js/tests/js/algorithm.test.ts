import * as geoarrow from "../../pkg/node";
import { makeVector } from "apache-arrow";
import { parseData, parseField } from "arrow-js-ffi";
import { it, expect } from "vitest";

geoarrow.set_panic_hook();

const UNIT_SQUARE: number[][] = [
  [0, 0],
  [1, 0],
  [1, 1],
  [0, 1],
  [0, 0],
];

// A WKB geometry, little endian: the byte order, the type code, the count of
// rings for a polygon, then the count of coordinates and the coordinates. A
// point carries neither count. The type code carries the dimension: 3000 above
// the base code is XYZM.
function wkb(baseType: 1 | 2 | 3, coords: number[][]): Uint8Array {
  const ordinates = coords[0].length;
  const counts = baseType - 1;
  const buf = Buffer.alloc(1 + 4 + counts * 4 + coords.length * ordinates * 8);
  let off = 0;
  buf.writeUInt8(1, off);
  off += 1;
  buf.writeUInt32LE(ordinates === 4 ? baseType + 3000 : baseType, off);
  off += 4;
  if (baseType === 3) {
    buf.writeUInt32LE(1, off);
    off += 4;
  }
  if (baseType !== 1) {
    buf.writeUInt32LE(coords.length, off);
    off += 4;
  }
  for (const coord of coords) {
    for (const ordinate of coord) {
      buf.writeDoubleLE(ordinate, off);
      off += 8;
    }
  }
  return new Uint8Array(buf);
}

// Each vertex takes a different Z and M, thus a swap of the two, or an index one
// off, changes the payload.
function unitSquareZm(): Uint8Array {
  const coords = UNIT_SQUARE.map(([x, y], i) => [x, y, 7.5 + i, 42 + 10 * i]);
  coords[coords.length - 1] = coords[0];
  return wkb(3, coords);
}

function square(): geoarrow.GeoArrowData {
  return geoarrow.GeoArrowData.fromWkb([wkb(3, UNIT_SQUARE)]);
}

function boundary(): geoarrow.GeoArrowData {
  return geoarrow.GeoArrowData.fromWkb([wkb(2, UNIT_SQUARE)]);
}

function point(x: number, y: number): geoarrow.GeoArrowData {
  return geoarrow.GeoArrowData.fromWkb([wkb(1, [[x, y]])]);
}

function parseFfi(result: {
  toFFI(): { schemaAddr(): number; arrayAddr(): number };
}) {
  const ffi = result.toFFI();
  const memory = geoarrow.wasmMemory();
  const field = parseField(memory.buffer, ffi.schemaAddr());
  return { field, data: parseData(memory.buffer, ffi.arrayAddr(), field.type) };
}

function values(result: { toTypedArray(): unknown }): Float64Array {
  return result.toTypedArray() as Float64Array;
}

it("signedArea: a counter clockwise unit square is 1.0", () => {
  const areas = values(geoarrow.signedArea(square()));
  expect(areas.length).toBe(1);
  expect(areas[0]).toBeCloseTo(1.0, 12);
});

it("euclideanLength: the boundary of the unit square is 4.0", () => {
  expect(values(geoarrow.euclideanLength(boundary()))[0]).toBeCloseTo(4.0, 12);
});

it("dimensions: a polygon reports 2", () => {
  const dims = geoarrow.dimensions(square()).toTypedArray() as Int32Array;
  expect(dims.length).toBe(1);
  expect(dims[0]).toBe(2);
});

it("chamberlainDuquetteArea: the signed and unsigned areas agree in magnitude", () => {
  const signed = values(geoarrow.chamberlainDuquetteSignedArea(square()));
  const unsigned = values(geoarrow.chamberlainDuquetteUnsignedArea(square()));
  expect(Math.abs(signed[0])).toBeCloseTo(unsigned[0], 6);
});

it.each([
  ["geodesicLength", geoarrow.geodesicLength],
  ["haversineLength", geoarrow.haversineLength],
  ["rhumbLength", geoarrow.rhumbLength],
] as const)("%s: a line on the sphere is longer than nothing", (_name, run) => {
  const lengths = values(run(boundary()));
  expect(lengths.length).toBe(1);
  expect(lengths[0]).toBeGreaterThan(0);
});

it.each([
  ["euclideanDistance", geoarrow.euclideanDistance, square],
  ["hausdorffDistance", geoarrow.hausdorffDistance, square],
  ["frechetDistance", geoarrow.frechetDistance, boundary],
] as const)("%s: a geometry to itself is 0.0", (_name, run, make) => {
  expect(values(run(make(), make()))[0]).toBeCloseTo(0, 12);
});

it("lineLocatePoint: a point beside the line falls between 0 and 1", () => {
  const fraction = values(
    geoarrow.lineLocatePoint(boundary(), point(1.0, 0.5)),
  )[0];
  expect(fraction).toBeGreaterThanOrEqual(0);
  expect(fraction).toBeLessThanOrEqual(1);
});

it("centroid: the centroid of the unit square reads back as (0.5, 0.5)", () => {
  const result = geoarrow.centroid(square());
  expect(result.numRows()).toBe(1);

  const { field, data } = parseFfi(result);
  expect(field.metadata.get("ARROW:extension:name")).toBe("geoarrow.point");
  // An interleaved geoarrow.point is a FixedSizeList<2, f64>, thus one child
  // buffer holds the x and y of every row.
  const coords = data.children[0].values as Float64Array;
  expect(coords[0]).toBeCloseTo(0.5, 12);
  expect(coords[1]).toBeCloseTo(0.5, 12);
});

it.each([
  ["interiorPoint", () => geoarrow.interiorPoint(square())],
  ["minimumRotatedRect", () => geoarrow.minimumRotatedRect(square())],
  ["boundingRect", () => geoarrow.boundingRect(square())],
  ["center", () => geoarrow.center(square())],
  ["concaveHull", () => geoarrow.concaveHull(square(), 2.0)],
  ["buffer", () => geoarrow.buffer(square(), 0.5)],
  ["densify", () => geoarrow.densify(boundary(), 0.25)],
  ["chaikinSmoothing", () => geoarrow.chaikinSmoothing(boundary(), 2)],
  ["lineInterpolatePoint", () => geoarrow.lineInterpolatePoint(boundary(), 0.5)],
  ["closestPoint", () => geoarrow.closestPoint(square(), point(2.0, 0.5))],
  ["rotate", () => geoarrow.rotate(square(), 45.0, 0.5, 0.5)],
  ["scale", () => geoarrow.scale(square(), 2.0, 2.0, 0.5, 0.5)],
  ["skew", () => geoarrow.skew(square(), 10.0, 5.0, 0.5, 0.5)],
  ["translate", () => geoarrow.translate(square(), 3.0, -2.0)],
  ["intersection", () => geoarrow.intersection(square(), square())],
  ["union", () => geoarrow.union(square(), square())],
  ["difference", () => geoarrow.difference(square(), square())],
  ["xor", () => geoarrow.xor(square(), square())],
] as const)("%s: gives one row that is not null", (_name, run) => {
  const result = run();
  expect(result.numRows()).toBe(1);
  expect(result.numNulls()).toBe(0);
});

// arrow-js-ffi does not parse the union container that `fromWkb` builds, thus
// these assertions cover the crossing of the wasm boundary only.
it.each([
  ["simplify", (d: geoarrow.GeoArrowData) => geoarrow.simplify(d, 0.0)],
  ["simplifyVw", (d: geoarrow.GeoArrowData) => geoarrow.simplifyVw(d, 0.0)],
  [
    "simplifyVwPreserve",
    (d: geoarrow.GeoArrowData) => geoarrow.simplifyVwPreserve(d, 0.0),
  ],
  ["convexHull", (d: geoarrow.GeoArrowData) => geoarrow.convexHull(d)],
] as const)("%s: an XYZM polygon crosses the wasm boundary", (_name, run) => {
  const data = geoarrow.GeoArrowData.fromWkb([unitSquareZm()]);
  expect(data.dimension()).toBe("Mixed");

  const result = run(data);
  expect(result.numRows()).toBe(1);
  expect(result.numNulls()).toBe(0);
});

it.each([
  ["contains", () => geoarrow.contains(square(), square())],
  ["intersects", () => geoarrow.intersects(square(), square())],
  [
    "relateBoolean",
    () => geoarrow.relateBoolean(square(), square(), "T*F**F***"),
  ],
] as const)("%s: a geometry against itself is true", (_name, run) => {
  const { data } = parseFfi(run());
  expect(data.length).toBe(1);
  expect(makeVector(data).get(0)).toBe(true);
});

it("isEmpty: a unit square is not empty", () => {
  const { data } = parseFfi(geoarrow.isEmpty(square()));
  expect(makeVector(data).get(0)).toBe(false);
});

it("relateBoolean: a malformed DE-9IM pattern is rejected", () => {
  expect(() => geoarrow.relateBoolean(square(), square(), "nope")).toThrow();
});
