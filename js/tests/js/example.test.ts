import * as geoarrow from "../../pkg/node";
import { parseField, parseVector } from "arrow-js-ffi";
import { it } from "vitest";

geoarrow.set_panic_hook();

const WASM_MEMORY = geoarrow.wasmMemory();

it("hello world", () => {
  // let xs = new Float64Array([1, 2, 3, 4]);
  // let ys = new Float64Array([5, 6, 7, 8]);
  // let separatedCoords = new geoarrow.SeparatedCoordBuffer(xs, ys);
  // let coords = geoarrow.CoordBuffer.fromSeparatedCoords(separatedCoords);
  // let pointArray = new geoarrow.PointData(coords);

  // let xOffset = new Float64Array([1, 2, 3, 4]);
  // let yOffset = new Float64Array([1, 2, 3, 4]);

  // let translatedPoints = pointArray.translate(
  //   geoarrow.BroadcastableFloat.fromArray(xOffset),
  //   geoarrow.BroadcastableFloat.fromArray(yOffset)
  // );

  // let ffiArray = translatedPoints.toFFI();
  // const field = parseField(WASM_MEMORY.buffer, ffiArray.field_addr());
  // const vector = parseVector(
  //   WASM_MEMORY.buffer,
  //   ffiArray.array_addr(),
  //   field.type
  // );

  // console.log(field.metadata);
  // console.log(vector.getChildAt(0)?.toArray());
});
