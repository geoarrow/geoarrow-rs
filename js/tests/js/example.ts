import * as test from "tape";
import * as geoarrow from "geoarrow-wasm";
import { readFileSync } from "fs";
import { RecordBatch, Table, tableFromIPC, tableToIPC } from "apache-arrow";
import { parseField } from "./field";
import { parseVector } from "./vector";
// let x = await import("arrow-js-ffi");
// import {parseField, parseVector} from "arrow-js-ffi";

geoarrow.set_panic_hook();

// @ts-expect-error
const WASM_MEMORY: WebAssembly.Memory = geoarrow.__wasm.memory;

test("hello world", (t) => {
  let xs = new Float64Array([1, 2, 3, 4]);
  let ys = new Float64Array([5, 6, 7, 8]);
  let separatedCoords = new geoarrow.SeparatedCoordBuffer(xs, ys);
  let coords = geoarrow.CoordBuffer.from_separated_coords(separatedCoords);
  let pointArray = new geoarrow.PointArray(coords, null);

  let xOffset = new Float64Array([1, 2, 3, 4]);
  let yOffset = new Float64Array([1, 2, 3, 4]);

  let translatedPoints = pointArray.translate(
    geoarrow.BroadcastableFloat.from_array(xOffset),
    geoarrow.BroadcastableFloat.from_array(yOffset)
  );

  let ffiArray = translatedPoints.to_ffi();
  const field = parseField(WASM_MEMORY.buffer, ffiArray.field_addr());
  const vector = parseVector(
    WASM_MEMORY.buffer,
    ffiArray.array_addr(),
    field.type
  );

  console.log(field.metadata);
  console.log(vector.getChildAt(0).toArray());

  t.end();
});
