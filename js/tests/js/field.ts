// @ts-nocheck

import * as arrow from "apache-arrow";
import { assert } from "./vector";

interface Flags {
  nullable: boolean;
  dictionaryOrdered: boolean;
  mapKeysSorted: boolean;
}

const UTF8_DECODER = new TextDecoder("utf-8");
// Note: it looks like duration types don't yet exist in Arrow JS
const formatMapping: Record<string, arrow.DataType | undefined> = {
  n: new arrow.Null(),
  b: new arrow.Bool(),
  c: new arrow.Int8(),
  C: new arrow.Uint8(),
  s: new arrow.Int16(),
  S: new arrow.Uint16(),
  i: new arrow.Int32(),
  I: new arrow.Uint32(),
  l: new arrow.Int64(),
  L: new arrow.Uint64(),
  e: new arrow.Float16(),
  f: new arrow.Float32(),
  g: new arrow.Float64(),
  z: new arrow.Binary(),
  // Z: Type.LargeBinary,
  u: new arrow.Utf8(),
  // U: Type.LargeUtf8,
  tdD: new arrow.DateDay(),
  tdm: new arrow.DateMillisecond(),
  tts: new arrow.TimeSecond(),
  ttm: new arrow.TimeMillisecond(),
  ttu: new arrow.TimeMicrosecond(),
  ttn: new arrow.TimeNanosecond(),
  tiM: new arrow.Interval(arrow.IntervalUnit.YEAR_MONTH),
  tiD: new arrow.Interval(arrow.IntervalUnit.DAY_TIME),
  tin: new arrow.Interval(arrow.IntervalUnit.MONTH_DAY_NANO),
};

/**
Parse an [`ArrowSchema`](https://arrow.apache.org/docs/format/CDataInterface.html#the-arrowschema-structure) C FFI struct into an `arrow.Field` instance. The `Field` is necessary for later using `parseVector` below.

- `buffer` (`ArrayBuffer`): The [`WebAssembly.Memory`](https://developer.mozilla.org/en-US/docs/WebAssembly/JavaScript_interface/Memory) instance to read from.
- `ptr` (`number`): The numeric pointer in `buffer` where the C struct is located.
 */
export function parseField(buffer: ArrayBuffer, ptr: number): arrow.Field {
  const dataView = new DataView(buffer);

  const formatPtr = dataView.getUint32(ptr, true);
  const formatString = parseNullTerminatedString(dataView, formatPtr);
  const namePtr = dataView.getUint32(ptr + 4, true);
  const metadataPtr = dataView.getUint32(ptr + 8, true);

  const name = parseNullTerminatedString(dataView, namePtr);
  const metadata = parseMetadata(dataView, metadataPtr);

  // Extra 4 to be 8-byte aligned
  const flags = parseFlags(dataView.getBigInt64(ptr + 16, true));
  const nChildren = dataView.getBigInt64(ptr + 24, true);

  const ptrToChildrenPtrs = dataView.getUint32(ptr + 32, true);
  const childrenFields: arrow.Field[] = new Array(Number(nChildren));
  for (let i = 0; i < nChildren; i++) {
    childrenFields[i] = parseField(
      buffer,
      dataView.getUint32(ptrToChildrenPtrs + i * 4, true)
    );
  }

  const primitiveType = formatMapping[formatString];
  if (primitiveType) {
    return new arrow.Field(name, primitiveType, flags.nullable, metadata);
  }

  // decimal
  if (formatString.slice(0, 2) === "d:") {
    const parts = formatString.slice(2).split(",");
    const precision = parseInt(parts[0]);
    const scale = parseInt(parts[1]);
    const bitWidth = parts[2] ? parseInt(parts[2]) : undefined;

    const type = new arrow.Decimal(scale, precision, bitWidth);
    return new arrow.Field(name, type, flags.nullable, metadata);
  }

  // timestamp
  if (formatString.slice(0, 2) === "ts") {
    let timeUnit: arrow.TimeUnit | null = null;
    switch (formatString[2]) {
      case "s":
        timeUnit = arrow.TimeUnit.SECOND;
        break;
      case "m":
        timeUnit = arrow.TimeUnit.MILLISECOND;
        break;
      case "u":
        timeUnit = arrow.TimeUnit.MICROSECOND;
        break;
      case "n":
        timeUnit = arrow.TimeUnit.NANOSECOND;
        break;

      default:
        throw new Error(`invalid timestamp ${formatString}`);
    }

    assert(formatString[3] === ":");
    let timezone: string | null = null;
    if (formatString.length > 4) {
      timezone = formatString.slice(4);
    }

    const type = new arrow.Timestamp(timeUnit, timezone);
    return new arrow.Field(name, type, flags.nullable, metadata);
  }

  // struct
  if (formatString === "+s") {
    const type = new arrow.Struct(childrenFields);
    return new arrow.Field(name, type, flags.nullable, metadata);
  }

  // list
  if (formatString === "+l") {
    assert(childrenFields.length === 1);
    const type = new arrow.List(childrenFields[0]);
    return new arrow.Field(name, type, flags.nullable, metadata);
  }

  // FixedSizeBinary
  if (formatString.slice(0, 2) === "w:") {
    // The size of the binary is the integer after the colon
    const byteWidth = parseInt(formatString.slice(2));
    const type = new arrow.FixedSizeBinary(byteWidth);
    return new arrow.Field(name, type, flags.nullable, metadata);
  }

  // FixedSizeList
  if (formatString.slice(0, 3) === "+w:") {
    assert(childrenFields.length === 1);
    // The size of the list is the integer after the colon
    const innerSize = parseInt(formatString.slice(3));
    const type = new arrow.FixedSizeList(innerSize, childrenFields[0]);
    return new arrow.Field(name, type, flags.nullable, metadata);
  }

  throw new Error(`Unsupported format: ${formatString}`);
}

function parseFlags(flag: bigint): Flags {
  if (flag === 0n) {
    return {
      nullable: false,
      dictionaryOrdered: false,
      mapKeysSorted: false,
    };
  }

  // https://stackoverflow.com/a/9954810
  let parsed = flag.toString(2);
  return {
    nullable: parsed[0] === "1" ? true : false,
    dictionaryOrdered: parsed[1] === "1" ? true : false,
    mapKeysSorted: parsed[2] === "1" ? true : false,
  };
}

/** Parse a null-terminated C-style string */
function parseNullTerminatedString(
  dataView: DataView,
  ptr: number,
  maxBytesToRead: number = Infinity
): string {
  const maxPtr = Math.min(ptr + maxBytesToRead, dataView.byteLength);
  let end = ptr;
  while (end < maxPtr && dataView.getUint8(end) !== 0) {
    end += 1;
  }

  return UTF8_DECODER.decode(new Uint8Array(dataView.buffer, ptr, end - ptr));
}

/**
 * Parse field metadata
 *
 * The metadata format is described here:
 * https://arrow.apache.org/docs/format/CDataInterface.html#c.ArrowSchema.metadata
 */
function parseMetadata(
  dataView: DataView,
  ptr: number
): Map<string, string> | null {
  const numEntries = dataView.getInt32(ptr, true);
  if (numEntries === 0) {
    return null;
  }

  const metadata: Map<string, string> = new Map();

  ptr += 4;
  for (let i = 0; i < numEntries; i++) {
    const keyByteLength = dataView.getInt32(ptr, true);
    ptr += 4;
    const key = UTF8_DECODER.decode(
      new Uint8Array(dataView.buffer, ptr, keyByteLength)
    );
    ptr += keyByteLength;

    const valueByteLength = dataView.getInt32(ptr, true);
    ptr += 4;
    const value = UTF8_DECODER.decode(
      new Uint8Array(dataView.buffer, ptr, valueByteLength)
    );
    ptr += valueByteLength;

    metadata.set(key, value);
  }

  return metadata;
}
