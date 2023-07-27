# JavaScript Bindings Architecture

The goal of the bindings is to be as slim as possible. As much as possible should be included in the core Rust code.

The JS bindings use [wasm-bindgen](https://rustwasm.github.io/docs/wasm-bindgen/) to generate a WebAssembly binary plus JavaScript functions (plus TypeScript types!) to interact with the Wasm.

## Arrays

...

One thing to keep an eye on is that the bindings have _another set_ of struct names that coincide with the core rust binding! So for example, there's a `LineStringArray` in the JS bindings. It needs to have that name so that JS gets that name, but this is a _different_ struct than the Rust core. So you can't use them interchangeably; you need to convert from one to the other (this conversion is `O(1)`).

## Scalars

Scalars are not yet implemented for JS. We need to figure out the right way to abstract over a scalar

## Algorithms

Algorithms are defined in individual modules which implement algorithms onto the existing wasm-bindgen structs.

Ideally the algorithm binding should be extremely minimal. For example, implementing `euclidean_length` in JS happens in [`euclidean_length.rs`](../../js/src/algorithm/geo/euclidean_length.rs). The _entire_ binding for this function is

```rs
#[wasm_bindgen]
impl LineStringArray {
    /// Calculation of the length of a Line
    #[wasm_bindgen(js_name = euclideanLength)]
    pub fn euclidean_length(&self) -> FloatArray {
        use geoarrow::algorithm::geo::EuclideanLength;
        FloatArray(EuclideanLength::euclidean_length(&self.0))
    }
}
```

Since the implementation is exactly the same for multiple geometry types, we use a macro to deduplicate the binding for multiple geometry array types.

In this example, you can see you need to apply `#[wasm_bindgen]` on the `impl`, as well as another `#[wasm_bindgen]` on the function itself. We rename the function for JS so that the Rust side can have idiomatic snake case naming, while the JS side has idiomatic camel case naming.

## Arrow FFI

Arrow defines a single memory specification for every implementation. This means that the way Arrow memory is laid out in WebAssembly's memory space is the same as in JavaScript's own memory. This means we can use the JS Arrow implementation to interpret Arrow memory from inside Wasm memory.

I [wrote more on this in a blog post](https://observablehq.com/@kylebarron/zero-copy-apache-arrow-with-webassembly), and have a [JS library here](https://github.com/kylebarron/arrow-js-ffi) that implements the reading across the Wasm boundary.

All that's needed on the Rust side is to create the structs that fulfill the [C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html). The code in the [`ffi` mod](../../js/src/ffi/) contains the `FFIArrowArray`, which stores pointers to both the [`ArrowSchema`](https://arrow.apache.org/docs/format/CDataInterface.html#the-arrowschema-structure) struct and the [`ArrowArray`](https://arrow.apache.org/docs/format/CDataInterface.html#the-arrowarray-structure) struct. Those can be read from JS using `arrow-js-ffi`.

## API Documentation

Anything documented with `///` in Rust gets converted to a [JSDoc comment](https://github.com/jsdoc/jsdoc) within the exported TypeScript `.d.ts` type file. We then use [TypeDoc](https://github.com/TypeStrong/typedoc) to generate an API documentation website from these types and docstrings.
