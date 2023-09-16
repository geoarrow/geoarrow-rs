# Rust Setup

## Install Rust

Follow the [official instructions](https://www.rust-lang.org/tools/install) to install Rust on your system.

## Editor environment

I use and recommend VSCode with the stellar [Rust Analyzer](https://marketplace.visualstudio.com/items?itemName=rust-lang.rust-analyzer) extension.

Note that Rust Analyzer works with the _currently active Cargo workspace_. This means that if you open this repository from the root, Rust Analyzer completions will work for the pure-Rust library, but **not for the JavaScript bindings** because the JS bindings are a _separate Cargo project_. You need to open VSCode from the `/js` folder in order for the completions to work for the JS bindings.

## System dependencies

One of the examples uses GDAL, and so you might need to have GDAL (3.6+) installed on your system, even if you're not running that example (I'm not sure).

No other system dependencies are required to my knowledge.

## Run tests

We use the default Cargo test runner, so just run:

```bash
cargo test --all-features
```

## Run linter

We use the default Cargo linter, so just run:

```bash
cargo clippy --all-features
```

## Format code

Cargo includes a default code formatter. If the code hasn't been formatted, it won't pass CI.

```bash
cargo fmt
```

## View crate documentation

Any object documented with `///` will be automatically documented.

To see the current crate documentation locally, run

```bash
cargo doc --open
```

See the sections in the [Rust Book](https://doc.rust-lang.org/book/ch14-02-publishing-to-crates-io.html#making-useful-documentation-comments) and the [Rustdoc guide](https://doc.rust-lang.org/rustdoc/index.html) for all the syntax supported in code comments.
