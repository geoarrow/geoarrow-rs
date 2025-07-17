# Contributing

Get [rust](https://rustup.rs/).
Then:

```shell
git clone git@github.com:geoarrow/geoarrow-rs.git
cd geoarrow-rs
cargo test --all-features
```

Use Github [pull requests](https://github.com/geoarrow/geoarrow-rs/pulls) to provide code and documentation.
Use [issues](https://github.com/geoarrow/geoarrow-rs/issues) to report bugs or request features.

## Conventional commits

We require all PRs to follow [conventional commits](https://www.conventionalcommits.org/en/v1.0.0/).
We squash merge, so only the PR title needs to follow the standard.
You'll get the idea by looking at a list of recent commits.
You can find our allowed task types [here](.github/workflows/conventional-commits.yml).

## Build issues

If you get the following error:

```text
  CMake Error at cmake/Ccache.cmake:10 (cmake_minimum_required):
    Compatibility with CMake < 3.5 has been removed from CMake.

    Update the VERSION argument <min> value.  Or, use the <min>...<max> syntax
    to tell CMake that the project requires at least <min> but has been updated
    to work with policies introduced by <max> or earlier.

    Or, add -DCMAKE_POLICY_VERSION_MINIMUM=3.5 to try configuring anyway.
  Call Stack (most recent call first):
    CMakeLists.txt:130 (include)
```

Fix by following those instructions:

```shell
export CMAKE_POLICY_VERSION_MINIMUM=3.5
cargo test --all-features
```
