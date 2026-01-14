
### 🛠️ Build with `wasm-pack build`

```
wasm-pack build
```

### 🔬 Test in Headless Browsers with `wasm-pack test`

```
wasm-pack test --headless --firefox
```

### 🎁 Publish to NPM with `wasm-pack publish`

```
wasm-pack publish
```


## Nix Flake Environment

We have a pre-configured nix flake environment with all necessary
dependencies. Enable it by:

```
nix develop
```

Or you can make it persist with `direnv` and `use flake` in `.envrc` file at top
level of this repo.
