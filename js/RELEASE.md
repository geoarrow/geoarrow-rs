## Release docs

```bash
export PATH="/opt/homebrew/opt/llvm/bin/:$PATH"
export CC=/opt/homebrew/opt/llvm/bin/clang
export AR=/opt/homebrew/opt/llvm/bin/llvm-ar
```

```
yarn build
cd pkg
npm publish
```

```
yarn build:geoparquet
cd pkg
npm publish
```

```
yarn build:flatgeobuf
cd pkg
npm publish
```
