#! /usr/bin/env bash
rm -rf tmp_build pkg
mkdir -p tmp_build

if [ "$ENV" == "DEV" ]; then
   BUILD="--dev"
   FLAGS="--features debug"
else
   BUILD="--release"
   FLAGS=""
fi

######################################
# Build node version into tmp_build/node
echo "Building node target"
wasm-pack build \
  $BUILD \
  --out-dir tmp_build/node \
  --out-name index \
  --target nodejs \
  --features geodesy \
  $FLAGS

# Build web version into tmp_build/esm
echo "Building esm target"
wasm-pack build \
  $BUILD \
  --out-dir tmp_build/esm \
  --out-name index \
  --target web \
  --features geodesy \
  $FLAGS

# Build bundler version into tmp_build/bundler
echo "Building bundler target"
wasm-pack build \
  $BUILD \
  --out-dir tmp_build/bundler \
  --out-name index \
  --target bundler \
  --features geodesy \
  $FLAGS

# Copy files into pkg/
mkdir -p pkg/{node,esm,bundler}

cp tmp_build/bundler/index* pkg/bundler/
cp tmp_build/esm/index* pkg/esm
cp tmp_build/node/index* pkg/node

cp tmp_build/bundler/{package.json,LICENSE,README.md} pkg/

# Create minimal package.json in esm/ folder with type: module
echo '{"type": "module"}' > pkg/esm/package.json

# Update files array in package.json using JQ
# Set module field to bundler/arrow1.js
# Set types field to bundler/arrow1.d.ts
jq '.files = ["*"] | .module="bundler/index.js" | .types="bundler/index.d.ts"' pkg/package.json > pkg/package.json.tmp

# Overwrite existing package.json file
mv pkg/package.json.tmp pkg/package.json

rm -rf tmp_build
