#! /usr/bin/env bash
rm -rf tmp_build pkg
mkdir -p tmp_build

# TODO:
# OUT_ROOT_DIR = tmp_build if not already set

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
  $FEATURES \
  $FLAGS

# Build web version into tmp_build/esm
echo "Building esm target"
wasm-pack build \
  $BUILD \
  --out-dir tmp_build/esm \
  --out-name index \
  --target web \
  $FEATURES \
  $FLAGS

# Build bundler version into tmp_build/bundler
echo "Building bundler target"
wasm-pack build \
  $BUILD \
  --out-dir tmp_build/bundler \
  --out-name index \
  --target bundler \
  $FEATURES \
  $FLAGS

# Copy files into pkg/
mkdir -p pkg/{node,esm,bundler}

cp tmp_build/bundler/index* pkg/bundler/
cp tmp_build/esm/index* pkg/esm
cp tmp_build/node/index* pkg/node

cp tmp_build/bundler/{package.json,LICENSE_APACHE,LICENSE_MIT,README.md} pkg/

# Create minimal package.json in esm/ folder with type: module
echo '{"type": "module"}' > pkg/esm/package.json

# A package.json in node/ lets npm resolve a file: install. wasm-pack emits
# CommonJS for the nodejs target, which is the default without a "type" field.
echo '{"main": "index.js", "types": "index.d.ts"}' > pkg/node/package.json

# Root package.json: name, files, the legacy module/types fields, and
# conditional exports (#422): node resolves the CommonJS build, everything else
# the ESM build. Relative asset paths inside the package bypass the export map.
jq --arg name "$NAME" '.files = ["*"]
  | .module = "bundler/index.js"
  | .types = "bundler/index.d.ts"
  | .name = $name
  | .exports = {
      ".": {
        "node": {"types": "./node/index.d.ts", "default": "./node/index.js"},
        "types": "./esm/index.d.ts",
        "default": "./esm/index.js"
      },
      "./bundler": {"types": "./bundler/index.d.ts", "default": "./bundler/index.js"},
      "./esm": {"types": "./esm/index.d.ts", "default": "./esm/index.js"},
      "./node": {"types": "./node/index.d.ts", "default": "./node/index.js"}
    }' pkg/package.json > pkg/package.json.tmp

# Overwrite existing package.json file
mv pkg/package.json.tmp pkg/package.json

rm -rf tmp_build
