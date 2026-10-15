#!/usr/bin/env bash
# Build this plugin. Node >= 20. TypeScript / tsdown are fetched via npx when absent.
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f tsconfig.json ]; then npx -y -p typescript tsc -p tsconfig.json; fi
if [ -f tsdown.config.ts ]; then npx -y tsdown; fi
echo "build ok: $(pwd)/lib"
