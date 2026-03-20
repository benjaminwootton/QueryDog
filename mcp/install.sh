#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Installing QueryDog MCP server..."

npm install
npm run build

echo ""
echo "Done! Add to Claude Desktop config:"
echo ""
echo '  "querydog": {'
echo '    "command": "node",'
echo '    "args": ["'$SCRIPT_DIR'/dist/index.js"]'
echo '  }'
