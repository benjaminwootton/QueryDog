#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Installing querydog CLI..."

# Install dependencies
echo "Installing dependencies..."
npm install

# Build TypeScript
echo "Building..."
npm run build

# Link globally
echo "Linking globally..."
npm link

echo ""
echo "Done! You can now run 'querydog' from anywhere."
echo ""
echo "Examples:"
echo "  querydog -e 2 tables"
echo "  querydog --env production queries --mode slowest"
echo "  querydog tui"
