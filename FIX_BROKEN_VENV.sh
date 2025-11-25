#!/usr/bin/env bash
# FIX_BROKEN_VENV.sh
#
# This script removes a broken .venv directory (often created by R's reticulate)
# and creates a fresh Python virtual environment with all required packages.
#
# Usage: bash FIX_BROKEN_VENV.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${SCRIPT_DIR}/.venv"

echo "=== Fixing Broken Python Virtual Environment ==="
echo ""

# Step 1: Remove existing broken .venv
if [ -d "${VENV_DIR}" ]; then
    echo "🗑️  Removing broken .venv directory..."
    rm -rf "${VENV_DIR}"
    echo "✓ Removed ${VENV_DIR}"
else
    echo "ℹ️  No existing .venv found (this is fine)"
fi

echo ""

# Step 2: Create fresh virtual environment
echo "🔨 Creating fresh virtual environment..."
bash "${SCRIPT_DIR}/scripts/utilities/setup_python_env.sh"

echo ""
echo "=== Done! ==="
echo ""
echo "Next steps:"
echo "1. Activate the environment: source .venv/bin/activate"
echo "2. Test imports: python -c 'import matplotlib; import pandas; print(\"✅ Success!\")'"
echo ""
echo "For R/RStudio:"
echo "1. Restart R session completely"
echo "2. Run: source('configure_python_env.R')"
