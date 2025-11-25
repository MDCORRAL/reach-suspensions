#!/usr/bin/env bash
set -euo pipefail

# This script creates a local Python virtual environment under .venv and
# installs the graph_scripts dependencies without touching the system
# (Homebrew-managed) Python installation. Use PYTHON_BIN to pick an
# alternative interpreter (e.g., python3.12) if needed.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${ROOT_DIR}/.venv"
PYTHON_BIN="${PYTHON_BIN:-python3}"

if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
  echo "Python interpreter not found: ${PYTHON_BIN}" >&2
  exit 1
fi

# Create the virtual environment if it does not exist.
if [[ ! -d "${VENV_DIR}" ]]; then
  echo "Creating virtual environment at ${VENV_DIR}" >&2
  "${PYTHON_BIN}" -m venv "${VENV_DIR}"
else
  echo "Using existing virtual environment at ${VENV_DIR}" >&2
fi

# Activate and install dependencies.
# shellcheck disable=SC1090
source "${VENV_DIR}/bin/activate"

python -m pip install --upgrade pip
python -m pip install -r "${ROOT_DIR}/graph_scripts/requirements.txt"

echo "\nDone. Activate the environment with:"
echo "  source ${VENV_DIR}/bin/activate"
echo "Set RETICULATE_PYTHON for RStudio sessions with:"
echo "  Sys.setenv(RETICULATE_PYTHON = '${VENV_DIR}/bin/python')"
