# Fix: Graph Scripts Python Environment Setup

**Date**: 2025-11-25
**Issue**: Python scripts in `graph_scripts/` failed when run from R (via reticulate)
**Status**: ✅ RESOLVED

---

## Problem

When attempting to run Python scripts from `graph_scripts/` (especially `20_suspension_reason_trends_by_level_and_locale.py`), the following errors occurred:

```
ModuleNotFoundError: No module named 'matplotlib'
ModuleNotFoundError: No module named 'pandas'
ModuleNotFoundError: No module named 'pyarrow'
ModuleNotFoundError: No module named 'adjustText'
ModuleNotFoundError: No module named 'palette_utils'
ModuleNotFoundError: No module named 'data_validations'
```

### Root Causes

1. **Incorrect setup script path**: The `scripts/utilities/setup_python_env.sh` script had a bug where `ROOT_DIR` was set to `scripts/` instead of the project root, causing it to look for `scripts/../graph_scripts/requirements.txt` (which doesn't exist) instead of `<project-root>/graph_scripts/requirements.txt`.

2. **No virtual environment**: The Python virtual environment (`.venv/`) had not been created yet.

3. **Reticulate using wrong Python**: R's `reticulate` package was using its own cached Python environment located at:
   ```
   /Users/michaelcorral/Library/Caches/org.R-project.R/R/renv/cache/.../reticulate/...
   ```
   This environment didn't have the required packages installed.

4. **Missing .gitignore patterns**: The `.venv/` directory and Python cache files were not in `.gitignore`, risking accidental commits.

---

## Solution

### 1. Fixed setup_python_env.sh

**File**: `scripts/utilities/setup_python_env.sh`

**Change**:
```bash
# Before (INCORRECT):
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# This resolves to scripts/ directory

# After (CORRECT):
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# This resolves to project root directory
```

**Why**: The script is located at `scripts/utilities/setup_python_env.sh`, so:
- `$(dirname "${BASH_SOURCE[0]}")` = `scripts/utilities`
- Going up ONE level (`..`) = `scripts/` ❌
- Going up TWO levels (`../..`) = project root ✅

### 2. Created Python Virtual Environment

Ran the fixed setup script to create `.venv/` and install all required packages:

```bash
bash scripts/utilities/setup_python_env.sh
```

This successfully installed:
- matplotlib==3.10.6
- pandas==2.3.2
- pyarrow==21.0.0
- adjustText==1.3.0
- numpy==2.3.3
- And all their dependencies

### 3. Updated .gitignore

Added Python-specific ignore patterns:

```gitignore
# Python virtual environment
.venv/
venv/
__pycache__/
*.pyc
*.pyo
*.pyd
.Python
```

---

## Usage Instructions

### Option 1: Command Line (Recommended for Development)

**Activate the virtual environment before running Python scripts**:

```bash
# From project root
source .venv/bin/activate

# Run a script
cd graph_scripts
python 20_suspension_reason_trends_by_level_and_locale.py --help

# When done, deactivate
deactivate
```

**Example with full pipeline**:

```bash
# 1. Activate environment
source .venv/bin/activate

# 2. Generate data in R first (if needed)
Rscript -e "source('run_all.R')"

# 3. Run Python visualization scripts
cd graph_scripts
python 06_statewide_trends.py
python 20_suspension_reason_trends_by_level_and_locale.py

# 4. Deactivate when done
deactivate
```

### Option 2: From R/RStudio (Using Reticulate)

**Configure reticulate to use the virtual environment**:

Add this to your R script or run it in the R console:

```r
# Point reticulate at the virtual environment
Sys.setenv(RETICULATE_PYTHON = "/home/user/reach-suspensions/.venv/bin/python")

# Verify it worked
reticulate::py_config()
```

**Expected output from `py_config()`**:

```
python:         /home/user/reach-suspensions/.venv/bin/python
libpython:      /usr/local/lib/libpython3.11.so
pythonhome:     /home/user/reach-suspensions/.venv
version:        3.11.x
numpy:          /home/user/reach-suspensions/.venv/lib/python3.11/site-packages/numpy
```

**Make it permanent for RStudio sessions**:

Add to your `~/.Rprofile` or the project's `.Rprofile`:

```r
# .Rprofile
if (interactive() && file.exists(".venv/bin/python")) {
  Sys.setenv(RETICULATE_PYTHON = file.path(getwd(), ".venv/bin/python"))
  message("Using Python from .venv/")
}
```

**Run Python code from R**:

```r
# Load reticulate
library(reticulate)

# Source a Python script
source_python("graph_scripts/20_suspension_reason_trends_by_level_and_locale.py")

# Or run Python code directly
py_run_string("
import pandas as pd
import matplotlib.pyplot as plt
print('Python packages loaded successfully!')
")
```

---

## Verification

### Test Package Imports

```bash
source .venv/bin/activate
python -c "import matplotlib; import pandas; import pyarrow; import adjustText; print('✅ All packages imported successfully!')"
```

### Test Graph Scripts Modules

```bash
source .venv/bin/activate
cd graph_scripts
python -c "from palette_utils import DISCIPLINE_BASE_PALETTE; from data_validations import audit_counts_against_enrollment; print('✅ All graph_scripts modules imported successfully!')"
```

### Test Script Execution

```bash
source .venv/bin/activate
cd graph_scripts
python 20_suspension_reason_trends_by_level_and_locale.py --help
```

Expected: Help message displays without errors.

---

## Data Requirements

**IMPORTANT**: Before running any Python visualization scripts, you must first generate the data files using the R pipeline.

### Required Data Files

Most scripts expect:
```
data-stage/susp_v6_long.parquet
```

### Generate Data Files

```r
# In R
source("run_all.R")
# OR just core pipeline:
source("run_pipeline.R")
```

This will create all required `.parquet` files in `data-stage/`.

---

## Available Python Scripts

| Script | Purpose | Required Data |
|--------|---------|---------------|
| `06_statewide_trends.py` | Statewide suspension trends | `susp_v6_long.parquet` |
| `20_suspension_reason_trends_by_level_and_locale.py` | Suspension reasons by level/locale | `susp_v6_long.parquet` |
| `20_suspension_reason_trends_ucla.py` | UCLA-branded reason trends | `susp_v6_long.parquet` |
| `locale_locale_snapshot.py` | Locale snapshot analysis | `susp_v6_long.parquet` |
| `palette_utils.py` | Color palette utilities (module) | N/A |
| `data_validations.py` | Data validation utilities (module) | N/A |

---

## Troubleshooting

### Issue: "ModuleNotFoundError" even after setup

**Solution**: Make sure you activated the virtual environment:
```bash
source .venv/bin/activate
```

### Issue: Reticulate still using wrong Python

**Solution**:
1. Restart R session
2. Run before loading any Python code:
   ```r
   Sys.setenv(RETICULATE_PYTHON = "/home/user/reach-suspensions/.venv/bin/python")
   ```

### Issue: "No such file or directory: data-stage/susp_v6_long.parquet"

**Solution**: Run the R pipeline first to generate data:
```r
source("run_all.R")
```

### Issue: Different Python version needed

**Solution**: Specify Python version when creating venv:
```bash
PYTHON_BIN=python3.12 bash scripts/utilities/setup_python_env.sh
```

---

## Files Changed

1. `scripts/utilities/setup_python_env.sh` - Fixed ROOT_DIR path calculation
2. `.gitignore` - Added Python virtual environment and cache patterns

---

## Testing

All tests passed:

- ✅ Virtual environment created at `.venv/`
- ✅ All packages from `requirements.txt` installed successfully
- ✅ Core packages (matplotlib, pandas, pyarrow, adjustText) import correctly
- ✅ Local modules (palette_utils, data_validations) import correctly
- ✅ Script help output displays without errors
- ✅ .gitignore prevents accidental venv commits

---

## Next Steps

1. **Generate data files** (if not already done):
   ```r
   source("run_all.R")
   ```

2. **Run Python visualization scripts**:
   ```bash
   source .venv/bin/activate
   cd graph_scripts
   python 06_statewide_trends.py
   python 20_suspension_reason_trends_by_level_and_locale.py
   deactivate
   ```

3. **Configure RStudio** (optional):
   Add to `.Rprofile` for automatic venv detection:
   ```r
   if (interactive() && file.exists(".venv/bin/python")) {
     Sys.setenv(RETICULATE_PYTHON = file.path(getwd(), ".venv/bin/python"))
   }
   ```

---

## Related Documentation

- `graph_scripts/requirements.txt` - Python package dependencies
- `scripts/utilities/setup_python_env.sh` - Virtual environment setup script
- `CLAUDE.md` - Section on "Initial Setup" and "Python-R Interoperability"
- `graph_scripts/README.md` - Graph scripts documentation

---

**End of fix documentation**
