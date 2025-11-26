# Setting Up Python for R (Reticulate)

**Quick Fix**: Configure R's `reticulate` to use the project's Python virtual environment.

---

## The Problem

When running Python scripts from R/RStudio, `reticulate` uses its own cached Python environment which doesn't have the required packages installed. This causes `ModuleNotFoundError` errors.

**Error indicators**:
- Error path contains: `.../Library/Caches/org.R-project.R/R/renv/cache/.../reticulate/...`
- Errors: `ModuleNotFoundError: No module named 'matplotlib'`

---

## Step-by-Step Solution

### Step 1: Create the Virtual Environment (One-Time Setup)

**On your local machine** (macOS), open Terminal and navigate to the project:

```bash
cd /path/to/reach-suspensions

# If you have an existing (possibly broken) .venv from reticulate, remove it first:
bash FIX_BROKEN_VENV.sh

# OR manually:
# rm -rf .venv
# bash scripts/utilities/setup_python_env.sh
```

**Common Issue**: If you see `No module named pip`, your `.venv/` was created by R's reticulate and is broken. Use `FIX_BROKEN_VENV.sh` to remove and recreate it.

This will:
- Create `.venv/` directory in the project root
- Install all required Python packages (matplotlib, pandas, pyarrow, etc.)

**Expected output**:
```
Creating virtual environment at /path/to/reach-suspensions/.venv
...
Successfully installed adjustText-1.3.0 matplotlib-3.10.6 pandas-2.3.2 pyarrow-21.0.0 ...
Done. Activate the environment with:
  source /path/to/reach-suspensions/.venv/bin/activate
```

### Step 2: Configure R to Use .venv

**Option A: Automatic Configuration (Recommended)**

In your R console or RStudio:

```r
# Run the configuration script
source("configure_python_env.R")
```

This will:
- Detect your project root
- Set the RETICULATE_PYTHON environment variable
- Verify all packages are available
- Display configuration details

**Expected output**:
```
=== Python Configuration ===
Project root: /path/to/reach-suspensions
Python path: /path/to/reach-suspensions/.venv/bin/python

python:         /path/to/reach-suspensions/.venv/bin/python
libpython:      ...
...

=== Verifying Required Packages ===
✓ matplotlib is available
✓ pandas is available
✓ pyarrow is available
✓ numpy is available
✓ adjustText is available

✅ All required packages are available!
```

**Option B: Manual Configuration**

Add this to your R script BEFORE loading any Python code:

```r
# Point reticulate at the virtual environment
Sys.setenv(RETICULATE_PYTHON = "/path/to/reach-suspensions/.venv/bin/python")

# Load reticulate
library(reticulate)

# Verify configuration
py_config()
```

Replace `/path/to/reach-suspensions` with your actual project path.

### Step 3: Make it Permanent (Optional)

To automatically use the `.venv/` in every R session, add to your `.Rprofile`:

```r
# Edit .Rprofile in project root
if (interactive() && file.exists(".venv/bin/python")) {
  Sys.setenv(RETICULATE_PYTHON = file.path(getwd(), ".venv/bin/python"))
  message("✓ Using Python from .venv/")
}
```

**How to edit .Rprofile**:

```r
# In R console
usethis::edit_r_profile(scope = "project")
# Or manually create/edit .Rprofile in project root
```

---

## Testing the Setup

### Test 1: Verify Python Configuration

```r
source("configure_python_env.R")
```

Should show all packages as available (✓).

### Test 2: Import Python Modules

```r
library(reticulate)

# Test core packages
py_run_string("
import matplotlib
import pandas
import pyarrow
import adjustText
print('✅ All packages imported successfully!')
")
```

### Test 3: Import Graph Scripts Modules

```r
library(reticulate)

# Test local modules
py_run_string("
import sys
sys.path.insert(0, 'graph_scripts')
from palette_utils import DISCIPLINE_BASE_PALETTE
from data_validations import ensure_audit_dir
print('✅ All graph_scripts modules imported!')
")
```

### Test 4: Run a Python Script

```r
library(reticulate)

# Make sure you've generated data first:
# source("run_all.R")

# Run a Python script
setwd("graph_scripts")
py_run_file("20_suspension_reason_trends_by_level_and_locale.py")
```

---

## Troubleshooting

### Issue 1: "Python virtual environment not found"

**Cause**: The `.venv/` directory doesn't exist.

**Solution**:
```bash
cd /path/to/reach-suspensions
bash scripts/utilities/setup_python_env.sh
```

### Issue 2: Still getting ModuleNotFoundError

**Cause**: Reticulate is not using the `.venv/` Python.

**Solution**:
```r
# 1. Restart R session (Session > Restart R in RStudio)

# 2. BEFORE loading reticulate, set the Python path:
Sys.setenv(RETICULATE_PYTHON = "/path/to/reach-suspensions/.venv/bin/python")

# 3. Load reticulate
library(reticulate)

# 4. Verify it's using the right Python
py_config()
# Should show: python: /path/to/reach-suspensions/.venv/bin/python
```

### Issue 3: py_config() shows wrong Python path

**Cause**: Reticulate already initialized with a different Python.

**Solution**:
```r
# 1. Completely restart R session (Session > Restart R)

# 2. Set RETICULATE_PYTHON FIRST (before loading reticulate)
Sys.setenv(RETICULATE_PYTHON = "/path/to/reach-suspensions/.venv/bin/python")

# 3. Then load reticulate
library(reticulate)
```

**Critical**: The environment variable must be set **before** `reticulate` is first loaded in an R session.

### Issue 4: "No module named pip" when running setup script

**Cause**: Existing `.venv/` was created by R's reticulate (using `uv`) and is broken/incomplete.

**Error looks like**:
```
Using existing virtual environment at /path/to/.venv
/Users/.../Library/Caches/org.R-project.R/R/reticulate/uv/cache/.../bin/python: No module named pip
```

**Solution**:
```bash
# Use the automated fix script
cd /path/to/reach-suspensions
bash FIX_BROKEN_VENV.sh

# OR manually:
# rm -rf .venv
# bash scripts/utilities/setup_python_env.sh
```

### Issue 5: .venv exists but packages missing

**Cause**: Incomplete installation.

**Solution**:
```bash
# Remove and recreate the virtual environment
cd /path/to/reach-suspensions
rm -rf .venv
bash scripts/utilities/setup_python_env.sh
```

### Issue 5: Running from wrong directory

**Cause**: Python scripts expect to be run from `graph_scripts/` directory.

**Solution**:
```r
# Option 1: Change directory
setwd("graph_scripts")
py_run_file("20_suspension_reason_trends_by_level_and_locale.py")

# Option 2: Use absolute paths in Python
py_run_string("
import sys
sys.path.insert(0, '/absolute/path/to/reach-suspensions/graph_scripts')
")
```

---

## Complete Workflow Example

```r
# 1. Configure Python environment (run once per session)
source("configure_python_env.R")

# 2. Generate data files if needed (R pipeline)
source("run_all.R")

# 3. Run Python visualization scripts
library(reticulate)

# Change to graph_scripts directory
setwd("graph_scripts")

# Run the script
py_run_file("20_suspension_reason_trends_by_level_and_locale.py")

# Or run with arguments
system("python 20_suspension_reason_trends_by_level_and_locale.py --levels Elementary Middle")
```

---

## Alternative: Run Python from Command Line

If R/reticulate continues to cause issues, you can run Python scripts directly from Terminal:

```bash
cd /path/to/reach-suspensions

# Activate virtual environment
source .venv/bin/activate

# Run scripts
cd graph_scripts
python 20_suspension_reason_trends_by_level_and_locale.py

# When done
deactivate
```

This bypasses R entirely and uses Python directly.

---

## Key Points to Remember

1. **Create .venv once** on your local machine: `bash scripts/utilities/setup_python_env.sh`
2. **Configure reticulate** every R session: `source("configure_python_env.R")`
3. **Order matters**: Set `RETICULATE_PYTHON` BEFORE loading `reticulate`
4. **Restart R** if reticulate already loaded with wrong Python
5. **Generate data first**: Run `source("run_all.R")` before Python scripts
6. **Alternative**: Use command line Python if R/reticulate is problematic

---

## Files Created

- `configure_python_env.R` - Automatic configuration script
- `SETUP_PYTHON_FOR_R.md` - This guide
- `.venv/` - Python virtual environment (created by setup script)

---

## Next Steps

1. **Create .venv** (if you haven't):
   ```bash
   bash scripts/utilities/setup_python_env.sh
   ```

2. **Test configuration**:
   ```r
   source("configure_python_env.R")
   ```

3. **Run your Python scripts**:
   ```r
   library(reticulate)
   setwd("graph_scripts")
   py_run_file("20_suspension_reason_trends_by_level_and_locale.py")
   ```

---

**Questions? See**: `docs/fixes/FIX_GRAPH_SCRIPTS_PYTHON_ENV.md` for more details.
