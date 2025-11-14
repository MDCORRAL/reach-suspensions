# Pipeline Failure Diagnosis - Deep Rooted Issues

**Date**: 2025-11-14
**Issue**: Pipeline failing at `R/04_feature_black_prop_quartiles.R` with Python/pyarrow errors

---

## Summary

The pipeline is failing because:
1. R's `arrow` package is attempting to use Python's `pyarrow` as a backend
2. R's `reticulate` package is trying to check if `pyarrow` is installed in a cached Python environment
3. The shell command `reticulate` uses to check for `pyarrow` has syntax errors (improperly escaped parentheses)
4. Even after installing `pyarrow`, `reticulate` is looking in a different Python environment than where it was installed

---

## Root Causes

### 1. **R arrow Package Configuration Issue**

The R `arrow` package (version 22.0.0) should have native C++ support for reading Parquet files and shouldn't need Python at all. The fact that it's trying to use Python suggests:

- **Possible Cause A**: The arrow package was installed without the C++ backend (common on macOS when binary isn't available)
- **Possible Cause B**: There's an environment variable or configuration forcing it to use Python backend
- **Possible Cause C**: The renv restore process didn't fully compile arrow's C++ dependencies

### 2. **reticulate Shell Command Bug**

The error messages show:
```
sh: -c: line 0: syntax error near unexpected token `0'
sh: -c: line 0: `'/Users/.../bin/python' -c import importlib.util, sys; sys.exit(0 if importlib.util.find_spec('pyarrow') else 1) >/dev/null 2>/dev/null'
```

The parentheses in `sys.exit(0 if ...)` aren't being properly escaped when passed to the shell. This is a bug in how `reticulate` constructs its shell commands.

**Note**: The codebase already has a workaround for this in `Analysis/21_teacher_diversity_regression.R` (lines 31-50), which writes the Python check to a temp file instead of using a one-liner.

### 3. **Python Environment Mismatch**

`reticulate` is managing its own Python environment via `uv` (a Python environment manager):
```
/Users/michaelcorral/Library/Caches/org.R-project.R/R/reticulate/uv/cache/archive-v0/7dpMhq2crjRewOW4omCgz/bin/python
```

This is different from the system Python where we installed `pyarrow`. So even though `pyarrow==21.0.0` is now installed in `/usr/local/bin/python`, `reticulate` won't find it.

---

## Immediate Solutions

### Option 1: Install pyarrow in reticulate's Python Environment (RECOMMENDED)

From R console:
```r
library(reticulate)
# This will install pyarrow in reticulate's managed Python environment
py_install("pyarrow==21.0.0")
```

### Option 2: Force arrow to Use Native C++ Backend

Check if arrow has C++ support and reinstall if needed:
```r
# Check arrow capabilities
arrow::arrow_info()

# If C++ support is missing, reinstall from source:
install.packages("arrow", type = "source")
# This will compile C++ backend (may take 10-20 minutes)
```

### Option 3: Create CSV Fallbacks

The error message suggests this workaround. Create CSV versions of the parquet files:
```r
library(arrow)
# You may need to do this on a machine where arrow works
read_parquet("data-stage/susp_v2.parquet") %>%
  write.csv("data-stage/susp_v2.csv", row.names = FALSE)
```

Then modify `04_feature_black_prop_quartiles.R` to try CSV fallback:
```r
v2 <- tryCatch(
  read_parquet(here("data-stage","susp_v2.parquet")),
  error = function(e) {
    message("Parquet read failed, trying CSV fallback...")
    read.csv(here("data-stage","susp_v2.csv"))
  }
)
```

---

## Long-term Solutions

### 1. **Ensure arrow Package is Properly Installed**

When setting up the project on a new machine:

```r
# Restore renv packages
renv::restore()

# Verify arrow has C++ support
arrow::arrow_info()
# Should show: C++ library version: 22.0.0 (or similar)

# If C++ support is missing, reinstall:
renv::install("arrow", type = "source", force = TRUE)
```

### 2. **Update CLAUDE.md with Setup Instructions**

Add to the "Initial Setup" section:

```markdown
### Verify Arrow Installation

After running `renv::restore()`, verify that the arrow package has C++ support:

```r
arrow::arrow_info()
```

Look for `C++ library version` in the output. If it's missing:

```r
# Reinstall arrow with C++ backend
renv::install("arrow", type = "source", force = TRUE)
```

This compilation may take 10-20 minutes but is necessary for native Parquet support.

### If Arrow C++ Installation Fails

As a fallback, install `pyarrow` in R's Python environment:

```r
library(reticulate)
py_install("pyarrow==21.0.0")
```
```

### 3. **Add Parquet Reading Utility with Automatic Fallbacks**

Create `R/read_parquet_safe.R`:

```r
#' Read parquet with automatic CSV fallback
read_parquet_safe <- function(path, csv_path = NULL) {
  if (is.null(csv_path)) {
    csv_path <- sub("\\.parquet$", ".csv", path)
  }

  # Try native arrow first
  result <- tryCatch(
    arrow::read_parquet(path),
    error = function(e) NULL
  )

  if (!is.null(result)) return(result)

  # Try CSV fallback if it exists
  if (file.exists(csv_path)) {
    message("Parquet read failed, using CSV fallback: ", basename(csv_path))
    return(read.csv(csv_path, stringsAsFactors = FALSE))
  }

  # Try to create CSV using Python if pyarrow is available
  message("Attempting to convert parquet to CSV using Python...")

  python_bin <- Sys.which(c("python3", "python"))[1]
  if (nchar(python_bin) == 0) {
    stop("No parquet reader available and no Python found for conversion")
  }

  script <- tempfile(fileext = ".py")
  on.exit(unlink(script))

  writeLines(c(
    "import sys",
    "import pyarrow.parquet as pq",
    "import pyarrow.csv as pc",
    sprintf("table = pq.read_table('%s')", path),
    sprintf("pc.write_csv(table, '%s')", csv_path)
  ), script)

  status <- system2(python_bin, args = script, stdout = FALSE, stderr = FALSE)

  if (status != 0) {
    stop("Failed to read parquet file and could not create CSV fallback")
  }

  message("Created CSV fallback, reading: ", basename(csv_path))
  read.csv(csv_path, stringsAsFactors = FALSE)
}
```

Then update pipeline scripts to use `read_parquet_safe()` instead of `read_parquet()`.

---

## Prevention Checklist

When setting up this project on a new machine:

- [ ] Run `renv::restore()` to install R packages
- [ ] Verify arrow C++ support: `arrow::arrow_info()`
- [ ] If arrow C++ is missing, reinstall: `renv::install("arrow", type = "source", force = TRUE)`
- [ ] Install Python dependencies: `pip install -r graph_scripts/requirements.txt`
- [ ] Verify pyarrow is accessible: `python -c "import pyarrow; print(pyarrow.__version__)"`
- [ ] If using reticulate, ensure pyarrow is in reticulate's environment: `reticulate::py_install("pyarrow==21.0.0")`
- [ ] Run a test pipeline stage to verify everything works

---

## What I've Done

1. ✅ Installed `pyarrow==21.0.0` and all Python dependencies from `graph_scripts/requirements.txt`
2. ✅ Verified pyarrow is now accessible in system Python
3. ⚠️ **However**: reticulate is using a different Python environment, so it may still not find pyarrow

---

## Next Steps for You

**Immediate (to get pipeline running)**:
1. Open R console in the project directory
2. Run:
   ```r
   library(reticulate)
   py_install("pyarrow==21.0.0")
   ```
3. Try running the pipeline again: `source("run_all.R")`

**If that still fails**:
1. Check arrow capabilities:
   ```r
   arrow::arrow_info()
   ```
2. If C++ support is missing, reinstall arrow from source (will take time):
   ```r
   renv::install("arrow", type = "source", force = TRUE)
   ```

**Long-term (prevent recurrence)**:
1. Document the arrow installation requirements in README.md
2. Consider adding the `read_parquet_safe()` utility for automatic fallbacks
3. Update setup documentation to include verification steps

---

## Technical Details

### Package Versions
- **R arrow**: 22.0.0 (from renv.lock)
- **Python pyarrow**: 21.0.0 (from requirements.txt, now installed)
- **reticulate**: Present in dependencies (manages Python interaction)

### Error Stack Trace
```
✗ Error in R/04_feature_black_prop_quartiles.R
Python interpreter(s) found (python, python3) are missing the pyarrow package.
```

This error is actually from `Analysis/21_teacher_diversity_regression.R`'s error handling pattern (lines 82-86, 100-106), which suggests similar fallback logic may have been added to other scripts or there's a shared utility being sourced.

### Files to Review
- `R/04_feature_black_prop_quartiles.R` - The failing script
- `Analysis/21_teacher_diversity_regression.R` - Has workaround for reticulate shell bug
- `renv.lock` - Package versions and dependencies
- `.Rprofile` - Activates renv
