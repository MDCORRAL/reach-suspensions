# Utility and Diagnostic Scripts

This directory contains utility scripts and diagnostic tools that support the main data processing pipeline but are not part of the core analysis workflow.

**Last Updated**: 2025-11-18

---

## Directory Structure

```
scripts/
├── diagnostics/            # Diagnostic and data quality scripts
└── utilities/              # Utility scripts for maintenance tasks
```

---

## 🔍 Diagnostics

Scripts for diagnosing data quality issues, validating pipeline outputs, and debugging problems.

### data_audit_analysis.R

**Purpose**: R-based data audit analysis
**Usage**: Validates data quality and identifies potential issues in the pipeline

```r
source("scripts/diagnostics/data_audit_analysis.R")
```

**Outputs**: Data quality reports in `outputs/data_audit/`

---

### data_audit_analysis.py

**Purpose**: Python-based data audit analysis
**Usage**: Validates data quality using Python/pandas

```bash
python scripts/diagnostics/data_audit_analysis.py
```

**Outputs**: Data quality reports and diagnostic files

---

### diagnostic_q4_python.py

**Purpose**: Q4 Black enrollment quartile diagnostic
**Usage**: Validates Q4 suspension data calculations

```bash
python scripts/diagnostics/diagnostic_q4_python.py
```

**Context**: Created to investigate and resolve Q4 suspension rate issues
**Related Documentation**: `docs/fixes/FIX_EXPLANATION_Q4_SUSPENSION_DATA.md`

---

## 🛠️ Utilities

Utility scripts for repository maintenance, consolidation, and special operations.

### consolidate_regression_script.R

**Purpose**: Consolidates regression analysis scripts
**Usage**: Merges multiple regression analysis approaches into a unified script

```r
source("scripts/utilities/consolidate_regression_script.R")
```

**Context**: Part of regression script standardization effort
**Related Documentation**: `docs/fixes/FIX_REGRESSION_SCRIPT.md`

---

## When to Use These Scripts

### Use Diagnostics When:
- Investigating data quality issues
- Validating pipeline outputs
- Debugging unexpected results
- Performing ad-hoc data exploration
- Cross-validating R and Python outputs

### Use Utilities When:
- Consolidating multiple scripts
- Performing repository maintenance
- Running one-time data operations
- Migrating code between versions

---

## Relationship to Main Pipeline

These scripts are **not** part of the main data processing pipeline (`R/` directory) or core analyses (`Analysis/` directory). They are:

- **Standalone**: Can be run independently
- **Diagnostic**: Used for investigation and validation
- **Ad-hoc**: Not required for standard pipeline execution
- **Maintenance**: Support repository organization and code quality

---

## Running Scripts

### R Scripts

```r
# Always source paths first if needed
source("R/00_paths.R")

# Then run diagnostic
source("scripts/diagnostics/data_audit_analysis.R")
```

### Python Scripts

```bash
# Ensure Python environment is activated
cd /path/to/reach-suspensions

# Run diagnostic
python scripts/diagnostics/diagnostic_q4_python.py
```

---

## Output Locations

- **Data audit reports**: `outputs/data_audit/`
- **Diagnostic files**: `outputs/` (varies by script)
- **Temporary files**: Not committed to git

---

## Adding New Scripts

When adding new diagnostic or utility scripts:

1. **Choose the right directory**:
   - Diagnostics → `scripts/diagnostics/`
   - Utilities → `scripts/utilities/`

2. **Use descriptive names**:
   - Good: `diagnostic_teacher_diversity.py`
   - Bad: `test.py`

3. **Include documentation**:
   - Add header comment with purpose and usage
   - Update this README with script description

4. **Follow conventions**:
   - R scripts: Use `source("R/00_paths.R")` for paths
   - Python scripts: Import from `dashboard/data_sources.py` for shared utilities

5. **Document related issues**:
   - If diagnostic addresses a specific issue, link to `docs/fixes/` documentation

---

## Related Documentation

- **Main Pipeline**: See `R/` directory and `CLAUDE.md`
- **Analysis Scripts**: See `Analysis/` directory
- **Fix Documentation**: See `docs/fixes/` for context on diagnostic scripts
- **Audit Reports**: See `docs/audits/` for formal audit documentation

---

**Questions?** See `../CLAUDE.md` for comprehensive project documentation.
