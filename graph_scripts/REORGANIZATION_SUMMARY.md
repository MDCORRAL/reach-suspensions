# Graph Scripts Reorganization Summary

**Date**: 2025-11-26
**Purpose**: Resolve numbering conflicts and improve organization

---

## Problem

The `graph_scripts/` directory had multiple files starting with "21", creating confusion and poor organization:

- `21_black_quartile_suspension_trends.R` (R script)
- `21_suspension_reason_composition_statewide.py` (Python script)
- `21_suspension_reason_proportions_with_totals.py` (Python script)
- `21_suspension_reason_replacement_analysis.py` (Python script)

This made it difficult to:
- Navigate the folder
- Understand which scripts belong together
- Add new scripts without creating more conflicts

---

## Solution

Implemented a clear numbering scheme to separate script types:

- **01-09**: Basic R visualization scripts
- **10-19**: R quartile and demographic analysis scripts
- **20-29**: Python suspension reason analysis scripts
- **30+**: Reserved for future analyses

---

## Changes Made

### File Renames

| Old Name | New Name | Type | Reason |
|----------|----------|------|--------|
| `21_black_quartile_suspension_trends.R` | `10_black_quartile_suspension_trends.R` | R | Moved to R quartile analysis range (10-19) |
| `21_suspension_reason_proportions_with_totals.py` | `22_suspension_reason_proportions_with_totals.py` | Python | Sequential numbering in reason analysis range (20-29) |
| `21_suspension_reason_replacement_analysis.py` | `23_suspension_reason_replacement_analysis.py` | Python | Sequential numbering in reason analysis range (20-29) |
| `21_suspension_reason_composition_statewide.py` | *No change* | Python | Kept as first script in reason analysis range |

### Updated Internal References

**In Python Scripts**:
- `22_suspension_reason_proportions_with_totals.py`:
  - Updated error message (line 47)
  - Updated output directory path (line 95): `"21_suspension_reason_proportions"` → `"22_suspension_reason_proportions"`

- `23_suspension_reason_replacement_analysis.py`:
  - Updated error message (line 47)

**In Documentation**:
- `Analysis/quartile_alignment_plan.md` (line 4):
  - Updated script reference from `21_black_quartile_suspension_trends.R` → `10_black_quartile_suspension_trends.R`

- `docs/protocols/CITATION_STANDARD.md` (line 104):
  - Updated script list to reflect new filename

- `graph_scripts/README.md`:
  - Added new "Script Organization" section explaining the numbering scheme
  - Documented the 01-09, 10-19, 20-29, 30+ ranges

---

## Current Organization

### R Scripts (01-19)

**Basic Visualizations (01-09)**:
- `01_statewide_disparities.R`
- `02_statewide_quartiles.R`
- `03_elementary_disparities.R`
- `04_elementary_quartiles.R`
- `05_unequal_burden.R`
- `07_quartile_enrollment_comparison.R`
- `08_comprehensive_rates_plots.R`
- `09_nonrace_demographic_trends.R`

**Quartile & Demographic Analyses (10-19)**:
- `10_black_quartile_suspension_trends.R` *(formerly 21)*

### Python Scripts (20-29)

**Suspension Reason Analyses (20-29)**:
- `20_suspension_reason_trends_by_level_and_locale.py`
- `20_suspension_reason_trends_ucla.py`
- `21_suspension_reason_composition_statewide.py` *(unchanged)*
- `22_suspension_reason_proportions_with_totals.py` *(formerly 21)*
- `23_suspension_reason_replacement_analysis.py` *(formerly 21)*

### Other Scripts

**Mixed-Type Scripts**:
- `06_statewide_trends.py` - Main Python trend generation script
- `locale_locale_snapshot.py` - Locale snapshot visualization

**Utility Modules**:
- `palette_utils.py` - UCLA color palettes
- `data_sources.py` - Shared data loading utilities
- `data_validations.py` - Data quality checks
- `plotting_helpers.py` - Reusable plotting functions
- `graph_utils.R` - R utility functions

**Documentation**:
- `README.md` - Main documentation (updated)
- `CUSTOM_GRAPH_WORKFLOW.md` - Custom chart creation guide
- `CONSISTENCY_REVIEW_REPORT.md` - Styling consistency review
- `REORGANIZATION_SUMMARY.md` - This file

---

## Impact Assessment

### Breaking Changes
**None** - All changes preserve backward compatibility:
- Git renames preserve file history
- No API changes to utility functions
- Output paths updated to match new script numbers

### Non-Breaking Changes
- Documentation updated to reference new filenames
- Internal script references updated (error messages, output paths)
- README.md enhanced with organization documentation

---

## Benefits

1. **Clear Organization**: Script numbering now reflects purpose and type
2. **No Conflicts**: Each script has a unique, meaningful number
3. **Easy Navigation**: Related scripts grouped by number range
4. **Future-Proof**: Reserved ranges (30+) for new analyses
5. **Better Discoverability**: Number prefixes make it obvious what each script does

---

## Migration Guide

### For Users Running Scripts

**Old commands**:
```bash
Rscript graph_scripts/21_black_quartile_suspension_trends.R
python graph_scripts/21_suspension_reason_proportions_with_totals.py
python graph_scripts/21_suspension_reason_replacement_analysis.py
```

**New commands**:
```bash
Rscript graph_scripts/10_black_quartile_suspension_trends.R
python graph_scripts/22_suspension_reason_proportions_with_totals.py
python graph_scripts/23_suspension_reason_replacement_analysis.py
```

### For Developers

If you have local branches or scripts that reference the old filenames:

1. **Update script paths** in any custom scripts or workflows
2. **Update documentation** that references old filenames
3. **Check output directories**:
   - `outputs/21_suspension_reason_proportions/` → `outputs/22_suspension_reason_proportions/`

---

## Future Considerations

### Recommended Additions

As the repository grows, consider:

1. **Subdirectories by type**:
   - `graph_scripts/r_scripts/` for R files
   - `graph_scripts/python_scripts/` for Python files
   - Keep utilities in root

2. **Consistent prefixes**:
   - `race_XX_*.R` for race-based analyses
   - `reason_XX_*.py` for reason-based analyses
   - `quartile_XX_*.R` for quartile analyses

3. **Documentation**:
   - Add script inventory table to README.md
   - Document expected inputs/outputs for each script

### Numbering Guidelines

When adding new scripts:

- **R race/demographic visualizations** → Use 01-09 range
- **R quartile analyses** → Use 10-19 range
- **Python reason analyses** → Use 20-29 range
- **New analysis types** → Start at 30

Avoid reusing numbers even if a script is deprecated. Instead, move deprecated scripts to `docs/archive/` or add a `DEPRECATED_` prefix.

---

## Related Documentation

- `graph_scripts/README.md` - Main graph scripts documentation
- `graph_scripts/CUSTOM_GRAPH_WORKFLOW.md` - Guide for creating custom charts
- `graph_scripts/CONSISTENCY_REVIEW_REPORT.md` - Styling consistency review
- `docs/protocols/CITATION_STANDARD.md` - Citation guidelines
- `Analysis/quartile_alignment_plan.md` - Quartile analysis alignment

---

## Questions or Issues

If you encounter any issues with the reorganization:

1. Check this summary for migration guidance
2. Review `graph_scripts/README.md` for updated organization
3. Verify script paths in your commands/workflows
4. Check output directories for renamed paths

---

**Reorganization completed**: 2025-11-26
**Git commits**: File renames preserve full history via `git mv`
