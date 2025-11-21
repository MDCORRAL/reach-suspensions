# Quick Start: Weighted Teacher Diversity Analysis

## TL;DR

```r
# In R console:
source("Analysis/21_weighted_teacher_diversity_by_quartile.R")
```

**Outputs:**
- 📊 4 PNG visualizations in `outputs/graphs/`
- 📄 3 CSV summary tables in `outputs/tables/`
- ⏱️ Runtime: ~30-60 seconds

---

## Prerequisites

✅ **Data files:**
```r
# Must exist:
data-stage/susp_v6_teacher_features.parquet

# If missing, run:
source("Analysis/18_merge_teacher_student.R")
```

✅ **R packages:**
```r
renv::restore()  # One-time setup
```

✅ **Environment variables (optional but recommended for ad hoc runs):**

These align with the project-level settings in `README.md` so scripts consistently locate data and project roots even outside `run_all.R`.

```r
# Common overrides:
# REACH_PROJECT_ROOT   - Explicit path to the repository root when running from another working dir
# REACH_DATA_DIR       - Where staged parquet/CSV files live (defaults to data-stage/ under the root)
# RAW_PATH             - Full path to copy_CDE_suspensions_1718-2324_sc_race.xlsx
# OTH_RAW_PATH         - Full path to copy_CDE_suspensions_1718-2324_sc_oth.xlsx
# TEACHER_RAW_DIR      - Directory containing stre*.txt teacher files
# RENV_CONFIG_AUTOLOADER_ENABLED=false  # disable auto-loading renv if it conflicts with your R session
```

---

## What You'll Get

### 1. Bar Chart by Quartile
**File:** `outputs/graphs/21_teacher_diversity_by_quartile.png`

Shows teacher race/ethnicity percentages for schools in each Black enrollment quartile.

**Key question answered:** Do schools with more Black students have more diverse teaching staff?

### 2. Trends Over Time
**File:** `outputs/graphs/21_teacher_diversity_trends.png`

Line plots showing how teacher diversity changed from 2018-19 onwards.

**Key question answered:** Is teacher diversity improving or declining in each quartile?

### 3. Distribution Boxplots
**File:** `outputs/graphs/21_teacher_diversity_distribution.png`

Shows variation within each quartile (not just averages).

**Key question answered:** Are quartile differences robust, or driven by outliers?

### 4. Suspension vs Diversity Scatterplot
**File:** `outputs/graphs/21_suspension_vs_diversity.png`

Plots suspension rates against teacher diversity by quartile.

**Key question answered:** Do suspension rates correlate with teacher diversity?
⚠️ **Caution:** Correlation ≠ causation!

---

## Key Results (Console Output)

```
=== ANALYSIS COMPLETE ===

Key findings:
1. Analyzed X unique schools across Y academic years
2. Used weighted averages (schools weighted by staff count)
3. Q1 (Lowest % Black) teacher diversity: XX.X% non-White
4. Q4 (Highest % Black) teacher diversity: XX.X% non-White
5. Q1 suspension rate: X.XX%
6. Q4 suspension rate: X.XX%
```

---

## Methodological Highlights

### ✅ Uses Weighted Averages
- Larger schools count more (proportional to staff size)
- Avoids bias from treating all schools equally
- Aggregates counts FIRST, then calculates percentages

### ✅ Includes Distribution Analysis
- Boxplots show spread within quartiles
- Identifies outliers
- Reports medians, quartiles, standard deviations

### ✅ Extensive Diagnostics
- Reports missing data patterns
- Flags small-n schools
- Validates data coverage

### ✅ Privacy-Protective
- Only reports aggregate statistics
- No individual schools identified
- Minimum thresholds applied

---

## Interpretation Warnings

❌ **DO NOT conclude:**
- "Teacher diversity causes suspension rate changes"
- "Schools should hire X% of Y race teachers"
- "Quartile Z has the 'best' or 'worst' staffing"

✅ **DO conclude:**
- "Teacher diversity patterns differ by school racial composition"
- "These differences correlate with suspension rates"
- "Further investigation needed to understand causal mechanisms"

**Unmeasured confounders:** leadership, funding, community context, local labor markets

---

## Common Issues

### "File not found" error
```r
# Run the merge first:
source("Analysis/18_merge_teacher_student.R")
```

### "Package not found" error
```r
# Restore R environment:
renv::restore()
```

### Very low teacher data coverage (<50%)
- Expected for earlier years (2017-18, 2018-19)
- Script filters to 2018-19 onwards for better coverage
- Check console output for coverage statistics

---

## Full Documentation

See `Analysis/21_ANALYSIS_GUIDE.md` for:
- Detailed methodology
- Interpretation guidelines
- Privacy and fairness considerations
- How to extend the analysis
- Troubleshooting

---

## Questions?

1. Read: `CLAUDE.md` (AI assistant guide)
2. Read: `Analysis/data_processing_overview.md` (pipeline docs)
3. Read: `Analysis/21_ANALYSIS_GUIDE.md` (this analysis)

---

**Created:** 2025-11-13
**Estimated time to run:** 30-60 seconds
**Required RAM:** ~2-4 GB
