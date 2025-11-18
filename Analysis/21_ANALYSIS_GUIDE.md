# Weighted Teacher Diversity Analysis Guide

**Script**: `Analysis/21_weighted_teacher_diversity_by_quartile.R`
**Created**: 2025-11-13
**Purpose**: Analyze teacher diversity by Black enrollment quartile using proper weighted averages

---

## Overview

This script examines the relationship between school racial composition (measured by Black enrollment quartile) and teacher workforce diversity. It uses **weighted aggregations** to ensure larger schools appropriately influence the results, avoiding the pitfall of simple averaging that treats a 10-teacher school the same as a 100-teacher school.

---

## Key Methodological Features

### 1. Weighted Averages (Not Simple Averages)

❌ **Wrong approach**: Calculate each school's percentage, then average percentages
✅ **Correct approach**: Sum teacher counts by race within quartile, then calculate proportions

**Why this matters:**
- A simple average of school-level percentages gives equal weight to all schools
- This skews results when school sizes vary dramatically
- Weighted aggregation ensures larger schools (with more teachers) contribute proportionally

**Example:**
- School A: 10 teachers, 20% non-White = 2 non-White teachers
- School B: 100 teachers, 30% non-White = 30 non-White teachers
- Simple average: (20% + 30%) / 2 = **25%** ❌
- Weighted average: (2 + 30) / (10 + 100) = **29.1%** ✅

### 2. Distribution Analysis

Beyond quartile-level averages, the script examines:
- **Histograms/boxplots** of school-level diversity within each quartile
- **Quartiles and standard deviations** to understand spread
- **Outlier detection** to identify whether differences are driven by a few extreme schools

### 3. Diagnostic Checks

The script includes extensive validation:
- **Missing value patterns**: Identifies data gaps
- **Small-n schools**: Flags schools with <5 or <10 teachers (unstable proportions)
- **Coverage statistics**: Reports teacher data availability by year
- **Duplicate checks**: Ensures no school-year is counted twice

### 4. Non-Causal Interpretation

**IMPORTANT**: This analysis identifies correlational patterns only.

❌ Do not conclude: "Teacher diversity causes lower/higher suspension rates"
✅ Appropriate interpretation: "Schools with higher Black enrollment have different teacher diversity patterns, which correlates with suspension rates"

**Confounding factors** (not measured):
- School leadership quality
- Community socioeconomic context
- District funding levels
- Local teacher labor markets
- School climate and culture

---

## How to Run

### Prerequisites

1. **Data files must exist:**
   ```r
   data-stage/susp_v6_teacher_features.parquet
   ```
   If missing, run:
   ```r
   source("Analysis/18_merge_teacher_student.R")
   ```

2. **R packages required** (via renv):
   ```r
   renv::restore()  # Installs exact package versions
   ```

### Execution

**Option 1: Run standalone**
```r
source("Analysis/21_weighted_teacher_diversity_by_quartile.R")
```

**Option 2: Run as part of full pipeline**
```r
source("run_all.R")  # Includes all analyses
```

### Expected runtime
- ~30-60 seconds on typical hardware
- Depends on number of school-year observations with teacher data

---

## Outputs

### Summary Tables (CSV)

All saved to `outputs/tables/`:

1. **`21_teacher_diversity_by_quartile_year.csv`**
   - Weighted averages by quartile AND academic year
   - Shows trends over time
   - Columns:
     - `academic_year`: e.g., "2018-19"
     - `black_prop_q_label`: Quartile label
     - `n_schools`: Number of schools in quartile-year
     - `total_teachers`: Aggregated teacher count
     - `pct_teachers_white`: % White teachers (weighted)
     - `pct_teachers_non_white`: % non-White teachers (weighted)
     - `pct_teachers_african_american`: % African American teachers
     - `suspension_rate`: Student suspension rate (weighted)

2. **`21_teacher_diversity_by_quartile_overall.csv`**
   - Overall quartile summary (pooled across all years)
   - Same structure as above, but aggregated
   - Most useful for high-level comparisons

3. **`21_teacher_diversity_distribution.csv`**
   - Distribution statistics within each quartile
   - Shows mean, median, SD, Q25, Q75 for school-level diversity
   - Reveals whether quartile differences are robust or driven by outliers

### Visualizations (PNG)

All saved to `outputs/graphs/`:

1. **`21_teacher_diversity_by_quartile.png`** (12" × 8")
   - Bar chart showing teacher race/ethnicity by quartile
   - Weighted averages (overall)
   - Grouped bars for White, Non-White, African American, Hispanic, Asian

2. **`21_teacher_diversity_trends.png`** (12" × 10")
   - Line plots showing trends over academic years
   - Separate panels for White vs Non-White teachers
   - One line per quartile

3. **`21_teacher_diversity_distribution.png`** (10" × 8")
   - Boxplots showing distribution of non-White teacher % by quartile
   - Each point = one school-year
   - Reveals spread and outliers within quartiles

4. **`21_suspension_vs_diversity.png`** (10" × 8")
   - Scatterplot: suspension rate vs non-White teacher %
   - One point per quartile
   - Point size indicates total teacher count
   - **Caution**: Correlation ≠ causation!

---

## Key Findings (Example)

*The script prints a summary to console. Example output:*

```
=== ANALYSIS COMPLETE ===

Key findings:
1. Analyzed 4,523 unique schools across 5 academic years
2. Used weighted averages (schools weighted by staff count)
3. Q1 (Lowest % Black) teacher diversity: 18.3% non-White
4. Q4 (Highest % Black) teacher diversity: 42.7% non-White
5. Q1 suspension rate: 2.14%
6. Q4 suspension rate: 5.68%

Outputs saved to:
  - outputs/tables/21_teacher_diversity_by_quartile_*.csv
  - outputs/graphs/21_teacher_diversity_*.png

IMPORTANT: These are correlational patterns only. Avoid causal interpretation.
Many unobserved factors (leadership, funding, community context) influence outcomes.
```

---

## Interpretation Guidelines

### What the analysis shows

1. **Teacher diversity varies by school racial composition**
   - Schools with higher Black enrollment tend to have more diverse teaching staff
   - This could reflect local labor markets, district hiring practices, or other factors

2. **Suspension rates correlate with both student and teacher demographics**
   - Schools in Q4 (highest Black enrollment) have higher suspension rates
   - These schools also have more diverse teaching staff
   - **Cannot conclude** teacher diversity causes suspension rates (or vice versa)

3. **Substantial within-quartile variation exists**
   - Distribution plots show wide spread within each quartile
   - Some schools in Q1 have very diverse staff; some schools in Q4 have mostly White staff
   - Quartile-level averages mask this heterogeneity

### What the analysis does NOT show

❌ **Causal effects** of teacher diversity on suspension rates
❌ **Individual teacher** or school identities (privacy protection)
❌ **Optimal** teacher diversity levels
❌ **Policy prescriptions** (requires additional context and analysis)

### Appropriate uses

✅ **Descriptive research**: Characterizing patterns in California schools
✅ **Hypothesis generation**: Motivating deeper investigations
✅ **Equity audits**: Identifying disparities for further examination
✅ **Resource allocation**: Understanding which schools face staffing challenges

---

## Technical Details

### Data Filtering

The analysis applies these filters:

1. **Geographic**: Campus-only data (no district/county aggregates)
2. **Special codes**: Excludes codes `0000000` and `0000001`
3. **Quartiles**: Excludes "Unknown" quartile
4. **Teacher data**: Requires `teacher_staff_count_total > 0`
5. **Enrollment data**: Requires `cumulative_enrollment > 0`
6. **Time period**: Focuses on 2018-19 onwards (better teacher data coverage)

For distribution analysis (boxplots), an additional filter:
7. **Minimum staff**: Includes only schools with ≥5 teachers (stable proportions)

### Weighted Aggregation Formula

For each quartile *q* in year *y*:

**Teacher diversity:**
```
pct_non_white(q, y) = sum(non_white_teachers) / sum(total_teachers) × 100
```

**Suspension rate:**
```
susp_rate(q, y) = sum(total_suspensions) / sum(total_enrollment) × 100
```

This is equivalent to a weighted average where each school is weighted by its teacher count (for diversity) or enrollment (for suspension rate).

### Missing Data Handling

- **Teacher data**: Left-joined from `teacher_staff_long.parquet`
  - Not all schools have teacher demographics (coverage ~60-80% depending on year)
  - Script reports coverage statistics in console

- **Quartile assignment**: Some schools lack Black enrollment quartile
  - Typically <5% of schools
  - Excluded from analysis

- **Small counts**: Very small schools (<5 teachers) included in weighted aggregates but excluded from distribution boxplots

---

## Privacy and Fairness Considerations

### Privacy Protection

✅ **Aggregated reporting only**: No individual schools identified in outputs
✅ **Minimum thresholds**: Small schools flagged but not separately reported
✅ **Group-level summaries**: All results are quartile-level or statewide

### Fairness Considerations

⚠️ **Do not use for high-stakes decisions**:
- Hiring or firing teachers based on race
- School rankings or ratings
- Resource allocation without additional context

✅ **Appropriate uses**:
- Understanding system-wide patterns
- Identifying equity gaps for intervention
- Informing recruitment and retention strategies

### Avoiding Misinterpretation

The script includes warnings in:
1. Console output (final summary)
2. Plot captions
3. This documentation

**Key message**: Correlation ≠ causation. Many unmeasured factors influence outcomes.

---

## Extending the Analysis

### Adding New Race/Ethnicity Categories

The script automatically detects available teacher race columns:
```r
teacher_race_cols <- c(
  "teacher_staff_count_african_american",
  "teacher_staff_count_asian",
  "teacher_staff_count_hispanic_or_latino",
  # Add new columns here
  "teacher_staff_count_multiracial"
)
```

If new categories exist in `teacher_staff_long.parquet`, they will be included automatically.

### Analyzing by Other Quartiles

To analyze by White or Hispanic quartiles instead:

1. Change grouping variable:
   ```r
   # Line ~90: Change black_prop_q to white_prop_q
   group_by(academic_year, white_prop_q, white_prop_q_label)
   ```

2. Update quartile labels:
   ```r
   # Line ~45: Change "Black" to "White"
   mutate(white_prop_q_label = get_quartile_label(white_prop_q, "White"))
   ```

3. Update color palette:
   ```r
   # Use white_quartile_colors from utils_keys_filters.R
   ```

### Adding School Level Stratification

To examine elementary vs middle vs high schools:

1. Add `school_level` to grouping:
   ```r
   group_by(academic_year, black_prop_q, school_level)
   ```

2. Filter to specific level before analysis:
   ```r
   analysis_df <- analysis_df %>%
     filter(school_level == "Elementary")
   ```

3. Update plot facets:
   ```r
   facet_wrap(~ school_level, ncol = 3)
   ```

---

## Troubleshooting

### Error: "Missing susp_v6_teacher_features.parquet"

**Cause**: Teacher-student merge hasn't run yet
**Solution**:
```r
source("Analysis/18_merge_teacher_student.R")
```

### Error: "No teacher_* columns found"

**Cause**: Merge ran but produced no teacher data
**Solution**:
1. Check teacher ingestion:
   ```r
   source("R/01c_ingest_teacher_demographics.R")
   ```
2. Verify teacher file exists:
   ```r
   file.exists("data-stage/teacher_staff_long.parquet")
   ```

### Warning: "Low teacher data coverage"

**Cause**: Many schools lack teacher demographics
**Impact**: Analysis valid but covers smaller sample
**Solution**: None required (expected); adjust interpretation accordingly

### No variation in results

**Cause**: Possible filtering too restrictive or data issue
**Solution**:
1. Check filter thresholds (line ~163)
2. Examine `analysis_df` in console:
   ```r
   table(analysis_df$black_prop_q_label)
   ```

---

## Citation

When using this analysis in reports or publications, cite:

> REACH Suspensions Analysis Pipeline. (2025). Weighted Teacher Diversity Analysis by School Racial Composition. California Department of Education suspension data, 2018-19 through 2023-24.

See `docs/protocols/CITATION_STANDARD.md` for full citation requirements.

---

## Contact

For questions or issues with this analysis:
1. Check existing documentation: `CLAUDE.md`, `Analysis/data_processing_overview.md`
2. Review audit reports: `COMPREHENSIVE_AUDIT_REPORT.md`
3. Open an issue in the repository

---

**Last updated**: 2025-11-13
**Script version**: 1.0
**Compatible with**: susp_v6_teacher_features.parquet (v6 pipeline)
