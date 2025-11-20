# Fix Summary: Scripts 24 & 25 Clustering Issue and Aggregation

**Date**: 2025-11-20
**Issue**: Clustering in regression analysis due to school-year-race-reason level data
**Scripts Affected**: Analysis/24_quartile_slope_comparison.R, Analysis/25_interaction_term_regression.R
**Status**: Script 24 already fixed, Script 25 fixed in this session

---

## Executive Summary

**Problem**: The CDE suspension data is reported at **school-year-race-reason** level, creating approximately 48 observations per school-year (8 races × 6 suspension reasons). Using this granular data directly in school-level regression analyses violates the **independence assumption** in linear regression, leading to:

- **Artificially inflated sample sizes** (N appears ~48x larger than actual school count)
- **Biased standard errors** (too small, making results appear more significant than they are)
- **Invalid p-values** and confidence intervals
- **Misleading statistical inference**

**Solution**: Aggregate data to **school-year level** before running regressions by:
- Summing suspensions across all races and reason categories
- Taking first value of school-level variables (teacher diversity, enrollment, etc.)
- Recalculating overall suspension rates

**Impact**: After aggregation, standard errors and p-values are valid for school-level analysis.

---

## Detailed Analysis

### The Clustering Problem Explained

#### Data Structure

**Raw CDE Data (school-year-race-reason level)**:
```
cds_school | academic_year | race         | reason           | suspensions | enrollment
-----------|---------------|--------------|------------------|-------------|------------
12345...   | 2023-24       | Black        | Defiance         | 10          | 500
12345...   | 2023-24       | Black        | Violent (Injury) | 5           | 500
12345...   | 2023-24       | Black        | Other            | 3           | 500
12345...   | 2023-24       | Hispanic     | Defiance         | 8           | 500
12345...   | 2023-24       | Hispanic     | Violent (Injury) | 2           | 500
... (~48 rows per school-year)
```

**Problem**: All 48 rows represent THE SAME SCHOOL in THE SAME YEAR. They are NOT independent observations.

**School-level variables** (like % White Teachers, charter status) are **duplicated** 48 times:
- Each row has the same `pct_white_teachers` value
- Each row has the same `charter_yn` value
- Each row has the same school characteristics

#### Why This Violates Independence

Linear regression assumes observations are **independent**:
- Observation 1 tells us nothing about Observation 2
- Residuals (errors) are uncorrelated
- Sample size N reflects truly independent units

**But with clustered data**:
- All 48 observations from the same school share common characteristics
- Residuals are correlated within schools (shared unobserved factors)
- Effective sample size is ~1/48th of what the regression thinks it is

#### Consequences for Statistical Inference

**Standard errors are biased downward**:
- Regression thinks it has 48 independent observations per school
- True independence: only 1 observation per school
- Standard errors appear **artificially small**
- Confidence intervals appear **artificially narrow**

**P-values are misleading**:
- Small standard errors → large t-statistics
- Large t-statistics → small p-values
- Results appear highly significant when they may not be

**Example**:
```
# With clustering (INCORRECT)
Coefficient: 0.050
Std. Error:  0.002
t-value:     25.0
p-value:     < 0.001 ***

# After aggregation (CORRECT)
Coefficient: 0.050  (same point estimate)
Std. Error:  0.010  (5x larger - more realistic)
t-value:     5.0
p-value:     < 0.001 *** (still significant, but less dramatic)
```

---

## Scripts Affected

### Script 24: Analysis/24_quartile_slope_comparison.R ✅ **ALREADY FIXED**

**Status**: This script already includes proper aggregation (as of commit prior to 2025-11-20).

**Evidence of Fix**:
- **Lines 109-166**: `aggregate_to_school_year()` function defined
- **Line 165**: Function called: `df_aggregated <- aggregate_to_school_year(df_raw)`
- **Line 171**: Analysis uses `df_aggregated` (school-year level)

**Aggregation Logic**:
```r
aggregate_to_school_year <- function(df) {
  agg_df <- df %>%
    group_by(cds_school, academic_year) %>%
    summarise(
      # Sum suspensions across all races and reasons
      across(any_of(susp_cols), ~sum(.x, na.rm = TRUE)),

      # Take first value of school-level variables
      across(any_of(constant_cols), ~first(.x)),

      # Count observations aggregated
      n_observations_aggregated = n(),

      .groups = "drop"
    )
  return(agg_df)
}
```

**Verification**:
```r
# Script 24 output shows:
# >>> Initial rows (school-year-race-reason): 3,402,282
# >>> Aggregated to 130,236 school-year observations
# >>> Average observations per school-year: 26.1

# This confirms ~26-48 observations per school-year were aggregated
```

**Additional Bug Fix**: Line 535 correctly calculates `slope_ratio <- q4_slope / q1_slope` (already fixed).

---

### Script 25: Analysis/25_interaction_term_regression.R ⚠️ **FIX APPLIED**

**Status**: Missing aggregation step. Fixed 2025-11-20.

**Problem Identified**:
- **Line 70**: Loads raw data: `df_raw <- arrow::read_parquet(MERGED_PATH)`
- **Line 152-214**: Prepares variables directly from `df_raw`
- **Line 269-291**: Filters to analysis sample (still at school-year-race-reason level)
- **Line 374**: Runs regression on unaggregated data: `fit <- lm(formula_obj, data = analysis_df, weights = cumulative_enrollment)`

**Impact of Missing Aggregation**:
- Sample size reported as ~3.4M rows instead of ~130K school-year observations
- Standard errors artificially small
- T-statistics artificially large
- P-values artificially small (Type I error inflation)

---

## Fix Applied to Script 25

### Changes Made (2025-11-20)

**1. Added aggregation function (new lines 80-136)**:
```r
# === 3) Aggregate to school-year level =========================================
message("\n>>> Aggregating to school-year level...")
message(">>> Initial rows (school-year-race-reason): ", format_number(nrow(df_raw)))

# CRITICAL: Aggregate to school-year level to avoid clustering issues
aggregate_to_school_year <- function(df) {
  # [Same logic as Script 24]
  agg_df <- df %>%
    group_by(cds_school, academic_year) %>%
    summarise(
      # Sum suspensions across all races and reasons
      across(any_of(susp_cols), ~sum(.x, na.rm = TRUE)),

      # Take first value of school-level variables
      across(any_of(constant_cols), ~first(.x)),

      # Count observations aggregated
      n_observations_aggregated = n(),

      .groups = "drop"
    )
  return(agg_df)
}

df_aggregated <- aggregate_to_school_year(df_raw)
```

**2. Updated variable extraction to use aggregated data**:
```r
# Old (line 155):
df <- df_raw %>%
  mutate(pct_white_teachers = extract_pct_white_teachers(.))

# New (line 213):
df <- df_aggregated %>%
  mutate(pct_white_teachers = extract_pct_white_teachers(.))
```

**3. Updated Black student percentage extraction** (lines 218-230):
```r
# Old:
if ("prop_black" %in% names(df_raw)) {

# New:
if ("prop_black" %in% names(df_aggregated)) {
```

**4. Renumbered sections** (3→4, 4→5, 5→6, etc.) to reflect new aggregation step.

---

## Expected Impact on Results

### Sample Size Changes

**Before Fix**:
```
>>> Analysis sample: 3,402,282 school-year-race-reason observations
```

**After Fix** (expected):
```
>>> Initial rows (school-year-race-reason): 3,402,282
>>> Aggregated to ~130,236 school-year observations
>>> Average observations per school-year: ~26.1
>>> Analysis sample: ~130,236 school-year observations
```

### Regression Coefficient Changes

**Point estimates (coefficients)** should remain similar:
- The relationship between % White Teachers and suspension rates should be consistent
- Interaction term direction should be unchanged

**Standard errors will increase**:
- Standard errors typically increase by √(clustering ratio)
- With ~26-48 obs per school, expect SEs to increase by ~5-7x
- This reflects true uncertainty in school-level analysis

**P-values may change**:
- If original p-value was very small (p < 0.001), likely still significant after fix
- If original p-value was marginal (p ~ 0.04), may become non-significant
- This is CORRECT - previous p-values were artificially optimistic

### Example Projection

**Before aggregation** (hypothetical, invalid):
```
Interaction term: pct_white_teachers:pct_black_students
  Coefficient: 0.000123
  Std. Error:  0.000005
  t-value:     24.6
  p-value:     < 0.001 ***
```

**After aggregation** (expected, valid):
```
Interaction term: pct_white_teachers:pct_black_students
  Coefficient: 0.000123  (similar)
  Std. Error:  0.000025  (~5x larger)
  t-value:     4.92
  p-value:     < 0.001 *** (still highly significant)
```

**Interpretation**: Even with corrected standard errors, the interaction is likely to remain statistically significant if the effect is real. If it becomes non-significant, the original result was a false positive due to clustering.

---

## Verification Steps

### 1. Run Fixed Script 25

```r
source("Analysis/25_interaction_term_regression.R")
```

**Check console output**:
- [ ] Aggregation message appears
- [ ] Initial rows: ~3.4M
- [ ] Aggregated rows: ~130K
- [ ] Analysis sample size: ~130K (not 3.4M)

### 2. Compare Outputs

**Tables to compare**:
- `outputs/tables/25_interaction_regression_results.csv` (before vs. after)

**Key metrics to check**:
- [ ] Coefficients similar magnitude
- [ ] Standard errors increased (~5-7x)
- [ ] P-values changed (usually increased, but still significant if real effect)
- [ ] R² may change slightly
- [ ] N observations reported in model summary

### 3. Review Summary Document

**Check**:
- [ ] `outputs/summaries/25_interaction_term_regression_SUMMARY.md` updated
- [ ] Sample size correctly reported
- [ ] Methodological notes mention aggregation
- [ ] P-values and significance markers reflect corrected analysis

### 4. Compare with Script 24

**Cross-validation**:
- Script 24 (quartile slopes) and Script 25 (interaction term) test the same hypothesis
- Both should reach similar conclusions after aggregation fix
- If Script 24 found steeper slopes in Q4, Script 25 should find positive interaction

---

## Implications for Published Results

### If Script 25 Results Were Already Published

**Action Required**:
1. **Re-run analysis** with fixed script
2. **Compare results**:
   - If conclusions unchanged (still significant), issue a **technical correction** noting corrected standard errors
   - If conclusions changed (became non-significant), issue a **retraction or major correction**
3. **Update all downstream documents** referencing these results

**Disclosure Template**:
> "An error in the analysis pipeline was identified and corrected on [date]. The original analysis used school-year-race-reason level data (N = 3.4M observations) without aggregating to school-year level, resulting in clustered observations and biased standard errors. The corrected analysis aggregates to school-year level (N = 130K observations) before regression. [Point estimates remained similar / changed substantially]. [Statistical significance was preserved / was no longer significant]. All tables and figures have been updated to reflect the corrected analysis."

---

## Documentation Updates

### Files Updated

1. **Script 25**: `Analysis/25_interaction_term_regression.R`
   - Added aggregation step (lines 80-136)
   - Updated to use `df_aggregated` instead of `df_raw`

2. **This Fix Summary**: `docs/fixes/FIX_SCRIPT_24_25_AGGREGATION.md`
   - Documents issue, fix, and verification

3. **Script 25 Summary** (to be regenerated): `outputs/summaries/25_interaction_term_regression_SUMMARY.md`
   - Will reflect corrected sample sizes and statistical inference

### Documentation to Review

After re-running scripts, update:
- [ ] `outputs/summaries/24_quartile_slope_comparison_SUMMARY.md` (verify mentions aggregation)
- [ ] `outputs/summaries/25_interaction_term_regression_SUMMARY.md` (regenerate)
- [ ] Any presentations or reports citing these analyses
- [ ] `Analysis/data_processing_overview.md` (if applicable)

---

## Prevention: Automatic Summary Generation

### Current Status

**Both scripts already include automatic summary generation**:

**Script 24** (lines 566-842):
```r
# === 10) Generate executive summary (automatic) =================================
message("\n>>> Generating executive summary...")

summary_content <- paste0(
  "# Analysis 24: Teacher Diversity and Suspension Rates by School Racial Composition - Executive Summary\n\n",
  "**Analysis Date**: ", Sys.Date(), "\n",
  "**Data Period**: 2018-19 through 2023-24 academic years\n",
  # ... [content generation]
  "**Sample Size Breakdown**:\n",
  "- **Raw observations**: 3,402,282 school-year-race-reason records (before aggregation)\n",
  "- **Aggregated observations**: ", format(n_obs, big.mark = ","), " school-year observations\n",
  # ... [mentions aggregation prominently]
)

writeLines(summary_content, summary_path)
```

**Script 25** (lines 691-877):
```r
# === 11) Generate summary markdown ===========================================
message("\n>>> Generating analysis summary...")

summary_md <- paste0(
  "# Analysis 25: Interaction Term Regression - Testing the Mismatch Hypothesis\n\n",
  "**Analysis Date**: ", Sys.Date(), "\n",
  "**Data Period**: 2018-19 through 2023-24 academic years\n",
  # ... [content generation]
)

writeLines(summary_md, here::here("outputs", "summaries", "25_interaction_term_regression_SUMMARY.md"))
```

### Recommendations

**No additional changes needed** for automatic summary generation. Both scripts:
- ✅ Generate summaries automatically at end of script execution
- ✅ Follow template structure (see `outputs/summaries/TEMPLATE_SUMMARY.md`)
- ✅ Include all required metadata (dates, sample sizes, methodology)
- ✅ Save to correct location (`outputs/summaries/`)

**Enhancement for Script 25**: After re-running with fix, the summary will automatically include:
- Corrected sample size (~130K instead of 3.4M)
- Valid standard errors and p-values
- Methodological note about aggregation

**Future Scripts**: Use these as templates. Always include:
1. Aggregation step if working with granular data
2. Automatic summary generation
3. Documentation of data transformations

---

## Related Issues

### Similar Analyses to Review

**Other scripts that may need aggregation checks**:
1. Any script using `susp_v6_teacher_features.parquet` directly in regression
2. Scripts analyzing suspension rates at school level
3. Scripts using CDE data without explicit aggregation

**Review list**:
- [ ] `Analysis/21_teacher_diversity_regression.R` - Check for aggregation
- [ ] `Analysis/22_black_suspension_teacher_demographics.R` - Check data level
- [ ] Any custom analyses by users

### Testing Protocol

**For future analyses**:
1. **Check data granularity**: What is the observation unit in the input file?
   - School-year-race-reason? → Need aggregation
   - School-year? → Ready for school-level analysis
   - District-year? → Ready for district-level analysis

2. **Verify sample size**: Does N match expected number of schools/districts?
   - If N is ~3.4M, likely clustered (need aggregation)
   - If N is ~130K, likely school-year level (correct)

3. **Check for duplicates**: Are school-level variables repeated?
   ```r
   # Test: Should be TRUE for each school-year
   df %>%
     group_by(cds_school, academic_year) %>%
     summarise(n_distinct_pct_white = n_distinct(pct_white_teachers)) %>%
     filter(n_distinct_pct_white > 1)
   # ^ Should return 0 rows if properly aggregated
   ```

---

## Summary of Actions Taken

**2025-11-20**:
- ✅ Investigated Script 24: Confirmed already fixed
- ✅ Investigated Script 25: Identified missing aggregation
- ✅ Applied fix to Script 25:
  - Added `aggregate_to_school_year()` function
  - Updated to use `df_aggregated` instead of `df_raw`
  - Renumbered sections
- ✅ Created diagnostic report: `docs/fixes/FIX_SCRIPT_24_25_AGGREGATION.md`
- ⏳ Pending: Re-run Script 25 to verify fix
- ⏳ Pending: Compare before/after results
- ⏳ Pending: Update summaries and documentation

---

## Contact and Questions

For questions about:
- **This fix**: See `docs/fixes/FIX_SCRIPT_24_25_AGGREGATION.md` (this document)
- **Aggregation methodology**: See `Analysis/24_quartile_slope_comparison.R` lines 109-166
- **Clustering in regression**: See econometrics textbooks (e.g., Cameron & Miller, 2015)
- **CDE data structure**: See `Analysis/data_processing_overview.md`

---

## References

**Cameron, A. Colin, and Douglas L. Miller.** (2015). "A Practitioner's Guide to Cluster-Robust Inference." *Journal of Human Resources*, 50(2), 317-372.

**Key insight**: "Clustering arises when observations within clusters are correlated... OLS standard errors are incorrect when errors are clustered."

---

**END OF FIX SUMMARY**

**Document Version**: 1.0
**Created**: 2025-11-20
**Last Updated**: 2025-11-20
**Related Scripts**: `Analysis/24_quartile_slope_comparison.R`, `Analysis/25_interaction_term_regression.R`
**Output Location**: `docs/fixes/FIX_SCRIPT_24_25_AGGREGATION.md`
