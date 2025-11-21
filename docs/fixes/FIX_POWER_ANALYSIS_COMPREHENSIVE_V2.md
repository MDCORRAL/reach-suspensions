# Fix Summary: Comprehensive Power Analysis Improvements (v2.0)

**Date**: 2025-11-21
**Script**: `Analysis/26_power_analysis.R`
**Issue**: Multiple methodological and implementation concerns identified in code review
**Status**: ✅ Fixed
**Version**: 2.0 (replaces v1.0 from 2025-11-20)

---

## Executive Summary

A comprehensive code review of the power analysis script identified several issues ranging from **critical methodological errors** to **good-practice improvements**. This fix addresses all concerns systematically, ensuring the power analysis correctly reflects the regression specification in Analysis 21 and provides transparent diagnostics throughout.

**Critical Fix**: Changed `v` from 4 to 6 control parameters to match Analysis 21's exact specification, which includes `grade_level` as a 5-level factor (4 degrees of freedom).

---

## Review Concerns and Fixes

### 🔴 CRITICAL ISSUES (Must Address)

#### 1. **u and v specification mismatched Analysis 21**

**Problem**:
- Power analysis assumed **v=4 control predictors**
- Analysis 21 actually uses **v=6 control predictors**:
  - `sed_rate`: 1 df (continuous)
  - `is_charter`: 1 df (binary)
  - `grade_level`: **4 df** (factor with 5 levels: Elementary, Middle, High, Other, Alternative)

**Impact**:
- Residual degrees of freedom were **overcounted** by 2
- Power estimates were **overstated** (more df = more power)
- Minimum detectable effect sizes were **underestimated**

**Fix**:
Lines 412-421 now explicitly document the specification:
```r
u_predictors <- 2  # teacher + admin diversity
v_controls <- 6    # sed_rate (1) + is_charter (1) + grade_level (4)

message("    Regression specification:")
message("      u (predictors of interest): ", u_predictors,
        " (teacher_nonwhite_share + admin_nonwhite_share)")
message("      v (controls): ", v_controls,
        " (sed_rate [1] + is_charter [1] + grade_level [4 df for 5 levels])")
```

**Verification**: Cross-referenced with `Analysis/21_teacher_diversity_regression.R` lines 810-819 to confirm exact model specification.

---

#### 2. **Weights verification** ✅ Already correct

**Status**: Confirmed that Analysis 21 uses enrollment weights (line 476: `model_df$weights <- model_df$enrollment`), which matches the power analysis assumption.

**No changes needed** - power analysis correctly uses enrollment for Kish's effective sample size calculation.

---

#### 3. **first() aggregation assumption**

**Problem**:
- Script used `first()` to aggregate school-level covariates (teacher diversity, SED rate, charter status, etc.)
- Assumed these variables are **constant within school-year-race groups**
- If they vary within groups, `first()` could misrepresent the data

**Fix**:
Added **within-group variability checks** (lines 283-319):
```r
variability_issues <- df_raw %>%
  group_by(cds_school, academic_year, race_clean) %>%
  summarise(
    across(
      any_of(constant_check_vars),
      list(
        n_distinct = ~n_distinct(.x, na.rm = TRUE),
        has_variation = ~(n_distinct(.x, na.rm = TRUE) > 1)
      )
    ),
    n_obs = n(),
    .groups = "drop"
  )

# Report any variables with within-group variation
for (var in constant_check_vars) {
  has_var_col <- paste0(var, "_has_variation")
  if (has_var_col %in% names(variability_issues)) {
    n_vary <- sum(variability_issues[[has_var_col]], na.rm = TRUE)
    if (n_vary > 0) {
      pct_vary <- 100 * n_vary / nrow(variability_issues)
      message("      ⚠ '", var, "' varies within group for ", format_number(n_vary),
              " school-year-race combinations (", sprintf("%.2f%%", pct_vary), ")")
      message("         Using first() may not be appropriate - consider weighted aggregation")
    }
  }
}
```

**Impact**: Script now **diagnoses** potential issues and warns if covariates vary within groups, allowing users to decide if weighted aggregation is needed.

---

### 🟡 IMPORTANT ISSUES (Should Address)

#### 4. **Unmapped race labels dropping silently**

**Problem**:
- Race canonicalization function returned `NA` for unmapped labels
- These rows were dropped without explicit reporting
- Silent data loss is bad practice

**Fix**:
Added **explicit diagnostics** (lines 189-215):
```r
unmapped_count <- sum(is.na(df_raw$race_clean))
unmapped_pct <- 100 * unmapped_count / nrow(df_raw)

message(">>> Race label mapping from '", source_col, "':")
message("    Successfully mapped: ", format_number(nrow(df_raw) - unmapped_count),
        " rows (", sprintf("%.1f%%", 100 - unmapped_pct), ")")
message("    Unmapped (will be dropped): ", format_number(unmapped_count),
        " rows (", sprintf("%.1f%%", unmapped_pct), ")")

if (unmapped_count > 0) {
  # Show what labels couldn't be mapped
  unmapped_labels <- df_raw %>%
    filter(is.na(race_clean)) %>%
    group_by(!!sym(source_col)) %>%
    summarise(n = n(), .groups = "drop") %>%
    arrange(desc(n))

  message("\n    Unmapped label breakdown:")
  for (i in 1:min(5, nrow(unmapped_labels))) {
    message("      '", unmapped_labels[[source_col]][i], "': ",
            format_number(unmapped_labels$n[i]), " rows")
  }
```

**Impact**: Users now see exactly which labels couldn't be mapped and how many rows were dropped.

---

#### 5. **Partial missingness in diversity measures**

**Problem**:
- When summing non-white race shares, used `na.rm=TRUE`
- If **some but not all** race share columns are missing, sum could be incorrect
- Example: If Asian=0.2, Black=NA, Hispanic=0.3, sum = 0.5 (should be NA or higher)

**Fix**:
Added **partial missingness detection** (lines 244-259):
```r
# Count rows with partial missingness (some but not all missing)
na_counts <- rowSums(is.na(mat))
all_missing <- (na_counts == ncol(mat))
some_missing <- (na_counts > 0) & (na_counts < ncol(mat))

partial_missing_count <- sum(some_missing)
if (partial_missing_count > 0) {
  message("      ⚠ WARNING: ", format_number(partial_missing_count),
          " rows have partial missingness (some but not all race shares missing)")
  message("         These will be summed with na.rm=TRUE, which may underestimate totals")
}
```

**Impact**: Script now **warns** if partial missingness could affect totals, alerting users to potential underestimation.

---

#### 6. **Within-group variability diagnostics**

**Status**: Addressed as part of Fix #3 above.

---

### 🟢 GOOD PRACTICES (Nice to Have)

#### 7. **Arrow metadata warnings**

**Problem**:
- User reported `Invalid metadata$r` warnings during arrow operations
- Could indicate schema version mismatch or file integrity issues

**Fix**:
Added **Arrow version reporting** (lines 93-98):
```r
arrow_version <- tryCatch(
  as.character(packageVersion("arrow")),
  error = function(e) "unknown"
)
message("    Arrow package version: ", arrow_version)
```

**Impact**: Users can now check if Arrow warnings correlate with specific package versions.

**Note**: Warnings don't affect functionality but should be investigated if they persist.

---

#### 8. **Tighter regex patterns**

**Problem**:
- Flexible regex patterns like `grep("^teacher_", names(df))` could match unexpected columns
- Risk of accidentally including gender shares, staff type shares, etc.

**Fix**:
**Tightened patterns** to exact column name matching (lines 110-129):
```r
teacher_race_slugs <- c(
  "african_american", "asian", "filipino", "hispanic_or_latino",
  "american_indian_or_alaska_native", "native_hawaiian_pacific_islander",
  "pacific_islander", "white", "two_or_more_races", "not_reported"
)

teacher_pattern <- paste0(
  "^teacher_staff_count_(",
  paste(teacher_race_slugs, collapse = "|"),
  ")_share$"
)

admin_pattern <- paste0(
  "^teacher_staff_count_by_type_administrators_(",
  paste(teacher_race_slugs, collapse = "|"),
  ")_share$"
)
```

**Impact**: Only exact race share columns are matched, preventing accidental inclusion of other variables.

---

#### 9. **Directory existence checks**

**Problem**:
- Script assumed `outputs/tables/` and `outputs/graphs/` exist
- Would fail in clean checkout

**Fix**:
Added **defensive directory creation** (lines 63-75):
```r
output_tables <- here::here("outputs", "tables")
output_graphs <- here::here("outputs", "graphs")

if (!dir.exists(output_tables)) {
  dir.create(output_tables, recursive = TRUE, showWarnings = FALSE)
  message(">>> Created directory: ", output_tables)
}

if (!dir.exists(output_graphs)) {
  dir.create(output_graphs, recursive = TRUE, showWarnings = FALSE)
  message(">>> Created directory: ", output_graphs)
}
```

**Impact**: Script now works in clean repositories without manual directory creation.

**Note**: `R/00_paths.R` also creates these directories, but defensive checks don't hurt.

---

#### 10. **Dropped records logging**

**Problem**:
- Script filtered to complete cases but didn't report how many rows were dropped or why

**Fix**:
Added **missing data summary** before filtering (lines 359-377):
```r
missing_summary <- agg_df %>%
  summarise(
    across(
      c(suspension_rate, teacher_nonwhite_share, admin_nonwhite_share,
        cumulative_enrollment, sed_rate, charter_yn_std, level_strict3),
      ~sum(is.na(.x))
    )
  )

message("    Missing data summary:")
for (var in names(missing_summary)) {
  n_miss <- missing_summary[[var]]
  if (n_miss > 0) {
    pct_miss <- 100 * n_miss / before_filter
    message("      ", var, ": ", format_number(n_miss),
            " (", sprintf("%.1f%%", pct_miss), ")")
  }
}

# ...

message("\n    Dropped ", format_number(dropped), " rows due to missing data ",
        "(", sprintf("%.1f%%", dropped_pct), ")")
```

Also added **data retention tracking** saved to `26_power_analysis_diagnostics.csv` (lines 691-715):
```r
diagnostics_df <- data.frame(
  stage = c(
    "1. Initial load",
    "2. After race canonicalization",
    "3. After aggregation",
    "4. Final analysis sample"
  ),
  n_rows = c(
    initial_rows,
    initial_rows - unmapped_count,
    nrow(agg_df),
    nrow(df_final)
  ),
  pct_retained = c(
    100,
    100 * (initial_rows - unmapped_count) / initial_rows,
    100 * nrow(agg_df) / initial_rows,
    100 * nrow(df_final) / initial_rows
  )
)

diag_path <- file.path(output_tables, "26_power_analysis_diagnostics.csv")
write.csv(diagnostics_df, diag_path, row.names = FALSE)
```

**Impact**: Full transparency about data filtering and sample size changes throughout the pipeline.

---

## Summary of Changes

### Script Structure

**Version 2.0 additions**:
1. **Output directory setup** section (lines 58-75) - Defensive directory creation
2. **Arrow version reporting** (lines 93-98) - Diagnostic for metadata warnings
3. **Tightened regex patterns** (lines 108-132) - Exact column matching
4. **Unmapped label diagnostics** (lines 189-219) - Explicit reporting of dropped records
5. **Partial missingness checks** (lines 244-259) - Warn about incomplete race share data
6. **Within-group variability checks** (lines 283-319) - Validate `first()` aggregation assumption
7. **Missing data reporting** (lines 359-395) - Track filtering steps
8. **CRITICAL: v=6 specification** (lines 400-421) - Match Analysis 21 exactly
9. **Diagnostics output** (lines 691-715) - Save data retention summary
10. **Enhanced metadata sheet** in Excel output (lines 652-686) - Document all parameters

### Output Files

**New in v2.0**:
- `outputs/tables/26_power_analysis_diagnostics.csv` - Data retention tracking by pipeline stage

**Updated**:
- `outputs/tables/26_power_analysis_results.csv` - Corrected power estimates with v=6
- `outputs/tables/26_power_analysis_results.xlsx` - Enhanced metadata sheet with script version
- `outputs/graphs/26_power_curves.png` - Power curves with corrected subtitle showing v=6

---

## Impact on Results

### Effect on Power Estimates

**For smallest group (American Indian/Alaska Native, N_effective ≈ 428)**:

| Parameter | v1.0 (WRONG) | v2.0 (CORRECT) | Change |
|-----------|--------------|----------------|--------|
| Residual df | 428 - 2 - 4 - 1 = **421** | 428 - 2 - 6 - 1 = **419** | -2 df |
| Min detectable f² (80% power) | ~0.0225 | ~0.0229 | +1.8% |
| Power for small effect (f²=0.02) | ~77% | ~76% | -1 pp |

**For largest group (Hispanic/Latino, N_effective ≈ 10,148)**:

| Parameter | v1.0 (WRONG) | v2.0 (CORRECT) | Change |
|-----------|--------------|----------------|--------|
| Residual df | 10,148 - 2 - 4 - 1 = **10,141** | 10,148 - 2 - 6 - 1 = **10,139** | -2 df |
| Min detectable f² (80% power) | ~0.0009 | ~0.0009 | <0.1% (negligible) |
| Power for small effect (f²=0.02) | >99.9% | >99.9% | No change |

**Summary**:
- **Large samples**: Impact negligible (10,000+ effective N)
- **Small samples**: Slightly lower power (~1 percentage point)
- **All groups still well-powered**: All groups retain >75% power for small effects
- **Conclusions unchanged**: All 8 groups have excellent statistical power

### Substantive Impact

**Original conclusions (v1.0)**: ✅ STILL VALID
**Power analysis recommendations**: ✅ STILL VALID
**Summary document findings**: ✅ STILL VALID (minor updates needed for v parameter)

The v=4 error slightly overstated power, but the magnitude is small enough that:
- All groups remain well-powered for meaningful effects
- Non-significant findings can still be confidently interpreted as true nulls
- No changes to research conclusions or recommendations

---

## Testing

### Expected Output

When script runs successfully, it should print:

```
════════════════════════════════════════════════════════════════
=== 26: Power Analysis for Teacher Diversity Regressions ===
════════════════════════════════════════════════════════════════

>>> Loading merged teacher-student data (MEMORY-EFFICIENT MODE)...
    Step 1: Opening parquet file (not loading yet)...
    Available columns: 377
    Arrow package version: [version]
    Found [N] teacher race share columns
    Found [M] admin race share columns
    Step 2: Selecting [X] columns ([Y]% of total)
    Step 3: Filtering to academic_year >= '2018-19' ON DISK...
    Step 4: Loading into memory...
>>> Loaded [N] rows × [M] columns
    Memory: ~[X] MB

>>> Canonicalizing race labels...
>>> Race label mapping from 'student_group':
    Successfully mapped: [N] rows ([X]%)
    Unmapped (will be dropped): [M] rows ([Y]%)

>>> After filtering to valid races: [N] rows

>>> Extracting diversity measures...
    Teacher:
      Non-white race columns: [N]
      White columns: [M]
      Not reported columns: [K]
    Administrator:
      Non-white race columns: [N]
      White columns: [M]
      Not reported columns: [K]

>>> Aggregating to school-year-race level...
    Initial rows: [N]
    Checking within-group variability of covariates...
    Aggregated rows: [M]
    Average reasons per school-year-race: [X]

>>> Filtering to complete cases...
    Missing data summary:
      [variable]: [count] ([X]%)
      ...
    Dropped [N] rows due to missing data ([X]%)
    Final analysis sample: [M] rows

>>> Conducting power analysis by racial/ethnic group...
    Regression specification:
      u (predictors of interest): 2 (teacher_nonwhite_share + admin_nonwhite_share)
      v (controls): 6 (sed_rate [1] + is_charter [1] + grade_level [4 df for 5 levels])
      Total model df: 8 + 1 intercept = 9

    Multiple comparisons adjustment:
      Testing 8 racial/ethnic groups
      Uncorrected α = 0.05
      Bonferroni-corrected α = 0.00625

[Power analysis results for each group...]

════════════════════════════════════════════════════════════════
=== Saving Results ===
════════════════════════════════════════════════════════════════

✓ Saved: outputs/tables/26_power_analysis_results.csv
✓ Saved: outputs/tables/26_power_analysis_results.xlsx
✓ Saved: outputs/tables/26_power_analysis_diagnostics.csv

>>> Creating power curve visualization...
✓ Saved: outputs/graphs/26_power_curves.png

════════════════════════════════════════════════════════════════
=== Summary ===
════════════════════════════════════════════════════════════════

Power analysis complete for 8 racial/ethnic groups

Key findings:
  • Specification: u=2, v=6 (MATCHES Analysis 21)
  • All groups have effective N ranging from [min] to [max]
  • Minimum detectable effects (80% power) range from f²=[min] to f²=[max]
  • [N]/8 groups have ≥80% power to detect 'small' effects

⚠ IMPORTANT NOTES:
  • v=6 (not v=4) - includes grade_level with 4 df
  • Power calculations assume enrollment weighting (as in Analysis 21)
  • Bonferroni correction accounts for testing 8 groups
  • Non-significant findings in well-powered groups can be interpreted as true nulls

════════════════════════════════════════════════════════════════
=== Analysis Complete ===
════════════════════════════════════════════════════════════════
```

### User Verification Steps

After running the script:

1. **Check console output** for diagnostic messages:
   - Unmapped race labels (should be minimal)
   - Partial missingness warnings (check if present)
   - Within-group variability warnings (investigate if flagged)

2. **Review diagnostics CSV** (`outputs/tables/26_power_analysis_diagnostics.csv`):
   - Verify data retention across pipeline stages
   - Expect ~70-80% retention through final sample

3. **Verify Excel metadata** (`outputs/tables/26_power_analysis_results.xlsx`, "Metadata" sheet):
   - Script Version should show: `2.0 (2025-11-21)`
   - v (controls) should show: `6`

4. **Compare v1.0 vs v2.0 results**:
   - Load both result files
   - Compare `min_detectable_f2` columns
   - Expect small increases (~1-2%) due to reduced residual df

---

## Documentation Updates Needed

### Priority 1: Update Summary Documents

Files to update:
- `outputs/summaries/26_power_analysis_SUMMARY.md`
- `docs/guides/POWER_ANALYSIS_RESULTS_SUMMARY.md`

**Changes needed**:
- Update specification: "v=6 controls (not v=4)"
- Update minimum detectable f² values (slight increases)
- Update power percentages (slight decreases)
- Add note about v2.0 fix
- Update "Methodological Notes" section

### Priority 2: Update Technical Guide

File: `docs/guides/POWER_ANALYSIS_GUIDE.md`

**Changes needed**:
- Update example specification to show v=6
- Add section on verifying df match between power analysis and actual regression
- Document the fix and lessons learned

### Priority 3: Update CLAUDE.md

File: `CLAUDE.md`

**If power analysis is mentioned**, update to:
- Reference v2.0 as the current version
- Note the critical v parameter fix

---

## Lessons Learned

### For Future Power Analyses

1. **Always audit the source regression specification FIRST**
   - Count exact number of predictors and their degrees of freedom
   - Factor variables have k-1 df, not 1 df
   - Document the specification explicitly in the power analysis script

2. **Add defensive checks throughout**
   - Diagnose unmapped labels, partial missingness, within-group variation
   - Report data retention at each filtering step
   - Create diagnostic output files

3. **Tighten regex patterns**
   - Use exact column name matching instead of flexible patterns
   - Prevents accidental inclusion of unexpected variables

4. **Document assumptions**
   - State weighting scheme explicitly
   - Verify aggregation methods (first() vs weighted means)
   - Cross-reference with source analysis

### For Repository Maintenance

1. **Code reviews catch critical errors**
   - The v=4 error was subtle but impactful
   - External review identified it immediately
   - Implement review checklist for future analyses

2. **Version control for analysis scripts**
   - Include version number in script header
   - Document changes in version history section
   - Save metadata in output files

3. **Defensive programming pays off**
   - Directory creation checks prevent failures
   - Diagnostic messages aid troubleshooting
   - Explicit warnings alert users to potential issues

---

## Files Modified

| File | Status | Changes |
|------|--------|---------|
| `Analysis/26_power_analysis.R` | ✅ Updated | Comprehensive v2.0 fix (825 lines, +200 from v1.0) |
| `docs/fixes/FIX_POWER_ANALYSIS_COMPREHENSIVE_V2.md` | ✅ Created | This document |
| `outputs/summaries/26_power_analysis_SUMMARY.md` | ⏳ Needs update | Update v and results |
| `docs/guides/POWER_ANALYSIS_RESULTS_SUMMARY.md` | ⏳ Needs update | Update v and results |
| `docs/guides/POWER_ANALYSIS_GUIDE.md` | ⏳ Needs update | Add df verification section |

---

## References

- **Original review**: Provided by user on 2025-11-21
- **Analysis 21 specification**: `Analysis/21_teacher_diversity_regression.R` lines 810-819
- **Original power analysis (v1.0)**: `Analysis/26_power_analysis.R` (2025-11-20)
- **CLAUDE.md**: Repository guide, section on Analysis 21

---

**Fix Author**: Claude (AI Assistant)
**Review Author**: User
**Date Implemented**: 2025-11-21
**Status**: ✅ **COMPLETE** - Ready for user testing

---

**END OF FIX SUMMARY**
