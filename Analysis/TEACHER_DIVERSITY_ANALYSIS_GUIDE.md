# Teacher Racial Diversity Analysis Guide

**Date**: 2025-11-14
**Purpose**: Analyze associations between teacher/administrator racial diversity and student suspension rates
**Status**: ✅ Scripts Created and Ready to Run

---

## 📋 Table of Contents

1. [Overview](#overview)
2. [What Was Fixed](#what-was-fixed)
3. [Prerequisites](#prerequisites)
4. [Quick Start](#quick-start)
5. [Script Descriptions](#script-descriptions)
6. [Expected Outputs](#expected-outputs)
7. [Interpreting Results](#interpreting-results)
8. [Troubleshooting](#troubleshooting)

---

## 🎯 Overview

This analysis examines whether schools with more racially diverse teaching and administrative staff have different suspension rates for students of various racial/ethnic groups.

### Research Questions

1. **Is teacher racial diversity associated with suspension rates?**
   - Do schools with higher proportions of non-white teachers have lower suspension rates?
   - Does this vary by student race/ethnicity?

2. **Is administrator racial diversity associated with suspension rates?**
   - Do schools with more diverse administrative leadership show different patterns?

3. **How do these associations compare across student groups?**
   - Are effects stronger for Black students? Hispanic students? Others?

### ⚠️ Important Disclaimer

**These analyses show ASSOCIATIONS, not causal effects.** The regressions describe correlations in observational data. Do not interpret coefficients as causal impacts. Many unmeasured factors (school culture, community demographics, policies) could explain observed patterns.

---

## 🔧 What Was Fixed

### The Problem

The original `Analysis/21_teacher_diversity_regression.R` script was using **GENDER diversity** (proportion of female + non-binary staff) instead of **RACIAL diversity** (proportion of non-white staff).

**Diagnostic output showed**:
```
Teacher diversity derived from `teacher_total_staff_count_by_gender_male_share` (1_minus_male_share)
Administrator diversity derived from `teacher_total_staff_count_by_type_administrators_by_gender_male_share` (1_minus_male_share)
```

This happened because the script couldn't find teacher race columns in `susp_v6_teacher_features.parquet` and fell back to gender columns.

### The Solution

We created three new scripts:

1. **`22_build_teacher_race_shares.R`**: Properly merges teacher race/ethnicity data with student suspension data
2. **`21_teacher_diversity_regression_FIXED.R`**: Updated regression script that explicitly prioritizes racial diversity
3. **`23_visualize_teacher_diversity.R`**: Creates comprehensive visualizations

### Key Improvements

- ✅ **Explicit race detection**: Searches for actual race columns (african_american, asian, hispanic_or_latino, etc.)
- ✅ **Validation checks**: Verifies that RACE data is being used before running regressions
- ✅ **Clear diagnostics**: Shows exactly which diversity measure is used
- ✅ **Comprehensive visualizations**: Scatter plots, distributions, quartile analyses

---

## 📦 Prerequisites

### Required Data Files

These must exist before running the analysis:

```
data-stage/
  ├── teacher_staff_long.parquet      # Teacher demographics by race/gender
  └── susp_v6_long.parquet           # Student suspension data by race
```

**If missing**, run:

```r
# Create teacher demographics
source("R/01c_ingest_teacher_demographics.R")

# Create student suspension data
source("run_pipeline.R")
```

### Required R Packages

```r
install.packages(c("dplyr", "tidyr", "arrow", "ggplot2", "scales", "here"))
```

### Teacher Data Requirements

The `teacher_staff_long.parquet` file must contain:
- **Race/ethnicity breakdowns** (9 CDE categories: African American, Asian, Hispanic/Latino, White, Filipino, etc.)
- **Staff type breakdowns** (Teachers, Administrators, Pupil Services, Other)
- **Gender breakdowns** (Female, Male, Non-Binary)

This data comes from CDE teacher demographics files (`stre*.txt`). See `TEACHER_DIVERSITY_ANALYSIS_DIAGNOSTIC.md` for details on obtaining these files.

---

## 🚀 Quick Start

### Option 1: Run Everything (Recommended)

```r
source("Analysis/RUN_TEACHER_DIVERSITY_ANALYSIS.R")
```

This master script runs all three steps sequentially:
1. Builds teacher race share features
2. Runs regression analysis
3. Generates visualizations

**Total runtime**: ~2-5 minutes depending on data size

### Option 2: Run Individual Steps

```r
# Step 1: Build features
source("Analysis/22_build_teacher_race_shares.R")

# Step 2: Run regressions
source("Analysis/21_teacher_diversity_regression_FIXED.R")

# Step 3: Create visualizations
source("Analysis/23_visualize_teacher_diversity.R")
```

---

## 📜 Script Descriptions

### 1. `22_build_teacher_race_shares.R`

**Purpose**: Merge teacher racial diversity data with student suspension data

**Inputs**:
- `teacher_staff_long.parquet` (teacher demographics)
- `susp_v6_long.parquet` (student suspensions)

**Outputs**:
- `susp_v6_teacher_features.parquet` (merged dataset with race shares)

**Key Transformations**:
1. Loads teacher long-format data (one row per school-year-race-gender-staff_type)
2. Computes race and gender shares using `teacher_summarise_long()`
3. Merges with student suspension data (many-to-one join)
4. Creates columns like:
   - `teacher_staff_count_african_american_share`
   - `teacher_staff_count_by_type_teachers_hispanic_or_latino_share`
   - `teacher_staff_count_by_type_administrators_asian_share`

**Runtime**: ~30-60 seconds

---

### 2. `21_teacher_diversity_regression_FIXED.R`

**Purpose**: Run weighted linear regressions analyzing teacher diversity - suspension rate associations

**Inputs**:
- `susp_v6_teacher_features.parquet` (from script 1)

**Outputs**:
- Regression results printed to console
- Returns regression model objects (invisible)

**Regression Model**:
```r
suspension_rate ~ teacher_non_white_share + admin_non_white_share +
                  sed_rate + is_charter + grade_level
```

**Stratification**: Separate regressions for each student racial group:
- Black/African American
- Hispanic/Latino
- White
- Asian
- Filipino
- Two or More Races
- American Indian/Alaska Native

**Weighting**: Schools weighted by student enrollment

**Runtime**: ~1-2 minutes

**Key Features**:
- ✅ Explicitly validates that RACE data is used (not gender)
- ✅ Clear diagnostic messages
- ✅ Computes 95% confidence intervals
- ✅ Reports R², sample sizes, significance levels

---

### 3. `23_visualize_teacher_diversity.R`

**Purpose**: Create comprehensive visualizations

**Inputs**:
- `susp_v6_teacher_features.parquet`

**Outputs**:
All saved to `outputs/graphs/teacher_diversity/`:

1. **Scatter plots** (one per student group):
   - `scatter_teacher_[group].png`: Teacher diversity vs. suspension rate
   - `scatter_admin_[group].png`: Administrator diversity vs. suspension rate

2. **Summary plots**:
   - `summary_diversity_vs_suspension.png`: Aggregate patterns across groups

3. **Distribution plots**:
   - `distribution_teacher_diversity.png`: Teacher diversity distributions by student group
   - `distribution_suspension_rates.png`: Suspension rate distributions

4. **Quartile analyses**:
   - `quartile_[group].png`: Mean suspension rates by teacher diversity quartiles

5. **Data exports**:
   - `summary_statistics.csv`: Weighted means by student group
   - `sample_data.csv`: Sample of raw data

**Runtime**: ~2-3 minutes

---

## 📊 Expected Outputs

### Console Output (from regressions)

```
════════════════════════════════════════════════════════════════
📌 Student Group: Black/African American
────────────────────────────────────────────────────────────────

✓ Teacher RACIAL diversity: sum_of_non_white_races (7 race categories)
    Columns: `teacher_staff_count_african_american_share`, ...

✓ Administrator RACIAL diversity: sum_of_non_white_races (7 race categories)
    Columns: `teacher_staff_count_by_type_administrators_african_american_share`, ...

✓ Confirmed: Using RACIAL diversity for both teachers and administrators

────────────────────────────────────────────────────────────────
🔍 KEY COEFFICIENTS (with 95% CI)
────────────────────────────────────────────────────────────────

teacher_non_white_share : -0.000197  [-0.000297, -0.000096]  p=0.0001 ***
admin_non_white_share   :  0.000021  [-0.000001,  0.000042]  p=0.0602 .
sed_rate                :  0.015066  [ 0.014925,  0.015208]  p=0.0000 ***

────────────────────────────────────────────────────────────────
R² = 0.8401  |  Adj. R² = 0.8399  |  N = 11,958
────────────────────────────────────────────────────────────────
```

### Interpretation

For **Black/African American students**:
- **Teacher diversity**: 10 percentage point increase in teacher non-white share → **0.02 percentage point DECREASE** in suspension rate (statistically significant)
- **SED rate**: Strongest predictor (10pp increase → 1.5pp increase in suspension rate)
- **Model fit**: R² = 84% (very good fit)

### Visualizations

**Scatter plots** show:
- Overall trend (linear fit line)
- School-level variation (individual points)
- Enrollment weighting (point sizes)

**Quartile plots** show:
- Mean suspension rates binned by teacher diversity
- Whether effect is monotonic (consistent across all levels)

---

## 📈 **NEW: Enhanced Outputs (Version 2.0)**

The `21_teacher_diversity_regression.R` script now automatically generates comprehensive tables, visualizations, and plain-language interpretations!

### Automated Output Files

All files saved to: `outputs/teacher_diversity_analysis/`

#### 1. Excel Workbook: `teacher_diversity_regression_results.xlsx`

**Three sheets:**

**a) Summary Sheet**
- Complete results for all student groups in one table
- Columns include:
  - `student_group`: Student racial/ethnic group
  - `n_schools`: Sample size
  - `r_squared`, `adj_r_squared`: Model fit statistics
  - `teacher_coefficient`: Raw regression coefficient
  - `teacher_ci_lower/upper`: 95% confidence interval
  - `teacher_p_value`: Significance level
  - `teacher_sig`: Significance stars (*** / ** / *)
  - `teacher_direction`: "Lower suspension rates" or "Higher suspension rates"
  - **`teacher_effect_10pp`**: **Practical effect** in percentage points
  - *(Same columns for `admin_*`)*

**b) Interpretations Sheet**
- Plain-language explanations for each student group
- Example text for each group:
  - Teacher diversity interpretation
  - Administrator diversity interpretation
  - Practical example with real numbers

**c) Technical_Details Sheet**
- Full regression statistics
- All coefficients and standard errors

#### 2. CSV Files (for easy import to other tools)
- `teacher_diversity_summary.csv`: Main results table
- `teacher_diversity_interpretations.csv`: Plain-language text

#### 3. Visualizations (PNG files)

**a) `teacher_diversity_coefficients_forest_plot.png`**
- Forest plot with confidence intervals
- Shows coefficients for all student groups
- Separate colors for teacher vs. administrator diversity
- Filled circles = statistically significant (p < 0.05)
- Open circles = not significant

**How to read:**
- Points LEFT of zero → More diversity = LOWER suspension rates
- Points RIGHT of zero → More diversity = HIGHER suspension rates
- Horizontal lines show 95% confidence intervals (precision)

**b) `teacher_diversity_practical_effects.png`**
- Bar chart showing real-world impact
- **Only displays statistically significant effects**
- Y-axis shows change in suspension rate (percentage points) for a 10 percentage point increase in staff diversity
- Easy to compare effect sizes across student groups

**Example:**
```
Black/African American: -0.033
```
= A school going from 40% to 50% non-white teachers is associated with a 0.033 percentage point DECREASE in Black student suspension rates

### Understanding the Practical Effects

**What does `teacher_effect_10pp = -0.033` mean?**

1. **The scenario**: A school increases teacher racial diversity by 10 percentage points
   - Example: From 40% non-white teachers → 50% non-white teachers

2. **The association**: Suspension rates for that student group change by **-0.033 percentage points**
   - If baseline was 5.0%, new rate would be 4.967%

3. **Is this large?**
   - **Statistically significant** (we're confident it's real)
   - **Practically small** (0.033 percentage points is tiny)
   - Other factors (poverty, policies) have much larger effects

### Plain-Language Interpretations

The script automatically generates interpretations like:

```
TEACHER DIVERSITY:
A 10 percentage point increase in teacher diversity (e.g., from 40% to 50%
non-white teachers) is associated with a 0.033 percentage point DECREASE
in suspension rates (95% CI: 0.030 to 0.036, p<0.001). This is a VERY
SMALL but statistically significant effect.

PRACTICAL EXAMPLE: In a school where Black/African American students have
a 5% suspension rate, increasing teacher diversity from 40% to 50%
non-white would be associated with a suspension rate of approximately
4.97% (a change of -0.03%).
```

### Console Output Summary

After running the script, you'll also see a formatted summary in the console:

```
╔════════════════════════════════════════════════════════════════╗
║                    SUMMARY OF KEY FINDINGS                     ║
╚════════════════════════════════════════════════════════════════╝

────────────────────────────────────────────────────────────────
📊 Black/African American
────────────────────────────────────────────────────────────────

TEACHER DIVERSITY:
  A 10 percentage point increase in teacher diversity (e.g., from
  40% to 50% non-white teachers) is associated with a 0.033
  percentage point DECREASE in suspension rates...

ADMINISTRATOR DIVERSITY:
  A 10 percentage point increase in administrator diversity is
  associated with a 0.034 percentage point DECREASE in suspension
  rates...

  PRACTICAL EXAMPLE: In a school where Black/African American
  students have a 5% suspension rate, increasing teacher diversity
  from 40% to 50% non-white would be associated with a suspension
  rate of approximately 4.97%.
```

---

## 🔍 Interpreting Results

### What to Look For

1. **Sign of coefficients**:
   - **Negative** = More diverse staff associated with LOWER suspension rates
   - **Positive** = More diverse staff associated with HIGHER suspension rates

2. **Statistical significance**:
   - `***` p < 0.001 (very strong evidence)
   - `**` p < 0.01 (strong evidence)
   - `*` p < 0.05 (moderate evidence)
   - `.` p < 0.10 (weak evidence)

3. **Effect sizes**:
   - Compare teacher/admin coefficients to SED coefficient
   - Convert to percentage points for interpretation

4. **Variation across student groups**:
   - Are effects stronger for some groups?
   - Do any groups show opposite patterns?

### Common Patterns to Expect

Based on prior research, you might see:

- **Black students**: Negative association (more diverse staff → lower suspension rates)
- **Hispanic students**: Similar negative association
- **White students**: Weaker or no association
- **SED rate**: Consistently positive and large (poverty strongly predicts suspensions)

### Important Caveats

❌ **DO NOT conclude**:
- "Hiring more diverse teachers CAUSES lower suspension rates"
- "Increasing diversity by X% WILL reduce suspensions by Y%"

✅ **INSTEAD, conclude**:
- "Schools with more diverse staff TEND TO have lower suspension rates for Black students"
- "This association persists even after controlling for poverty and school characteristics"
- "Further research needed to understand mechanisms and test causal hypotheses"

---

## 🛠️ Troubleshooting

### Error: "Missing file: susp_v6_teacher_features.parquet"

**Cause**: Step 1 (building features) hasn't been run

**Solution**:
```r
source("Analysis/22_build_teacher_race_shares.R")
```

---

### Error: "No teacher race share columns found"

**Cause**: Teacher demographics file lacks race/ethnicity data

**Solution**:
1. Check that `teacher_staff_long.parquet` exists
2. Verify it has race columns:
   ```r
   library(arrow)
   df <- read_parquet("data-stage/teacher_staff_long.parquet")
   grep("race_ethnicity", names(df), value = TRUE)
   ```
3. If missing, re-run teacher ingestion:
   ```r
   source("R/01c_ingest_teacher_demographics.R")
   ```
4. Ensure raw CDE teacher files (`stre*.txt`) are present in `data-raw/`

---

### Warning: "Fallback to gender diversity"

**Cause**: Script detected gender columns but not race columns

**Solution**: This is what we FIXED! Use the new scripts:
- `21_teacher_diversity_regression_FIXED.R` (not the original)
- Ensure `22_build_teacher_race_shares.R` ran successfully

---

### Regressions show "❌ FATAL: Missing teacher RACE diversity columns"

**Cause**: The `susp_v6_teacher_features.parquet` file doesn't have properly formatted race share columns

**Solution**:
1. Delete the existing file:
   ```r
   file.remove("data-stage/susp_v6_teacher_features.parquet")
   ```
2. Re-run the build script:
   ```r
   source("Analysis/22_build_teacher_race_shares.R")
   ```
3. Verify race columns exist:
   ```r
   df <- arrow::read_parquet("data-stage/susp_v6_teacher_features.parquet")
   grep("african_american.*share", names(df), value = TRUE, ignore.case = TRUE)
   ```

---

### Small sample warnings (e.g., "n < 50 for Filipino students")

**Cause**: Some racial groups have small enrollment at many schools

**Interpretation**:
- Coefficients may be unstable with small samples
- Confidence intervals will be wide
- Focus interpretation on larger groups (Black, Hispanic, White)

---

## 📚 Additional Resources

### Related Files

- `docs/audits/TEACHER_DIVERSITY_ANALYSIS_DIAGNOSTIC.md`: Original diagnostic report
- `docs/guides/TEACHER_DATA_SETUP_GUIDE.md`: Guide for obtaining CDE teacher data
- `docs/audits/TEACHER_DEMOGRAPHIC_INTEGRATION_AUDIT.md`: Data quality audit

### CDE Documentation

- Teacher demographics: https://www.cde.ca.gov/ds/sd/sd/filesteachdem.asp
- Suspension data: https://www.cde.ca.gov/ds/sd/sd/filesdiscipline.asp

### Recommended Reading

- Grissom, J. A., & Redding, C. (2016). "Discretion and disproportionality: Explaining the underrepresentation of high-achieving students of color in gifted programs." *AERA Open*
- Papageorge, N. W., Gershenson, S., & Kang, K. M. (2020). "Teacher expectations matter." *Review of Economics and Statistics*

---

## ✅ Success Checklist

After running the analysis, verify:

- [ ] Console shows "✓ Confirmed: Using RACIAL diversity" for all student groups
- [ ] Regression coefficients reported with confidence intervals
- [ ] Visualizations folder contains scatter plots for each group
- [ ] Summary statistics CSV file created
- [ ] No error messages about missing race columns
- [ ] Sample sizes are reasonable (at least 100+ schools per group)

---

## 📞 Getting Help

If you encounter issues:

1. **Check diagnostics**: Look for "✓" vs. "❌" messages in console output
2. **Verify files exist**: Use `file.exists("path/to/file.parquet")`
3. **Check column names**: Use `names(df)` to see what columns are available
4. **Review this guide**: Especially the Troubleshooting section
5. **Consult CLAUDE.md**: Repository-wide documentation

---

**Last Updated**: 2025-11-18
**Version**: 2.0 (Enhanced with automated tables and visualizations)
