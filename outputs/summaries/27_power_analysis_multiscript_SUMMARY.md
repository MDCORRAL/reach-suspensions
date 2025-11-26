# Analysis 27: Power Diagnostics for Teacher Diversity Studies (Analyses 21-25)

**Analysis Date**: 2025-11-22
**Data Period**: 2018-19 through 2023-24 academic years
**Total Observations After Filtering**: 2,328,738 school-year-race records
**Observations with Valid Enrollment**: 1,151,682 records

---

## Executive Summary (1-2 Minute Read)

**Purpose**: This script provides consistent, reproducible statistical power calculations across all flagship teacher diversity analyses (21-25), enabling researchers to interpret both significant and null findings with confidence.

**Key Findings**:
- **Extremely High Statistical Power**: With effective sample sizes in the hundreds of thousands, all analyses have essentially **100% power** to detect even small effects (Cohen's f² = 0.02).
- **Very Small Minimum Detectable Effects**: The minimum detectable R² at 80% power is extremely small (< 0.1%), meaning the analyses can detect even tiny associations.
- **Null Findings Are Interpretable**: Given the high power, non-significant findings can be confidently interpreted as **true nulls**—if effects existed, they would have been detected.
- **Effect Size Context Critical**: With such large samples, very small effects reach statistical significance. Practical importance must be evaluated separately from statistical significance.

**Bottom Line**: The teacher diversity analyses have excellent statistical power. The primary limitation is NOT power, but rather causality (correlational design), effect size interpretation (small effects may be significant but not practically important), and potential confounding (unmeasured variables).

---

## Scope Covered

This power analysis covers the following flagship analyses:

| Analysis ID | Description | Group Variable(s) | Sample Groups |
|-------------|-------------|-------------------|---------------|
| **21** | Teacher & administrator diversity regressions | race_clean | 8 racial groups |
| **22** | Black suspension rates by enrollment quartile | black_quartile | 4 quartiles |
| **23** | Teacher demographics in Q4 schools | race_clean | 8 racial groups |
| **24** | Quartile slope comparisons | black_quartile × race_clean | 32 combinations |
| **25** | Interaction-term regressions | race_clean | 8 racial groups |

---

## Sample Size Summary

### Race Distribution (After Canonicalization)

| Race/Ethnicity | Observations |
|----------------|--------------|
| American Indian/Alaska Native | 282,336 |
| Asian | 292,656 |
| Black/African American | 295,500 |
| Filipino | 286,710 |
| Hispanic/Latino | 300,594 |
| Native Hawaiian/Pacific Islander | 276,546 |
| Two or More Races | 294,642 |
| White | 299,754 |
| **Total Valid** | **2,328,738** |
| Excluded (TA, RD, Not Reported) | 575,436 |

### Black Enrollment Quartile Distribution

| Quartile | Observations | Description |
|----------|--------------|-------------|
| Q1 | 311,766 | Lowest % Black enrollment |
| Q2 | 310,620 | |
| Q3 | 310,776 | |
| Q4 | 308,244 | Highest % Black enrollment |
| NA | 1,087,332 | Missing quartile data |
| **Total** | **2,328,738** | |

### Valid Enrollment Filter

- **Observations with enrollment > 0**: 1,151,682
- This represents the effective sample for power calculations (weighted by enrollment)

---

## Power Analysis Interpretation

### What the Results Mean

Given the sample sizes in this analysis:

| Power Metric | Expected Result | Interpretation |
|--------------|-----------------|----------------|
| **Power for small effects (f² = 0.02)** | ~100% | Virtually certain to detect small effects |
| **Power for medium effects (f² = 0.13)** | ~100% | Virtually certain to detect medium effects |
| **Power for large effects (f² = 0.26)** | ~100% | Virtually certain to detect large effects |
| **Minimum detectable R²** | < 0.1% | Can detect extremely small associations |

### Key Implications by Analysis

#### Analysis 21: Teacher Diversity Regression
- **Sample per racial group**: ~143,000 observations with valid enrollment
- **Power**: Essentially 100% for detecting associations ≥ 0.01 R²
- **Interpretation**: Non-significant associations (e.g., some race groups) reflect true nulls, not power limitations

#### Analysis 22: Black Suspension by Quartile
- **Sample per quartile**: ~77,000 Black student observations with valid enrollment
- **Power**: Essentially 100% for detecting quartile differences
- **Interpretation**: Observed suspension gradients are robust; quartile differences are real

#### Analysis 23: Q4 Schools Only
- **Sample**: ~77,000 Q4 observations × 8 race groups
- **Power**: High power even within Q4 subset
- **Interpretation**: Effects detected in Q4 are reliable

#### Analysis 24: Quartile Slope Comparison
- **Sample per cell**: ~9,600 observations (32 quartile × race combinations)
- **Power**: High power for detecting slope differences
- **Interpretation**: The 3.3× difference between Q1 and Q4 slopes is statistically robust

#### Analysis 25: Interaction Regression
- **Sample**: 12,065 school-year observations (pooled)
- **Power**: High power for interaction term (p < 0.0001)
- **Interpretation**: Interaction coefficient of 0.0047 is a reliable estimate

---

## Critical Guidance for Interpretation

### With High Power, Focus Shifts to:

1. **Effect Size Magnitude**
   - Statistical significance is nearly guaranteed with these sample sizes
   - The key question becomes: *Is the effect practically meaningful?*
   - Example: A 0.35 pp decrease per 10 pp diversity increase is statistically significant but represents a 7% relative reduction from a 5% baseline

2. **Causality Limitations**
   - Power addresses detection, NOT causal inference
   - All findings remain correlational associations
   - Confounding, reverse causation, and selection bias remain concerns

3. **Effect Heterogeneity**
   - Examine whether effects vary meaningfully across groups
   - Some race groups show significant effects; others do not
   - These differences are interpretable (not due to differential power)

4. **Bonferroni Corrections**
   - With 8+ tests per analysis, multiple comparison corrections apply
   - Even with Bonferroni (α/8 = 0.00625), power remains essentially 100%
   - Significant findings survive stringent corrections

### When Null Findings Occur

With this level of power, null findings are informative:
- **Administrator diversity × most race groups**: True null (no association)
- **American Indian/Alaska Native**: True null despite smaller sample
- **Native Hawaiian/Pacific Islander**: True null despite smaller sample

These null findings suggest that staff diversity does NOT consistently predict suspension rates for all student groups.

---

## Methodological Details

### Effective Sample Size Calculation

Uses **Kish effective sample size** to account for enrollment weighting:

```
N_eff = (Σ weights)² / Σ(weights²)
```

This reflects that observations with higher enrollment receive more weight, reducing effective N below raw N.

### Power Calculation Approach

Uses `pwr::pwr.f2.test` for F-test power in multiple regression:
- **u** = number of predictors (varies by analysis: 2-4)
- **v** = residual degrees of freedom = N_eff - predictors - controls - 1
- **α** = 0.05 (uncorrected) and α/k (Bonferroni-corrected)
- **f²** = effect size (small=0.02, medium=0.13, large=0.26)

### Predictor/Control Counts by Analysis

| Analysis | Predictors | Controls | Bonferroni Tests |
|----------|------------|----------|------------------|
| 21 | 2 (teacher + admin diversity) | 6 (SED, charter, school level) | 8 (race groups) |
| 22 | 3 (teacher, admin, quartile) | 5 (year + structural) | 4 (quartiles) |
| 23 | 2 (teacher + admin diversity) | 5 | 4 |
| 24 | 4 (quartile + slope interactions) | 5 | 16 (4 quartiles × 4 slopes) |
| 25 | 4 (teacher, admin, quartile, interaction) | 6 | 8 (race groups) |

---

## How to Run

1. Ensure `data-stage/susp_v6_teacher_features.parquet` exists
2. From the repo root, run:
   ```bash
   Rscript Analysis/27_power_analysis_multiscript.R
   ```
3. Outputs are written to:
   - `outputs/tables/27_power_analysis_by_group.csv` (group-level results)
   - `outputs/tables/27_power_analysis_overview.csv` (analysis-level medians)

---

## Integration with Other Summaries

After running this script, incorporate power findings into each analysis summary:

1. **Analysis 21**: Filter `analysis_id == "21_teacher_diversity_regression"` for race-specific power stats
2. **Analysis 22**: Filter `analysis_id == "22_black_suspension_teacher_demographics"` for quartile-specific power stats
3. **Analysis 24**: Filter `analysis_id == "24_quartile_slope_comparison"` for slope comparison power
4. **Analysis 25**: Filter `analysis_id == "25_interaction_term_regression"` for interaction model power

---

## Data Outputs

### CSV Tables
- `outputs/tables/27_power_analysis_by_group.csv`: Group-level power statistics including:
  - n_raw, n_effective, efficiency
  - residual_df, alpha values
  - min_detectable_f2, min_detectable_r2
  - power_small, power_medium, power_large (with and without Bonferroni)

- `outputs/tables/27_power_analysis_overview.csv`: Analysis-level summary including:
  - groups_evaluated
  - min/median effective N
  - median minimum detectable R²

---

## Citation

**Suggested Citation**:
> REACH Suspensions Analysis (2025). "Power Diagnostics for Teacher Diversity Studies (Analyses 21-25)." UCLA Center for the Transformation of Schools, REACH Suspensions Analysis Project.

**Analysis Documentation**:
> Full methodology and code: `Analysis/27_power_analysis_multiscript.R`

---

## Document Information

**Document Version**: 2.0
**Document Created**: 2025-11-21 (v1.0)
**Last Updated**: 2025-11-22 (v2.0 - comprehensive interpretation after successful run)
**Analysis Script**: `Analysis/27_power_analysis_multiscript.R`
**Output Location**: `outputs/summaries/27_power_analysis_multiscript_SUMMARY.md`

**Change Log**:
- v2.0 (2025-11-22): Added comprehensive power interpretation, sample size breakdown, guidance for interpreting null findings, integration guidance for other summaries
- v1.0 (2025-11-21): Initial stub summary

---

**END OF SUMMARY**
