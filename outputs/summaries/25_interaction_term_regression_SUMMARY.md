# Analysis 25: Interaction Term Regression - Testing the Mismatch Hypothesis

**Analysis Date**: 2025-11-20
**Data Period**: 2018-19 through 2023-24 academic years
**Academic Years Included**: 2019-20, 2021-22, 2022-23, 2023-24
**Total Schools Analyzed**: 2,904 unique schools across California
**School-Year Observations**: 7,808

---

## Research Question

**Is the association between teacher racial composition (% White Teachers) and suspension rates moderated by student racial composition (% Black Students)?**

In other words: Does the "White Teacher Effect" become stronger as Black student enrollment increases?

---

## Hypothesis: The "Mismatch Hypothesis"

**H0 (Null)**: The interaction coefficient = 0
  - The association between % White Teachers and suspension rates is the same regardless of % Black Students

**H1 (Alternative)**: The interaction coefficient > 0
  - The association between % White Teachers and suspension rates becomes STRONGER (more positive) as % Black Students increases
  - This would indicate that racial "mismatch" amplifies disciplinary disparities

---

## Major Findings

### 1. **Hypothesis Test Result**

✗ **HYPOTHESIS NOT SUPPORTED**

The interaction term is **NOT STATISTICALLY SIGNIFICANT** (p = 0.4367).

**Interpretation**: There is no evidence that the association between % White Teachers and suspension rates varies by % Black student enrollment.

### 2. **Interaction Coefficient**

| Parameter | Estimate | Std. Error | 95% CI | p-value |
|-----------|----------|------------|--------|--------|
| **Interaction: % White Teachers × % Black Students** | 1450.786962 | 1865.281066 | [-2205.664209, 5107.238132] | 0.4367 |

**What this means**:
- For every 1 percentage point increase in % Black Students, the slope (effect) of % White Teachers on suspension rates changes by 1450.7870 percentage points.
- Since this is **positive**, the effect of % White Teachers becomes MORE POSITIVE (steeper upward slope) as % Black Students increases.

### 3. **Marginal Effects at Different Black Enrollment Levels**

The effect of a **10 percentage point increase** in % White Teachers (e.g., from 40% to 50% White teachers) varies by school racial composition:

| Black Student Enrollment Level | % Black Students | Effect on Suspension Rate | Interpretation |
|-------------------------------|------------------|---------------------------|----------------|
| **Low** (10th percentile) | 1.7% | +47685.164 pp | Moderate |
| **Medium** (50th percentile) | 4.8% | +93230.550 pp | Moderate |
| **High** (90th percentile) | 16.3% | +259224.448 pp | Moderate |

### 4. **Full Regression Results**

**Formula**: `Suspension Rate ~ % White Teachers * % Black Students + Controls`

| Term | Coefficient | SE | 95% CI | p-value | Sig |
|------|-------------|----|---------|---------|---------|
| (Intercept) | -44224.021312 | 235206.793047 | [-505292.4199, 416844.3772] | 0.8509 |  |
| pct_white_teachers | 2305.760785 | 15781.082291 | [-28629.3931, 33240.9147] | 0.8838 |  |
| pct_black_students | -16267.296555 | 27816.269255 | [-70794.6448, 38260.0517] | 0.5587 |  |
| is_charter | -38432.023183 | 6222.869928 | [-50630.5173, -26233.5291] | < 0.001 | \*\*\* |
| school_level_factorMiddle | 57965.992641 | 4704.839289 | [48743.2458, 67188.7395] | < 0.001 | \*\*\* |
| school_level_factorHigh | 39671.613719 | 3704.311804 | [32410.1691, 46933.0584] | < 0.001 | \*\*\* |
| school_level_factorOther | 38717.579693 | 5913.008856 | [27126.4964, 50308.6630] | < 0.001 | \*\*\* |
| school_level_factorAlternative | -11126.064653 | 12467.915084 | [-35566.5222, 13314.3929] | 0.3722 |  |
| pct_white_teachers:pct_black_students | 1450.786962 | 1865.281066 | [-2205.6642, 5107.2381] | 0.4367 |  |

**Model Fit**:
- **R²**: 0.0628
- **Adjusted R²**: 0.0618
- **N**: 7,808 school-year observations
- **Weighted by**: Student enrollment

---

## Interpretation and Implications

### What This Analysis Tells Us

1. **The "Mismatch Hypothesis" is not supported**: The association between teacher racial composition and suspension rates does NOT significantly vary by student racial composition.

2. **Uniform associations**: The relationship between % White Teachers and suspension rates appears consistent across schools with different racial compositions.

### Comparison to Analysis 24 (Quartile Slope Comparison)

**Analysis 24** ran separate regressions for each quartile of Black student enrollment and visually compared slopes.

**Analysis 25** (this analysis) uses a pooled regression with an interaction term to formally test whether slopes differ.

**Advantages of the interaction term approach**:
- Provides a formal statistical test of slope differences
- Uses all data simultaneously (more statistical power)
- Produces a single coefficient quantifying the moderation effect
- Easier to interpret and communicate

**Complementary approaches**: Both analyses should reach similar conclusions if the pattern is consistent.

---

## Limitations and Caveats

### **CRITICAL: Correlational, Not Causal**

This analysis uses **observational data and weighted linear regression**, which can detect **associations** but cannot prove **causation**.

**What we CAN say**:
- There is a statistically significant interaction between % White Teachers and % Black Students in predicting suspension rates
- The association between teacher race and suspension rates varies by student racial composition

**What we CANNOT say**:
- Changing teacher racial composition would *cause* changes in suspension rates
- Teacher race *causes* different discipline practices
- The interaction represents a causal mechanism

### **Confounding Variables**

Many unmeasured factors could influence both variables:
- Historical segregation patterns
- Neighborhood socioeconomic conditions
- School resources and funding
- Administrative leadership quality
- District policies and enforcement
- School culture and climate

### **Model Assumptions**

This analysis assumes:
1. **Linear interaction**: The moderation effect is linear (constant across all levels)
2. **Additive effects**: The interaction adds to main effects
3. **Independence**: School-year observations are independent (may not hold if same schools appear multiple years)
4. **Homoscedasticity**: Variance of residuals is constant

### **Ecological Fallacy**

This is a school-level analysis. School-level patterns may not reflect individual teacher or student experiences.

---

## Data Outputs Available

### **Tables**
1. `25_interaction_regression_results.csv` - Full regression results with coefficients, SEs, CIs, p-values
2. `25_interaction_regression_results.xlsx` - Excel version with multiple sheets (coefficients + model statistics)

**Output Location**: `outputs/tables/`

### **Visualizations**
1. `25_interaction_marginal_effects.png` - Interaction plot showing predicted suspension rates at different levels of Black student enrollment

**Output Location**: `outputs/graphs/`

### **This Summary**
`25_interaction_term_regression_SUMMARY.md` - Executive summary (this document)

**Output Location**: `outputs/summaries/`

---

## Citation

**Suggested Citation**:
> UCLA Center for the Transformation of Schools (2025). "Interaction Term Regression: Testing the Mismatch Hypothesis." REACH Suspensions Analysis Project.

**Data Source**:
> California Department of Education. "Suspension Data File." 2018-19 through 2023-24. Retrieved from https://www.cde.ca.gov/ds/sd/sd/  
> California Department of Education. "Teacher Demographics Data." 2018-19 through 2023-24. Retrieved from https://www.cde.ca.gov/ds/sd/df/

**Analysis Documentation**:
> Full methodology and code available at: `Analysis/25_interaction_term_regression.R`

---

## Document Information

**Document Version**: 1.0  
**Document Created**: 2025-11-20  
**Analysis Script**: `Analysis/25_interaction_term_regression.R`  
**Output Location**: `outputs/summaries/25_interaction_term_regression_SUMMARY.md`  

---

**END OF SUMMARY**

