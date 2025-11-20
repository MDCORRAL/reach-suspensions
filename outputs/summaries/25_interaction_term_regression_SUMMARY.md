# Analysis 25: Interaction Term Regression - Testing the Mismatch Hypothesis

**Analysis Date**: 2025-11-20
**Data Period**: 2018-19 through 2023-24 academic years
**Academic Years Included**: 2019-20, 2021-22, 2022-23, 2023-24
**Total Schools Analyzed**: 4,359 unique schools across California
**School-Year Observations**: 427,842

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

✓ **HYPOTHESIS SUPPORTED**

The interaction term is **POSITIVE** and **STATISTICALLY SIGNIFICANT** (p < 0.001).

**Interpretation**: The association between % White Teachers and suspension rates is **AMPLIFIED** in schools with higher % Black student enrollment.

### 2. **Interaction Coefficient**

| Parameter | Estimate | Std. Error | 95% CI | p-value |
|-----------|----------|------------|--------|--------|
| **Interaction: % White Teachers × % Black Students** | 0.001892 | 0.000046 | [0.001803, 0.001981] | < 0.001 |

**What this means**:
- For every 1 percentage point increase in % Black Students, the slope (effect) of % White Teachers on suspension rates changes by 0.0019 percentage points.
- Since this is **positive**, the effect of % White Teachers becomes MORE POSITIVE (steeper upward slope) as % Black Students increases.

### 3. **Marginal Effects at Different Black Enrollment Levels**

The effect of a **10 percentage point increase** in % White Teachers (e.g., from 40% to 50% White teachers) varies by school racial composition:

| Black Student Enrollment Level | % Black Students | Effect on Suspension Rate | Interpretation |
|-------------------------------|------------------|---------------------------|----------------|
| **Low** (10th percentile) | 1.8% | +0.037 pp | Very small |
| **Medium** (50th percentile) | 5.5% | +0.107 pp | Small |
| **High** (90th percentile) | 20.3% | +0.386 pp | Small |

**Key Insight**: The effect at high Black enrollment is **10.4x** larger than at low Black enrollment.

### 4. **Full Regression Results**

**Formula**: `Suspension Rate ~ % White Teachers * % Black Students + Controls`

| Term | Coefficient | SE | 95% CI | p-value | Sig |
|------|-------------|----|---------|---------|---------|
| (Intercept) | 0.005085 | 0.007795 | [-0.0102, 0.0204] | 0.5142 |  |
| pct_white_teachers | 0.000244 | 0.000524 | [-0.0008, 0.0013] | 0.6411 |  |
| pct_black_students | -0.027369 | 0.000678 | [-0.0287, -0.0260] | < 0.001 | \*\*\* |
| is_charter | -0.025901 | 0.000174 | [-0.0262, -0.0256] | < 0.001 | \*\*\* |
| school_level_factorMiddle | 0.039857 | 0.000171 | [0.0395, 0.0402] | < 0.001 | \*\*\* |
| school_level_factorHigh | 0.024610 | 0.000134 | [0.0243, 0.0249] | < 0.001 | \*\*\* |
| school_level_factorOther | 0.025185 | 0.000195 | [0.0248, 0.0256] | < 0.001 | \*\*\* |
| school_level_factorAlternative | 0.011941 | 0.000302 | [0.0113, 0.0125] | < 0.001 | \*\*\* |
| pct_white_teachers:pct_black_students | 0.001892 | 0.000046 | [0.0018, 0.0020] | < 0.001 | \*\*\* |

**Model Fit**:
- **R²**: 0.1878
- **Adjusted R²**: 0.1877
- **N**: 427,842 school-year observations
- **Weighted by**: Student enrollment

---

## Interpretation and Implications

### What This Analysis Tells Us

1. **The "Mismatch Hypothesis" is supported**: The association between teacher racial composition and suspension rates is significantly moderated by student racial composition.

2. **Context matters**: The same change in teacher demographics (e.g., +10pp White teachers) has different associations with suspension rates depending on the school's student racial composition.

3. **Amplification in majority-Black schools**: Schools with higher Black student enrollment show stronger associations between White teacher representation and suspension rates.

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

