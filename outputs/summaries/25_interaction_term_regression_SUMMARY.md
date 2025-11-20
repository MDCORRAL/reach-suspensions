# Analysis 25: Interaction Term Regression - Testing the Mismatch Hypothesis

**Analysis Date**: 2025-11-20
**Data Period**: 2018-19 through 2023-24 academic years
**Academic Years Included**: 2019-20, 2021-22, 2022-23, 2023-24
**Total Schools Analyzed**: 4,359 unique schools across California
**School-Year Observations**: 12,065

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
| **Interaction: % White Teachers × % Black Students** | 192.617432 | 27.987908 | [137.756633, 247.478232] | < 0.001 |

**What this means**:
- For every 1 percentage point increase in % Black Students, the slope (effect) of % White Teachers on suspension rates changes by 192.6174 percentage points.
- Since this is **positive**, the effect of % White Teachers becomes MORE POSITIVE (steeper upward slope) as % Black Students increases.

### 3. **Marginal Effects at Different Black Enrollment Levels**

The effect of a **10 percentage point increase** in % White Teachers (e.g., from 40% to 50% White teachers) varies by school racial composition:

| Black Student Enrollment Level | % Black Students | Effect on Suspension Rate | Interpretation |
|-------------------------------|------------------|---------------------------|----------------|
| **Low** (10th percentile) | 1.9% | +3976.595 pp | Moderate |
| **Medium** (50th percentile) | 6.0% | +11946.332 pp | Moderate |
| **High** (90th percentile) | 23.2% | +45022.004 pp | Moderate |

**Key Insight**: The effect at high Black enrollment is **11.3x** larger than at low Black enrollment.

### 4. **Full Regression Results**

**Formula**: `Suspension Rate ~ % White Teachers * % Black Students + Controls`

| Term | Coefficient | SE | 95% CI | p-value | Sig |
|------|-------------|----|---------|---------|---------|
| (Intercept) | 231.948506 | 4802.111871 | [-9180.9628, 9644.8598] | 0.9615 |  |
| pct_white_teachers | 31.484454 | 322.693476 | [-601.0466, 664.0155] | 0.9223 |  |
| pct_black_students | -2781.171586 | 416.758764 | [-3598.0858, -1964.2574] | < 0.001 | \*\*\* |
| is_charter | -2462.151345 | 107.246123 | [-2672.3710, -2251.9317] | < 0.001 | \*\*\* |
| school_level_factorMiddle | 3711.325837 | 105.747404 | [3504.0439, 3918.6078] | < 0.001 | \*\*\* |
| school_level_factorHigh | 1998.672231 | 82.659042 | [1836.6472, 2160.6972] | < 0.001 | \*\*\* |
| school_level_factorOther | 2269.978431 | 119.988112 | [2034.7824, 2505.1744] | < 0.001 | \*\*\* |
| school_level_factorAlternative | 1348.837775 | 185.802682 | [984.6346, 1713.0409] | < 0.001 | \*\*\* |
| pct_white_teachers:pct_black_students | 192.617432 | 27.987908 | [137.7566, 247.4782] | < 0.001 | \*\*\* |

**Model Fit**:
- **R²**: 0.1620
- **Adjusted R²**: 0.1615
- **N**: 12,065 school-year observations
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

