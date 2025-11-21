# Power Analysis Guide for REACH Suspensions Analysis

**Last Updated**: 2025-11-21
**Purpose**: Guide for conducting and interpreting power analyses for regression models
**Related Scripts**: `Analysis/21_teacher_diversity_regression.R`, `Analysis/26_power_analysis.R`

---

## Table of Contents

1. [What is Power Analysis?](#what-is-power-analysis)
2. [Why Power Analysis Matters](#why-power-analysis-matters)
3. [When to Conduct Power Analysis](#when-to-conduct-power-analysis)
4. [Implementation in This Project](#implementation-in-this-project)
5. [Interpreting Results](#interpreting-results)
6. [Special Considerations](#special-considerations)
7. [Common Pitfalls](#common-pitfalls)
8. [References](#references)

---

## What is Power Analysis?

**Statistical power** is the probability of correctly rejecting a false null hypothesis (i.e., detecting a true effect when one exists). It is calculated as:

```
Power = 1 - β (where β is the Type II error rate)
```

Power depends on four interrelated factors:

1. **Sample size (N)**: Larger samples → higher power
2. **Effect size (Δ)**: Larger effects → higher power
3. **Significance level (α)**: Higher α → higher power (but more false positives)
4. **Statistical test**: Different tests have different power characteristics

**Types of Power Analysis:**

- **A priori (prospective)**: Determine required sample size BEFORE data collection
- **Sensitivity analysis**: Determine minimum detectable effect size given actual N
- **Post-hoc (retrospective)**: Calculate achieved power after analysis (use with caution!)

---

## Why Power Analysis Matters

### 1. **Interpreting Null Results**

A non-significant finding can mean:
- **Scenario A**: No true effect exists (correct conclusion)
- **Scenario B**: Effect exists but study is underpowered (Type II error)

**Without power analysis, you cannot distinguish between A and B.**

Example from your data:
```
Analysis 21: Filipino students regression
- Result: p = 0.23 (not significant)
- WITHOUT power analysis: "No association between teacher diversity and suspensions"
- WITH power analysis: "Study powered to detect effects of f² ≥ 0.08; observed effect
  may be real but smaller than detectable threshold"
```

### 2. **Multiple Comparisons**

Your Analysis 21 tests **8 racial/ethnic groups** simultaneously:
- Black/African American
- White
- Hispanic/Latino
- American Indian/Alaska Native
- Asian
- Filipino
- Native Hawaiian/Pacific Islander
- Two or More Races

**Risk**: With 8 tests at α = 0.05, probability of at least 1 false positive ≈ 34%

**Solutions**:
- Bonferroni correction: α/8 = 0.00625 per test
- Power analysis shows how this affects minimum detectable effects

### 3. **Weighted Regression Complications**

Your regressions use **enrollment weights**, which affects effective sample size:

```
Effective N = (Σw)² / Σw²
```

Where `w` = enrollment weights.

**Impact**: Effective N is typically **smaller** than actual N, reducing power.

Example:
```
Actual N = 10,000 school-year combinations
Effective N = 7,500 (due to unequal weights)
Power loss ≈ 13%
```

---

## When to Conduct Power Analysis

### **Always Conduct:**

1. **Before submitting manuscripts**: Reviewers increasingly demand power analyses
2. **When reporting null findings**: Essential for interpreting non-significant results
3. **For grant proposals**: Demonstrate adequate sample size for proposed analyses
4. **After data collection**: Sensitivity analysis to characterize detection limits

### **Types for Your Analyses:**

| Analysis | Power Analysis Type | Purpose |
|----------|---------------------|---------|
| Analysis 21 (Teacher diversity regressions) | **Sensitivity analysis** | Determine minimum detectable effects for each racial group |
| Analysis 24 (Quartile slope comparison) | **Interaction power** | Assess power to detect slope differences across quartiles |
| Analysis 25 (Interaction term regression) | **Interaction power** | Assess power to detect moderation effects |

---

## Implementation in This Project

### **Running Power Analysis:**

```r
# Execute power analysis script
source("Analysis/26_power_analysis.R")
```

**Outputs:**
- `outputs/tables/26_power_analysis_results.csv`: Summary table
- `outputs/tables/26_power_analysis_results.xlsx`: Detailed results with interpretation guide
- `outputs/graphs/26_power_curves.png`: Visualization of power by effect size

### **What Gets Calculated:**

For each racial/ethnic group:

1. **Actual sample size** (N complete cases)
2. **Effective sample size** (N_effective accounting for weights)
3. **Minimum detectable effect size** (with 80% power)
4. **Power for standard benchmarks**:
   - Small effect: f² = 0.02
   - Medium effect: f² = 0.15
   - Large effect: f² = 0.35
5. **Bonferroni-adjusted thresholds** (for multiple comparisons)

### **Effect Size Metrics:**

We use **Cohen's f²** for multiple regression:

```
f² = R² / (1 - R²)
```

Where:
- R² = variance explained by predictors of interest (teacher + admin diversity)
- Calculated AFTER controlling for other predictors (SED rate, charter status, school level)

**Interpretation:**
- f² = 0.02 → Small effect (R² ≈ 0.02)
- f² = 0.15 → Medium effect (R² ≈ 0.13)
- f² = 0.35 → Large effect (R² ≈ 0.26)

**In practical terms** (for your research):
```
f² = 0.02: Teacher diversity explains 2% of suspension rate variance
           (after controlling for SED, charter, school level)

f² = 0.15: Teacher diversity explains 13% of variance
           (substantial but not dominant factor)

f² = 0.35: Teacher diversity explains 26% of variance
           (major determinant of suspension rates)
```

---

## Interpreting Results

### **Example Output:**

```
Student Group: Black/African American
  N (actual): 12,453 school-year-race combinations
  N (effective): 9,876 (weight efficiency: 79%)
  Minimum detectable f² (80% power): 0.009
  Power for medium effect (f² = 0.15): 99.8%
  Interpretation: ADEQUATELY POWERED for medium effects
```

**What this means:**
- ✅ With 99.8% power, you'll almost certainly detect a medium effect if it exists
- ✅ Can detect effects as small as f² = 0.009 with 80% confidence
- ✅ Non-significant results are likely true nulls (not underpowered)

### **Example: Underpowered Group:**

```
Student Group: Native Hawaiian/Pacific Islander
  N (actual): 245 school-year-race combinations
  N (effective): 198 (weight efficiency: 81%)
  Minimum detectable f² (80% power): 0.18
  Power for medium effect (f² = 0.15): 67%
  Interpretation: UNDERPOWERED for medium effects
```

**What this means:**
- ⚠️ Only 67% chance of detecting a medium effect
- ⚠️ Can only reliably detect effects of f² ≥ 0.18 (larger than "medium")
- ⚠️ Non-significant results are ambiguous (could be true null OR underpowered)

**Reporting recommendation:**
```
"Due to limited sample size (N_eff = 198), analyses for Native Hawaiian/Pacific
Islander students should be considered exploratory. This sample provides adequate
power (80%) to detect large effects (f² ≥ 0.18) but is underpowered for smaller
effects. Non-significant findings do not rule out associations below this threshold."
```

---

## Special Considerations

### **1. Weighted Regression**

Your regressions weight by enrollment, which:
- Gives more influence to larger schools (intended)
- Reduces effective sample size (unintended but necessary)

**Calculation:**
```r
weights <- enrollment[keep]
n_effective <- (sum(weights)^2) / sum(weights^2)
```

**Weight efficiency**: Ratio of effective N to actual N
- 100% = all schools same size (equal weights)
- <100% = unequal weights reduce effective N
- Typical range in your data: 75-85%

### **2. Multiple Comparisons**

With 8 racial groups tested simultaneously, use **Bonferroni correction**:

```
α_bonferroni = 0.05 / 8 = 0.00625 per test
```

**Impact on power:**
- More stringent threshold → require larger effects for significance
- Minimum detectable effect sizes increase by ~30%

**When to apply:**
- If you're making **family-wise** claims ("teacher diversity affects suspension rates")
- NOT needed if each group is reported separately with appropriate caveats

**Alternatives:**
- False Discovery Rate (FDR) correction (less conservative)
- Report both corrected and uncorrected results
- Pre-specify primary vs. exploratory analyses

### **3. Clustering**

Your data has nested structure:
```
Schools → Years → Racial groups
```

**Current approach:** Aggregate to school-year-race level, ignore year-to-year correlation within schools

**Conservative justification:** Standard errors may be slightly underestimated, but:
- Reduces false positives (conservative direction)
- Simplifies interpretation
- Power calculations based on aggregated N are valid

**Alternative:** Multi-level models with random effects for schools
- More complex
- Requires additional assumptions
- May not change substantive conclusions
- Would require different power analysis approach (simulation-based)

### **4. Interaction Effects (Analyses 24 & 25)**

**Key insight:** Interaction effects require **much larger samples** than main effects.

Rule of thumb: Need **4× the sample size** to detect an interaction of the same magnitude as a main effect.

For Analysis 25 (interaction term regression):
- Testing: % White Teachers × % Black Students
- Requires power analysis specific to interaction terms
- Use `pwr.f2.test()` with interaction as the predictor of interest

---

## Common Pitfalls

### **❌ PITFALL 1: Post-hoc power for observed effects**

**Wrong:**
```
"We found p = 0.08 (not significant). Post-hoc power = 45%, suggesting
the study was underpowered."
```

**Why wrong:** Post-hoc power calculated using the observed effect size is a **direct transformation of the p-value** and adds no new information.

**Right approach:**
```
"We found p = 0.08 (not significant). Based on our sample size, we had
80% power to detect effects of f² ≥ 0.12. The observed effect (f² = 0.08)
is below our detection threshold, suggesting this may represent either a
true null or a small effect beyond our study's resolution."
```

### **❌ PITFALL 2: Ignoring effect size context**

**Wrong:**
```
"Study is underpowered (only 60% power for small effects)."
```

**Why wrong:** Small effects (f² = 0.02) may not be **practically significant** in your context.

**Right approach:**
```
"Study has 60% power for small effects (f² = 0.02, equivalent to 2% variance
explained) but 95% power for medium effects (f² = 0.15, equivalent to 13%
variance explained). Given the complex causal pathways and multiple determinants
of suspension rates, we prioritized power to detect medium or larger effects."
```

### **❌ PITFALL 3: Not adjusting for multiple comparisons**

**Wrong:**
```
"Teacher diversity significantly associated with suspensions for Asian students
(p = 0.03) but not other groups."
```

**Why wrong:** With 8 tests, p = 0.03 is not significant after Bonferroni correction (α = 0.00625).

**Right approach:**
```
"Teacher diversity showed an association with suspensions for Asian students
(p = 0.03), which does not survive Bonferroni correction for multiple comparisons
(α = 0.00625). This finding should be considered exploratory and requires
replication."
```

### **❌ PITFALL 4: Confusing power with precision**

**Wrong:**
```
"Large sample size ensures accurate estimates."
```

**Why wrong:** Large N increases **power** (ability to detect effects) but does not eliminate **bias** (confounding, measurement error).

**Right:**
```
"Large sample size provides adequate power to detect medium effects but does
not address potential confounding by unmeasured variables or measurement error
in self-reported data."
```

---

## Reporting Template

### **For Manuscript Methods Section:**

```
Statistical Power

Power analyses were conducted to determine the minimum detectable effect sizes
for regression analyses. We used Cohen's f² as the effect size metric for
multiple regression, where f² = R²/(1-R²) represents the variance explained by
teacher and administrator racial diversity after controlling for socioeconomic
disadvantage rate, charter status, and school level.

Sample sizes were adjusted for unequal enrollment weights using the formula
N_effective = (Σw)²/Σw², where w represents student enrollment. Effective sample
sizes ranged from [MIN] (for [SMALLEST GROUP]) to [MAX] (for [LARGEST GROUP]).

With α = 0.05 and target power of 80%, minimum detectable effect sizes ranged
from f² = [MIN] to f² = [MAX] across racial/ethnic groups. [X] of [Y] groups
([ %]) had adequate power (≥80%) to detect medium effects (f² = 0.15, equivalent
to 13% variance explained), while [X] groups were underpowered for effects below
f² = [THRESHOLD].

For analyses involving [N] simultaneous tests across racial/ethnic groups, we
also report Bonferroni-adjusted significance thresholds (α = 0.05/[N] = [VALUE])
to control family-wise error rates.

Power calculations were conducted using the pwr package (version X.X) in R
(version X.X.X). Full power analysis results are provided in Supplementary
Table S[X].
```

### **For Results Section:**

```
Teacher diversity showed statistically significant associations with suspension
rates for [GROUPS WITH SIG RESULTS] (p < 0.05; see Table [X]). Non-significant
associations for [GROUPS WITHOUT SIG RESULTS] should be interpreted in light of
statistical power: [GROUP A] had adequate power (>80%) to detect medium effects,
suggesting the null finding represents a true absence of association, while
[GROUP B] had limited power ([X]%) and cannot rule out small-to-medium effects
below the detection threshold (f² = [VALUE]).
```

---

## References

### **Key Papers:**

1. **Cohen, J. (1988)**. *Statistical Power Analysis for the Behavioral Sciences* (2nd ed.). Lawrence Erlbaum Associates.
   - Defines effect size benchmarks (small/medium/large)

2. **Hoenig, J. M., & Heisey, D. M. (2001)**. The abuse of power: The pervasive fallacy of power calculations for data analysis. *The American Statistician*, 55(1), 19-24.
   - Critiques post-hoc power analysis

3. **Gelman, A., & Carlin, J. (2014)**. Beyond power calculations: Assessing Type S (sign) and Type M (magnitude) errors. *Perspectives on Psychological Science*, 9(6), 641-651.
   - Modern alternative to traditional power analysis

4. **Champely, S. (2020)**. pwr: Basic Functions for Power Analysis. R package version 1.3-0.
   - Documentation for R implementation

### **Online Resources:**

- G*Power (free software): https://www.psychologie.hhu.de/arbeitsgruppen/allgemeine-psychologie-und-arbeitspsychologie/gpower
- Interactive power calculators: https://rpsychologist.com/d3/cohend/
- Effect size databases: https://www.metafor-project.org/doku.php/plots:forest_plot

---

## Appendix: Technical Details

### **Power for Multiple Regression (pwr.f2.test)**

Formula used in `Analysis/26_power_analysis.R`:

```r
pwr.f2.test(
  u = 2,              # Number of predictors tested (teacher + admin diversity)
  v = n_eff - u - v - 1,  # Error degrees of freedom
  f2 = effect_size,   # Cohen's f²
  sig.level = 0.05,   # Alpha level
  power = 0.80        # Target power
)
```

Where:
- `u` = # of predictors of interest (numerator df)
- `v` = # of residual df = N - # of all predictors - 1
- Assumes **2** predictors of interest (teacher, admin diversity)
- Assumes **4** control predictors (SED rate, charter, + 2 school level dummies)

### **Effect Size Conversion:**

```
f² = R² / (1 - R²)
R² = f² / (1 + f²)

Examples:
  f² = 0.02 → R² = 0.0196 ≈ 2%
  f² = 0.15 → R² = 0.1304 ≈ 13%
  f² = 0.35 → R² = 0.2593 ≈ 26%
```

### **Weighted Regression Effective N:**

```r
# Kish's design effect
weights <- enrollment[keep]
sum_w <- sum(weights)
sum_w2 <- sum(weights^2)
n_effective <- sum_w^2 / sum_w2

# Interpretation:
# n_effective < n_actual when weights are unequal
# Reduction factor: n_effective / n_actual
```

---

**Last Updated**: 2025-11-21
**Next Review**: Before manuscript submission

*For questions or clarifications, see `Analysis/26_power_analysis.R` script or contact the statistical methods lead.*
