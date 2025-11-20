# Analysis 24: Quartile Slope Comparison

**Created**: 2025-11-20
**Script**: `Analysis/24_quartile_slope_comparison.R`
**Purpose**: Test whether the association between teacher racial diversity and suspension rates differs across quartiles of Black student enrollment

---

## Hypothesis

**Research Question**: Does the racial composition of staff play a more critical role in discipline outcomes in majority-Black environments compared to majority-White environments?

**Specific Hypothesis**: The slope (coefficient) relating **% White Teachers** to **Suspension Rate** should be steeper (more positive) in **Q4** (highest % Black students) compared to **Q1** (lowest % Black students).

**Visual Test**: A "direct eyeball comparison" of slope steepness across quartiles to assess the hypothesis.

---

## Methodology

### 1. Data Preparation
- **Input**: `data-stage/susp_v6_teacher_features.parquet` (merged teacher-student data)
- **Sample**: School-year observations from 2018-19 onwards
- **Filters**:
  - Valid Black enrollment quartile (Q1-Q4)
  - Has teacher diversity data
  - Has suspension rate data
  - School-level data only (excludes aggregates)
  - Positive enrollment for weighting

### 2. Quartile Segmentation
Schools are divided into **four quartiles** based on **% Black Student Enrollment**:
- **Q1**: Lowest % Black (0-25th percentile)
- **Q2**: Low-Medium % Black (25-50th percentile)
- **Q3**: Medium-High % Black (50-75th percentile)
- **Q4**: Highest % Black (75-100th percentile)

### 3. Regression Analysis
**Four separate linear regressions** (one per quartile):

**Formula**:
```
Suspension Rate (%) ~ % White Teachers + Controls
```

**Controls** (if available):
- Socioeconomically Disadvantaged (SED) rate
- Charter status
- School level (Elementary/Middle/High)

**Weighting**: Weighted by student enrollment for representativeness

**Key Metric**: Coefficient for `% White Teachers` in each quartile

### 4. Visualization
**Faceted Scatter Plot** (2x2 grid):
- **X-axis**: % White Teachers (0-100%)
- **Y-axis**: Suspension Rate (%) - **FIXED SCALE across all panels**
- **Points**: Individual school-year observations (with transparency for overplotting)
- **Lines**: Linear regression trend line with 95% confidence interval
- **Facets**: One panel per quartile (Q1, Q2, Q3, Q4)

**Critical Design Feature**: Fixed y-axis scale ensures that visual comparison of slope angles is accurate and not distorted by different axis ranges.

---

## Expected Outputs

### 1. Regression Results Table
**File**: `outputs/tables/24_quartile_slope_comparison_coefficients.csv`

**Contents**:
- Quartile labels
- Sample size (N schools)
- Regression coefficient for % White Teachers
- Standard error
- p-value
- 95% Confidence interval
- R² and Adjusted R²
- Significance indicators (*, **, ***)
- Plain-language interpretation

### 2. Visualization
**File**: `outputs/graphs/24_quartile_slope_comparison.png`

**Specifications**:
- Dimensions: 12" × 10" @ 300 DPI
- Format: PNG with white background
- Layout: 2×2 faceted grid
- Fixed y-axis scales for direct comparison
- UCLA-branded color scheme

---

## How to Run

### Option 1: Direct Execution
```r
source("Analysis/24_quartile_slope_comparison.R")
```

### Option 2: From RStudio
1. Open `Analysis/24_quartile_slope_comparison.R` in RStudio
2. Source the script (Ctrl+Shift+S / Cmd+Shift+S)

### Option 3: From Command Line
```bash
Rscript Analysis/24_quartile_slope_comparison.R
```

---

## Expected Findings

If the hypothesis is **supported**:
- Q4 slope > Q1 slope (more positive coefficient in Q4)
- Visual inspection shows steeper line angle in Q4 panel vs. Q1 panel
- Interpretation: The association between % White Teachers and Suspension Rate is STRONGER in majority-Black schools

If the hypothesis is **not supported**:
- Q4 slope ≤ Q1 slope
- Lines appear similar or flatter in Q4 vs. Q1
- Interpretation: The association does not differ meaningfully across quartiles

---

## Interpretation Guidelines

### Statistical Significance
- **p < 0.05**: Statistically significant association (marked with *)
- **p < 0.01**: Highly significant (**)
- **p < 0.001**: Very highly significant (***)

### Coefficient Interpretation
- **Positive coefficient**: Higher % White Teachers → Higher Suspension Rate
- **Negative coefficient**: Higher % White Teachers → Lower Suspension Rate
- **Magnitude**: Change in suspension rate (percentage points) per 1% increase in White teachers

**Example**: Coefficient = 0.05 means a 10% increase in White teachers (e.g., 40% → 50%) is associated with a 0.5 percentage point increase in suspension rate.

### Visual Interpretation
- **Steep upward slope**: Strong positive association
- **Flat/horizontal line**: No association
- **Steep downward slope**: Strong negative association
- **Compare angles across panels**: Steeper in Q4 vs. Q1 supports hypothesis

---

## Important Caveats

1. **Correlation, Not Causation**: These are observational patterns. The analysis describes associations in the data, not causal effects.

2. **Confounding Variables**: Many unobserved factors influence both teacher diversity and suspension rates:
   - School leadership quality
   - Community demographics
   - Funding levels
   - Local policies
   - Historical context

3. **Ecological Fallacy**: School-level patterns may not reflect individual-level mechanisms.

4. **Multiple Comparisons**: Running 4 separate regressions increases the risk of false positives. Consider using a Bonferroni-adjusted significance threshold (p < 0.0125 instead of p < 0.05).

5. **Selection Effects**: Schools with different racial compositions may differ in systematic ways beyond what is controlled for.

---

## Next Steps

If you want to strengthen the analysis:

1. **Formal Slope Comparison**: Test interaction between quartile and % White Teachers
   ```r
   lm(suspension_rate ~ pct_white_teachers * quartile_factor + controls)
   ```

2. **Bootstrapping**: Estimate confidence intervals for slope differences

3. **Sensitivity Analysis**: Test robustness to different quartile definitions or sample restrictions

4. **Mechanism Analysis**: Explore what mediates the relationship (if it exists)

5. **Longitudinal Analysis**: Track schools over time to reduce confounding

---

## References

**Builds on**:
- `Analysis/21_weighted_teacher_diversity_by_quartile.R`: Quartile-based aggregation
- `Analysis/21_teacher_diversity_regression.R`: Regression framework
- `docs/protocols/PROTOCOL_TEACHER_DATA_MERGE.md`: Data merge protocol

**Related Documentation**:
- `Analysis/TEACHER_DIVERSITY_ANALYSIS_GUIDE.md`: Overview of teacher diversity analyses
- `Analysis/21_ANALYSIS_GUIDE.md`: Weighted diversity analysis guide
- `docs/protocols/CITATION_STANDARD.md`: Citation requirements for outputs

---

**Questions or Issues?**
Consult the documentation in `docs/` or review the inline comments in the script.
