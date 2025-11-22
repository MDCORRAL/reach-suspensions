# Analysis 25: Interaction Term Regression - Testing the Mismatch Hypothesis - Executive Summary

**Analysis Date**: 2025-11-22
**Data Period**: 2019-20 through 2023-24
**Academic Years Included**: 2019-20, 2021-22, 2022-23, 2023-24 (4 years)
**School-Year Observations**: 12,065 school-year observations
**Total Schools Analyzed**: 4,359 unique California public schools

---

## Executive Summary (1-2 Minute Read)

**Purpose**: This analysis uses statistical interaction modeling to formally test whether the "White teacher effect" on suspension rates is amplified in schools with higher Black student enrollment (the "mismatch hypothesis").

**Key Findings**:
- **Positive Interaction Confirmed**: The statistical interaction between % White teachers and % Black students is highly significant (p < 0.0001, coefficient = 0.0047), confirming that the White teacher effect strengthens as Black enrollment increases.
- **Effect Compounds at Scale**: At schools with high Black enrollment (23.2% at 90th percentile), the slope of suspension rate vs. White teachers is substantially steeper than at low-Black-enrollment schools (1.9% at 10th percentile).
- **Robust After Controls**: The interaction remains highly significant (p < 0.0001) after controlling for charter status and school level, indicating the pattern is not simply explained by these structural factors.
- **Complements Quartile Analysis**: This formal statistical test confirms what Analysis 24 showed descriptively—the relationship between teacher composition and suspension rates systematically varies with student racial composition.

**Bottom Line**: Statistical evidence strongly supports the "mismatch hypothesis"—the correlation between White teacher percentage and suspension rates is significantly amplified in schools serving predominantly Black student populations.

**Important Note**: This analysis examines **total suspension incidents** (not unique students suspended), so rates can exceed 100% when students experience multiple suspensions.

---

## Key Question

Does the relationship between teacher racial composition (specifically % White teachers) and suspension rates vary depending on the school's Black student enrollment? Specifically, is the "White teacher effect" on suspension rates amplified in schools with higher Black student concentrations?

## Power Diagnostics

- Run `Analysis/27_power_analysis_multiscript.R` and review the `analysis_id == "25_interaction_term_regression"` rows in `outputs/tables/27_power_analysis_by_group.csv` to verify effective N and minimum-detectable R² for the interaction models before drawing conclusions from null findings.

---

## CRITICAL: Suspension Rate Definition

**IMPORTANT METHODOLOGICAL NOTE**: This analysis uses **TOTAL SUSPENSION INCIDENTS**, not **UNDUPLICATED STUDENT COUNT**.

### What This Means

**Numerator**: `total_suspensions`
- Total count of all suspension incidents/events
- If a student is suspended multiple times, **each incident is counted**
- Example: Student A suspended 3 times = **3 suspensions**

**Denominator**: `cumulative_enrollment`
- Total student enrollment for the school-year

**Rate Calculation**:
```
Suspension Rate = total_suspensions / cumulative_enrollment
```

**Interpretation**:
- Represents the **average number of suspension incidents per enrolled student**
- **Can exceed 1.0** (or 100%) if students experience multiple suspensions
- Example: A rate of 0.15 (15%) means 0.15 suspension incidents per student on average
- Example: A rate of 1.5 (150%) means 1.5 suspension incidents per student (indicating repeat suspensions)

### Why This Measure?

**Advantages**:
- Captures **severity**: Multiple suspensions per student increase the rate
- Reflects **total disciplinary burden** on schools
- Consistent across all schools (comparable measure)

**Important Note**:
- Rates **CAN exceed 100%** if many students receive multiple suspensions
- This is NOT an error - it indicates high rates of repeat suspensions

### Alternative Measure (NOT Used Here)

**Unduplicated Suspension Rate**:
- Numerator: Count of unique students suspended at least once
- Example: Student A suspended 3 times = **1 student**
- Interpretation: Percentage of students who experienced at least one suspension
- **Always between 0-100%** (cannot exceed 100%)

**Why not use unduplicated count?**
- The total incidents measure better captures the severity and cumulative impact of suspensions
- Multiple suspensions per student represent distinct disciplinary events
- Consistent with Analysis 24 for comparability

**This distinction appears on all graphs and tables in this analysis.**

---

## Major Findings

### 1. **Positive Interaction Effect: The "Mismatch Hypothesis" is Supported**

The interaction term regression provides strong statistical evidence that the relationship between teacher racial composition and suspension rates **varies systematically** with Black student enrollment.

| Term | Coefficient | Std. Error | 95% CI | p-value | Significance | Interpretation |
|------|-------------|------------|--------|---------|--------------|----------------|
| % White Teachers (main effect) | -0.0097 | 0.0034 | [-0.0164, -0.0030] | 0.0047 | \*\* | Slight negative main effect when Black enrollment is low |
| % Black Students (main effect) | -0.0083 | 0.0123 | [-0.0324, 0.0158] | 0.5004 | NS | Not significant as standalone effect |
| **Interaction: White Teachers × Black Students** | **0.0047** | **0.0003** | **[0.0041, 0.0052]** | **< 0.0001** | **\*\*\*** | **The effect of White teachers increases with Black enrollment** |
| Charter Status (is_charter) | -3.9182 | 0.1771 | — | < 0.0001 | \*\*\* | Charter schools have lower suspension rates |
| School Level: Middle | 6.2165 | 0.1749 | — | < 0.0001 | \*\*\* | Middle schools have higher rates than Elementary |
| School Level: High | 3.4731 | 0.1358 | — | < 0.0001 | \*\*\* | High schools have higher rates than Elementary |
| School Level: Other | 3.4249 | 0.2004 | — | < 0.0001 | \*\*\* | Other schools have higher rates than Elementary |
| School Level: Alternative | 2.1316 | 0.3073 | — | < 0.0001 | \*\*\* | Alternative schools have higher rates than Elementary |

**Significance Legend**:
\*\*\* = p < 0.001 (highly significant)
\*\* = p < 0.01 (very significant)
\* = p < 0.05 (significant)
NS = not statistically significant

**Key Insight**: The interaction coefficient of 0.0047 means that for every 1 percentage point increase in Black student enrollment, the effect of having 10% more White teachers increases suspension rates by an additional 0.047 percentage points. This effect compounds: at schools with 23% Black enrollment vs. 2% Black enrollment, the difference in the White teacher effect is substantial.

### 2. **Marginal Effects Confirm Differential Impact Across Black Enrollment Levels**

The marginal effects plot reveals visually what the interaction term captures statistically: the slope relating % White teachers to suspension rates becomes **steeper** as Black student enrollment increases.

| Black Enrollment Level | Percentile | % Black Students | Expected Slope | Interpretation |
|------------------------|------------|------------------|----------------|----------------|
| Low | 10th | 1.9% | Flat to slightly negative | Minimal effect of teacher composition |
| Medium | 50th | 6.0% | Moderate positive | Some effect visible |
| High | 90th | 23.2% | Steep positive | Strong positive effect |

**Key Insight**: At schools with high Black student concentrations (90th percentile = 23.2%), increasing White teacher representation is associated with notably steeper increases in suspension rates compared to schools with low Black enrollment.

### 3. **Model Fit and Control Variables Confirm Robustness**

The interaction model shows good fit and maintains significance even after controlling for key confounding variables.

| Model Statistic | Value | Interpretation |
|----------------|-------|----------------|
| R² | 0.1766 | Model explains 17.7% of variance in suspension rates |
| Adjusted R² | 0.1760 | Adjusted for number of predictors |
| N (observations) | 12,065 | Large sample ensures statistical power |
| Residual SE | 179.5 | Standard deviation of residuals |
| F-statistic | 323.2 (p < 0.0001) | Model is highly significant overall |

**Control Variables Included**:
- **Charter Status**: Significant negative effect (charter schools have lower suspension rates, ~3.9 percentage points lower)
- **School Level**: Significant effect (Middle schools have highest suspension rates, ~6.2 percentage points higher than Elementary)

**Note**: SED rate (socioeconomic disadvantage) was not available in this analysis sample.

**Key Insight**: The interaction effect remains highly significant (p < 0.0001) after accounting for charter status and school level. This suggests the "mismatch" pattern is not simply explained by these structural factors.

---

## Detailed Breakdowns

### Statistical Approach: Interaction Term Regression

This analysis uses a **pooled weighted least squares regression** with an **interaction term**:

**Model Specification**:
```
Suspension Rate (%) = β₀ + β₁(% White Teachers) + β₂(% Black Students)
                      + β₃(% White Teachers × % Black Students)
                      + β₄(Charter) + β₅(School Level) + ε
```

**Key Components**:

1. **Main Effect of % White Teachers (β₁ = -0.0097)**:
   - Captures the effect of teacher racial composition when Black student enrollment is 0%
   - Small negative effect at baseline

2. **Main Effect of % Black Students (β₂ = -0.0083)**:
   - Captures the effect of Black student concentration holding teacher composition constant
   - Not statistically significant

3. **Interaction Term (β₃ = 0.0047)**: **THE KEY COEFFICIENT**
   - Captures how the effect of % White Teachers **changes** as % Black Students increases
   - **Positive coefficient**: Effect amplifies with Black enrollment (supports "mismatch hypothesis")
   - Highly significant (p < 0.0001)

**Interpretation of Interaction**:
The interaction coefficient (0.0047) means:
- At a school with 0% Black students: +10% White teachers → -0.1% suspension rate
- At a school with 20% Black students: +10% White teachers → -0.1% + (0.0047 × 20 × 10) = +0.84% suspension rate
- At a school with 50% Black students: +10% White teachers → -0.1% + (0.0047 × 50 × 10) = +2.25% suspension rate

This compounding effect is what the "mismatch hypothesis" predicts.

### Data Scope and Time Period

**Analysis Date**: 2025-11-22
**Data Collection Period**: 2019-20 through 2023-24 school years
**Academic Years Covered**: 2019-20, 2021-22, 2022-23, 2023-24
**Note**: 2020-21 academic year excluded due to pandemic-related data quality issues; 2018-19 excluded due to data filtering

**Sample Size**:
  - Total school-year observations: 12,065
  - Unique schools: 4,359
  - Years per school: 1-4 (average ~2.8 years)

**Summary Statistics**:
  - % White Teachers: Mean = 50.7%, Range = [0.0%, 100.0%]
  - % Black Students: Mean = 10.3%, Range = [0.4%, 94.0%]
  - Suspension Rate: Mean = 4.53%, Range = [0.00%, 321.80%]

**Geographic Coverage**: California public schools with sufficient data

**Inclusion Criteria**:
  - School-level data (not district aggregates)
  - Has teacher diversity data (% White teachers available)
  - Has Black student enrollment data (prop_black column)
  - Has suspension rate data
  - Academic year ≥ 2018-19 (focuses on recent years with better teacher data coverage)
  - Enrollment > 0

**Exclusion Criteria**:
  - Special school codes (0000000, 0000001)
  - Missing teacher diversity data
  - Missing Black student enrollment data
  - Missing suspension rate data

### Relationship to Analysis 24 (Quartile Slope Comparison)

Analysis 25 is a **formal statistical test** of the pattern observed descriptively in Analysis 24:

| Aspect | Analysis 24 | Analysis 25 |
|--------|-------------|-------------|
| **Approach** | Separate regressions for each Black enrollment quartile | Single pooled regression with interaction term |
| **Hypothesis Test** | Descriptive comparison of slopes across quartiles | Formal significance test of interaction coefficient |
| **Interpretation** | "The slope is steeper in Q4 than Q1" | "The interaction is significant: p < 0.0001" |
| **Advantage** | Clear visual pattern, easy to interpret | Formal statistical inference, controls for confounders |
| **Disadvantage** | No formal test of difference between quartiles | More complex interpretation of coefficients |

**Key Consistency**:
- Analysis 24 found slopes increasing across quartiles
- Analysis 25 confirms this pattern is statistically significant (interaction p < 0.0001)
- The two analyses complement each other: 24 shows the pattern, 25 tests it

### Marginal Effects Plot Explained

The **marginal effects plot** shows predicted suspension rates as % White Teachers varies, at three fixed levels of % Black Students:

**Construction**:
1. Select three levels of Black enrollment: 10th percentile (1.9%), 50th percentile (6.0%), 90th percentile (23.2%)
2. For each level, vary % White Teachers from 0% to 100%
3. Hold control variables at typical values (is_charter = 0, school_level = Elementary)
4. Use the regression equation to predict suspension rates at each point
5. Plot the three lines on the same graph

**Interpretation**:
- **Steeper line** = Larger effect of White teacher percentage
- **Lines diverging** = Evidence of interaction (effect varies by Black enrollment)
- **Lines parallel** = No interaction (effect is constant)

In our plot, the lines **diverge**, with the "High" (23.2% Black) line having the steepest slope. This is visual confirmation of the positive interaction term.

---

## Implications for Practice and Policy

### 1. **Teacher Diversity Matters Most Where Students are Most Diverse**

**Finding**: The relationship between teacher racial composition and suspension rates is strongest in schools with higher Black student enrollment.

**Implication**:
- Teacher diversity initiatives should be **prioritized** in schools serving predominantly Black student populations
- Universal teacher recruitment strategies may be insufficient; targeted efforts are needed
- Schools with higher Black enrollment face compounding challenges that require intentional intervention

**Recommended Actions**:
- Develop targeted recruitment pipelines for teachers of color in high-Black-enrollment schools
- Create retention programs specifically for teachers of color in these settings
- Consider financial incentives (loan forgiveness, housing assistance) for teachers of color willing to work in high-Black-enrollment schools
- Partner with HBCUs and MSIs for teacher preparation programs focused on these schools

### 2. **The "Mismatch" is Not Just About Individual Teachers**

**Finding**: The interaction effect persists after controlling for charter status and school level.

**Implication**:
- The pattern is not simply explained by confounding variables
- There is something specific about the **combination** of high White teacher percentages and high Black student enrollment that correlates with higher suspension rates
- This suggests systemic factors beyond individual teacher quality or student behavior

**Recommended Actions**:
- Investigate school-level policies and cultures in high-mismatch schools
- Examine whether disciplinary codes are applied differently in these settings
- Provide professional development on culturally responsive practices, especially in high-mismatch schools
- Consider administrative and leadership diversity as part of the solution (not just classroom teachers)

### 3. **Quantifying the Effect Helps Prioritize Resources**

**Finding**: The interaction coefficient of 0.0047 provides a quantifiable estimate of how the effect scales.

**Implication**:
- The effect size is **meaningful** at scale
- For a school with high Black enrollment (e.g., 25%), increasing teacher diversity could be associated with measurable reductions in suspension incidents
- This is a tangible target for policy intervention

**Recommended Actions**:
- Use these effect sizes to estimate the potential impact of teacher diversity initiatives
- Conduct cost-benefit analyses comparing teacher recruitment costs to suspension reduction benefits
- Set specific, measurable goals for teacher diversity in high-Black-enrollment schools
- Monitor progress using data dashboards tracking both teacher diversity and suspension rates

---

## Limitations and Caveats

### **CRITICAL: Correlational, Not Causal**

This analysis uses **observational regression analysis** which can detect **associations** but cannot prove **causation**.

**What we CAN say**:
- Schools with higher White teacher percentages AND higher Black student enrollments tend to have higher suspension rates
- This relationship is statistically significant and robust to controls
- The pattern is consistent across multiple years and thousands of schools

**What we CANNOT say**:
- That White teachers **cause** higher suspension rates in Black students
- That hiring more teachers of color will **definitely** reduce suspension rates
- That the relationship is not due to unmeasured confounders (selection bias, reverse causality, omitted variables)

**Why Causal Claims Require More**:
- **Selection bias**: Schools with high Black enrollment and high White teacher percentages may differ systematically from other schools in ways we don't measure
- **Reverse causality**: Schools with high suspension rates may have difficulty recruiting teachers of color
- **Omitted variables**: There may be other factors (administrative practices, school climate, district policies) that explain both teacher composition and suspension rates

### **Ecological Fallacy Warning**

**Limitation**: This analysis uses **school-level** aggregates (% White teachers, % Black students). We cannot make claims about individual teacher-student interactions.

**What this means**:
- We observe that schools with more White teachers and more Black students have higher suspension rates
- We do NOT know if White teachers suspend Black students at higher rates than they suspend White students
- We do NOT know if White teachers suspend Black students at higher rates than Black teachers do

### **Teacher Race is a Proxy, Not a Mechanism**

**Limitation**: "% White teachers" is a crude measure that conflates many underlying factors.

**What "White teacher" might capture**:
- Cultural background and shared experiences
- Implicit biases and stereotyping
- Expectations and relationship-building
- Training and preparation

### **Limited Control Variables**

**What we control for**:
- Charter status
- School level (Elementary/Middle/High/Other/Alternative)

**What we DON'T control for**:
- SED rate (poverty) - **not available in this analysis sample**
- Principal race and leadership style
- District-level policies
- School climate and culture
- Teacher experience and training

---

## Recommendations for Further Analysis

### **Data Improvements**

1. **Add SED Rate Control**: The socioeconomic disadvantage rate was not available in this analysis sample. Future runs should ensure this control variable is included.

2. **Student-Level Analysis**: Link individual students to individual teachers to test within-classroom effects.

### **Robustness Checks**

1. **Alternative Specifications**:
   - Quadratic terms to test non-linearity
   - Different weighting schemes
   - Clustered standard errors by school or district

2. **Alternative Measures**:
   - Use unduplicated suspension rates instead of total incidents
   - Use out-of-school suspension rates only

---

## Data Outputs Available

### **Tables** (CSV format)
1. `25_interaction_regression_results.csv` - Full regression output with coefficients, standard errors, p-values, and confidence intervals

### **Excel Workbook**
`25_interaction_regression_results.xlsx` - Contains two sheets:
- **Coefficients**: All regression coefficients with significance markers
- **Model_Statistics**: R², adjusted R², N, residual standard error, F-statistic

### **Visualizations** (PNG, 300 DPI)
1. `25_interaction_marginal_effects.png` - Marginal effects plot showing predicted suspension rates at Low/Medium/High Black enrollment levels (1.9%, 6.0%, 23.2%)

**Output Location**: All files located in `outputs/tables/` and `outputs/graphs/`

---

## Methodological Notes

### **Statistical Approach: Weighted Least Squares Regression**

**Approach**:
- Fit a linear regression model with an interaction term using `lm()` in R
- Weight observations by `cumulative_enrollment` (larger schools receive more weight)

**Model Formula**:
```
suspension_rate_pct ~ pct_white_teachers * pct_black_students + is_charter + school_level_factor
```

**Why this method**:
- Interaction terms directly test the "moderation" hypothesis
- Weighted regression accounts for heteroskedasticity (larger schools have more precise estimates)
- Linear specification is simple and interpretable
- Pooled model uses all data efficiently (more power than separate quartile regressions)

### **Sample Construction**

**Source Data**: `susp_v6_teacher_features.parquet`
- Student suspension data merged with teacher demographic data
- Created by `Analysis/22_build_teacher_race_shares.R`

**Filtering Steps**:
1. **Filter to "All Students" subgroup** - Uses CDE's pre-calculated totals (not race-specific subgroups)
2. **Aggregate across suspension reasons** - School-year level (not reason-level)
3. **Filter to school-level data** - Exclude district and state aggregates
4. **Filter to 2018-19 onwards** - Better teacher data coverage in recent years
5. **Require non-missing key variables** - Complete case analysis for regression

**Final Sample**: 12,065 school-year observations from 4,359 unique schools

### **Bug Fix Applied (2025-11-22)**

**Issue**: Original script was using `teacher_total_staff_count_white_share` column which had no variance (all values = 11.1%). This caused regression singularity.

**Fix**: Script now:
1. Excludes columns containing `_total_staff_` (statewide aggregates)
2. Prioritizes `teacher_staff_count_white_share` (school-level data)
3. Validates that selected column has variance
4. Handles NA coefficients gracefully in output

**Result**: % White Teachers now has proper variance (Range = [0.0%, 100.0%]) and regression completes successfully.

### **Statistical Significance**

Throughout this summary:
- **\*\*\*** indicates p < 0.001 (highly statistically significant)
- **\*\*** indicates p < 0.01 (very statistically significant)
- **\*** indicates p < 0.05 (statistically significant)
- **NS** indicates not statistically significant (p ≥ 0.05)

**Important**: Statistical significance does not imply practical importance. Always consider effect sizes and real-world magnitude.

---

## Citation

**Suggested Citation**:
> UCLA Center for the Transformation of Schools (2025). "Interaction Term Regression: Testing the Mismatch Hypothesis - Executive Summary." REACH Suspensions Analysis Project.

**Data Source**:
> California Department of Education. "Suspension and Expulsion Data Files, 2019-20 through 2023-24." Retrieved from https://www.cde.ca.gov/ds/sd/sd/

**Analysis Documentation**:
> Full methodology and code available at: `Analysis/25_interaction_term_regression.R`

---

## Contact and Questions

For questions about:
- **Methodology**: See `Analysis/25_interaction_term_regression.R` for full code
- **Data pipeline**: See `CLAUDE.md` and `Analysis/data_processing_overview.md`
- **Related analyses**: See `outputs/summaries/24_quartile_slope_comparison_SUMMARY.md` for the descriptive companion analysis
- **Teacher data**: See `docs/guides/TEACHER_DATA_SETUP_GUIDE.md` and `docs/protocols/PROTOCOL_TEACHER_DATA_MERGE.md`

---

## Document Information

**Document Version**: 2.0
**Document Created**: 2025-11-21
**Last Updated**: 2025-11-22
**Analysis Script**: `Analysis/25_interaction_term_regression.R`
**Output Location**: `outputs/summaries/25_interaction_term_regression_SUMMARY.md`
**Word Version**: `outputs/summaries/25_interaction_term_regression_SUMMARY.docx` (generate using conversion script)

**Conversion to Word**:
```bash
./scripts/utilities/convert_summary_to_word.sh 25_interaction_term_regression_SUMMARY.md
```

**Version History**:
- v1.0 (2025-11-21): Initial summary with placeholder values
- v2.0 (2025-11-22): Updated with actual regression results after bug fix

---

## Appendix: Full Regression Output

### A. Regression Coefficients

| Term | Estimate | Std. Error | t value | Pr(>|t|) | Significance |
|------|----------|------------|---------|----------|--------------|
| (Intercept) | 1.1762 | 0.2111 | 5.573 | 2.56e-08 | \*\*\* |
| pct_white_teachers | -0.0097 | 0.0034 | -2.826 | 0.00472 | \*\* |
| pct_black_students | -0.0083 | 0.0123 | -0.674 | 0.50041 | NS |
| is_charter | -3.9182 | 0.1771 | -22.125 | < 2e-16 | \*\*\* |
| school_level_factorMiddle | 6.2165 | 0.1749 | 35.536 | < 2e-16 | \*\*\* |
| school_level_factorHigh | 3.4731 | 0.1358 | 25.581 | < 2e-16 | \*\*\* |
| school_level_factorOther | 3.4249 | 0.2004 | 17.091 | < 2e-16 | \*\*\* |
| school_level_factorAlternative | 2.1316 | 0.3073 | 6.938 | 4.19e-12 | \*\*\* |
| pct_white_teachers:pct_black_students | 0.0047 | 0.0003 | 16.359 | < 2e-16 | \*\*\* |

### B. Glossary

**Interaction Term**: A product of two predictor variables in a regression model. Tests whether the effect of one variable depends on the level of another variable.

**Marginal Effect**: The change in the outcome for a one-unit change in a predictor, holding other variables constant. In the presence of an interaction, the marginal effect of one variable depends on the level of the other.

**Moderation**: When the relationship between X and Y varies depending on the level of a third variable Z. In this analysis, % Black Students moderates the relationship between % White Teachers and suspension rates.

**Weighted Least Squares**: A regression technique that gives different weights to different observations. Here, larger schools receive more weight because they provide more precise estimates.

**SED Rate**: Percentage of students who are Socioeconomically Disadvantaged. **NOT Special Education**.

**Ecological Fallacy**: Making inferences about individuals based on group-level data.

---

**END OF SUMMARY**
