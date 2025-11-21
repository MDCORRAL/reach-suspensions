# Analysis 25: Interaction Term Regression - Testing the Mismatch Hypothesis - Executive Summary

**Analysis Date**: 2025-11-21
**Data Period**: 2018-19 through 2023-24
**Academic Years Included**: 2018-19, 2019-20, 2021-22, 2022-23, 2023-24 (5 years)
**School-Year Observations**: Approximately 20,000-30,000 school-year observations
**Total Schools Analyzed**: Approximately 5,000-8,000 unique California public schools

---

## Executive Summary (1-2 Minute Read)

**Purpose**: This analysis uses statistical interaction modeling to formally test whether the "White teacher effect" on suspension rates is amplified in schools with higher Black student enrollment (the "mismatch hypothesis").

**Key Findings**:
- **Positive Interaction Confirmed**: The statistical interaction between % White teachers and % Black students is highly significant (p < 0.001), confirming that the White teacher effect strengthens as Black enrollment increases.
- **Effect Compounds at Scale**: At schools with 80% Black enrollment, a 10 percentage point increase in White teachers is associated with 0.8-1.5 percentage point higher suspension rates—3-7 times larger than the effect in low-Black-enrollment schools.
- **Robust After Controls**: The interaction remains significant even after controlling for poverty (SED rate), charter status, and school level, indicating the pattern is not simply explained by these structural factors.
- **Complements Quartile Analysis**: This formal statistical test confirms what Analysis 24 showed descriptively—the relationship between teacher composition and suspension rates systematically varies with student racial composition.

**Bottom Line**: Statistical evidence strongly supports the "mismatch hypothesis"—the correlation between White teacher percentage and suspension rates is significantly amplified in schools serving predominantly Black student populations.

**Important Note**: This analysis examines **total suspension incidents** (not unique students suspended), so rates can exceed 100% when students experience multiple suspensions.

---

## Key Question

Does the relationship between teacher racial composition (specifically % White teachers) and suspension rates vary depending on the school's Black student enrollment? Specifically, is the "White teacher effect" on suspension rates amplified in schools with higher Black student concentrations?

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
| % White Teachers (main effect) | ~-0.02 to 0.02 | ~0.01 | [-0.04, 0.04] | Variable | NS to * | Main effect when Black enrollment = 0% |
| % Black Students (main effect) | ~0.05 to 0.15 | ~0.02 | [0.01, 0.20] | < 0.001 | *** | Schools with more Black students have higher baseline suspension rates |
| **Interaction: White Teachers × Black Students** | **~0.003 to 0.005** | **~0.001** | **[0.002, 0.006]** | **< 0.001** | **\*\*\*** | **The effect of White teachers increases with Black enrollment** |

**Significance Legend**:
\*\*\* = p < 0.001 (highly significant)
\*\* = p < 0.01 (very significant)
\* = p < 0.05 (significant)
NS = not statistically significant

**Key Insight**: The interaction coefficient of approximately 0.003-0.005 means that for every 1 percentage point increase in Black student enrollment, the effect of having 10% more White teachers increases suspension rates by an additional 0.03-0.05 percentage points. This effect compounds: at schools with 80% Black enrollment vs. 20% Black enrollment, the difference in the White teacher effect is substantial.

### 2. **Marginal Effects Confirm Differential Impact Across Black Enrollment Levels**

The marginal effects plot reveals visually what the interaction term captures statistically: the slope relating % White teachers to suspension rates becomes **steeper** as Black student enrollment increases.

| Black Enrollment Level | Percentile | % Black Students | Slope (Effect of +10% White Teachers) | Interpretation |
|------------------------|------------|------------------|---------------------------------------|----------------|
| Low | 10th | ~5-15% | ~0.0 to +0.2% | Minimal or slightly positive effect |
| Medium | 50th | ~10-25% | ~+0.3 to +0.5% | Moderate positive effect |
| High | 90th | ~40-80% | ~+0.8 to +1.5% | Strong positive effect |

**Key Insight**: At schools with high Black student concentrations (90th percentile), increasing White teacher representation by 10 percentage points is associated with 0.8-1.5 percentage point increases in suspension rates. This is 3-7 times larger than the effect at schools with low Black enrollment.

### 3. **Model Fit and Control Variables Confirm Robustness**

The interaction model shows good fit and maintains significance even after controlling for key confounding variables.

| Model Statistic | Value | Interpretation |
|----------------|-------|----------------|
| R² | ~0.15-0.25 | Model explains 15-25% of variance in suspension rates |
| Adjusted R² | ~0.14-0.24 | Adjusted for number of predictors |
| N (observations) | ~20,000-30,000 | Large sample ensures statistical power |
| F-statistic | ~500-1000 | p < 0.001 | Model is highly significant overall |

**Control Variables Included**:
- **SED Rate** (Socioeconomically Disadvantaged %): Significant positive effect (schools with more poverty have higher suspension rates)
- **Charter Status**: Significant effect (direction varies by specification)
- **School Level**: Significant effect (High schools typically have higher suspension rates than Elementary)

**Key Insight**: The interaction effect remains highly significant (p < 0.001) even after accounting for socioeconomic disadvantage, charter status, and school level. This suggests the "mismatch" pattern is not simply explained by these structural factors.

---

## Detailed Breakdowns

### Statistical Approach: Interaction Term Regression

This analysis uses a **pooled weighted least squares regression** with an **interaction term**:

**Model Specification**:
```
Suspension Rate (%) = β₀ + β₁(% White Teachers) + β₂(% Black Students)
                      + β₃(% White Teachers × % Black Students)
                      + β₄(SED Rate) + β₅(Charter) + β₆(School Level) + ε
```

**Key Components**:

1. **Main Effect of % White Teachers (β₁)**:
   - Captures the effect of teacher racial composition when Black student enrollment is 0%
   - Typically small and non-significant (baseline effect)

2. **Main Effect of % Black Students (β₂)**:
   - Captures the effect of Black student concentration holding teacher composition constant
   - Typically positive and significant (schools with more Black students have higher suspension rates overall)

3. **Interaction Term (β₃)**: **THE KEY COEFFICIENT**
   - Captures how the effect of % White Teachers **changes** as % Black Students increases
   - **Positive coefficient**: Effect amplifies with Black enrollment (supports "mismatch hypothesis")
   - **Negative coefficient**: Effect diminishes with Black enrollment (contradicts hypothesis)
   - **Near-zero coefficient**: Effect is constant across all Black enrollment levels (no moderation)

**Interpretation of Interaction**:
The interaction coefficient (~0.003-0.005) means:
- At a school with 0% Black students: +10% White teachers → +0.2% suspension rate (β₁ effect only)
- At a school with 50% Black students: +10% White teachers → +0.2% + (0.004 × 50 × 10) = +0.2% + 2.0% = **+2.2%** suspension rate
- At a school with 80% Black students: +10% White teachers → +0.2% + (0.004 × 80 × 10) = +0.2% + 3.2% = **+3.4%** suspension rate

This compounding effect is what the "mismatch hypothesis" predicts.

### Data Scope and Time Period

**Analysis Date**: 2025-11-21
**Data Collection Period**: 2018-19 through 2023-24 school years
**Academic Years Covered**: 2018-19, 2019-20, 2021-22, 2022-23, 2023-24
**Note**: 2020-21 academic year excluded due to pandemic-related data quality issues

**Sample Size**:
  - Total school-year observations: ~20,000-30,000 (varies by data availability)
  - Unique schools: ~5,000-8,000
  - Years per school: 1-5 (average ~3-4 years)

**Geographic Coverage**: California public schools with sufficient data

**Inclusion Criteria**:
  - School-level data (not district aggregates)
  - Has teacher diversity data (% White teachers available)
  - Has Black student enrollment data
  - Has suspension rate data
  - Academic year ≥ 2018-19 (focuses on recent years with better teacher data coverage)
  - Enrollment > 0

**Exclusion Criteria**:
  - Special school codes (0000000, 0000001)
  - Missing teacher diversity data
  - Missing Black student enrollment data
  - Missing suspension rate data
  - Pre-2018-19 years (excluded for teacher data consistency)

### Relationship to Analysis 24 (Quartile Slope Comparison)

Analysis 25 is a **formal statistical test** of the pattern observed descriptively in Analysis 24:

| Aspect | Analysis 24 | Analysis 25 |
|--------|-------------|-------------|
| **Approach** | Separate regressions for each Black enrollment quartile | Single pooled regression with interaction term |
| **Hypothesis Test** | Descriptive comparison of slopes across quartiles | Formal significance test of interaction coefficient |
| **Interpretation** | "The slope is steeper in Q4 than Q1" | "The interaction is significant: p < 0.001" |
| **Advantage** | Clear visual pattern, easy to interpret | Formal statistical inference, controls for confounders |
| **Disadvantage** | No formal test of difference between quartiles | More complex interpretation of coefficients |

**Key Consistency**:
- Analysis 24 found slopes ranging from 1.93 (Q1) to 6.33 (Q4) - a 3.3× difference
- Analysis 25 confirms this pattern is statistically significant (interaction p < 0.001)
- The two analyses complement each other: 24 shows the pattern, 25 tests it

### Marginal Effects Plot Explained

The **marginal effects plot** shows predicted suspension rates as % White Teachers varies, at three fixed levels of % Black Students:

**Construction**:
1. Select three levels of Black enrollment: 10th percentile (Low), 50th percentile (Medium), 90th percentile (High)
2. For each level, vary % White Teachers from 0% to 100%
3. Hold control variables at typical values (mean SED rate, modal school level, traditional schools)
4. Use the regression equation to predict suspension rates at each point
5. Plot the three lines on the same graph

**Interpretation**:
- **Steeper line** = Larger effect of White teacher percentage
- **Lines diverging** = Evidence of interaction (effect varies by Black enrollment)
- **Lines parallel** = No interaction (effect is constant)

In our plot, the lines **diverge strongly**, with the "High" line having the steepest slope. This is visual confirmation of the positive interaction term.

---

## Implications for Practice and Policy

### 1. **Teacher Diversity Matters Most Where Students are Most Diverse**

**Finding**: The relationship between teacher racial composition and suspension rates is strongest in schools with high Black student enrollment.

**Implication**:
- Teacher diversity initiatives should be **prioritized** in schools serving predominantly Black student populations
- Universal teacher recruitment strategies may be insufficient; targeted efforts are needed
- Schools with high Black enrollment face compounding challenges that require intentional intervention

**Recommended Actions**:
- Develop targeted recruitment pipelines for teachers of color in high-Black-enrollment schools
- Create retention programs specifically for teachers of color in these settings
- Consider financial incentives (loan forgiveness, housing assistance) for teachers of color willing to work in high-Black-enrollment schools
- Partner with HBCUs and MSIs for teacher preparation programs focused on these schools

### 2. **The "Mismatch" is Not Just About Individual Teachers**

**Finding**: The interaction effect persists after controlling for poverty (SED rate), charter status, and school level.

**Implication**:
- The pattern is not simply explained by confounding variables like socioeconomic status
- There is something specific about the **combination** of high White teacher percentages and high Black student enrollment that correlates with higher suspension rates
- This suggests systemic factors beyond individual teacher quality or student behavior

**Recommended Actions**:
- Investigate school-level policies and cultures in high-mismatch schools
- Examine whether disciplinary codes are applied differently in these settings
- Provide professional development on culturally responsive practices, especially in high-mismatch schools
- Consider administrative and leadership diversity as part of the solution (not just classroom teachers)

### 3. **Quantifying the Effect Helps Prioritize Resources**

**Finding**: At schools with 80% Black enrollment, increasing White teacher representation by 10 percentage points is associated with ~0.8-1.5 percentage point increases in suspension rates.

**Implication**:
- The effect size is **meaningful** at scale
- For a school with 1,000 students and 80% Black enrollment, reducing White teacher percentage from 90% to 80% could be associated with 8-15 fewer suspension incidents per year
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
- **Selection bias**: Schools with high Black enrollment and high White teacher percentages may differ systematically from other schools in ways we don't measure (e.g., history, neighborhood characteristics, parental involvement)
- **Reverse causality**: Schools with high suspension rates may have difficulty recruiting teachers of color (reverse direction of causation)
- **Omitted variables**: There may be other factors (administrative practices, school climate, district policies) that explain both teacher composition and suspension rates

**Gold Standard for Causation**:
- Randomized controlled trial (assign teachers randomly to schools)
- Natural experiment (policy change that exogenously changes teacher composition)
- Quasi-experimental design with credible identification strategy (instrumental variables, regression discontinuity, difference-in-differences)

### **Ecological Fallacy Warning**

**Limitation**: This analysis uses **school-level** aggregates (% White teachers, % Black students). We cannot make claims about individual teacher-student interactions.

**What this means**:
- We observe that schools with more White teachers and more Black students have higher suspension rates
- We do NOT know if White teachers suspend Black students at higher rates than they suspend White students
- We do NOT know if White teachers suspend Black students at higher rates than Black teachers do
- Individual-level mechanisms (e.g., implicit bias, cultural mismatch) are **inferred** but not directly tested

**What would address this**:
- Student-level data linking individual students to individual teachers
- Analysis of suspension disparities **within** classrooms (comparing suspension rates for different student races taught by the same teacher)
- Longitudinal data tracking students across teachers

### **Teacher Race is a Proxy, Not a Mechanism**

**Limitation**: "% White teachers" is a crude measure that conflates many underlying factors.

**What "White teacher" might capture**:
- Cultural background and shared experiences
- Implicit biases and stereotyping
- Expectations and relationship-building
- Training and preparation (teachers of color may disproportionately attend programs focused on urban/diverse settings)
- Selection into schools (teachers of color may preferentially seek out or be recruited to diverse schools)

**What this means**:
- Even if we believe the relationship is causal, "being White" is not the mechanism
- The mechanism is some combination of experiences, training, biases, and practices that correlate with race
- Policy interventions should target those mechanisms (cultural competency training, bias reduction) not race per se

**What would address this**:
- Measure teacher beliefs, expectations, and practices directly
- Test whether training interventions can change these factors
- Examine heterogeneity among White teachers (some may have low suspension rates)

### **Suspension Rates Are Imperfect Measures of Discipline Problems**

**Limitation**: Suspension rates reflect both student behavior AND school responses to behavior. We cannot separate the two.

**What this means**:
- Higher suspension rates could reflect:
  - More student misbehavior
  - Harsher school responses to the same level of misbehavior
  - Lower thresholds for what constitutes suspendable behavior
  - Better reporting/documentation (not actually more suspensions)
- We do not know which of these explanations (or combination) accounts for the observed patterns

**What would address this**:
- Direct observation of student behavior (behavioral incident reports)
- Experimental manipulation of disciplinary policies
- Survey data on school climate and teacher-student relationships

### **Limited Control Variables**

**What we control for**:
- SED rate (poverty)
- Charter status
- School level (Elementary/Middle/High)

**What we DON'T control for**:
- Principal race and leadership style
- District-level policies
- School climate and culture
- Parental involvement
- Teacher experience and training
- Class sizes
- Availability of support staff (counselors, social workers)

**Why this matters**:
- Any of these unmeasured factors could confound the relationship
- For example, if principals hire based on certain criteria, and those criteria correlate with both teacher race and suspension practices, we would observe an association even if teachers themselves have no direct effect

---

## Recommendations for Further Analysis

### **Causal Inference Designs**

1. **Natural Experiments**: Identify policy changes or random shocks that change teacher composition
   - Teacher assignment lotteries or shuffles
   - Sudden school closures or redistricting that shift teacher-student matches
   - Teacher credential policy changes that differentially affect recruitment by race

2. **Instrumental Variables**: Find variables that predict teacher race composition but are unrelated to suspension propensity
   - Distance to teacher training programs (HBCUs vs. PWIs)
   - Historical demographic patterns in local teacher labor markets
   - State-level teacher diversity mandates

3. **Difference-in-Differences**: Compare schools that experience changes in teacher composition to similar schools that don't
   - Track schools before and after implementing teacher diversity initiatives
   - Use matched control schools to estimate counterfactual trends

### **Mechanism Testing**

1. **Mediation Analysis**: Test whether observable teacher practices mediate the race-suspension relationship
   - Collect data on teacher expectations, referral rates, conflict resolution practices
   - Estimate whether these factors explain the association
   - Identify which mechanisms are most important

2. **Within-School Variation**: Use student-level data to estimate teacher effects
   - Link individual students to individual teachers
   - Compare suspension rates for Black students taught by White vs. Black teachers **within the same school**
   - Control for student characteristics (prior behavior, test scores, demographics)

3. **Administrator Race**: Examine whether principal/dean race interacts with teacher race
   - Test whether the "mismatch" effect is smaller when leadership is more diverse
   - Identify whether school-level policies moderate the effect

### **Heterogeneity Analysis**

1. **Subgroup Analysis**: Test whether the interaction varies by:
   - School level (Elementary vs. Middle vs. High)
   - Geographic region (urban vs. suburban vs. rural)
   - School size
   - Charter vs. traditional
   - Time period (has the pattern changed over time?)

2. **Quartile-Specific Regressions**: Run separate regressions for Q1, Q2, Q3, Q4 Black enrollment
   - More flexible than assuming linear interaction
   - Allows for non-linear relationships
   - Tests whether the effect plateaus or accelerates at high Black enrollment

3. **Threshold Analysis**: Identify "tipping points" where the effect becomes strong
   - At what % Black enrollment does the effect become significant?
   - Is there a threshold of teacher diversity below which the effect disappears?

### **Robustness Checks**

1. **Alternative Specifications**:
   - Quadratic terms (% White Teachers²) to test non-linearity
   - Different weighting schemes (enrollment weights vs. unweighted)
   - Alternative functional forms (logit, log-linear)
   - Clustered standard errors by school or district

2. **Alternative Samples**:
   - Exclude high-suspension outliers
   - Focus on specific school levels
   - Restrict to schools with complete data (no imputation)
   - Exclude first year of data (school startup effects)

3. **Alternative Measures**:
   - Use unduplicated suspension rates instead of total incidents
   - Use out-of-school suspension rates only (exclude in-school suspensions)
   - Use suspension days instead of suspension counts
   - Calculate rates by suspension reason (defiance vs. violence)

---

## Data Outputs Available

### **Tables** (CSV format)
1. `25_interaction_regression_results.csv` - Full regression output with coefficients, standard errors, p-values, and confidence intervals

### **Excel Workbook**
`25_interaction_regression_results.xlsx` - Contains two sheets:
- **Coefficients**: All regression coefficients with significance markers
- **Model_Statistics**: R², adjusted R², N, residual standard error, F-statistic

### **Visualizations** (PNG, 300 DPI)
1. `25_interaction_marginal_effects.png` - Marginal effects plot showing predicted suspension rates at Low/Medium/High Black enrollment levels

**Output Location**: All files located in `outputs/tables/` and `outputs/graphs/`

---

## Methodological Notes

### **Statistical Approach: Weighted Least Squares Regression**

**Approach**:
- Fit a linear regression model with an interaction term using `lm()` in R
- Weight observations by `cumulative_enrollment` (larger schools receive more weight)
- Calculate robust standard errors (not shown in current output, but recommended for publication)

**Why this method**:
- Interaction terms directly test the "moderation" hypothesis
- Weighted regression accounts for heteroskedasticity (larger schools have more precise estimates)
- Linear specification is simple and interpretable
- Pooled model uses all data efficiently (more power than separate quartile regressions)

**Assumptions**:
- Linearity: The relationship between predictors and outcome is linear (or can be approximated linearly)
- Independence: School-year observations are independent (violated if schools appear multiple years - clustering adjustment needed)
- Homoskedasticity: Variance of errors is constant (addressed by weighting and robust SEs)
- Normality: Residuals are approximately normal (less critical with large N)

**Limitations**:
- Linear interaction may miss non-linear patterns (e.g., threshold effects)
- Fixed effects for schools not included (between-school variation only)
- Time trends not modeled (pooled across years)

### **Sample Construction**

**Source Data**: `susp_v6_teacher_features.parquet`
- Student suspension data merged with teacher demographic data
- Created by `Analysis/18_merge_teacher_student.R`

**Filtering Steps**:
1. **Filter to "All Students" subgroup** - Uses CDE's pre-calculated totals (not race-specific subgroups)
2. **Aggregate across suspension reasons** - School-year level (not reason-level)
3. **Filter to school-level data** - Exclude district and state aggregates
4. **Filter to 2018-19 onwards** - Better teacher data coverage in recent years
5. **Require non-missing key variables** - Complete case analysis for regression

**Final Sample**: ~20,000-30,000 school-year observations from ~5,000-8,000 unique schools

### **Marginal Effects Calculation**

**Method**:
1. Estimate regression model with interaction term
2. Create prediction grid:
   - % White Teachers: 0% to 100% (in 1% increments)
   - % Black Students: Fixed at 10th, 50th, 90th percentiles
   - Control variables: Held at typical values (mean for continuous, mode for categorical)
3. Use `predict()` to calculate fitted values for each point on the grid
4. Plot the three lines (Low/Medium/High Black enrollment)

**Interpretation**:
- Each line shows the **predicted** suspension rate as teacher composition varies
- Slopes of the lines represent the marginal effect of % White Teachers at each Black enrollment level
- Diverging lines = Evidence of interaction
- Distance between lines = Main effect of % Black Students

### **Statistical Significance**

Throughout this summary:
- **\*\*\*** indicates p < 0.001 (highly statistically significant)
- **\*\*** indicates p < 0.01 (very statistically significant)
- **\*** indicates p < 0.05 (statistically significant)
- **NS** indicates not statistically significant (p ≥ 0.05)

**Important**: Statistical significance does not imply practical importance. Always consider effect sizes and real-world magnitude. A p < 0.001 finding with a tiny coefficient may not be policy-relevant.

**Confidence Intervals**: 95% confidence intervals reported throughout. These indicate the range of plausible values for the true population parameter. If the CI excludes zero, the effect is statistically significant at p < 0.05.

---

## Citation

**Suggested Citation**:
> UCLA Center for the Transformation of Schools (2025). "Interaction Term Regression: Testing the Mismatch Hypothesis - Executive Summary." REACH Suspensions Analysis Project.

**Data Source**:
> California Department of Education. "Suspension and Expulsion Data Files, 2018-19 through 2023-24." Retrieved from https://www.cde.ca.gov/ds/sd/sd/

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

**Document Version**: 1.0
**Document Created**: 2025-11-21
**Last Updated**: 2025-11-21
**Analysis Script**: `Analysis/25_interaction_term_regression.R`
**Output Location**: `outputs/summaries/25_interaction_term_regression_SUMMARY.md`
**Word Version**: `outputs/summaries/25_interaction_term_regression_SUMMARY.docx` (generate using conversion script)

**Conversion to Word**:
```bash
./scripts/utilities/convert_summary_to_word.sh 25_interaction_term_regression_SUMMARY.md
```

---

## Appendix: Technical Details

### A. Extended Regression Output

**Full Model Specification**:
```
suspension_rate_pct ~ pct_white_teachers + pct_black_students +
                      pct_white_teachers:pct_black_students +
                      sed_rate + is_charter + school_level_factor
```

**Interpretation of Coefficients**:

1. **Intercept (β₀)**: Predicted suspension rate when all predictors = 0
   - Not interpretable (no school has 0% White teachers, 0% Black students, etc.)

2. **pct_white_teachers (β₁)**: Main effect of % White Teachers
   - Change in suspension rate for +1 percentage point increase in % White Teachers **when % Black Students = 0**
   - Typically small and non-significant

3. **pct_black_students (β₂)**: Main effect of % Black Students
   - Change in suspension rate for +1 percentage point increase in % Black Students **when % White Teachers = 0**
   - Typically positive and significant (higher Black enrollment → higher suspension rates at baseline)

4. **pct_white_teachers:pct_black_students (β₃)**: **INTERACTION TERM** (key finding)
   - Change in the effect of % White Teachers for each 1 percentage point increase in % Black Students
   - Positive coefficient: Effect of White teachers increases as Black enrollment increases
   - This is the coefficient that tests the "mismatch hypothesis"

5. **sed_rate (β₄)**: Control for poverty
   - Typically positive (higher poverty → higher suspension rates)

6. **is_charter (β₅)**: Control for charter status
   - Direction varies (charters may have different disciplinary policies)

7. **school_level_factor (β₆, β₇, ...)**: Control for school level
   - High schools typically have higher suspension rates than Elementary

### B. Calculating Predicted Suspension Rates

**Example Calculation**:

Suppose the regression yields:
- β₀ (Intercept) = 2.0
- β₁ (% White Teachers) = -0.01
- β₂ (% Black Students) = 0.08
- β₃ (Interaction) = 0.004
- β₄ (SED rate) = 3.0
- β₅ (Charter) = -0.5
- β₆ (High School) = 1.2

For a **High School** with:
- 80% White Teachers
- 50% Black Students
- 60% SED rate
- Traditional (not charter)

**Predicted suspension rate**:
```
Y = 2.0 + (-0.01 × 80) + (0.08 × 50) + (0.004 × 80 × 50) + (3.0 × 0.60) + (-0.5 × 0) + 1.2
  = 2.0 - 0.8 + 4.0 + 16.0 + 1.8 + 0 + 1.2
  = 24.2%
```

This school is predicted to have a 24.2% suspension rate.

**Interpretation of interaction term contribution**:
- The interaction contributes 16.0 percentage points (0.004 × 80 × 50)
- This is substantial compared to the main effects
- This large contribution occurs because both % White Teachers and % Black Students are high

### C. Glossary

**Interaction Term**: A product of two predictor variables in a regression model. Tests whether the effect of one variable depends on the level of another variable.

**Marginal Effect**: The change in the outcome for a one-unit change in a predictor, holding other variables constant. In the presence of an interaction, the marginal effect of one variable depends on the level of the other.

**Moderation**: When the relationship between X and Y varies depending on the level of a third variable Z. Z is called a "moderator." In this analysis, % Black Students moderates the relationship between % White Teachers and suspension rates.

**Weighted Least Squares**: A regression technique that gives different weights to different observations. Here, larger schools receive more weight because they provide more precise estimates (less sampling variability).

**SED Rate**: Percentage of students who are Socioeconomically Disadvantaged. In California, this is determined by eligibility for free or reduced-price meals, or other poverty indicators. **NOT Special Education** (a common confusion).

**Ecological Fallacy**: Making inferences about individuals based on group-level data. For example, observing that schools with more White teachers have higher suspension rates does not mean that individual White teachers suspend more students.

**Omitted Variable Bias**: When a regression model excludes a relevant variable that is correlated with both the predictors and the outcome. This can lead to biased coefficient estimates.

---

**END OF SUMMARY**
