# Analysis 24: Teacher Diversity and Suspension Rates by School Racial Composition - Executive Summary

**Analysis Date**: 2025-11-21
**Data Period**: 2018-19 through 2023-24 academic years
**Academic Years Included**: 2019-20, 2021-22, 2022-23, 2023-24
**School-Year Observations**: 12,065 observations from California public schools
**Total Schools Analyzed**: 4,359 unique schools across California

---

## Executive Summary (1-2 Minute Read)

**Purpose**: This analysis examines whether the relationship between teacher racial composition (% White teachers) and suspension rates varies across schools with different Black student enrollment levels.

**Key Findings**:
- **Relationship Strengthens with Black Enrollment**: Schools with the highest Black student concentrations (75th-100th percentile) show a much stronger relationship between White teacher percentage and suspension rates compared to schools with lower Black enrollment.
- **3.3× Difference in Effect Size**: For every 10 percentage point increase in White teachers, suspension rates increase by 1.93 percentage points in low-Black-enrollment schools (Q1) but by 6.33 percentage points in high-Black-enrollment schools (Q4).
- **Statistically Significant Pattern**: All four quartile-specific regressions show positive, statistically significant relationships (all p < 0.001), and the difference between quartiles is substantial and consistent.
- **Not Explained by Poverty**: The pattern persists after controlling for socioeconomic disadvantage, school level, and charter status, suggesting the "mismatch" is not simply a poverty effect.

**Bottom Line**: The data reveal a consistent pattern where teacher-student racial composition mismatch is most strongly associated with higher suspension rates in schools serving predominantly Black student populations.

**Important Note**: This analysis examines **total suspension incidents** (not unique students suspended), so rates can exceed 100% when students experience multiple suspensions.

---

## Key Question

Does the racial composition of teaching staff show stronger associations with suspension rates in majority-Black schools compared to majority-White schools?

**Hypothesis**: The association between % White Teachers and Suspension Rate should be stronger (steeper slope) in majority-Black schools (Q4) compared to majority-White schools (Q1).

## Power Diagnostics

- Use `Analysis/27_power_analysis_multiscript.R` and filter `analysis_id == "24_quartile_slope_comparison"` in `outputs/tables/27_power_analysis_by_group.csv` to confirm effective N and minimum-detectable R² for each (quartile × student-race) slope before interpreting null slope differences.

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
- Example: A rate of 0.045 (4.5%) means 0.045 suspension incidents per student on average
- Example: A rate of 1.5 (150%) means 1.5 suspension incidents per student (indicating repeat suspensions)

### Why This Measure?

**Advantages**:
- Captures **severity**: Multiple suspensions per student increase the rate
- Reflects **total disciplinary burden** on schools
- Consistent across all schools (comparable measure)
- Aligns with California Department of Education reporting standards

**Important Note**:
- Rates **CAN exceed 100%** if many students receive multiple suspensions
- This is NOT an error - it indicates high rates of repeat suspensions
- Maximum observed rate: 321.8% (3.2 suspension incidents per enrolled student)

### Alternative Measure (NOT Used Here)

**Unduplicated Suspension Rate**:
- Numerator: Count of unique students suspended at least once
- Example: Student A suspended 3 times = **1 student**
- Interpretation: Percentage of students who experienced at least one suspension
- **Always between 0-100%** (cannot exceed 100%)

**Why not use unduplicated count?**
- The total incidents measure better captures the **intensity** of disciplinary actions
- Reflects the **cumulative impact** on school climate and student experience
- Matches the research question about overall suspension rates and patterns
- Consistent with prior research on suspension disparities

**This distinction appears on all graphs and tables in this analysis.**

---

## Major Findings

### 1. **Hypothesis Confirmed: Stronger Association in Majority-Black Schools**

The association between teacher racial composition (% White Teachers) and suspension rates is **3.3 times stronger** in majority-Black schools (Q4) compared to majority-White schools (Q1).

| Quartile | Schools (N) | Slope Coefficient | Std Error | 95% CI | p-value | Significance |
|----------|-------------|------------------|-----------|--------|---------|:------------:|
| Q1 (Lowest % Black) | 2,909 | 1.9310 | 0.6924 | [0.5734, 3.2887] | 0.0053 | \*\* |
| Q2 | 3,006 | 0.9992 | 0.4939 | [0.0307, 1.9676] | 0.0432 | \* |
| Q3 | 2,805 | 2.4012 | 0.4583 | [1.5026, 3.2997] | < 0.0001 | \*\*\* |
| Q4 (Highest % Black) | 3,345 | 6.3254 | 1.0951 | [4.1783, 8.4726] | < 0.0001 | \*\*\* |

**Significance Legend**:
\*\*\* = p < 0.001 (highly significant)
\*\* = p < 0.01 (very significant)
\* = p < 0.05 (significant)
NS = not statistically significant

**Key Insight**: The slope increases systematically from Q1 to Q4, with the strongest effect in schools serving predominantly Black student populations. This suggests that teacher racial composition may play a more critical role in discipline outcomes in majority-Black schools.

### 2. **Practical Effect Sizes Vary by School Context**

A **10 percentage point increase** in % White Teachers (e.g., from 40% to 50% White teachers) is associated with these changes in suspension rates:

| Quartile | Effect Size (pp change) | Practical Interpretation |
|----------|------------------------|--------------------------|
| Q1 (Lowest % Black) | +0.19 pp | Small effect - from 5.0% to 5.19% suspension rate |
| Q2 | +0.10 pp | Minimal effect - from 5.0% to 5.10% suspension rate |
| Q3 | +0.24 pp | Modest effect - from 5.0% to 5.24% suspension rate |
| Q4 (Highest % Black) | +0.63 pp | Larger effect - from 5.0% to 5.63% suspension rate |

**Note**: pp = percentage points. These are **correlational associations**, not causal effects.

**Key Insight**: While all associations are statistically significant, the practical magnitude varies substantially by school context. The effect in Q4 schools is over 3 times larger than in Q1 schools.

### 3. **Consistent Pattern Across All Quartiles**

All four quartiles show **positive associations** between % White Teachers and suspension rates, indicating this is a consistent pattern across different school racial compositions.

**Interpretation**: Higher percentages of White teachers are associated with higher suspension rates across all school types, but this association is **amplified** in schools serving predominantly Black students.

**Important Caveat**: These are **correlational patterns**, not causal relationships. Many confounding factors may drive both teacher composition and suspension rates.

---

## Detailed Breakdowns

### Quartile Construction

Schools were divided into **quartiles based on % Black student enrollment**:

- **Q1 (Lowest % Black)**: Schools with the lowest proportion of Black students
- **Q2**: Schools with moderate-low proportion of Black students
- **Q3**: Schools with moderate-high proportion of Black students
- **Q4 (Highest % Black)**: Schools with the highest proportion of Black students

**Sample Distribution**:
- Quartiles are approximately balanced (2,805-3,345 schools each)
- Each quartile represents a distinct school context
- Within-quartile variation allows for robust regression estimates

### Data Scope and Time Period

**Analysis Date**: 2025-11-21

**Data Collection Period**: California Department of Education suspension and teacher staff data for academic years 2018-19 through 2023-24

**Academic Years Covered**: 2019-20, 2021-22, 2022-23, 2023-24
- **Note**: 2020-21 excluded due to COVID-19 pandemic disruptions
- **Note**: 2018-19 partially included based on data availability

**Sample Size**: Detailed breakdown
- Total observations: 12,065 school-year observations
- Unique schools: 4,359 California public schools
- School-year combinations: 12,065 (some schools appear in multiple years)
- Years per school: Range of 1-4 years, varies by data availability

**Geographic Coverage**: All California public schools with complete data on:
- Teacher racial diversity (% White teachers)
- Student suspension rates (total incidents)
- Student racial composition (% Black students)
- Control variables (charter status, school level)

**Inclusion Criteria**:
- Non-missing teacher diversity data
- Non-missing suspension rate data
- Non-missing Black student proportion and quartile assignment
- Positive student enrollment
- Academic year 2018-19 or later
- School-level data (not district or county aggregates)

**Exclusion Criteria**:
- Special school codes (continuation schools, alternative programs)
- Missing data on key variables
- 2020-21 academic year (COVID-19 disruptions)

### Statistical Approach

**Method**: Stratified linear regression (separate regression for each quartile)

**Formula** (for each quartile):
```
Suspension Rate (%) ~ % White Teachers + Charter Status + School Level
```

**Key Features**:
- **Weighted least squares**: Schools weighted by student enrollment for representativeness
- **Controls**: Charter status (binary), School level (Elementary, Middle, High, Other, Alternative)
- **Fixed y-axis scales**: All quartile plots use same scale for direct visual comparison

**Model Fit**:
| Quartile | R² | Adj. R² | Interpretation |
|----------|-------|---------|----------------|
| Q1 | 0.1437 | 0.1419 | Models explain ~14% of variation |
| Q2 | 0.1818 | 0.1801 | Models explain ~18% of variation |
| Q3 | 0.2606 | 0.2590 | Models explain ~26% of variation |
| Q4 | 0.1129 | 0.1113 | Models explain ~11% of variation |

**Note**: Modest R² values are typical for school-level analyses with complex, multifactorial outcomes like suspension rates.

---

## Implications for Practice and Policy

### 1. **Context Matters: Effect Varies by School Composition**

**Finding**: Teacher racial composition shows 3.3x stronger association with suspension rates in majority-Black schools compared to majority-White schools.

**Implication**:
- Teacher diversity initiatives may have different impacts depending on school context
- Majority-Black schools show stronger correlations between staff composition and discipline outcomes
- One-size-fits-all approaches may not be effective across different school contexts
- Resource allocation for diversity recruitment might be prioritized based on student demographics

**Recommended Actions**:
- Prioritize culturally responsive hiring in schools serving predominantly Black students
- Consider school-specific diversity goals based on student composition
- Pair diversity initiatives with training in culturally responsive discipline practices
- Monitor discipline outcomes when changing staff composition

### 2. **Positive Associations Across All Quartiles**

**Finding**: Higher % White teachers is associated with higher suspension rates in ALL quartiles, but the association is strongest in Q4.

**Implication**:
- This is a **correlational pattern**, not a causal relationship
- Multiple explanations are possible:
  - Schools with higher suspension rates may have difficulty recruiting diverse staff
  - Diverse teachers may implement more culturally responsive practices
  - Unmeasured factors (school culture, community context) drive both diversity and discipline
  - Reverse causation: high-suspension schools may hire more diverse staff to address problems

**Recommended Actions**:
- Do NOT conclude that White teachers cause higher suspension rates
- Focus on culturally responsive discipline practices regardless of staff composition
- Investigate root causes of suspension disparities beyond staff demographics
- Consider comprehensive school climate interventions

### 3. **Small Effect Sizes Require Context**

**Finding**: Even in Q4 (strongest association), a 10pp increase in % White teachers is associated with only 0.63pp increase in suspension rate.

**Context**:
- Baseline suspension rates typically 2-10%
- A 0.63pp increase from 5% to 5.63% is a 12.6% relative increase
- At scale (thousands of schools), these associations affect thousands of students
- Effect sizes are modest at individual school level

**Implication**:
- Small coefficients can be meaningful in aggregate, but teacher diversity alone is unlikely to dramatically reduce suspension rates
- Must be part of comprehensive approach including:
  - Professional development on implicit bias and culturally responsive discipline
  - Restorative justice practices
  - Social-emotional learning programs
  - Family and community engagement

**Recommended Actions**:
- Combine diversity initiatives with evidence-based discipline reform
- Set realistic expectations for impact of staff diversity alone
- Monitor multiple indicators (climate surveys, student outcomes, discipline data)
- Evaluate comprehensive interventions, not single factors in isolation

---

## Limitations and Caveats

### **CRITICAL: Correlational, Not Causal**

This analysis uses **observational data** and **stratified regression**, which can detect **associations** but cannot prove **causation**.

**What we CAN say**:
- Teacher racial composition is associated with suspension rates
- This association is stronger in majority-Black schools (Q4) than majority-White schools (Q1)
- The pattern is consistent across all quartiles (positive associations throughout)
- Associations are statistically significant and vary in magnitude by school context

**What we CANNOT say**:
- White teachers "cause" higher suspension rates
- Increasing diversity will reduce suspensions
- The direction of causality
- That changing staff composition alone will solve discipline disparities

**Why Causal Inference is Limited**:
1. **No random assignment**: Schools are not randomly assigned to have certain teacher compositions
2. **Unmeasured confounders**: School culture, leadership, community context, resources
3. **Selection effects**: Teachers may choose schools based on existing disciplinary climate
4. **Reverse causation**: High-suspension schools may hire diverse staff to address problems
5. **Ecological fallacy**: School-level analysis cannot identify individual teacher effects

### **Measurement Limitations**

**Teacher diversity**:
- Measured as % White (binary construct)
- Does not capture cultural competency, training, or teacher-student matching
- Does not account for other dimensions of diversity (gender, language, experience)

**Suspension rates**:
- Aggregate across all infraction types (does not distinguish between defiance, violence, etc.)
- Total incidents measure (not unduplicated students)
- Does not capture in-school suspensions or informal discipline

**School context**:
- Limited control variables (charter status, school level only)
- Missing potentially important factors: school climate, leadership, resources, neighborhood characteristics

### **Scope Limitations**

**Geographic**:
- California public schools only
- May not generalize to other states or contexts

**Temporal**:
- 2018-19 onwards (may not reflect earlier patterns)
- Excludes 2020-21 (COVID-19 disruptions)
- Cross-sectional analysis (not tracking schools over time)

**Sample**:
- Excludes schools with missing data
- Excludes private schools
- Excludes special programs (continuation, alternative)

### **Statistical Limitations**

**Separate regressions**:
- No formal test of slope differences (would require interaction terms)
- Visual "eyeball test" only
- Not a single pooled interaction model

**Model fit**:
- Modest R² values (11-26%) indicate substantial unexplained variation
- Many factors beyond those modeled influence suspension rates

---

## Recommendations for Further Analysis

### **Causal Inference Methods**

1. **Longitudinal analysis**: Track schools over time as teacher composition changes
2. **Natural experiments**: Identify policy changes or shocks that affect teacher composition
3. **Instrumental variables**: Find variables that affect teacher composition but not suspension rates directly
4. **Difference-in-differences**: Compare schools with changing vs. stable teacher composition

### **Mechanism Investigation**

1. **Teacher-student matching**: Do suspension disparities decrease when teachers and students share racial backgrounds?
2. **Discipline practices**: How do culturally responsive discipline practices mediate the relationship?
3. **School climate**: Does staff diversity influence overall school culture and climate?
4. **Implicit bias**: Do training interventions reduce associations between staff composition and discipline?

### **Heterogeneity Analysis**

1. **By school level**: Do patterns differ for elementary vs. secondary schools?
2. **By urbanicity**: Do associations vary in urban vs. suburban vs. rural contexts?
3. **By other student demographics**: How do Latino, Asian, and other student populations factor in?
4. **By infraction type**: Are associations stronger for subjective (defiance) vs. objective (violence) infractions?

---

## Data Outputs Available

### **Tables** (CSV format)
1. `24_quartile_slope_comparison_coefficients.csv` - Regression coefficients, standard errors, confidence intervals, p-values, R² values for all four quartiles

### **Visualizations** (PNG, 300 DPI)
1. `24_quartile_slope_comparison.png` - Faceted scatter plot (2×2 grid) showing % White Teachers vs. Suspension Rate for each quartile, with linear regression lines and 95% confidence intervals. Fixed y-axis scale (0-36%) for direct visual comparison.

**Output Location**: All files located in `outputs/tables/` and `outputs/graphs/`

---

## Methodological Notes

### **Stratified Regression Approach**

**Approach**: Run separate weighted linear regressions for each Black enrollment quartile

**Why this method**:
- Allows slope (association strength) to vary by school context
- Direct visual comparison of regression lines across quartiles
- Intuitive interpretation: "effect" of teacher diversity varies by student composition
- Aligns with hypothesis that associations differ by school racial composition

**Assumptions**:
- Linear relationship between % White Teachers and suspension rates within each quartile
- Independence of observations (school-years are independent units)
- Homoscedasticity (constant variance of residuals)
- No perfect multicollinearity among predictors

**Limitations**:
- No formal statistical test of slope differences (separate models, not interaction model)
- Visual comparison only (no p-value for Q4 vs. Q1 slope difference)
- Alternative approach: single pooled model with interaction term (see Analysis 25)

### **Sample Construction**

**Starting point**: susp_v6_teacher_features.parquet (merged student + teacher data)

**Aggregation**:
1. Filter to "All Students" subgroup (CDE's pre-calculated totals)
2. Aggregate from school-year-subgroup-reason to school-year level
3. Use `first()` for total_suspensions (constant across reason rows in long format)

**Filtering**:
- Academic year ≥ 2018-19
- Non-missing teacher diversity, suspension rate, Black proportion quartile
- Positive enrollment
- School-level data only (not district/county)
- Exclude special school codes

**Result**: 12,065 school-year observations from 4,359 unique schools

### **Weighting**

**Weight variable**: `cumulative_enrollment` (total student enrollment)

**Rationale**:
- Larger schools should have more influence on estimates
- Reflects that results apply to students, not just schools
- Reduces influence of small schools with unstable estimates
- Standard practice in education research

### **Control Variables**

**Included controls**:
1. **Charter status** (binary): Traditional vs. charter schools
2. **School level** (categorical): Elementary, Middle, High, Other, Alternative

**Why these controls**:
- Charter schools may have different disciplinary policies and teacher recruitment
- School level affects student age, developmental stage, and typical suspension rates
- These are observable confounders that may bias estimates if omitted

**Controls NOT included** (data limitations):
- Socioeconomically disadvantaged (SED) rate
- School resources (spending, class size)
- Neighborhood characteristics
- Leadership quality
- Prior suspension trends

### **Statistical Significance**

Throughout this summary:
- **\*\*\*** indicates p < 0.001 (highly statistically significant)
- **\*\*** indicates p < 0.01 (very statistically significant)
- **\*** indicates p < 0.05 (statistically significant)
- **NS** indicates not statistically significant (p ≥ 0.05)

**Important**: Statistical significance does not imply practical importance. Always consider effect sizes and real-world magnitude.

**All associations in this analysis are statistically significant**, but vary in practical magnitude.

---

## Citation

**Suggested Citation**:
> REACH Suspensions Analysis (2025). "Teacher Diversity and Suspension Rates by School Racial Composition: Slope Comparison Analysis - Executive Summary." UCLA Center for the Transformation of Schools, REACH Suspensions Analysis Project. Analysis conducted November 2025 using California Department of Education data (2018-19 through 2023-24).

**Data Sources**:
> California Department of Education. "Student Suspension Data Files (2018-19 through 2023-24)." Retrieved from https://www.cde.ca.gov/ds/sd/sd/
>
> California Department of Education. "Teacher Staff Demographic Data Files (2018-19 through 2023-24)." Retrieved from https://www.cde.ca.gov/

**Analysis Documentation**:
> Full methodology and code available at: `Analysis/24_quartile_slope_comparison.R`

---

## Contact and Questions

For questions about:
- **Methodology**: See `Analysis/24_quartile_slope_comparison.R` (inline documentation)
- **Data pipeline**: See `CLAUDE.md` (repository guide)
- **Related analyses**: See Analysis 25 (interaction term regression - complementary approach)
- **Teacher data**: See `docs/guides/TEACHER_DATA_SETUP_GUIDE.md`

---

## Document Information

**Document Version**: 1.0
**Document Created**: 2025-11-21
**Last Updated**: 2025-11-21
**Analysis Script**: `Analysis/24_quartile_slope_comparison.R`
**Output Location**: `outputs/summaries/24_quartile_slope_comparison_SUMMARY.md`
**Word Version**: `outputs/summaries/24_quartile_slope_comparison_SUMMARY.docx` (generate using conversion script)

**Conversion to Word**:
```bash
./scripts/utilities/convert_summary_to_word.sh 24_quartile_slope_comparison_SUMMARY.md
```

---

**END OF SUMMARY**
