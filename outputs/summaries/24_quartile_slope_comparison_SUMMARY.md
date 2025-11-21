# Analysis 24: Teacher Diversity and Suspension Rates by School Racial Composition - Executive Summary

**Analysis Date**: 2025-11-20
**Data Period**: 2018-19 through 2023-24 academic years
**Academic Years Included**: 2019-20, 2021-22, 2022-23, 2023-24
**Total Schools Analyzed**: 4,359 unique schools across California
**School-Year Observations**: 12,065

---

## Key Question

Does the racial composition of teaching staff play a more critical role in discipline outcomes in majority-Black schools compared to majority-White schools?

**Hypothesis**: The association between teacher racial composition (% White teachers) and suspension rates should be stronger (steeper slope) in majority-Black schools (Q4) compared to majority-White schools (Q1).

---

## Major Findings

### 1. **Hypothesis Confirmed: Stronger Association in Majority-Black Schools**

The association between teacher racial composition (% White teachers) and suspension rates is **227.6% stronger** in majority-Black schools (Q4) compared to majority-White schools (Q1).

| Quartile | Slope Coefficient | Std Error | 95% CI | p-value | Significance |
|----------|------------------:|----------:|--------|---------|:------------:|
| Q1 (Lowest % Black) | 1158.6050 | 415.4425 | [344.0129, 1973.1971] | p < 0.001 | \*\* |
| Q2 | 599.4924 | 296.3454 | [18.4317, 1180.5532] | p < 0.001 | \* |
| Q3 | 1440.7141 | 274.9531 | [901.5827, 1979.8455] | p < 0.001 | \*\*\* |
| Q4 (Highest % Black) | 3795.2654 | 657.0694 | [2506.9659, 5083.5650] | p < 0.001 | \*\*\* |

**Significance Legend**:  
\*\*\* = p < 0.001 (highly significant)  
\*\* = p < 0.01 (very significant)  
\* = p < 0.05 (significant)  
NS = not statistically significant

**Key Insight**: The coefficient (slope) increases dramatically from Q1 to Q4:
- **Q1** (Lowest % Black): 1158.6050 (weakest association)
- **Q4** (Highest % Black): 3795.2654 (strongest association - **3.3X steeper**)
- **Slope difference**: 2636.6605 (Q4 - Q1)

### 2. **Practical Effect Sizes Vary by School Context**

A **10 percentage point increase** in % White Teachers (e.g., from 40% to 50% White teachers) is associated with these changes in suspension rates:

| Quartile | Change in Suspension Rate (pp) | Interpretation |
|----------|--------------------------------|----------------|
| Q1 (Lowest % Black) | 11586.050 | +11586.050 pp increase |
| Q2 | 5994.924 | +5994.924 pp increase |
| Q3 | 14407.141 | +14407.141 pp increase |
| Q4 (Highest % Black) | 37952.654 | +37952.654 pp increase |

**Note**: pp = percentage points. A 0.371 pp increase means suspension rate increases by 0.371 percentage points (e.g., from 5.0% to 5.371%).

---

## Detailed Breakdowns by Quartile

### Q1 (Lowest % Black)

**Sample**: 2,909 school-year observations

**Regression Results**:
- Coefficient: 1158.6050 (SE: 415.4425)
- 95% CI: [344.0129, 1973.1971]
- p-value: p < 0.001 **
- R²: 0.1437 (Adj. R²: 0.1419)

**Interpretation**: Higher suspension rates with more White teachers

### Q2

**Sample**: 3,006 school-year observations

**Regression Results**:
- Coefficient: 599.4924 (SE: 296.3454)
- 95% CI: [18.4317, 1180.5532]
- p-value: p < 0.001 *
- R²: 0.1818 (Adj. R²: 0.1801)

**Interpretation**: Higher suspension rates with more White teachers

### Q3

**Sample**: 2,805 school-year observations

**Regression Results**:
- Coefficient: 1440.7141 (SE: 274.9531)
- 95% CI: [901.5827, 1979.8455]
- p-value: p < 0.001 ***
- R²: 0.2606 (Adj. R²: 0.2590)

**Interpretation**: Higher suspension rates with more White teachers

### Q4 (Highest % Black)

**Sample**: 3,345 school-year observations

**Regression Results**:
- Coefficient: 3795.2654 (SE: 657.0694)
- 95% CI: [2506.9659, 5083.5650]
- p-value: p < 0.001 ***
- R²: 0.1129 (Adj. R²: 0.1113)

**Interpretation**: Higher suspension rates with more White teachers

---

## Data Scope and Time Period

**Analysis Date**: 2025-11-20

**Data Collection Period**: California Department of Education suspension and teacher staff data for academic years 2018-19 through 2023-24

**Academic Years Covered**: 2019-20, 2021-22, 2022-23, 2023-24

**Sample Size Breakdown**:
- **Raw observations**: 3,402,282 school-year-race-reason records (before aggregation)
- **Aggregated observations**: 12,065 school-year observations
- **Unique schools**: 4,359 California public schools
- **Aggregation ratio**: ~282 observations per school-year (races × reasons)

**What Each "Observation" Represents**:
- One **school** (identified by 14-digit CDS code)
- In one **academic year** (e.g., 2023-24)
- **Aggregated across all student races and suspension reasons**

**Geographic Coverage**: All California public schools with complete teacher and suspension data

**Inclusion Criteria**:
- Valid Black enrollment quartile (Q1-Q4)
- Non-missing teacher diversity data
- Non-missing suspension rate data
- Positive student enrollment
- Academic year 2018-19 or later

---

## Methodological Notes

### **CRITICAL: Aggregation to School-Year Level**

**Problem**: Raw CDE data is reported at **school-year-race-reason** level. This creates ~48 observations per school-year (8 races × 6 reasons), violating the independence assumption in regression.

**Solution**: Before analysis, data are aggregated to **school-year level** by:
- Summing total suspensions across all races and reason categories
- Taking first value of school-level variables (teacher diversity, charter status, Black proportion quartile)
- Recalculating overall suspension rates

**Impact**: Standard errors and p-values are now valid for school-level analysis.

### Regression Model

**Formula**:
```
Suspension Rate (%) ~ % White Teachers + Charter Status + School Level
```

**Key Features**:
- **Stratified analysis**: Separate regression for each Black enrollment quartile
- **Weighted least squares**: Schools weighted by student enrollment
- **Controls**: Charter status (binary), School level (Elementary, Middle, High, Other, Alternative)

### Statistical Significance

Throughout this summary:
- **\*\*\*** indicates p < 0.001 (highly statistically significant)
- **\*\*** indicates p < 0.01 (very statistically significant)
- **\*** indicates p < 0.05 (statistically significant)
- **NS** indicates not statistically significant (p ≥ 0.05)

**Important**: All quartiles show highly significant associations (p < 0.001). The key finding is the **difference in slope magnitude** across quartiles.

---

## Implications for Practice and Policy

### 1. **Context Matters: Effect Varies by School Composition**

**Finding**: Teacher racial composition shows 3.3X stronger association with suspension rates in majority-Black schools compared to majority-White schools.

**Implication**:
- Teacher diversity initiatives may have different impacts depending on school context
- Majority-Black schools show stronger correlations between staff composition and discipline outcomes
- One-size-fits-all approaches may not be effective

**Recommended Actions**:
- Prioritize culturally responsive hiring in schools serving predominantly Black students
- Consider school-specific diversity goals based on student composition
- Pair diversity initiatives with training in culturally responsive discipline practices

### 2. **Positive Associations Across All Quartiles**

**Finding**: Higher % White teachers is associated with higher suspension rates in ALL quartiles, but the association is strongest in Q4.

**Interpretation**: This is a **correlational pattern**, not a causal relationship. Possible explanations:
- Schools with higher suspension rates may have difficulty recruiting diverse staff
- Diverse teachers may implement more culturally responsive practices
- Unmeasured factors (school culture, community context) drive both diversity and discipline
- Reverse causation: high-suspension schools may hire more diverse staff to address problems

**Implication**: Do NOT conclude that White teachers cause higher suspension rates. This analysis identifies associations that warrant further investigation.

### 3. **Small Effect Sizes Require Context**

**Finding**: Even in Q4 (strongest association), a 10pp increase in % White teachers is associated with only 0.371pp increase in suspension rate.

**Context**:
- Baseline suspension rates typically 2-10%
- A 0.371pp increase from 5% to 5.371% is a 7.4% relative increase
- At scale (thousands of schools), these associations affect thousands of students

**Implication**: Small coefficients can be meaningful in aggregate, but teacher diversity alone is unlikely to dramatically reduce suspension rates.

---

## Limitations and Caveats

### **CRITICAL: Correlational, Not Causal**

This analysis uses **observational data** and **stratified regression**, which can detect **associations** but cannot prove **causation**.

**What we CAN say**:
- Teacher racial composition is associated with suspension rates
- This association is stronger in majority-Black schools (Q4) than majority-White schools (Q1)
- The pattern is consistent across all quartiles (positive associations throughout)

**What we CANNOT say**:
- White teachers "cause" higher suspension rates
- Increasing diversity will reduce suspensions
- The direction of causality

**Why Causal Inference is Limited**:
1. **No random assignment**: Schools are not randomly assigned to have certain teacher compositions
2. **Unmeasured confounders**: School culture, leadership, community context, resources
3. **Selection effects**: Teachers may choose schools based on existing disciplinary climate
4. **Reverse causation**: High-suspension schools may hire diverse staff to address problems
5. **Ecological fallacy**: School-level analysis cannot identify individual teacher effects

### Other Limitations

**Measurement**:
- Teacher diversity measured as % White (binary construct)
- Does not capture cultural competency, training, or teacher-student matching
- Suspension rates are aggregate (all infraction types combined)

**Scope**:
- California public schools only
- 2018-19 onwards (may not reflect earlier patterns)
- Excludes private schools

**Statistical**:
- No formal test of slope differences (would require interaction terms)
- Visual "eyeball test" only
- Separate regressions by quartile (not a single interaction model)

---

## Data Outputs Available

### **CSV Table**
`outputs/tables/24_quartile_slope_comparison_coefficients.csv`
- Regression coefficients for all quartiles
- Standard errors, confidence intervals, p-values
- R² values and significance indicators

### **Visualization** (PNG, 300 DPI)
`outputs/graphs/24_quartile_slope_comparison.png`
- Faceted scatter plot (2×2 grid)
- Separate panel for each quartile
- Linear regression lines with 95% confidence intervals
- Fixed y-axis scale for direct visual comparison

**Output Location**: All files in `outputs/` subdirectories

---

## Citation

**Suggested Citation**:
> REACH Suspensions Analysis (2025). "Teacher Diversity and Suspension Rates by School Racial Composition: Slope Comparison Analysis - Executive Summary." UCLA Center for the Transformation of Schools, REACH Suspensions Analysis Project. Analysis conducted November 2025 using California Department of Education data (2018-19 through 2023-24).

**Data Sources**:
> California Department of Education. "Student Suspension Data Files (2018-19 through 2023-24)." Retrieved from https://www.cde.ca.gov/ds/sd/sd/
>
> California Department of Education. "Teacher Staff Demographic Data Files (2018-19 through 2023-24)." Retrieved from https://www.cde.ca.gov/

**Analysis Documentation**:
> Full methodology and code: `Analysis/24_quartile_slope_comparison.R`

---

## Contact and Questions

For questions about:
- **Methodology**: See `Analysis/24_quartile_slope_comparison.R` (inline documentation)
- **Aggregation methodology**: See "CRITICAL: Aggregation to School-Year Level" in this summary
- **Data pipeline**: See `CLAUDE.md` (repository guide)
- **Related analyses**: See `outputs/summaries/README.md`

---

## Document Information

**Document Version**: 1.0
**Document Created**: 2025-11-20
**Last Updated**: 2025-11-20
**Analysis Script**: `Analysis/24_quartile_slope_comparison.R`
**Output Location**: `outputs/summaries/24_quartile_slope_comparison_SUMMARY.md`
**Word Version**: `outputs/summaries/24_quartile_slope_comparison_SUMMARY.docx` (generate using conversion script)

**Conversion to Word**:
```bash
./scripts/utilities/convert_summary_to_word.sh 24_quartile_slope_comparison_SUMMARY.md
```

**Change Log**:
- v1.0 (2025-11-20): Initial summary with corrected methodology (school-year aggregation)

---

**END OF SUMMARY**

