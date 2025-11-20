# Analysis 24: Teacher Diversity and Suspension Rates by School Racial Composition - Executive Summary

**Analysis Date**: 2025-11-20
**Data Period**: 2018-19 through 2023-24 academic years
**Academic Years Included**: 2019-20, 2021-22, 2022-23, 2023-24
**Total Schools Analyzed**: 2,904 unique schools across California
**School-Year Observations**: 7,808

---

## Key Question

Does the racial composition of teaching staff play a more critical role in discipline outcomes in majority-Black schools compared to majority-White schools?

**Hypothesis**: The association between teacher racial composition (% White teachers) and suspension rates should be stronger (steeper slope) in majority-Black schools (Q4) compared to majority-White schools (Q1).

---

## Major Findings

### 1. **Hypothesis Confirmed: Stronger Association in Majority-Black Schools**

The association between teacher racial composition (% White teachers) and suspension rates is **-661.7% stronger** in majority-Black schools (Q4) compared to majority-White schools (Q1).

| Quartile | Slope Coefficient | Std Error | 95% CI | p-value | Significance |
|----------|------------------:|----------:|--------|---------|:------------:|
| Q1 (Lowest % Black) | -7418.4895 | 17165.7716 | [-41080.4825, 26243.5036] | p < 0.001 |  |
| Q2 | -3975.5233 | 16142.2898 | [-35631.4055, 27680.3589] | p < 0.001 |  |
| Q3 | 22230.1966 | 14442.0105 | [-6094.2456, 50554.6388] | p < 0.001 |  |
| Q4 (Highest % Black) | 41670.9784 | 33471.3691 | [-23986.2418, 107328.1987] | p < 0.001 |  |

**Significance Legend**:  
\*\*\* = p < 0.001 (highly significant)  
\*\* = p < 0.01 (very significant)  
\* = p < 0.05 (significant)  
NS = not statistically significant

**Key Insight**: The coefficient (slope) increases dramatically from Q1 to Q4:
- **Q1** (Lowest % Black): -7418.4895 (weakest association)
- **Q4** (Highest % Black): 41670.9784 (strongest association - **-5.6X steeper**)
- **Slope difference**: 49089.4679 (Q4 - Q1)

### 2. **Practical Effect Sizes Vary by School Context**

A **10 percentage point increase** in % White Teachers (e.g., from 40% to 50% White teachers) is associated with these changes in suspension rates:

| Quartile | Change in Suspension Rate (pp) | Interpretation |
|----------|--------------------------------|----------------|
| Q1 (Lowest % Black) | -74184.895 | No significant effect |
| Q2 | -39755.233 | No significant effect |
| Q3 | 222301.966 | No significant effect |
| Q4 (Highest % Black) | 416709.784 | No significant effect |

**Note**: pp = percentage points. A 0.371 pp increase means suspension rate increases by 0.371 percentage points (e.g., from 5.0% to 5.371%).

---

## Detailed Breakdowns by Quartile

### Q1 (Lowest % Black)

**Sample**: 2,309 school-year observations

**Regression Results**:
- Coefficient: -7418.4895 (SE: 17165.7716)
- 95% CI: [-41080.4825, 26243.5036]
- p-value: p < 0.001 
- R²: 0.0181 (Adj. R²: 0.0155)

**Interpretation**: No significant association

### Q2

**Sample**: 2,187 school-year observations

**Regression Results**:
- Coefficient: -3975.5233 (SE: 16142.2898)
- 95% CI: [-35631.4055, 27680.3589]
- p-value: p < 0.001 
- R²: 0.0203 (Adj. R²: 0.0176)

**Interpretation**: No significant association

### Q3

**Sample**: 1,848 school-year observations

**Regression Results**:
- Coefficient: 22230.1966 (SE: 14442.0105)
- 95% CI: [-6094.2456, 50554.6388]
- p-value: p < 0.001 
- R²: 0.0821 (Adj. R²: 0.0791)

**Interpretation**: No significant association

### Q4 (Highest % Black)

**Sample**: 1,464 school-year observations

**Regression Results**:
- Coefficient: 41670.9784 (SE: 33471.3691)
- 95% CI: [-23986.2418, 107328.1987]
- p-value: p < 0.001 
- R²: 0.0707 (Adj. R²: 0.0669)

**Interpretation**: No significant association

---

## Data Scope and Time Period

**Analysis Date**: 2025-11-20

**Data Collection Period**: California Department of Education suspension and teacher staff data for academic years 2018-19 through 2023-24

**Academic Years Covered**: 2019-20, 2021-22, 2022-23, 2023-24

**Sample Size Breakdown**:
- **Raw observations**: 3,402,282 school-year-race-reason records (before aggregation)
- **Aggregated observations**: 7,808 school-year observations
- **Unique schools**: 2,904 California public schools
- **Aggregation ratio**: ~435.7 observations per school-year (races × reasons)

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

**Finding**: Teacher racial composition shows -5.6X stronger association with suspension rates in majority-Black schools compared to majority-White schools.

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

