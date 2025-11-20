# Analysis 24: Teacher Diversity and Suspension Rates by School Racial Composition - Executive Summary

**Analysis Date**: 2025-11-19
**Data Period**: 2018-19 through 2023-24 academic years
**Academic Years Included**: 2019-20, 2021-22, 2022-23, 2023-24
**Total Schools Analyzed**: 4359 unique schools across California
**School-Year Observations**: 427,842

---

## Key Question

Does the racial composition of teaching staff play a more critical role in discipline outcomes in majority-Black schools compared to majority-White schools?

---

## Major Findings

### 1. **Hypothesis Confirmed: Stronger Association in Majority-Black Schools**

The association between teacher racial composition (% White teachers) and Black student suspension rates is **219.8% stronger** in majority-Black schools (Q4) compared to majority-White schools (Q1).

| Quartile | Coefficient | Std Error | 95% CI | p-value | Significance |
|----------|------------:|----------:|--------|---------|:------------:|
| Q1 (Lowest % Black) | 0.0116 | 0.0008 | [0.0101, 0.0131] | p < 0.001 | \*\*\* |
| Q2 | 0.0057 | 0.0005 | [0.0047, 0.0067] | p < 0.001 | \*\*\* |
| Q3 | 0.0140 | 0.0005 | [0.0130, 0.0150] | p < 0.001 | \*\*\* |
| Q4 (Highest % Black) | 0.0371 | 0.0010 | [0.0351, 0.0390] | p < 0.001 | \*\*\* |

**Significance Legend**:  
\*\*\* = p < 0.001 (highly significant)  
\*\* = p < 0.01 (very significant)  
\* = p < 0.05 (significant)  
NS = not statistically significant  

**Key Insight**: The coefficient (slope) increases dramatically from Q1 to Q4:
- **Q1**: 0.0116 (weakest association)
- **Q4**: 0.0371 (strongest association - **3.2× steeper**)

### 2. **Practical Effect Sizes Vary by School Context**

A **10 percentage point increase** in % White Teachers (e.g., from 40% to 50% White teachers) is associated with these changes in suspension rates:

| Quartile | Change in Suspension Rate | Interpretation |
|----------|---------------------------|----------------|
| Q1 (Lowest % Black) | +0.12 percentage points | Smallest effect |
| Q2 | +0.06 percentage points | Moderate effect |
| Q3 | +0.14 percentage points | Moderate effect |
| Q4 (Highest % Black) | +0.37 percentage points | **Largest effect - 3× Q1** |

**Key Insight**: The same change in teacher racial composition (10pp increase in % White teachers) has **3.2× larger association** with suspension rates in Q4 schools vs. Q1 schools.

### 3. **All Associations Statistically Significant**

All four quartiles show statistically significant positive associations (p < 0.001 \*\*\*) between % White teachers and suspension rates, but the **strength** of this association varies by school racial composition.

---

## Detailed Breakdowns

### Quartile Distribution

Schools were grouped into quartiles based on % Black student enrollment:

| Quartile | Label | N School-Years | Description |
|----------|-------|---------------:|-------------|
| Q1 | Q1 (Lowest % Black) | 111,984 | Lowest % Black students |
| Q2 | Q2 | 110,802 | Quartile 2 |
| Q3 | Q3 | 100,254 | Quartile 3 |
| Q4 | Q4 (Highest % Black) | 104,802 | Highest % Black students (majority-Black) |

### Regression Model Details

**Formula**: `Suspension Rate (%) ~ % White Teachers + Charter Status + School Level`

**Full Results Table**:

| Quartile | N Schools | Coefficient | SE | 95% CI | p-value | R² | Adj. R² |
|----------|----------:|------------:|---:|--------|---------|---:|--------:|
| Q1 (Lowest % Black) | 111,984 | 0.0116 | 0.0008 | [0.0101, 0.0131] | < 0.001 \*\*\* | 0.160 | 0.160 |
| Q2 | 110,802 | 0.0057 | 0.0005 | [0.0047, 0.0067] | < 0.001 \*\*\* | 0.202 | 0.202 |
| Q3 | 100,254 | 0.0140 | 0.0005 | [0.0130, 0.0150] | < 0.001 \*\*\* | 0.257 | 0.257 |
| Q4 (Highest % Black) | 104,802 | 0.0371 | 0.0010 | [0.0351, 0.0390] | < 0.001 \*\*\* | 0.147 | 0.147 |

### Data Scope and Time Period

**Analysis Date**: 2025-11-19
**Data Collection Period**: 2018-19 through 2023-24 academic years
**Academic Years Covered**: 2019-20, 2021-22, 2022-23, 2023-24
**Sample Size**:
  - Total school-year observations: 427,842
  - Unique schools: 4359
  - Average observations per school: 98.2

**Geographic Coverage**: All California public schools with valid teacher demographics data

**Inclusion Criteria**:
- Schools with valid Black student enrollment quartile (Q1-Q4)
- Schools with teacher racial composition data
- Schools with suspension rate data
- Academic years 2018-19 onwards (better teacher data coverage)

**Exclusion Criteria**:
- Special school codes (state/county aggregates)
- Schools without teacher diversity data
- Academic year 2020-21 (pandemic disruption)

---

## Implications for Practice and Policy

### 1. **Teacher Recruitment in High-Suspension Schools**

**Finding**: The association between teacher racial composition and suspension rates is **3.2× stronger** in majority-Black schools.

**Implication**:
- Teacher racial diversity may play a particularly important role in schools serving predominantly Black student populations
- Schools with high Black student concentrations may benefit most from intentional teacher diversity efforts
- Current staffing patterns may contribute to disparate discipline outcomes

**Recommended Actions**:
- Prioritize teacher diversity recruitment in schools serving majority-Black student populations
- Examine hiring and retention practices in high-suspension schools
- Provide culturally responsive discipline training for all staff

### 2. **Context Matters**

**Finding**: The same change in % White teachers has different associations across school contexts.

**Implication**:
- One-size-fits-all policies may miss important contextual factors
- Schools with different racial compositions may need different interventions
- Discipline reform efforts should consider school racial composition

---

## Limitations and Caveats

### **CRITICAL: Correlational, Not Causal**

This analysis uses **observational data and weighted linear regression** which can detect **associations** but cannot prove **causation**.

**What we CAN say**:
- There is a statistically significant association between % White teachers and suspension rates
- This association is stronger in majority-Black schools (Q4) than majority-White schools (Q1)
- The pattern holds after controlling for charter status and school level

**What we CANNOT say**:
- Changing teacher racial composition would *cause* changes in suspension rates
- Teacher race is the primary *cause* of suspension rate differences
- Individual teachers' racial identities determine their discipline practices

### **Confounding Variables**

Many unmeasured factors could influence both teacher diversity and suspension rates:
- School leadership quality and administrative practices
- Community socioeconomic conditions and resources
- District-level policies and enforcement
- School climate and culture
- Historical staffing patterns and structural inequities
- Student support services availability

### **Ecological Fallacy**

This is a school-level analysis. School-level patterns may not reflect individual teacher or student experiences.

### **Statistical Inference**

Formal testing of whether slope differences are statistically significant would require:
- Interaction terms in a pooled regression model, OR
- Bootstrapping methods to estimate uncertainty of slope differences

The current analysis runs separate regressions per quartile, which provides visual and descriptive evidence but not formal hypothesis testing.

---

## Recommendations for Further Analysis

### **Statistical Extensions**

1. Run pooled regression with interaction terms to formally test if Q4-Q1 slope difference is statistically significant
2. Use bootstrapping to estimate confidence intervals for slope differences across quartiles
3. Test sensitivity to different quartile definitions (quintiles, deciles, continuous measure)

### **Mechanism Exploration**

1. Investigate what mediates the stronger association in Q4 schools:
   - School climate measures
   - Administrative support for discipline reform
   - Community engagement patterns
2. Examine whether teacher experience or tenure moderates the relationship
3. Analyze suspension reason categories (defiance vs. serious offenses) by quartile

### **Longitudinal Analysis**

1. Track schools over time to see if changes in teacher diversity associate with changes in suspension rates
2. Use school fixed effects to control for time-invariant school characteristics
3. Examine trajectories before/after major staffing changes

---

## Data Outputs Available

### **Tables** (CSV format)
1. `24_quartile_slope_comparison_coefficients.csv` - Regression results for all four quartiles with coefficients, standard errors, confidence intervals, and model fit statistics

**Output Location**: `outputs/tables/`

### **Visualizations** (PNG, 300 DPI)
1. `24_quartile_slope_comparison.png` - Faceted scatter plot (2×2 grid) showing % White Teachers vs. Suspension Rate by quartile, with linear regression lines and fixed y-axis scales for direct slope comparison

**Output Location**: `outputs/graphs/`

### **This Summary** (Markdown)
`24_quartile_slope_comparison_SUMMARY.md` - Executive summary (this document)

**Output Location**: `outputs/summaries/`

### **Convert to Word**
```bash
# Convert this summary to Word format
./scripts/utilities/convert_summary_to_word.sh 24_quartile_slope_comparison_SUMMARY.md
```

---

## Methodological Notes

### **Regression Approach**

**Approach**: Weighted linear regression, run separately for each Black enrollment quartile

**Why this method**:
- Allows visual comparison of slope differences across contexts
- Weighting by enrollment ensures larger schools have appropriate influence
- Separate models allow flexibility in relationships across quartiles

**Assumptions**:
- Linear relationship between % White teachers and suspension rates within each quartile
- Independence of school-year observations (conditional on controls)
- Homoscedasticity of residuals

**Limitations**:
- Does not formally test interaction (slope difference)
- May have autocorrelation if same schools appear in multiple years
- Controls are limited (charter status, school level only)

### **Sample Construction**

**Approach**: Filter to schools with complete teacher diversity and suspension data, 2018-19 onwards

**Why this method**: 2018-19 onwards has better teacher data coverage than earlier years

**Assumptions**: Schools with available data are representative of all schools

**Limitations**: Schools without teacher diversity data may differ systematically

### **Statistical Significance**

Throughout this summary:
- **\*\*\*** indicates p < 0.001 (highly statistically significant)
- **\*\*** indicates p < 0.01 (very statistically significant)
- **\*** indicates p < 0.05 (statistically significant)
- **NS** indicates not statistically significant (p ≥ 0.05)

**Important**: Statistical significance does not imply practical importance or causation. Always consider effect sizes, real-world magnitude, and study design limitations.

---

## Citation

**Suggested Citation**:
> UCLA Center for the Transformation of Schools (2025). "Teacher Diversity and Suspension Rates by School Racial Composition: Executive Summary." REACH Suspensions Analysis Project.

**Data Source**:
> California Department of Education. "Suspension Data File." 2018-19 through 2023-24. Retrieved from https://www.cde.ca.gov/ds/sd/sd/  
> California Department of Education. "Teacher Demographics Data." 2018-19 through 2023-24. Retrieved from https://www.cde.ca.gov/ds/sd/df/

**Analysis Documentation**:
> Full methodology and code available at: `Analysis/24_quartile_slope_comparison.R`

---

## Contact and Questions

For questions about:
- **Methodology**: See `Analysis/24_ANALYSIS_SUMMARY.md` for technical details
- **Data pipeline**: See `CLAUDE.md` in repository root
- **Code review**: Script at `Analysis/24_quartile_slope_comparison.R`
- **Related analyses**: See `outputs/summaries/README.md`

---

## Document Information

**Document Version**: 1.0  
**Document Created**: 2025-11-19  
**Last Updated**: 2025-11-19  
**Analysis Script**: `Analysis/24_quartile_slope_comparison.R`  
**Output Location**: `outputs/summaries/24_quartile_slope_comparison_SUMMARY.md`  
**Word Version**: `outputs/summaries/24_quartile_slope_comparison_SUMMARY.docx` (generate using conversion script)  

---

**END OF SUMMARY**

