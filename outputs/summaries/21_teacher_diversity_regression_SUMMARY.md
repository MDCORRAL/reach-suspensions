# Analysis 21: Teacher & Administrator Racial Diversity and Student Suspension Rates - Executive Summary

**Analysis Date**: 2025-11-20
**Data Period**: 2017-18 through 2023-24 academic years
**Academic Years Included**: 2017-18, 2018-19, 2019-20, 2021-22, 2022-23, 2023-24 (6 years; 2020-21 excluded due to COVID)
**Total Observations**: 515,947 school-year-race combinations (aggregated from 3,402,282 reason-level records)
**Total Unique Schools**: Approximately 21,700 California public schools

---

## Key Question

Is teacher and administrator racial diversity (proportion of non-white staff) associated with student suspension rates, and do these associations vary by student racial/ethnic group?

---

## Major Findings

### 1. **Teacher Diversity Shows Small but Consistent Negative Associations**

Teacher racial diversity (proportion of non-white teachers) is associated with lower suspension rates for several major student groups:

| Student Group | Effect (10pp diversity increase) | 95% Confidence Interval | Sample Size | Significance |
|---------------|----------------------------------|------------------------|-------------|--------------|
| **Black/African American** | -0.35 pp | [-0.44, -0.25] | 11,959 | p < 0.001 \*\*\* |
| **Hispanic/Latino** | -0.18 pp | [-0.20, -0.16] | 21,706 | p < 0.001 \*\*\* |
| **White** | -0.10 pp | [-0.14, -0.07] | 17,019 | p < 0.001 \*\*\* |
| **Filipino** | -0.04 pp | [-0.06, -0.02] | 6,644 | p < 0.001 \*\*\* |
| American Indian/Alaska Native | -0.16 pp | [-0.46, +0.14] | 1,116 | NS (p=0.300) |
| Asian | -0.01 pp | [-0.02, +0.01] | 11,460 | NS (p=0.333) |
| Native Hawaiian/Pacific Islander | -0.17 pp | [-0.37, +0.04] | 1,084 | NS (p=0.110) |
| Two or More Races | +0.05 pp | [-0.01, +0.10] | 11,578 | NS (p=0.092) |

**Significance Legend**:
- \*\*\* = p < 0.001 (highly statistically significant)
- \*\* = p < 0.01 (very statistically significant)
- \* = p < 0.05 (statistically significant)
- NS = not statistically significant (p ≥ 0.05)

**Note**: Effect sizes shown for a 10 percentage point increase in diversity (e.g., from 40% to 50% non-white teachers). "pp" = percentage points.

**Key Insight**: Teacher diversity shows statistically significant associations with lower suspension rates for the four largest student groups (Black, Hispanic, White, Filipino), accounting for ~73,000 school-year-race observations. Effects are small but consistent in direction.

### 2. **Administrator Diversity Shows Limited Associations**

Administrator racial diversity (proportion of non-white administrators) shows fewer consistent associations:

| Student Group | Effect (10pp diversity increase) | Sample Size | Significance |
|---------------|----------------------------------|-------------|--------------|
| **Black/African American** | -0.40 pp | 11,959 | p = 0.011 \* |
| **Asian** | +0.05 pp | 11,460 | p = 0.018 \* |
| Hispanic/Latino | -0.05 pp | 21,706 | NS (p=0.202) |
| White | +0.01 pp | 17,019 | NS (p=0.827) |
| Filipino | -0.02 pp | 6,644 | NS (p=0.597) |
| American Indian/Alaska Native | -0.03 pp | 1,116 | NS (p=0.953) |
| Native Hawaiian/Pacific Islander | +0.27 pp | 1,084 | NS (p=0.441) |
| Two or More Races | +0.14 pp | 11,578 | NS (p=0.090) |

**Key Insight**: Administrator diversity shows significant associations for only 2 of 8 groups. The positive association for Asian students (higher diversity → slightly higher suspension rates) is unexpected and may reflect confounding factors.

### 3. **Effect Sizes Are Small Compared to Structural Factors**

To contextualize these findings, control variables in the model show much larger associations:

**Example: Black/African American Students**
- **Teacher diversity** (40% → 50%): -0.35 pp effect
- **Charter school status**: -5.5 pp effect (16× larger)
- **Middle vs Elementary school**: +10.0 pp effect (29× larger)

**Interpretation**: While teacher diversity shows statistically significant associations, structural factors (school type, grade level) have substantially larger associations with suspension rates. A baseline suspension rate of 5% would decrease to 4.65% with a 10pp diversity increase, representing a 7% relative reduction.

---

## Detailed Breakdowns by Student Group

### Black/African American Students

**Sample**: 11,959 school-year-race combinations

**Regression Statistics**:
- R² = 0.147 (model explains 14.7% of variation)
- Highly significant overall model (p < 0.001)
- 58.9% of observations have <50 Black students enrolled

**Teacher Diversity**:
- Coefficient: -0.0345
- 95% CI: [-0.044, -0.025]
- p < 0.001 \*\*\* (highly significant)
- Effect: 10pp diversity increase → 0.35pp decrease in suspension rate

**Administrator Diversity**:
- Coefficient: -0.0398
- 95% CI: [-0.071, -0.009]
- p = 0.011 \* (significant)
- Effect: 10pp diversity increase → 0.40pp decrease in suspension rate

**Interpretation**: Both teacher and administrator diversity show significant negative associations with Black student suspension rates. This is the strongest evidence of diversity associations in the data.

### Hispanic/Latino Students

**Sample**: 21,706 school-year-race combinations (largest group)

**Regression Statistics**:
- R² = 0.182 (highest explanatory power)
- Only 8.2% of observations have <50 Hispanic/Latino students

**Teacher Diversity**:
- Coefficient: -0.0179
- 95% CI: [-0.020, -0.016]
- p < 0.001 \*\*\*
- Effect: -0.18pp per 10pp diversity increase

**Administrator Diversity**:
- Coefficient: -0.0052
- 95% CI: [-0.013, +0.003]
- p = 0.202 (not significant)

**Interpretation**: Teacher diversity shows significant association; administrator diversity does not.

### White Students

**Sample**: 17,019 school-year-race combinations

**Regression Statistics**:
- R² = 0.091
- 36.8% of observations have <50 White students

**Teacher Diversity**:
- Coefficient: -0.0104
- 95% CI: [-0.014, -0.007]
- p < 0.001 \*\*\*
- Effect: -0.10pp per 10pp diversity increase

**Administrator Diversity**:
- Coefficient: +0.0012
- 95% CI: [-0.009, +0.012]
- p = 0.827 (not significant)

**Interpretation**: Teacher diversity shows small but significant negative association; administrator diversity shows no association.

### Asian Students

**Sample**: 11,460 school-year-race combinations

**Regression Statistics**:
- R² = 0.092
- Very low baseline suspension rates (~0.3%)
- 49.6% of observations have <50 Asian students

**Teacher Diversity**:
- Coefficient: -0.0006
- p = 0.333 (not significant)

**Administrator Diversity**:
- Coefficient: +0.0045
- 95% CI: [+0.001, +0.008]
- p = 0.018 \*
- Effect: +0.05pp per 10pp diversity increase

**Interpretation**: Teacher diversity shows no association. Administrator diversity shows unexpected positive association (higher diversity → slightly higher suspension rates), possibly due to confounding factors.

### Filipino Students

**Sample**: 6,644 school-year-race combinations

**Regression Statistics**:
- R² = 0.079
- 80.5% of observations have <50 Filipino students

**Teacher Diversity**:
- Coefficient: -0.0038
- 95% CI: [-0.006, -0.002]
- p < 0.001 \*\*\*
- Effect: -0.04pp per 10pp diversity increase

**Administrator Diversity**:
- Not statistically significant (p = 0.597)

**Interpretation**: Small negative teacher diversity association; no administrator diversity effect.

### Smaller Sample Groups

**American Indian/Alaska Native** (N = 1,116):
- 89.6% have <50 students
- No significant associations detected (limited statistical power)

**Native Hawaiian/Pacific Islander** (N = 1,084):
- 95.6% have <50 students
- No significant associations detected (limited statistical power)

**Two or More Races** (N = 11,578):
- 69.7% have <50 students
- No significant associations detected

---

## Data Scope and Time Period

**Analysis Date**: 2025-11-20

**Data Collection Period**: California Department of Education suspension and teacher staff data for academic years 2017-18 through 2023-24

**Academic Years Covered**:
- 2017-18, 2018-19, 2019-20, 2021-22, 2022-23, 2023-24
- **Total: 6 academic years**
- 2020-21 excluded due to COVID-19 disruptions

**Sample Size Breakdown**:
- **Raw observations**: 3,402,282 school-year-race-reason records
- **Aggregated observations**: 515,947 school-year-race combinations
- **Unique schools**: Approximately 21,700 California public schools
- **Student groups analyzed**: 8 racial/ethnic groups
- **Average reasons per school-year-race**: 6.6 (defiance, violence, drugs, etc.)

**What Each "Observation" Represents**:
- One **school** (identified by 14-digit CDS code)
- In one **academic year** (e.g., 2023-24)
- For one **student racial/ethnic group** (e.g., Black/African American)
- **Aggregated across all suspension reasons** (to avoid clustering issues)

**Geographic Coverage**: All California public schools reporting complete data

**Inclusion Criteria**:
- Non-missing teacher diversity measure
- Non-missing administrator diversity measure
- Non-missing suspension rate
- Positive student enrollment (>0) for the racial group

**Exclusion Criteria**:
- Schools missing teacher demographic data
- Student groups with zero enrollment
- 2020-21 academic year

---

## Methodological Notes

### **CRITICAL: Aggregation to School-Year-Race Level (Methodological Fix, 2025-11-20)**

**Problem Identified**: Raw CDE data is reported at **school-year-race-reason** level (6 suspension reasons per school-year-race combination). Running regressions on this data treats 6 clustered observations as independent, which:
- Underestimates standard errors by ~√6 ≈ 2.45×
- Produces artificially small p-values
- Leads to anti-conservative inference (too many "significant" findings)

**Solution Implemented**: Before regression, data are aggregated to **school-year-race level** by:
- Summing total suspensions across all reason categories
- Taking first value of school-level variables (teacher diversity, charter status, enrollment)
- Recalculating suspension rates

**Impact**:
- Reduced observations: 3,402,282 → 515,947
- Standard errors now appropriate for unit of analysis
- P-values and confidence intervals now valid

This fix was implemented on 2025-11-20 and is documented in `Analysis/21_teacher_diversity_regression.R` (lines 214-294).

### Regression Model

**Formula**:
```
Suspension Rate ~ Teacher Non-White Share + Admin Non-White Share +
                  Charter Status + School Level
```

**Key Features**:
- **Weighted least squares**: Schools weighted by student enrollment
- **Controls**: Charter status (binary), School level (5 categories: Elementary, Middle, High, Other, Alternative)
- **Stratified**: Separate regressions for each student racial/ethnic group
- **Diversity measurement**: Proportion of non-white staff (0-1 scale)

### Diversity Measure Construction

**Teacher Racial Diversity**:
- Sum of 84 non-white race share columns
- Includes: African American, Asian, Filipino, Hispanic/Latino, American Indian/Alaska Native, Native Hawaiian/Pacific Islander, Pacific Islander (legacy), Two or More Races
- Excludes: White, Not Reported
- Data source: CDE teacher staff files (stre*.txt)

**Administrator Racial Diversity**:
- Sum of 14 non-white administrator race share columns
- Uses `teacher_*_by_type_administrators_*_share` columns
- Same race categories as teacher measure

**Validation**: Script explicitly verifies RACE measures are used (not gender diversity) and matches canonical `TEACHER_RACE_SLUGS` pattern.

### Statistical Significance

Throughout this summary:
- **\*\*\*** indicates p < 0.001 (highly statistically significant)
- **\*\*** indicates p < 0.01 (very statistically significant)
- **\*** indicates p < 0.05 (statistically significant)
- **NS** indicates not statistically significant (p ≥ 0.05)

**Important**: Statistical significance does not imply practical importance. With large sample sizes (1,084 to 21,706 observations per group), even small effects reach statistical significance.

### Model Fit

- R² ranges from 0.079 (Filipino) to 0.199 (American Indian/Alaska Native)
- Most models explain 8-20% of suspension rate variation
- 80-92% of variation remains unexplained
- Control variables (charter status, school level) often show larger effects than diversity measures

---

## Implications for Practice and Policy

### 1. **Staff Diversity: Valuable But Not a "Silver Bullet"**

**Finding**: Teacher diversity shows statistically significant associations with lower suspension rates for major student groups, but effect sizes are small.

**Implication**:
- Diversifying teaching staff is valuable for many reasons:
  - Role models and representation
  - Cultural competency and understanding
  - Community connections
- **However**, diversity alone is unlikely to substantially reduce suspension disparities
- Do not expect measurable suspension rate changes solely from hiring more diverse staff
- Combine diversity initiatives with evidence-based discipline reforms

**Recommended Actions**:
- Continue efforts to recruit and retain diverse educators
- Provide cultural responsiveness training for ALL staff (regardless of race)
- Implement restorative justice practices
- Revise disciplinary codes to reduce subjective infractions

### 2. **Prioritize Structural and Policy Interventions**

**Finding**: Charter status and school level show associations 15-30× larger than staff diversity.

**Implication**:
- School structure, policies, and disciplinary frameworks have larger associations with suspension outcomes
- Resource allocation should prioritize:
  - Policy changes (alternative consequences, restorative justice)
  - Training programs (implicit bias, de-escalation, trauma-informed practices)
  - Support systems (counseling, mentoring, behavioral interventions)
  - Leadership development focused on equitable discipline

**Examples of High-Impact Interventions** (from research literature):
- Restorative justice circles
- Positive Behavioral Interventions and Supports (PBIS)
- Social-emotional learning curricula
- Administrator training in equitable discipline
- Clear, objective discipline codes (reducing subjectivity)

### 3. **Context-Specific Approaches Needed**

**Finding**: Associations vary substantially by student racial/ethnic group (significant for some, not others).

**Implication**:
- One-size-fits-all diversity initiatives unlikely to work uniformly
- Schools should consider:
  - Which student groups face highest suspension rates?
  - What is the local community racial/ethnic composition?
  - What disciplinary practices are currently in use?
  - Who makes suspension decisions (teachers, administrators, both)?

**Recommended Actions**:
- Conduct local data analysis before implementing interventions
- Engage with affected communities to understand context
- Pilot interventions and evaluate effectiveness locally
- Adapt strategies based on student population needs

### 4. **Beware of Paradoxical Findings and Confounding**

**Finding**: Some groups show unexpected positive associations (e.g., Asian students: higher admin diversity → higher suspension rates).

**Interpretation**: These are **not causal effects**. Likely explanations:
- Schools with diverse administrators may be in communities with existing discipline challenges
- Districts may hire diverse administrators specifically to address problem schools (reverse causation)
- Geographic/regional factors not captured in model
- Unmeasured confounders (school culture, leadership quality, resources)

**Implication**: Do NOT conclude that diverse administrators cause higher suspension rates. This demonstrates the limitation of observational data and the need for causal inference methods.

---

## Limitations and Caveats

### **CRITICAL: Correlational, Not Causal**

This analysis uses **observational data** and **linear regression**, which can detect **associations** but cannot prove **causation**.

**What we CAN say**:
- There are small statistical associations between staff racial diversity and suspension rates
- These associations vary by student racial/ethnic group
- Effect sizes are small compared to structural factors
- Results hold after controlling for charter status and school level

**What we CANNOT say**:
- Staff diversity "causes" changes in suspension rates
- Increasing diversity will reduce suspensions
- The direction of causality (diversity → suspensions vs. suspensions → hiring diversity)
- That confounding factors are not responsible for observed associations

**Why Causal Inference is Limited**:

1. **No random assignment**: Schools are not randomly assigned to have diverse staff
2. **Unmeasured confounders**: Many factors not controlled:
   - School leadership quality and experience
   - Community socioeconomic context
   - School resources and funding levels
   - Historical disciplinary culture
   - Teacher experience and training
   - School climate and culture
   - District policies
   - Neighborhood characteristics
3. **Selection effects**:
   - Diverse staff may choose schools with specific characteristics
   - Districts may hire diverse staff for struggling schools
   - Geographic clustering (diverse staff in diverse communities)
4. **Ecological fallacy**: School-level analysis cannot identify individual teacher effects
5. **Reverse causation**: High-suspension schools may hire diverse staff to address problems

### Measurement Limitations

**Staff Diversity**:
- Uses proportion non-white as single metric
- Does not capture:
  - Specific racial/ethnic match between staff and students
  - Cultural competency or training
  - Teacher quality or effectiveness
  - Years of experience
  - Whether staff are new hires or long-tenured

**Suspension Rates**:
- Aggregate measure, does not distinguish:
  - In-school vs. out-of-school suspensions
  - Suspension length
  - Subjective vs. objective infractions
  - Repeat vs. first-time suspensions
  - Whether alternative consequences were available

**Missing Controls**:
- No measures of:
  - School climate or culture
  - Restorative justice implementation
  - Teacher-student relationships
  - District-level policies
  - Family engagement
  - Community resources

### Statistical Limitations

1. **Cross-sectional design**:
   - Cannot assess whether increasing diversity over time changes outcomes
   - Cannot separate school fixed effects from time-varying factors
   - Combines multiple years into pooled analysis

2. **Large samples, small effects**:
   - Sample sizes of 1,000-22,000 per group
   - Very small effects reach statistical significance
   - Statistical significance ≠ practical importance

3. **Model specification**:
   - Linear model may miss non-linear relationships
   - May miss interaction effects (e.g., diversity effect varies by school composition)
   - Low R² (8-20%) indicates substantial unexplained variation

4. **Multiple testing**:
   - Testing 8 groups × 2 predictors = 16 hypothesis tests
   - Expected false positives: 16 × 0.05 = 0.8
   - Bonferroni correction (p < 0.003) would eliminate marginal findings

### Scope Limitations

1. **Geographic**: California only, may not generalize to other states
2. **School type**: Public schools only, excludes private schools
3. **Time period**: 2017-18 through 2023-24, reflects specific policy era
4. **Student groups**:
   - Small groups (American Indian, Pacific Islander) lack statistical power
   - Groups with low baseline rates (Asian, Filipino) show minimal variation
   - "Two or More Races" is heterogeneous category

---

## Recommendations for Further Analysis

### Causal Inference Methods

To move beyond correlation:

1. **Difference-in-differences**: Track schools that increase diversity over time vs. stable schools
2. **Instrumental variables**: Use policy changes or demographic shifts as instruments
3. **Regression discontinuity**: Examine schools just above/below diversity thresholds
4. **Propensity score matching**: Compare similar schools with different diversity levels
5. **Randomized controlled trials**: Randomly assign diversity interventions (e.g., recruitment programs)

### Mechanism Exploration

To understand *how* diversity might matter:

1. **Teacher-student race match**: Examine whether having same-race teachers affects outcomes
2. **Cultural competency**: Measure and control for training, culturally responsive teaching
3. **School climate surveys**: Survey students and staff on inclusivity, belonging, fairness
4. **Discipline decision pathways**: Identify who makes referral and suspension decisions
5. **Qualitative research**: Interview teachers, administrators, students about diversity's role

### Interaction Effects

Test whether diversity effects vary by:

1. **School composition**: Does diversity matter more in diverse vs. homogeneous schools?
2. **Community context**: Urban vs. rural, high vs. low poverty, segregated vs. integrated
3. **School size**: Large vs. small schools
4. **Policy environment**: Schools with restorative justice vs. traditional discipline codes
5. **Leadership quality**: Strong vs. weak principal leadership

### Longitudinal Studies

Track schools over time:

1. **Panel data models**: Control for school fixed effects (unobserved time-invariant factors)
2. **Teacher turnover analysis**: Examine impact of diversity changes within schools
3. **Student cohort tracking**: Follow same students across grades with different teachers
4. **Dynamic models**: Assess lagged effects (does diversity take time to affect culture?)

---

## Data Outputs Available

### **Excel Workbook**
`outputs/teacher_diversity_analysis/teacher_diversity_regression_results.xlsx`
- **Sheet 1 - Summary**: Coefficients, confidence intervals, p-values, effect sizes for all groups
- **Sheet 2 - Interpretations**: Plain-language interpretations for each student group
- **Sheet 3 - Technical_Details**: Full regression output statistics

### **CSV Tables**
1. `teacher_diversity_summary.csv` - Main results table
2. `teacher_diversity_interpretations.csv` - Plain-language findings

### **Visualizations** (PNG, 300 DPI)
1. `teacher_diversity_coefficients_forest_plot.png` - Forest plot with 95% confidence intervals
2. `teacher_diversity_practical_effects.png` - Bar chart of effect sizes (percentage point changes)

**Output Location**: All files located in `outputs/teacher_diversity_analysis/`

---

## Citation

**Suggested Citation**:
> REACH Suspensions Analysis (2025). "Teacher and Administrator Racial Diversity and Student Suspension Rates: Regression Analysis by Student Race/Ethnicity - Executive Summary." UCLA Center for the Transformation of Schools, REACH Suspensions Analysis Project. Analysis conducted November 2025 using California Department of Education data (2017-18 through 2023-24).

**Data Sources**:
> California Department of Education. "Student Suspension Data Files (2017-18 through 2023-24)." Retrieved from https://www.cde.ca.gov/ds/sd/sd/
>
> California Department of Education. "Teacher Staff Demographic Data Files (2017-18 through 2023-24)." Retrieved from https://www.cde.ca.gov/

**Analysis Documentation**:
> Full methodology and code: `Analysis/21_teacher_diversity_regression.R`
>
> Diagnostic tool: `scripts/diagnostics/investigate_sample_sizes.R`
>
> Detailed technical summary: `Analysis/21_teacher_diversity_regression_SUMMARY.md` (in `Analysis/` folder, comprehensive technical documentation)

---

## Contact and Questions

For questions about:
- **Methodology**: See `Analysis/21_teacher_diversity_regression.R` (inline documentation)
- **Aggregation methodology**: See technical notes in this summary (Section: "CRITICAL: Aggregation to School-Year-Race Level")
- **Data pipeline**: See `CLAUDE.md` (repository guide)
- **Teacher data setup**: See `docs/guides/TEACHER_DATA_SETUP_GUIDE.md`
- **Related analyses**: See `outputs/summaries/README.md`

---

## Document Information

**Document Version**: 3.0 (Corrected Methodology)
**Document Created**: 2025-11-19 (v1.0)
**Last Updated**: 2025-11-20 (v3.0 - implemented aggregation fix)
**Analysis Script**: `Analysis/21_teacher_diversity_regression.R`
**Output Location**: `outputs/summaries/21_teacher_diversity_regression_SUMMARY.md`
**Word Version**: `outputs/summaries/21_teacher_diversity_regression_SUMMARY.docx` (generate using conversion script)

**Conversion to Word**:
```bash
./scripts/utilities/convert_summary_to_word.sh 21_teacher_diversity_regression_SUMMARY.md
```

**Change Log**:
- v3.0 (2025-11-20): **MAJOR UPDATE** - Implemented aggregation to school-year-race level to properly handle clustering. Updated all sample sizes, effect sizes, and standard errors. Results now reflect corrected methodology.
- v2.0 (2025-11-19): Updated with explicit academic years, escaped significance markers, enhanced metadata
- v1.0 (2025-11-19): Initial summary created

**Methodological Note**: Version 3.0 (2025-11-20) represents a significant methodological improvement. Previous versions (v1.0, v2.0) ran regressions on reason-level data, which treated clustered observations as independent and resulted in underestimated standard errors. Version 3.0 aggregates to school-year-race level before regression, ensuring valid statistical inference. Effect size estimates are similar, but standard errors and p-values are now correct.

---

**END OF SUMMARY**
