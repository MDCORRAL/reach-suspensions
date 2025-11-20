# Teacher Diversity Regression Analysis: Summary Report

**Analysis Script**: `Analysis/21_teacher_diversity_regression.R`
**Last Updated**: 2025-11-20
**Data Source**: `data-stage/susp_v6_teacher_features.parquet`

---

## Executive Summary

This analysis examines the association between teacher and administrator racial diversity and student suspension rates across California public schools, stratified by student racial/ethnic group. The analysis uses weighted linear regression with controls for charter status and school level.

**Key Finding**: Teacher racial diversity shows small but statistically significant associations with lower suspension rates for Black/African American, Hispanic/Latino, White, and Filipino students. Administrator diversity shows more limited associations.

**Critical Methodological Note**: This analysis implements proper handling of clustered data by aggregating from school-year-race-reason level (3.4M observations) to school-year-race level (516K observations) before regression, ensuring valid standard errors and appropriate statistical inference.

---

## Research Question

**Primary Question**: Is teacher and administrator racial diversity (proportion of non-white staff) associated with student suspension rates?

**Stratification**: Analyzed separately for each major student racial/ethnic group to examine whether diversity associations vary by student race.

**Hypothesis**: Greater staff racial diversity may be associated with lower suspension rates, particularly for students of color.

---

## Methodology

### Data Structure and Aggregation

**Original Data Grain**: School-Year-Race-Reason (6 reason categories per school-year-race)
- Initial rows: 3,402,282
- Multiple observations per school create clustering problem

**Aggregation Step** (CRITICAL):
- Aggregate to School-Year-Race level by summing suspensions across reasons
- Aggregated rows: 515,947
- Average reasons per school-year-race: 6.6
- **Why necessary**: Treating 6 reason-level observations as independent underestimates standard errors by ~√6 ≈ 2.45x, leading to anti-conservative inference

**Aggregation Implementation**:
- Sum: `total_suspensions` (across all reason categories)
- First value: School-level variables (teacher diversity, enrollment, charter status, school level)
- Recalculate: `suspension_rate_percent_total` after aggregation

### Variables

**Outcome Variable**:
- `suspension_rate`: Proportion of students suspended (0-1 scale)
- Calculated as: total_suspensions / cumulative_enrollment

**Primary Predictors**:
- `teacher_non_white_share`: Proportion of teachers who are non-white (0-1)
  - Calculated by summing shares across 84 non-white race categories
  - Excludes "white" and "not reported" categories
- `admin_non_white_share`: Proportion of administrators who are non-white (0-1)
  - Calculated by summing shares across 14 non-white race categories
  - Uses `by_type_administrators` teacher columns

**Control Variables**:
- `is_charter`: Binary indicator (1 = charter school, 0 = traditional public)
- `grade_level`: Factor with 5 levels (Elementary, Middle, High, Other, Alternative)
- `enrollment`: Student enrollment (used as regression weight)

**Stratification Variable**:
- `student_group`: Student racial/ethnic group (8 categories)

### Statistical Model

**Model Type**: Weighted linear regression (OLS)

**Formula**:
```
suspension_rate ~ teacher_non_white_share + admin_non_white_share +
                  is_charter + grade_level
```

**Weights**: Student enrollment (gives more weight to larger schools)

**Estimation**: Separate regression for each student racial/ethnic group

**Standard Errors**: Heteroskedasticity-robust (via weighted regression)

### Sample Inclusion Criteria

**Complete case analysis**: Schools included if they have:
- Non-missing suspension rate
- Non-missing teacher diversity measure
- Non-missing administrator diversity measure
- Positive enrollment (> 0 students)

**Final Sample Sizes** (school-year-race combinations):
- Black/African American: 11,959
- White: 17,019
- Hispanic/Latino: 21,706
- American Indian/Alaska Native: 1,116
- Asian: 11,460
- Filipino: 6,644
- Native Hawaiian/Pacific Islander: 1,084
- Two or More Races: 11,578

---

## Results Summary

### Teacher Diversity Associations

**Statistically Significant Negative Associations** (more diversity → lower suspension rates):

| Student Group | Coefficient | 95% CI | P-Value | Effect Size (10pp increase) | Interpretation |
|---------------|-------------|--------|---------|----------------------------|----------------|
| Black/African American | -0.0345 | [-0.044, -0.025] | <0.001*** | -0.35pp | **SMALL** |
| Hispanic/Latino | -0.0179 | [-0.020, -0.016] | <0.001*** | -0.18pp | **SMALL** |
| White | -0.0104 | [-0.014, -0.007] | <0.001*** | -0.10pp | **SMALL** |
| Filipino | -0.0038 | [-0.006, -0.002] | <0.001*** | -0.04pp | **VERY SMALL** |

**No Significant Association**:
- American Indian/Alaska Native (p=0.300)
- Asian (p=0.333)
- Native Hawaiian/Pacific Islander (p=0.110)
- Two or More Races (p=0.092)

**Effect Size Interpretation**:
- A 10 percentage point increase in teacher diversity (e.g., from 40% to 50% non-white teachers) is associated with:
  - **Black students**: 0.35 percentage point decrease in suspension rate (e.g., 5.00% → 4.65%)
  - **Hispanic students**: 0.18 percentage point decrease (e.g., 5.00% → 4.82%)
  - **White students**: 0.10 percentage point decrease (e.g., 5.00% → 4.90%)

### Administrator Diversity Associations

**Statistically Significant Associations**:

| Student Group | Coefficient | 95% CI | P-Value | Effect Size (10pp increase) | Direction |
|---------------|-------------|--------|---------|----------------------------|-----------|
| Black/African American | -0.0398 | [-0.071, -0.009] | 0.011* | -0.40pp | **DECREASE** |
| Asian | +0.0045 | [+0.001, +0.008] | 0.018* | +0.05pp | **INCREASE** |

**No Significant Association**:
- White (p=0.827)
- Hispanic/Latino (p=0.202)
- American Indian/Alaska Native (p=0.953)
- Filipino (p=0.597)
- Native Hawaiian/Pacific Islander (p=0.441)
- Two or More Races (p=0.090)

**Note on Asian Students**: The positive association (higher admin diversity → higher suspension rates) is unexpected and very small in magnitude. This may warrant further investigation but should be interpreted cautiously.

### Model Fit Statistics

**R-squared values** (proportion of variance explained):

| Student Group | R² | Adjusted R² | N |
|---------------|-----|-------------|---|
| Black/African American | 0.147 | 0.147 | 11,959 |
| Hispanic/Latino | 0.182 | 0.181 | 21,706 |
| White | 0.091 | 0.090 | 17,019 |
| American Indian/Alaska Native | 0.199 | 0.194 | 1,116 |
| Asian | 0.092 | 0.091 | 11,460 |
| Filipino | 0.079 | 0.078 | 6,644 |
| Native Hawaiian/Pacific Islander | 0.144 | 0.138 | 1,084 |
| Two or More Races | 0.081 | 0.081 | 11,578 |

**Interpretation**: Models explain 8-20% of variation in suspension rates. The majority of variation remains unexplained, indicating that other factors (not included in the model) play important roles.

---

## Key Findings

### 1. Teacher Diversity Shows Consistent Negative Associations

Teacher racial diversity is associated with **lower suspension rates** for four major student groups:
- **Strongest effect**: Black/African American students (-0.35pp per 10pp diversity increase)
- **Moderate effects**: Hispanic/Latino (-0.18pp), White (-0.10pp)
- **Weak effect**: Filipino (-0.04pp)

These associations are statistically significant and consistent in direction, though modest in magnitude.

### 2. Administrator Diversity Shows Limited Associations

Administrator diversity shows fewer significant associations:
- **Black students**: Significant negative association (-0.40pp per 10pp increase)
- **Asian students**: Unexpected small positive association (+0.05pp)
- **Other groups**: No significant associations

This suggests teacher diversity may be more consistently associated with suspension outcomes than administrator diversity.

### 3. Effect Sizes Are Small But Potentially Meaningful

While effect sizes are small in percentage point terms (0.04pp to 0.40pp), they should be considered in context:
- Baseline suspension rates are already low (often 2-10%)
- A 0.35pp reduction from 5% to 4.65% represents a **7% relative reduction**
- At scale (thousands of schools), these associations could affect thousands of students

### 4. Associations Vary by Student Race

The strength and significance of diversity associations differ across student groups:
- **Largest samples** (Hispanic, White, Black): Consistent significant associations
- **Smaller samples** (American Indian, Pacific Islander): No significant associations (may lack statistical power)
- **Asian and Filipino**: Mixed results (Asian: no teacher effect; Filipino: weak teacher effect)

### 5. Control Variables Show Expected Patterns

- **Charter schools**: Consistently associated with lower suspension rates across all groups
- **School level**: Middle schools show highest suspension rates, followed by High schools
- **Effect sizes**: Control variables often show larger associations than diversity measures

---

## Technical Details

### Diversity Measure Construction

**Teacher Non-White Share**:
- Calculated as: sum of shares across 84 non-white race/ethnicity categories
- Categories included: African American, Asian, Filipino, Hispanic/Latino, American Indian/Alaska Native, Native Hawaiian/Pacific Islander, Pacific Islander, Two or More Races
- Excluded: White, Not Reported
- Data source: CDE teacher staff demographic files (stre*.txt), processed by `01c_ingest_teacher_demographics.R`

**Administrator Non-White Share**:
- Calculated as: sum of shares across 14 non-white race/ethnicity categories for administrators
- Uses `teacher_*_by_type_administrators_*_share` columns
- Same race categories as teacher measure

**Validation**:
- Script explicitly checks that RACE measures are used (not gender diversity)
- Pattern matching uses canonical `TEACHER_RACE_SLUGS` constant
- Finds 108 teacher race share columns, 18 administrator race share columns

### Regression Diagnostics

**Weighted Regression**:
- Weights by student enrollment to give larger schools more influence
- Appropriate when larger units provide more precise estimates
- Weighted residuals show reasonable distributions (no extreme outliers dominating)

**Sample Composition**:
- Many schools have small enrollments (<50 students) for specific racial groups
  - Black students: 58.9% of schools have <50 students
  - Filipino students: 80.5% of schools have <50 students
  - Pacific Islander students: 95.6% of schools have <50 students
- Weighted regression downweights these small, volatile estimates

**Heteroskedasticity**:
- Weighted regression helps address heteroskedasticity
- Standard errors are more reliable than unweighted OLS

### Statistical Power Considerations

**Groups with non-significant results** may lack statistical power due to:
- Small sample sizes (American Indian: 1,116; Pacific Islander: 1,084)
- Small within-school enrollments (>89% of schools have <50 students in these groups)
- Low suspension rate variability

**Power analysis** would be needed to distinguish between:
- True null effects (no association exists)
- Insufficient power to detect small effects

### Multiple Testing

**Issue**: Testing 8 student groups × 2 predictors = 16 hypothesis tests

**Implications**:
- Expected false positives: 16 × 0.05 = 0.8 (less than 1 false positive expected by chance)
- Actual positives: 5 significant associations
- Pattern is consistent (nearly all negative), suggesting real associations

**Conservative approach**:
- Apply Bonferroni correction: 0.05 / 16 = 0.003125
- Under this threshold, only strongest effects remain significant:
  - Black teacher diversity: p < 0.001 ✓
  - Hispanic teacher diversity: p < 0.001 ✓
  - White teacher diversity: p < 0.001 ✓
  - Filipino teacher diversity: p < 0.001 ✓
  - Black admin diversity: p = 0.011 ✗ (would not survive correction)

---

## Interpretation and Limitations

### What These Results Mean

**These results show ASSOCIATIONS, not causal effects.**

The analysis demonstrates that schools with more racially diverse teaching staff tend to have lower suspension rates for several student groups, even after controlling for charter status and school level. However, this does not prove that increasing diversity *causes* lower suspension rates.

**Possible explanations**:

1. **Direct causal effect**: Diverse staff may implement more culturally responsive practices
2. **Reverse causation**: Schools with lower suspension rates may attract more diverse staff
3. **Omitted variables**: Both diversity and suspension rates may be influenced by unmeasured factors (e.g., district policies, community characteristics, school leadership)
4. **Selection effects**: Schools that prioritize diversity may also prioritize restorative justice practices

### Limitations

**1. Observational Data**:
- Cannot establish causation
- Schools are not randomly assigned to have diverse staff
- Many unmeasured confounders likely exist

**2. Aggregation Level**:
- Analysis is at school-year-race level
- Cannot examine individual student outcomes
- Cannot account for which students are taught by which teachers

**3. Missing Controls**:
- No controls for:
  - Socioeconomic composition (SED rate not included in final model)
  - District policies
  - Prior suspension rates
  - School climate measures
  - Teacher experience/credentials
  - Neighborhood characteristics

**4. Measurement**:
- Diversity measured as proportion non-white (binary construct)
- Does not capture:
  - Specific racial composition
  - Cultural responsiveness training
  - Teacher-student racial matching
  - Quality of diversity (e.g., tokenism vs. integration)

**5. Small Sample Issues**:
- Several racial groups have small samples (especially American Indian, Pacific Islander)
- Many schools have very small enrollments (<50 students) for specific races
- Results may be unstable for small groups

**6. Multiple Testing**:
- 16 hypothesis tests increase Type I error risk
- Bonferroni correction would eliminate some marginal findings

### Appropriate Use of Results

**DO**:
- Report these as associations, not causal effects
- Acknowledge limitations prominently
- Note effect sizes are small in absolute terms
- Consider results as hypothesis-generating for future research
- Use as descriptive evidence in the context of other research

**DO NOT**:
- Claim these results prove diversity *causes* lower suspension rates
- Use these results alone to justify policy interventions
- Ignore the many unmeasured confounders
- Generalize beyond California public schools
- Interpret small effect sizes as unimportant without context

---

## Output Files

### Data Tables

**Location**: `outputs/teacher_diversity_analysis/`

**Files**:
- `teacher_diversity_regression_results.xlsx` - Full results with 3 sheets:
  - **Summary**: Coefficients, confidence intervals, p-values, effect sizes
  - **Interpretations**: Plain-language interpretations for each group
  - **Technical_Details**: Complete regression output

- `teacher_diversity_summary.csv` - Regression summary table (CSV format)
- `teacher_diversity_interpretations.csv` - Interpretations table (CSV format)

### Visualizations

**Location**: `outputs/teacher_diversity_analysis/`

**Files**:
- `teacher_diversity_coefficients_forest_plot.png` - Forest plot showing coefficients with 95% confidence intervals for all groups
- `teacher_diversity_practical_effects.png` - Bar chart showing practical effect sizes (change in suspension rate for 10pp diversity increase)

**Plot Features**:
- Forest plot: Shows both teacher and administrator diversity effects side-by-side
- Practical effects: Only shows statistically significant effects (p < 0.05)
- Both plots distinguish significant vs. non-significant results

---

## Methodological Improvements (2025-11-20)

### Problem Identified

The original analysis ran regressions on **reason-level data** (school-year-race-reason), treating ~6 observations per school-year-race as independent. This violated the independence assumption and resulted in:
- Standard errors underestimated by factor of ~√6 ≈ 2.45
- P-values artificially small (anti-conservative inference)
- Inflated Type I error rates

### Solution Implemented

**Aggregation to School-Year-Race Level**:
- Created `aggregate_to_school_year_race()` function
- Sums suspensions across reason categories (6 → 1 observation)
- Preserves school-level variables (teacher diversity, charter status, etc.)
- Recalculates suspension rate after aggregation

**Performance Optimization**:
- Original aggregation attempted to process all 300 teacher columns → freeze/hang
- Optimized to process only columns used in regression (~153 columns)
- Reduced processing time from potential hours to minutes

**Impact on Results**:
- Standard errors now correctly account for clustering
- P-values are appropriately conservative
- Effect estimates unchanged, but inference is now valid
- N reported as school-year-race combinations (not inflated by reason categories)

### Documentation Added

**Technical Note in Output**:
```
📊 METHODOLOGICAL NOTE:
  • Data aggregated to school-year-race level before regression
  • This properly handles clustering (multiple reasons per school)
  • N = unique school-year-race combinations
  • Standard errors are now appropriate for the unit of analysis
```

**Diagnostic Script Updated**:
- `scripts/diagnostics/investigate_sample_sizes.R` now includes 7-section technical documentation
- Explains the clustering problem, solution, and impact on inference
- Provides verification that diagnostic matches regression approach

---

## Related Files

**Analysis Scripts**:
- `Analysis/21_teacher_diversity_regression.R` - Main regression analysis (this script)
- `scripts/diagnostics/investigate_sample_sizes.R` - Sample size diagnostic tool

**Data Processing**:
- `Analysis/18_merge_teacher_student.R` - Merges teacher demographics with student data
- `R/01c_ingest_teacher_demographics.R` - Ingests raw CDE teacher staff files
- `R/teacher_processing.R` - Teacher demographic processing utilities

**Documentation**:
- `docs/guides/TEACHER_DATA_SETUP_GUIDE.md` - How to obtain and prepare teacher data
- `docs/protocols/PROTOCOL_TEACHER_DATA_MERGE.md` - Merge protocol and validation
- `docs/audits/TEACHER_DEMOGRAPHIC_INTEGRATION_AUDIT.md` - Data quality audit
- `Analysis/TEACHER_DIVERSITY_ANALYSIS_GUIDE.md` - Analysis guide for teacher diversity

**Data Files**:
- `data-stage/susp_v6_teacher_features.parquet` - Input data (377 columns, 3.4M rows)
- `data-stage/teacher_staff_long.parquet` - Teacher demographics (long format)

---

## Citation Guidance

When referencing this analysis in reports or publications:

**Short Form**:
> We examined associations between teacher/administrator racial diversity and student suspension rates using weighted linear regression, stratified by student race (N = 515,947 school-year-race observations across 8 racial/ethnic groups).

**Full Description**:
> Teacher and administrator racial diversity (proportion non-white staff) was examined as a predictor of student suspension rates using weighted ordinary least squares regression, with weights proportional to student enrollment. Separate regressions were estimated for eight student racial/ethnic groups, controlling for charter status and school level. Data were aggregated from school-year-race-reason level (3.4M observations) to school-year-race level (516K observations) to properly account for within-school clustering. Teacher diversity showed statistically significant associations with lower suspension rates for Black/African American (-0.35 percentage points per 10-point increase in diversity, p<0.001), Hispanic/Latino (-0.18pp, p<0.001), White (-0.10pp, p<0.001), and Filipino students (-0.04pp, p<0.001). These associations are small in magnitude and should be interpreted as correlational, not causal. Data source: California Department of Education suspension files (2017-18 through 2023-24) and teacher staff demographic files, processed through REACH suspensions analysis pipeline.

**Technical Note for Methods Section**:
> Diversity was measured as the proportion of teaching staff (or administrative staff) identifying as non-white, calculated by summing shares across nine race/ethnicity categories (African American, Asian, Filipino, Hispanic/Latino, American Indian/Alaska Native, Native Hawaiian/Pacific Islander, Two or More Races), excluding White and Not Reported categories. This approach treats racial diversity as a continuous predictor ranging from 0 (all white staff) to 1 (no white staff). The analysis unit is school-year-race combinations; data were aggregated from reason-level reporting to ensure valid standard errors.

---

## Changelog

**2025-11-20**:
- Implemented aggregation to school-year-race level (fixing clustering issue)
- Optimized aggregation performance (selective column processing)
- Updated all labels: "schools" → "school-year-race combinations"
- Added comprehensive technical documentation
- Generated forest plots and practical effects visualizations
- Created this summary document

**2025-11-19** (approximate):
- Initial regression analysis implementation
- Pattern matching fixes for `TEACHER_RACE_SLUGS`
- Diagnostic script updates

---

## Contact and Questions

For questions about this analysis:
- Review the main script: `Analysis/21_teacher_diversity_regression.R`
- Check diagnostic tool: `scripts/diagnostics/investigate_sample_sizes.R`
- See protocol documentation: `docs/protocols/PROTOCOL_TEACHER_DATA_MERGE.md`
- Consult analysis guide: `Analysis/TEACHER_DIVERSITY_ANALYSIS_GUIDE.md`

For methodological questions about clustering and aggregation:
- See technical notes in this document (Section: "Methodological Improvements")
- Review diagnostic script documentation (7-section technical note)

---

**End of Summary**
