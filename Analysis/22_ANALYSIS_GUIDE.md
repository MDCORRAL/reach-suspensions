# Analysis 22: Black Student Suspension Rates with Teacher Demographics

**Created**: 2025-11-18
**Script**: `Analysis/22_black_suspension_rates_teacher_demographics.R`
**Purpose**: Analyze Black student suspension rates by school racial composition with comprehensive teacher and administrator demographic breakdowns

---

## Overview

This analysis addresses two key research questions:

1. **How do Black student suspension rates vary by school racial composition across years, and what is the teacher/administrator demographic profile of these schools?**
   - Groups schools by Black enrollment quartiles (Q1 = lowest % Black students, Q4 = highest % Black students)
   - Calculates weighted Black student suspension rates for each quartile-year combination
   - Aggregates teacher and administrator racial demographics by quartile

2. **What are the teacher/administrator demographics of schools with the highest Black student suspension rates within each quartile?**
   - Identifies schools in the top 10% of Black suspension rates within each quartile
   - Analyzes teacher and administrator racial composition of these high-suspension schools
   - Compares demographics between all schools and high-suspension schools

---

## Methodology

### Data Sources
- **Student Data**: `susp_v6_teacher_features.parquet` or `susp_v6_teacher_long.parquet` (merged student-teacher data)
- **Years Covered**: 2018-19 onwards (best teacher data coverage)
- **Unit of Analysis**: School-year observations
- **Student Group**: Black/African American students only

### Key Definitions

**Black Enrollment Quartiles (`black_prop_q`)**:
- Schools are grouped into quartiles based on the proportion of Black students in total enrollment
- Q1 = Schools with 0-25th percentile Black student share (lowest)
- Q2 = Schools with 25-50th percentile Black student share
- Q3 = Schools with 50-75th percentile Black student share
- Q4 = Schools with 75-100th percentile Black student share (highest)

**Suspension Rate Calculation**:
- Uses **weighted averages**: Sum of suspensions ÷ Sum of enrollment
- Minimum threshold: Schools with ≥10 Black students for stable rate estimates
- Formula: `black_suspension_rate = total_black_suspensions / total_black_students`

**High Suspension Schools**:
- Defined as schools in the **top 10% (90th percentile)** of Black student suspension rates **within their quartile**
- Ensures comparison of schools with similar racial composition
- Example: A Q1 school with 5% Black suspension rate might be "high" for Q1, while a Q4 school needs >10% to qualify

**Teacher Demographics**:
- **Total Staff**: All teachers, administrators, and staff combined
- **By Race/Ethnicity**: African American, White, Hispanic/Latino, Asian, etc.
- **By Staff Type**: Teachers, Administrators, Pupil Services, Other
- **Cross-tabulated**: Race × Staff Type (e.g., African American teachers, White administrators)

### Analysis Steps

1. **Load and Filter Data**:
   - Read merged student-teacher parquet file
   - Filter to Black students only (`canon_race_label(subgroup) == "Black/African American"`)
   - Filter to campus-level data (exclude special codes)
   - Remove schools with missing quartile assignments

2. **Analysis 1: Quartile-Year Aggregation**:
   - Group by `academic_year` and `black_prop_q`
   - Calculate:
     - Number of schools per quartile
     - Total Black students and suspensions (weighted sums)
     - Black student suspension rate (weighted average)
     - Teacher demographic totals (sum of staff counts)
     - Teacher demographic percentages (calculated from aggregated counts)

3. **Analysis 2: High Suspension School Identification**:
   - Calculate school-level Black suspension rates
   - Within each quartile-year, identify schools at ≥90th percentile
   - Aggregate teacher demographics for these high-suspension schools
   - Compare to overall quartile demographics

4. **Calculate Teacher Percentages**:
   - Overall staff race percentages: `pct = race_count_sum / total_staff_sum × 100`
   - Staff type percentages: `pct = type_count_sum / total_staff_sum × 100`
   - Race within staff type: `pct = race_type_count_sum / type_total_sum × 100`

---

## Outputs

### Tables (CSV & Excel)

**1. `outputs/tables/22_black_suspension_by_quartile_year_teacher.csv`**
- Main quartile-year summary table
- Columns:
  - `academic_year`, `black_prop_q`, `black_prop_q_label`
  - `n_schools` - Number of schools in quartile
  - `total_black_students` - Sum of Black student enrollment
  - `total_black_suspensions` - Sum of Black student suspensions
  - `black_suspension_rate` - Weighted suspension rate
  - `teacher_staff_count_total_sum` - Total staff count
  - `teacher_staff_count_african_american_sum`, `teacher_staff_count_white_sum`, etc.
  - `teacher_staff_count_african_american_pct`, `teacher_staff_count_white_pct`, etc.
  - `teacher_staff_count_total_by_type_teachers_sum`, `teacher_staff_count_total_by_type_administrators_sum`
  - Race × Staff Type breakdowns with percentages

**2. `outputs/tables/22_high_suspension_schools_teacher_demographics.csv`**
- Summary of high-suspension schools by quartile-year
- Columns:
  - `academic_year`, `black_prop_q`, `black_prop_q_label`
  - `n_high_suspension_schools` - Count of schools in top 10%
  - `avg_black_suspension_rate`, `median_black_suspension_rate`, `max_black_suspension_rate`
  - `total_black_students`, `total_black_suspensions`
  - Teacher demographic aggregations (same structure as table 1)

**3. `outputs/tables/22_high_suspension_schools_detailed.csv`**
- School-level detail for all high-suspension schools
- Columns:
  - `academic_year`, `cds_school`, `black_prop_q_label`
  - `school_black_enrollment`, `school_black_suspensions`, `school_black_suspension_rate`
  - `suspension_rate_percentile` - Percentile rank within quartile
  - Individual school teacher demographics

**4. `outputs/tables/22_black_suspension_teacher_analysis.xlsx`**
- Excel workbook with three sheets:
  - **Quartile_Year_Summary**: Table 1 (all schools)
  - **High_Suspension_Summary**: Table 2 (high-suspension aggregates)
  - **High_Suspension_Schools**: Table 3 (school-level detail)

### Visualizations (PNG)

**1. `outputs/graphs/22_black_suspension_rates_by_quartile.png`**
- **Type**: Line plot with points and labels
- **X-axis**: Academic year
- **Y-axis**: Black student suspension rate (%)
- **Lines**: One per Black enrollment quartile (color-coded)
- **Purpose**: Shows trends in Black suspension rates over time by school racial composition

**2. `outputs/graphs/22_teacher_demographics_comparison.png`**
- **Type**: Grouped bar chart
- **Facets**: One panel per race/ethnicity (African American, White, Hispanic/Latino)
- **X-axis**: Black enrollment quartile
- **Y-axis**: Percentage of staff (%)
- **Bars**: Two per quartile (All Schools vs High Suspension Schools)
- **Purpose**: Compares teacher race/ethnicity between all schools and high-suspension schools

**3. `outputs/graphs/22_admin_teacher_demographics_high_suspension.png`**
- **Type**: Grouped bar chart
- **Facets**: Administrators vs Teachers
- **X-axis**: Black enrollment quartile
- **Y-axis**: Percentage of staff type (%)
- **Bars**: African American vs White (within each staff type)
- **Purpose**: Compares racial composition of administrators vs teachers in high-suspension schools

---

## Usage

### Prerequisites

1. **Required Data Files**:
   ```
   data-stage/susp_v6_teacher_features.parquet
   (or data-stage/susp_v6_teacher_long.parquet)
   ```

2. **Generate Merged Data** (if not already done):
   ```r
   # Ensure teacher data is ingested
   source("R/01c_ingest_teacher_demographics.R")

   # Merge teacher and student data
   source("Analysis/18_merge_teacher_student.R")
   ```

3. **Check Data Availability**:
   ```r
   # Verify file exists
   file.exists("data-stage/susp_v6_teacher_features.parquet")
   ```

### Running the Analysis

**Option 1: Using the runner script**
```r
source("run_22_analysis.R")
```

**Option 2: Direct execution**
```r
source("Analysis/22_black_suspension_rates_teacher_demographics.R")
```

**Option 3: Add to pipeline**
```r
# In run_all.R, add after line 28:
run("Analysis/22_black_suspension_rates_teacher_demographics.R")
```

### Expected Runtime
- **Duration**: 1-3 minutes (depends on data size)
- **Memory**: ~500MB-1GB
- **Output Size**: ~5-10 CSV files, 3 PNG files

---

## Interpreting Results

### Key Findings to Look For

1. **Suspension Rate Patterns**:
   - Do schools with higher Black enrollment (Q4) have higher Black suspension rates?
   - Are trends consistent across years, or do rates vary?
   - How large are the differences between quartiles?

2. **Teacher Demographic Patterns**:
   - What is the racial composition of teachers/administrators in each quartile?
   - Do schools with higher Black enrollment have more diverse staff?
   - Are there differences between teacher and administrator demographics?

3. **High Suspension School Characteristics**:
   - Within each quartile, what distinguishes high-suspension schools?
   - Do high-suspension schools have different teacher demographics than typical schools?
   - Are the differences more pronounced in certain quartiles?

### Example Interpretation

**Scenario**: Q4 schools (highest % Black students) show:
- Black suspension rate: 12%
- Teacher demographics: 75% White, 15% African American
- High-suspension Q4 schools: 80% White, 10% African American

**Interpretation**:
- Schools with predominantly Black students have the highest suspension rates for Black students
- These schools are primarily staffed by White teachers/administrators
- Within Q4, schools with even fewer Black staff members tend to have higher Black suspension rates
- **Caution**: This is correlational only. Many factors (funding, leadership, policies, community context) influence outcomes

### Important Caveats

1. **Correlation ≠ Causation**: Teacher demographics and suspension rates are associated, but this does not prove causation
2. **Confounding Factors**: School funding, neighborhood characteristics, school policies, leadership quality, and many other factors influence both teacher hiring and disciplinary practices
3. **Aggregation**: School-level and quartile-level aggregations may mask important within-school and within-quartile variation
4. **Data Limitations**: Teacher race data may have missing values or reporting inconsistencies
5. **Time Period**: Analysis focuses on 2018-19 onwards due to better teacher data coverage; earlier years may not be representative

---

## Customization

### Modifying the Top Suspension Threshold

Default: Top 10% (90th percentile)

To identify top 5% instead:
```r
# In the script, find this line (around line 189):
is_top_decile = suspension_rate_percentile >= 90

# Change to:
is_top_5pct = suspension_rate_percentile >= 95
```

### Adding More Race/Ethnicity Groups

The script dynamically includes all available race columns. To focus on specific groups:

```r
# After loading data, filter to specific races:
teacher_race_cols <- grep(
  "^teacher_staff_count_(african_american|white|hispanic_or_latino|asian)($|_share$)",
  names(black_students),
  value = TRUE,
  perl = TRUE
)
```

### Changing the Minimum Enrollment Threshold

Default: ≥10 Black students

```r
# Find this line (around line 180):
school_black_enrollment >= 10

# Change to your preferred threshold:
school_black_enrollment >= 20  # For more stable estimates
school_black_enrollment >= 5   # For more coverage
```

### Analyzing Other Student Groups

To analyze suspension rates for other racial groups (e.g., Hispanic/Latino students):

```r
# Change the filter on line 78:
canon_race_label(subgroup) == "Black/African American"

# To:
canon_race_label(subgroup) == "Hispanic/Latino"

# And update variable names throughout (e.g., black_suspension_rate → hispanic_suspension_rate)
```

---

## Troubleshooting

### Error: "Missing merged teacher-student data"

**Cause**: The merged parquet file doesn't exist

**Solution**:
```r
# Run the merge script
source("Analysis/18_merge_teacher_student.R")

# Or run the full pipeline
source("run_all.R")
```

### Error: "No teacher_* columns found"

**Cause**: Teacher data wasn't properly merged

**Solution**:
1. Check that teacher TXT files exist in `data-raw/` (see `docs/guides/TEACHER_DATA_SETUP_GUIDE.md`)
2. Re-run teacher ingestion:
   ```r
   source("R/01c_ingest_teacher_demographics.R")
   ```
3. Re-run merge:
   ```r
   source("Analysis/18_merge_teacher_student.R")
   ```

### Warning: "Teacher race demographics not available"

**Cause**: Teacher data doesn't include race/ethnicity breakdowns

**Solution**:
- Check that CDE teacher TXT files include race columns
- See `docs/audits/TEACHER_DEMOGRAPHIC_INTEGRATION_AUDIT.md` for data quality checks
- Some outputs will be skipped, but core analysis will still run

### Error: "object 'dp_out' not found"

**Cause**: Path configuration script wasn't sourced

**Solution**:
```r
source("R/00_paths.R")
```
(The analysis script sources this automatically, but if running interactively, ensure it's sourced first)

### No high suspension schools identified

**Possible causes**:
1. Minimum enrollment threshold too high (reduce from 10 to 5)
2. Quartile has very few schools (check `n_schools` in output)
3. Data quality issues (check for missing suspension rates)

---

## Technical Notes

### Weighting Approach

The analysis uses **aggregate-then-calculate** weighting:
```r
# Correct (weighted):
suspension_rate = sum(suspensions) / sum(enrollment)

# NOT (unweighted):
suspension_rate = mean(school_rates)
```

This ensures larger schools appropriately influence quartile-level estimates.

### Handling of Missing Data

- **Missing quartile assignments**: Excluded from analysis
- **Missing teacher data**: Retained in suspension calculations, but excluded from teacher demographic summaries
- **Zero enrollment**: Excluded (suspension rate undefined)
- **Suppressed values** (asterisks): Should already be handled in v6 data pipeline

### Performance Considerations

- **Large datasets**: Script processes ~100K-500K student-year-race observations
- **Memory usage**: Moderate (aggregations reduce data size)
- **Optimization tips**:
  - Filter to specific years if analyzing recent data only
  - Use `col_select` when reading parquet to load only needed columns
  - Increase minimum enrollment threshold to reduce noise

---

## References

### Related Scripts
- `Analysis/02_black_rates_by_quartiles.R` - Canonical quartile analysis (no teacher data)
- `Analysis/21_weighted_teacher_diversity_by_quartile.R` - Teacher diversity by quartile (all students)
- `Analysis/18_merge_teacher_student.R` - Creates merged teacher-student data

### Documentation
- `docs/guides/TEACHER_DATA_SETUP_GUIDE.md` - Teacher data acquisition
- `docs/protocols/PROTOCOL_TEACHER_DATA_MERGE.md` - Merge protocol
- `docs/audits/TEACHER_DEMOGRAPHIC_INTEGRATION_AUDIT.md` - Data quality audit
- `CLAUDE.md` - Comprehensive repository guide

### Data Sources
- CDE Suspension Data: 2017-18 through 2023-24
- CDE Teacher Staff Data: 2017-18 through 2023-24
- Source: California Department of Education (https://www.cde.ca.gov)

---

## Contact & Support

For questions about this analysis:
1. Review the script comments in `Analysis/22_black_suspension_rates_teacher_demographics.R`
2. Check the comprehensive guide in `CLAUDE.md`
3. Review related analysis guides in `Analysis/`

**Last Updated**: 2025-11-18
