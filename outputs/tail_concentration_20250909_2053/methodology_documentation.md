# Tail Concentration Analysis: Data Processing and Methodology Documentation

## Data Sources and Scope

### Primary Dataset
- **Source**: California Department of Education suspension data (susp_v5.parquet)
- **Original Records**: Raw dataset contained school-level suspension counts by demographic subgroup
- **Time Period**: Academic years 2017-18 through 2023-24 (excluding 2019-20 due to data availability)

### Supplementary Data
- **School Characteristics**: susp_v6_features.parquet containing school type classifications
- **Purpose**: Used to distinguish traditional vs non-traditional schools and provide institutional context

## Data Processing Steps

### 1. Record Selection Criteria

**Inclusion Criteria**:
- Records coded as "Total" or "All Students" in `reporting_category` field
- Schools with valid enrollment data (`cumulative_enrollment > 0`)
- Records with non-negative suspension counts
- Valid academic year information

**Exclusion Criteria**:
- Demographic subgroup records (to avoid double-counting)
- Schools with missing or zero enrollment
- Records with negative suspension counts
- Invalid or missing year data

### 2. Data Quality Controls

**Applied Filters**:
```
Original dataset: [Unknown total records]
After filtering to "Total/All Students": [Filtered count]
After removing invalid enrollment: [Valid enrollment count]
After removing negative suspensions: [Clean suspension count]
Final analytical dataset: 60,011 school-year records
```

**Geographic Scope**: Statewide California analysis
**School Types**: All public schools reporting suspension data

### 3. Variable Definitions

**Primary Measure**: `total_suspensions` - Total suspension events (not unduplicated students)
**Enrollment**: `cumulative_enrollment` - Total students enrolled during academic year
**School Classification**: Traditional vs Non-traditional based on school type designations

### 4. Analytical Approach

**Concentration Calculation**:
1. Schools ranked by total suspension events within each academic year
2. Percentile cutoffs calculated (5th, 10th, 20th percentiles)
3. Cumulative suspension shares computed for top percentiles
4. Results expressed as percentage of total statewide suspension events

**School Counts by Year**:
- 2017: 9,947 schools
- 2018: 9,961 schools  
- 2019: 10,064 schools
- 2021: 10,013 schools
- 2022: 10,024 schools
- 2023: 10,002 schools

## Methodological Decisions and Limitations

### Key Decisions

**Focus on Total Events**: Used total suspension events rather than unduplicated student counts to capture volume impact on school operations and student time lost.

**Annual Analysis**: Calculated concentration separately for each academic year to account for temporal variation and avoid cross-year averaging that could mask trends.

**Statewide Scope**: Analyzed all California public schools together rather than stratifying by district or region to assess overall state-level concentration patterns.

### Important Limitations

**Data Aggregation**: 
- Analysis combines all suspension types (in-school, out-of-school, various durations)
- Does not distinguish between suspension severity or circumstances
- Cannot account for appropriateness of individual disciplinary decisions

**Missing Context**:
- No adjustment for student population characteristics that may legitimately drive higher suspension needs
- Limited ability to distinguish between problematic concentration and appropriate specialization
- School type classifications may not capture all relevant institutional differences

**Temporal Coverage**:
- Missing 2019-20 academic year data (likely COVID-related)
- Cannot assess pandemic impacts on concentration patterns
- Limited ability to establish long-term trends

### Statistical Considerations

**Unit of Analysis**: School-year combinations (not schools or students)
**Weighting**: Unweighted analysis treats all schools equally regardless of size
**Outlier Treatment**: No outlier exclusion applied; extreme values retained to capture full concentration

## Validation and Quality Assurance

### Data Consistency Checks
- Verified year-over-year school counts within reasonable ranges
- Confirmed suspension totals align with expected statewide patterns
- Cross-checked concentration calculations using multiple methods

### Analytical Robustness
- Replicated key findings using alternative concentration measures
- Tested sensitivity to different percentile cutoffs
- Verified results against theoretical expectations for highly skewed distributions

## Interpretation Guidelines

### What the Results Show
The concentration percentages represent the share of total statewide suspension events accounted for by schools in the specified percentile ranges. For example, "Top 5% of schools account for 35.3% of suspensions" means that the 497 schools with the highest suspension counts in 2017 were responsible for more than one-third of all suspension events across California's nearly 10,000 schools.

### What the Results Don't Show
- Whether high-suspension schools are appropriately serving high-need populations
- Quality or appropriateness of disciplinary decisions
- Underlying factors driving suspension patterns
- Effectiveness of disciplinary policies or practices

### Appropriate Uses
- Identifying magnitude of concentration for policy planning
- Targeting resources and interventions
- Establishing baseline for evaluating policy reforms
- Comparing concentration patterns across years or jurisdictions

### Inappropriate Uses
- Making causal claims about school effectiveness
- Judging appropriateness of individual school practices
- Assuming concentration necessarily indicates problems
- Drawing conclusions about specific disciplinary incidents

## Replication Information

### Software and Packages
- R Statistical Software (version 4.x)
- Primary packages: dplyr, arrow, readr, ineq, scales
- Analysis script: 16_tail_concentration_analysis.R

### Data Requirements for Replication
- School-level suspension counts by academic year
- School enrollment data
- School type/characteristic classifications
- Consistent school identifiers across years

This methodology produces defensible estimates of suspension concentration while acknowledging inherent limitations in administrative data analysis. The approach prioritizes transparency and replicability while maintaining appropriate caution about causal interpretation.