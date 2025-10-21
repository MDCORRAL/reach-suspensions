# Data Processing Overview: REACH Suspension Analysis Pipeline

## Purpose of This Document

This document provides complete transparency into the data processing pipeline that produces all datasets, metrics, and visualizations in the REACH suspension analysis dashboard. It explains:

- **What data sources** are used and where they come from
- **How data is transformed** through each processing stage
- **Why specific filters and calculations** are applied
- **Which outputs** feed into dashboard visualizations and analyses
- **What quality controls** ensure data accuracy

This resource allows users and reviewers to trace any dashboard metric back to its raw data source and understand every transformation applied along the way.

---

## Data Sources

### Primary Suspension Data
The pipeline processes California Department of Education (CDE) suspension data spanning academic years 2017-18 through 2023-24. The raw data file contains school-level suspension counts disaggregated by:
- **Race/ethnicity** (school-race level)
- **Suspension reasons** (violence with/without injury, weapons, illicit drugs, defiance, other)
- **Grade ranges** and school characteristics

**Source file**: `data-raw/copy_CDE_suspensions_1718-2324_sc_race.xlsx`

The pipeline can override this location using the `RAW_PATH` environment variable for flexible deployment.

### Demographic Data
Additional demographic breakdowns come from a parallel CDE dataset providing suspension rates for:
- Students with Disabilities (SPED)
- English Learners (EL)
- Socioeconomically Disadvantaged (SED)
- Foster Youth
- Homeless Youth
- Migrant Students
- Sex (Male/Female)

**Source file**: `data-raw/copy_CDE_suspensions_1718-2324_sc_oth.xlsx`

This can be overridden via the `OTH_RAW_PATH` environment variable.

### Path Configuration
The pipeline centralizes all path handling in `R/00_paths.R`:
- **Project root**: Determined by `REACH_PROJECT_ROOT` environment variable or current working directory
- **Staged outputs**: Written to `REACH_DATA_DIR` if set, otherwise `<project>/data-stage/`
- **Raw inputs**: Expected in `data-raw/`
- **Final outputs**: Graphs and tables go to `outputs/`

All directories are created automatically if missing.

---

## Pipeline Architecture

The data processing pipeline consists of **sequential stages** that progressively enrich the dataset. Each stage:

1. **Reads** the output from the previous stage
2. **Computes** new features or applies filters
3. **Validates** the results with diagnostic summaries
4. **Writes** a new staged Parquet file for the next stage

This modular design allows:
- **Traceability**: Each intermediate dataset can be inspected
- **Flexibility**: Individual stages can be re-run without reprocessing everything
- **Validation**: Stage outputs include row counts and distribution summaries

### Current Staged Datasets

| Dataset | Rows | Description |
|---------|------|-------------|
| `susp_v5.parquet` | 567,047 | School-race level data with quartiles and reason shares (wide format) |
| `susp_v5_long.parquet` | 3,402,282 | School-race-reason level data (long format) |
| `oth_long.parquet` | 558,431 | Demographic subgroup suspension rates (long format) |
| `susp_v6_features.parquet` | 60,188 | School-year analytic dataset with all demographics (wide format) |
| `susp_v6_long.parquet` | 3,402,282 | School-race level data with school attributes (long format) |

---

## Detailed Processing Stages

### Stage 01: Base Ingestion (`R/01_ingest_v0.R`)

**Purpose**: Transform raw Excel data into a clean, standardized base dataset.

**Processing Steps**:
1. **Read** all worksheets from the suspension Excel file
2. **Normalize** column names to lowercase with underscores
3. **Derive** temporal fields:
   - `year` (end year, e.g., 2018 for 2017-18)
   - `academic_year` (e.g., "2017-18")
4. **Identify** all numeric-like columns automatically
5. **Create suppression flags** before parsing numbers:
   - CDE suppresses small cell counts with asterisks (*) to protect privacy
   - Flags capture whether any numeric field was suppressed for each row
6. **Parse** numeric strings to actual numbers (after flagging suppressions)
7. **Trim** charter school flags to canonical values

**Validation**: Prints row/column counts, academic year distribution, charter school breakdown

**Outputs**:
- `data-stage/susp_v0.parquet` (base dataset)
- `column_dictionary_v0.csv` (metadata)

**Rationale**: Preserving suppression information before numeric conversion ensures we can later identify which schools have privacy-protected data, maintaining transparency about data limitations.

---

### Stage 02: Locale Classification (`R/02_feature_locale_simple.R`)

**Purpose**: Standardize school locale into consistent categories for urban/rural analysis.

**Processing Steps**:
1. **Standardize** CDE locale descriptions into five categories:
   - `City` (urban core)
   - `Suburban` (urban periphery)
   - `Town` (small urban clusters)
   - `Rural` (non-urban)
   - `Unknown` (missing data)
2. **Apply** consistent factor ordering for all analyses

**Validation**: Prints counts by locale category to confirm coverage

**Outputs**: `data-stage/susp_v1.parquet`

**Rationale**: CDE uses detailed locale codes; this step creates analyst-friendly categories used across all visualizations while preserving the original detail in case it's needed.

---

### Stage 02b: Remove "All Charter" Aggregates (`R/02b_drop_charter_all.R`)

**Purpose**: Prevent double-counting in charter school analyses.

**Processing Steps**:
1. **Remove** rows where `charter_yn == "All"` (statewide charter aggregates)
2. **Retain** Yes/No charter flags for individual schools

**Validation**: Confirms row count reduction matches expected "All" rows

**Outputs**: `data-stage/susp_v1_noall.parquet`

**Rationale**: The raw data includes statewide charter totals alongside individual schools. Removing these aggregates ensures that when we sum charter schools, we count each school once.

---

### Stage 03: Enrollment Size Quartiles (`R/03_feature_size_quartiles_TA.R`)

**Purpose**: Enable analysis by school size to account for small-school variability.

**Processing Steps**:
1. **Filter** to campus-level records only (exclude district/county/state aggregates)
2. **Extract** total enrollment for "All Students" subgroup at each campus-year
3. **Calculate** year-specific quartiles (Q1–Q4) using the first non-missing enrollment value
4. **Assign** quartile labels:
   - `Q1 (Smallest)` to `Q4 (Largest)`
   - `Unknown` for schools with missing/zero enrollment
5. **Join** quartile indicators to all race rows

**Validation**: Table 1 shows near-equal campus counts per quartile (~2,500 per quartile per year)

**Outputs**: `data-stage/susp_v2.parquet`

**Table 1. Enrollment Quartile Distribution by Year**
| Academic year | Enrollment quartile | Campuses |
|---|---|---|
| 2017-18 | Q1 (Smallest) | 2,487 |
| 2017-18 | Q2 | 2,487 |
| 2017-18 | Q3 | 2,487 |
| 2017-18 | Q4 (Largest) | 2,486 |
| 2017-18 | Unknown | 106 |
| 2018-19 | Q1 (Smallest) | 2,491 |
| 2018-19 | Q2 | 2,490 |
| 2018-19 | Q3 | 2,490 |
| 2018-19 | Q4 (Largest) | 2,490 |
| 2018-19 | Unknown | 70 |
| 2019-20 | Q1 (Smallest) | 2,516 |
| 2019-20 | Q2 | 2,516 |
| 2019-20 | Q3 | 2,516 |
| 2019-20 | Q4 (Largest) | 2,516 |
| 2021-22 | Q1 (Smallest) | 2,504 |
| 2021-22 | Q2 | 2,503 |
| 2021-22 | Q3 | 2,503 |
| 2021-22 | Q4 (Largest) | 2,503 |
| 2022-23 | Q1 (Smallest) | 2,506 |
| 2022-23 | Q2 | 2,506 |
| 2022-23 | Q3 | 2,506 |
| 2022-23 | Q4 (Largest) | 2,506 |
| 2023-24 | Q1 (Smallest) | 2,500 |
| 2023-24 | Q2 | 2,501 |
| 2023-24 | Q3 | 2,501 |
| 2023-24 | Q4 (Largest) | 2,500 |
| 2023-24 | Unknown | 1 |

**Rationale**: Year-specific quartiles account for enrollment trends over time. A school that was "large" in 2017-18 might be "medium" in 2023-24 if overall enrollment shifted. This ensures quartile comparisons are meaningful within each academic year.

---

### Stage 04: Racial Composition Quartiles (`R/04_feature_black_prop_quartiles.R`)

**Purpose**: Classify schools by racial composition to study how school demographics relate to suspension patterns.

**Processing Steps**:
1. **Calculate** enrollment proportions for each campus-year:
   - `prop_black = enroll_Black / enroll_All`
   - `prop_white = enroll_White / enroll_All`
   - `prop_hispanic = enroll_Hispanic / enroll_All`
2. **Compute** year-specific quartiles for each race share metric
3. **Label** quartiles descriptively:
   - `Q1 (Lowest % Black)` to `Q4 (Highest % Black)`
   - Similar labels for White and Hispanic shares
4. **Guard** against duplicate campus-year keys and invalid proportions
5. **Join** share metrics to all race rows

**Validation**: Table 2 shows quartile coverage for Black enrollment share

**Outputs**: `data-stage/susp_v3.parquet`

**Table 2. Black Enrollment Quartile Coverage (Campus-Year Level)**
| Academic year | Quartile label | Campuses |
|---|---|---|
| 2017-18 | Q1 (Lowest % Black) | 1,373 |
| 2017-18 | Q2 | 1,373 |
| 2017-18 | Q3 | 1,373 |
| 2017-18 | Q4 (Highest % Black) | 1,372 |
| 2017-18 | Unknown | 4,562 |
| 2018-19 | Q1 (Lowest % Black) | 1,353 |
| 2018-19 | Q2 | 1,353 |
| 2018-19 | Q3 | 1,352 |
| 2018-19 | Q4 (Highest % Black) | 1,352 |
| 2018-19 | Unknown | 4,621 |
| 2019-20 | Q1 (Lowest % Black) | 1,342 |
| 2019-20 | Q2 | 1,342 |
| 2019-20 | Q3 | 1,342 |
| 2019-20 | Q4 (Highest % Black) | 1,341 |
| 2019-20 | Unknown | 4,697 |
| 2021-22 | Q1 (Lowest % Black) | 1,302 |
| 2021-22 | Q2 | 1,302 |
| 2021-22 | Q3 | 1,302 |
| 2021-22 | Q4 (Highest % Black) | 1,301 |
| 2021-22 | Unknown | 4,806 |
| 2022-23 | Q1 (Lowest % Black) | 1,300 |
| 2022-23 | Q2 | 1,299 |
| 2022-23 | Q3 | 1,299 |
| 2022-23 | Q4 (Highest % Black) | 1,299 |
| 2022-23 | Unknown | 4,827 |
| 2023-24 | Q1 (Lowest % Black) | 1,283 |
| 2023-24 | Q2 | 1,282 |
| 2023-24 | Q3 | 1,282 |
| 2023-24 | Q4 (Highest % Black) | 1,282 |
| 2023-24 | Unknown | 4,874 |

**Rationale**: Many schools in California have very small Black student populations, resulting in high "Unknown" counts (schools where Black enrollment is zero or suppressed). Quartile analysis focuses on schools with measurable Black enrollment to study suspension disparities in racially diverse contexts.

---

### Stage 05: School Level Classification (`R/05_feature_school_level.R`)

**Purpose**: Categorize schools by grade span (Elementary/Middle/High) and identify alternative education settings.

**Processing Steps**:
1. **Extract** minimum and maximum grades from grade range field
2. **Normalize** early grades (PK/TK/K → 0) for consistent ordering
3. **Map** grade spans to school levels:
   - Elementary: Primarily K–5
   - Middle: Primarily 6–8
   - High: Primarily 9–12
   - Other: Mixed or unusual spans
4. **Override** to "Alternative" when school name/type matches patterns:
   - Juvenile court schools
   - Community day schools
   - Continuation schools
   - Alternative education programs
   - Independent study

**Validation**: Prints unique campus counts by school level and year

**Outputs**: `data-stage/susp_v4.parquet`

**Rationale**: Suspension rates and disciplinary practices differ systematically by grade level and school type. Alternative schools serve students with special circumstances and typically have higher suspension rates. This classification enables appropriate comparisons within similar school contexts.

---

### Stage 06: Suspension Reason Shares (`R/06_feature_reason_shares.R`)

**Purpose**: Calculate what proportion of each school's suspensions fall into different offense categories.

**Processing Steps**:
1. **Compute** proportions for each suspension reason:
   - Violence with injury (`prop_susp_vi`)
   - Violence without injury (`prop_susp_vn`)
   - Weapons possession (`prop_susp_wp`)
   - Illicit drugs (`prop_susp_id`)
   - Defiance/disruption (`prop_susp_def`)
   - Other causes (`prop_susp_oth`)
2. **Apply** safety rules:
   - Only calculate proportions when both numerator and denominator are positive
   - Emit `NA` for schools with zero total suspensions (avoid division by zero)
3. **Validate** that all proportions fall within (0,1]
4. **Generate** both wide and long formats

**Validation**: Table 3 shows how many schools have calculable shares for each reason

**Outputs**:
- `data-stage/susp_v5.parquet` (wide format: one row per school-race)
- `data-stage/susp_v5_long.parquet` (long format: one row per school-race-reason)

**Table 3. Schools with Valid Reason Shares by Year**
| Academic Year | Total Suspending Schools | Violence w/ Injury | Violence w/o Injury | Weapons | Illicit Drugs | Defiance | Other |
|:---|---:|---:|---:|---:|---:|---:|---:|
| 2017-18 | 31,720 | 15,285 | 27,106 | 9,892 | 12,248 | 13,629 | 10,643 |
| 2018-19 | 31,401 | 15,081 | 26,826 | 9,223 | 13,063 | 12,331 | 9,465 |
| 2019-20 | 27,678 | 13,008 | 22,893 | 7,036 | 11,296 | 8,139 | 7,352 |
| 2021-22 | 28,734 | 14,563 | 24,279 | 9,757 | 11,987 | 6,585 | 8,822 |
| 2022-23 | 30,443 | 15,853 | 26,086 | 9,748 | 13,883 | 6,179 | 8,872 |
| 2023-24 | 30,384 | 16,163 | 26,385 | 8,257 | 12,093 | 4,993 | 8,028 |

**Rationale**: Not all schools report suspensions for all reasons. This table documents data availability. The sharp decline in defiance suspensions after 2019-20 reflects California's ban on suspending students in grades K-8 for defiance/disruption (AB 420/SB 419).

---

### Demographic Data Processing (`R/01b_ingest_demographics.R`)

**Purpose**: Integrate additional demographic breakdowns beyond race/ethnicity.

**Processing Steps**:
1. **Read** the "Other" (OTH) demographic Excel file
2. **Standardize** subgroup labels using canonical mappings:
   - Normalize inconsistent category names across years
   - Map codes to full descriptive labels
   - Group subgroups into categories (SPED, EL, SED, etc.)
3. **Aggregate** enrollment and suspension totals per campus-year-subgroup
4. **Calculate** suspension rates using `safe_rate()`:
   - Only compute rates when enrollment ≥ 10 (minimum threshold for stability)
   - Emit `NA` for small subgroups to avoid misleading rates from tiny samples
5. **Validate** available categories and subgroup names

**Outputs**: `data-stage/oth_long.parquet`

**Rationale**: The 10-student minimum threshold balances privacy protection with analytical utility. Suspension rates for very small subgroups are unstable; a single incident can create a 50%+ rate. This threshold aligns with CDE's own suppression rules.

---

### Stage 22: Build Version 6 Features (`R/22_build_v6_features.R`)

**Purpose**: Create the final analytic dataset by merging all race, demographic, and school characteristics into a single campus-year record.

**Processing Steps**:

#### 1. Load and Prepare Base Data
- Read `susp_v5.parquet` (race data with quartiles)
- Read `oth_long.parquet` (demographic subgroups)
- Read `susp_v5_long.parquet` (race-reason long data)
- Apply `filter_campus_only()` to exclude district/county/state aggregates
- Build canonical school ID keys with padded CDS codes

#### 2. Extract School-Year Core Roster
- Filter to "All Students" subgroup (one record per campus-year)
- Select school identifiers, names, types, locale, enrollment quartiles, and racial composition quartiles

#### 3. Aggregate Demographic Rates
For each demographic domain, the pipeline:
- Filters to the topline subgroup (e.g., "Students with Disabilities" for SPED)
- Removes impossible values (suspensions > enrollment)
- Aggregates to campus-year level using safe division (numerator / denominator)
- Handles missing data appropriately

Domains processed:
- **SPED**: Students with Disabilities
- **EL**: English Learners
- **SED**: Socioeconomically Disadvantaged
- **Foster**: Foster Youth
- **Homeless**: Homeless Youth
- **Migrant**: Migrant Students
- **Sex**: Male and Female breakdowns

#### 4. Identify Traditional Schools
Schools are classified as **traditional** when:
- School type contains grade-level descriptors (elementary, middle, high) OR "traditional", AND
- School type does NOT match alternative patterns:
  - Community day
  - Juvenile court
  - Continuation
  - Alternative education
  - Opportunity schools
  - Independent study

**Rationale**: Traditional and alternative schools serve fundamentally different populations with different disciplinary contexts. Alternative schools often serve students with prior disciplinary issues or special circumstances, resulting in structurally higher suspension rates. Separating these allows fair comparisons.

#### 5. Data Quality Checks
- **Uniqueness**: Assert one row per campus-year (no duplicates)
- **Valid ranges**: Confirm suspension rates fall between 0 and 1
- **Completeness**: Backfill missing school names from prior years

**Outputs**:
- `data-stage/susp_v6_features.parquet` (60,188 rows: campus-year wide format)
- `data-stage/susp_v6_long.parquet` (3,402,282 rows: school-race long format with school attributes)

---

### Example Analysis: SPED Suspension Rates by School Racial Composition

To demonstrate pipeline output quality, Stage 22 includes an embedded analysis examining Students with Disabilities (SPED) suspension patterns across schools with different Black enrollment shares.

**Sample Restrictions**:
- Traditional schools only (excludes alternative education)
- Known Black enrollment quartile (excludes schools with zero/suppressed Black enrollment)
- Positive SPED enrollment (≥1 student with disabilities)
- **Result**: 18,106 campus-year records out of 31,429 total traditional campus-years

**Table 4. SPED Suspension Rates by Black Enrollment Quartile (Weighted)**
| Black Enrollment Quartile | Schools | SPED Suspensions | SPED Enrollment | Suspension Rate | Mean Rate | Median Rate |
|:---|---:|---:|---:|---:|---:|---:|
| Q1 (Lowest % Black) | 4,531 | 42,985 | 804,403 | 5.34% | 4.56% | 2.94% |
| Q2 | 4,670 | 34,347 | 710,785 | 4.83% | 3.93% | 2.13% |
| Q3 | 4,368 | 41,247 | 695,968 | 5.93% | 4.78% | 2.78% |
| Q4 (Highest % Black) | 4,537 | 48,001 | 639,452 | 7.51% | 6.02% | 3.45% |

**Interpretation**:
- **Weighted rate**: Total SPED suspensions ÷ total SPED enrollment (gives more weight to large schools)
- **Mean rate**: Average of school-level rates (each school weighted equally)
- **Median rate**: Typical school's rate (robust to outliers)

Schools with the highest Black enrollment percentages (Q4) have SPED suspension rates 40% higher than schools with the lowest Black percentages (Q1), even among students with disabilities. This pattern holds across weighted, mean, and median measures, indicating it's not driven by a few outlier schools.

---

## Downstream Analysis Usage

### Statewide Trend Graphs (`graph_scripts/06_statewide_trends.py`)
**Data sources**:
- `susp_v6_long.parquet` (school-race-year data)
- `susp_v6_features.parquet` (school attributes, particularly `is_traditional`)

**Processing**:
1. Filter to campus-level traditional schools
2. Aggregate to statewide totals by race × year
3. Calculate pooled suspension rates (total suspensions ÷ total enrollment)
4. Generate trend line visualizations

**Validation**: `--diagnostics-only` mode outputs CSV files for cross-validation with R analyses

---

### Analysis 02: Black Suspension Rates by Racial Composition (`Analysis/02_*.R`)
**Data sources**:
- `susp_v6_long.parquet`

**Processing**:
1. Filter to campus-level records with valid Black enrollment quartiles
2. Aggregate suspensions and enrollment by year × quartile
3. Calculate pooled suspension rates
4. Generate quartile-specific trend visualizations
5. Break down by suspension reason using reason share proportions

**Outputs**: Graphs showing how suspension patterns vary by school racial composition

---

### Analysis 15: Intersectional Demographic Analysis (`Analysis/15_*.R`)
**Data sources**:
- `susp_v5.parquet` (race data with school attributes)
- `oth_long.parquet` (demographic subgroups)

**Processing**:
1. Build canonical keys for both datasets
2. Validate presence of all required demographic categories
3. Standardize subgroup labels using `canonicalize_demo()`
4. Cap impossible values (suspensions > enrollment)
5. Join school attributes (locale, school level, traditional status)
6. Backfill missing school characteristics using district-level modal values
7. Create intersectional combinations (e.g., Black SPED students in elementary schools)

**Outputs**: Excel summaries and CSV flags documenting intersectional suspension patterns

**Rationale**: Suspension disparities compound at intersections. Black students with disabilities may face different treatment than White students with disabilities, and these patterns may vary by school level. This analysis documents these intersectional patterns.

---

### Analysis 18: Comprehensive Suspension Rates (`Analysis/18_comprehensive_suspension_rates_analysis.R`)
**Data sources**:
- `susp_v6_long.parquet`
- `susp_v6_features.parquet`

**Processing**:
1. Restrict to traditional campus-level schools
2. Calculate pooled elementary school suspension rates by race × year
3. Compare to Python pipeline (`06_statewide_trends.py`) outputs
4. Validate that American Indian/Alaska Native rates match within rounding tolerance

**Purpose**: Cross-validation between R and Python workflows ensures consistent results across all dashboard components

**Outputs**: `diagnostic_alignment_elementary.csv` (pipeline validation report)

---

## Key Helper Functions and Utilities

### `R/utils_keys_filters.R`
**Campus filtering**: `filter_campus_only()` removes:
- District aggregates (`aggregate_level` != School)
- Statewide placeholder codes (`0000000`, `0000001`)

**School level classification**:
- `span_label()`: Maps grade ranges to Elementary/Middle/High/Other
- `is_alt()`: Identifies alternative education programs

**Race standardization**:
- `canon_race_label()`: Harmonizes race/ethnicity labels across years

**Key builders**:
- `build_keys()`: Creates standardized CDS codes with proper padding
- `ensure_keys()`: Validates key consistency for joins

**Locale standardization**:
- `locale_levels`: Defines factor ordering for City/Suburban/Town/Rural/Unknown

---

### `R/demographic_labels.R`
Provides the complete mapping between:
- CDE demographic codes (e.g., "SWD")
- Canonical labels (e.g., "Students with Disabilities")
- Category assignments (e.g., "Special Education")

This ensures consistent labeling across all analyses and prevents mismatches between datasets from different years.

---

### `R/ingest_helpers.R`
**Column type detection**:
- `numeric_cols()`: Identifies which columns contain numeric data

**Safe calculations**:
- `safe_rate()`: Computes rates with minimum enrollment thresholds
- `safe_div()`: Division with zero-denominator protection
- `safe_max()`: Maximum with NA handling

**Data quality**:
- `drop_impossible()`: Removes records where suspensions > enrollment
- `rng_ok()`: Validates that rates fall within [0, 1]

---

## Data Quality Assurance

### Suppression Handling
- All suppression flags are created **before** numeric parsing
- Analysts can identify privacy-protected cells and interpret zeros appropriately
- Small cell counts (<10 students in most cases) are flagged but not removed

### Rate Calculation Safety
- Rates only computed when enrollment ≥ 10 (unless explicitly overridden)
- Division by zero returns `NA` rather than error
- Impossible values (suspensions > enrollment) flagged and removed

### Uniqueness Checks
- Campus-year datasets assert exactly one row per campus-year
- Duplicate detection prevents accidental double-counting

### Cross-Validation
- Python and R pipelines process the same source data
- Diagnostic outputs compare results within rounding tolerance
- Statewide totals verified against CDE published reports

### Range Validation
- Proportions constrained to (0, 1]
- Enrollment counts must be non-negative
- Suspension counts must be ≤ enrollment counts

---

## Final Datasets and Their Purposes

### `susp_v5.parquet` (567,047 rows)
**Grain**: One row per school-race-year combination

**Contents**:
- All enrollment and suspension counts by race
- Enrollment size quartiles
- Racial composition quartiles (Black, White, Hispanic)
- Suspension reason shares (proportions)
- School characteristics (locale, level, charter status)

**Used by**: Race-specific analyses, quartile comparisons, reason-specific graphs

---

### `susp_v5_long.parquet` (3,402,282 rows)
**Grain**: One row per school-race-reason-year combination

**Contents**: Same as `susp_v5` but with suspension reasons in rows rather than columns

**Used by**: Reason-specific visualizations, stacked bar charts, trend analyses by offense type

---

### `oth_long.parquet` (558,431 rows)
**Grain**: One row per school-subgroup-year combination

**Contents**:
- Demographic subgroup enrollment
- Demographic subgroup suspension counts
- Demographic subgroup suspension rates
- Canonical subgroup labels and categories

**Used by**: Demographic analyses (SPED, EL, SED, etc.), intersectional studies

---

### `susp_v6_features.parquet` (60,188 rows)
**Grain**: One row per school-year (campus-year level)

**Contents**:
- School identifiers and names
- Locale and school level classifications
- Traditional vs. alternative school flag
- Enrollment size quartiles
- Racial composition quartiles
- SPED, EL, SED, Foster, Homeless, Migrant suspension rates
- Sex-disaggregated suspension rates

**Used by**: School-level analyses, demographic comparisons, regression models, dashboard filters

---

### `susp_v6_long.parquet` (3,402,282 rows)
**Grain**: One row per school-race-year combination

**Contents**: Race-specific suspension data joined with school attributes from `susp_v6_features`

**Used by**: Cross-tabulations, statewide trends, race × demographic intersections

---

## Change Log and Versioning

### Dataset Versions
- **v0**: Raw ingestion
- **v1**: Locale added
- **v1_noall**: Charter "All" rows removed
- **v2**: Enrollment quartiles added
- **v3**: Racial composition quartiles added
- **v4**: School level classification added
- **v5**: Suspension reason shares added
- **v6**: Full demographic integration (final analytic dataset)

### Why Versioned Stages?
Each intermediate version can be inspected, validated, and used for specific analyses. If an issue is discovered, developers can trace back to the exact stage where it occurred and re-run only the necessary downstream stages.

---

## Questions or Issues?

For questions about data sources, processing decisions, or specific metrics, please refer to:
- **Raw data documentation**: CDE's FileSpec documentation for suspension data files
- **Code**: All processing scripts in `R/` directory with inline comments
- **Analysis scripts**: `Analysis/` directory for specific research questions
- **Validation outputs**: `outputs/` directory for diagnostic CSVs and summary tables

This pipeline is designed for **complete transparency**. Every number in every dashboard visualization can be traced back through these processing stages to the original CDE data file.
