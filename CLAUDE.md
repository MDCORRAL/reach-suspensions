# CLAUDE.md: AI Assistant Guide for REACH Suspensions Analysis

**Last Updated**: 2025-11-13
**Repository**: REACH Suspensions Analysis Pipeline
**Primary Languages**: R (data processing), Python (visualization)

---

## Table of Contents

1. [Repository Overview](#repository-overview)
2. [Architecture & Directory Structure](#architecture--directory-structure)
3. [Development Workflows](#development-workflows)
4. [Code Conventions & Standards](#code-conventions--standards)
5. [Data Pipeline](#data-pipeline)
6. [Testing & Validation](#testing--validation)
7. [Common Tasks](#common-tasks)
8. [Key Files Reference](#key-files-reference)
9. [Troubleshooting](#troubleshooting)

---

## Repository Overview

### Purpose
This repository implements a research-grade data analysis pipeline for analyzing student suspension data from the California Department of Education (CDE). The pipeline:
- Ingests raw CDE suspension data (2017-18 through 2023-24)
- Processes and enriches data through multiple stages
- Integrates teacher demographic data
- Generates publication-quality visualizations
- Produces interactive HTML dashboards
- Supports intersectional demographic analyses

### Key Characteristics
- **Research-Grade**: Extensive validation, audit trails, and documentation
- **Reproducible**: Package versioning (renv), environment variables, portable paths
- **Staged Processing**: Progressive data enrichment with validation at each step
- **Dual-Language**: R for data processing, Python for publication graphics
- **Transparent**: Comprehensive documentation and data lineage tracking

### Technologies
- **R**: dplyr, tidyr, arrow (parquet), ggplot2, testthat
- **Python**: pandas, matplotlib, pyarrow
- **Data Formats**: Parquet (staged data), Excel (raw CDE data), CSV (diagnostics)
- **Package Management**: renv (R), requirements.txt (Python)

---

## Architecture & Directory Structure

### Core Directories

```
reach-suspensions/
├── R/                          # Core data processing pipeline (5,408 lines)
│   ├── 00_paths.R              # Path configuration (ALWAYS source first)
│   ├── 01_ingest_*.R           # Data ingestion scripts
│   ├── 02-06_feature_*.R       # Feature engineering stages
│   ├── 22_build_v6_features.R  # Final dataset assembly
│   ├── utils_keys_filters.R    # Canonical definitions (CRITICAL)
│   ├── teacher_processing.R    # Teacher demographic utilities
│   ├── demographic_labels.R    # Demographic label mappings
│   ├── ingest_helpers.R        # Shared ingestion utilities
│   └── run_helper.R            # Pipeline execution utilities
│
├── Analysis/                   # Research-specific analyses (20+ scripts)
│   ├── 02_black_rates_by_quartiles.R  # Canonical quartile analysis
│   ├── 15_merge_demographic_categories.R
│   ├── 16_tail_concentration_analysis.R
│   ├── 17_tail_concentration_by_level.R
│   ├── 18_merge_teacher_student.R
│   └── data_processing_overview.md   # 660-line pipeline documentation
│
├── graph_scripts/              # Python visualization pipeline
│   ├── 06_statewide_trends.py  # Main trend generation
│   ├── palette_utils.py        # UCLA-branded color palettes
│   ├── data_sources.py         # Shared data loading utilities
│   └── requirements.txt        # Python dependencies (pinned versions)
│
├── data-stage/                 # Staged datasets (Parquet files)
│   ├── susp_v0.parquet         # Raw ingestion
│   ├── susp_v1.parquet         # + locale
│   ├── susp_v2.parquet         # + enrollment quartiles
│   ├── susp_v3.parquet         # + racial composition quartiles
│   ├── susp_v4.parquet         # + school level classification
│   ├── susp_v5.parquet         # + suspension reason shares
│   ├── susp_v6_features.parquet    # Final wide-format dataset
│   ├── susp_v6_long.parquet        # Final long-format dataset
│   ├── teacher_staff_long.parquet  # Teacher demographics
│   └── susp_v6_teacher_features.parquet  # Merged student + teacher data
│
├── outputs/                    # Analysis outputs
│   ├── graphs/                 # PNG/SVG visualizations
│   ├── tables/                 # Excel summaries
│   └── data_audit/             # Validation reports
│
├── tests/testthat/             # Unit tests
│   ├── test_utils_keys_filters.R
│   ├── test_teacher_processing.R
│   └── test_demographic_labels.R
│
├── dashboard/                  # Interactive web dashboard
│   ├── build_data.py           # Dashboard data preparation
│   └── data/                   # JSON payloads for web app
│
├── run_all.R                   # Execute full pipeline
├── run_pipeline.R              # Execute core pipeline only
├── renv.lock                   # R package versions (196KB)
└── .Renviron.example           # Environment variable template
```

### Data Flow Architecture

```
Raw CDE Excel Files
        ↓
[01_ingest_v0.R] → susp_v0.parquet
        ↓
[02_feature_locale_simple.R] → susp_v1.parquet
        ↓
[02b_drop_charter_all.R] → susp_v1_noall.parquet
        ↓
[03_feature_size_quartiles_TA.R] → susp_v2.parquet
        ↓
[04_feature_black_prop_quartiles.R] → susp_v3.parquet
        ↓
[05_feature_school_level.R] → susp_v4.parquet
        ↓
[06_feature_reason_shares.R] → susp_v5.parquet + susp_v5_long.parquet
        ↓
[22_build_v6_features.R] → susp_v6_features.parquet + susp_v6_long.parquet
        ↓
[Analysis/*.R] → Visualizations, Tables, Reports
```

**Parallel Pipeline: Teacher Demographics**
```
CDE Teacher TXT Files (stre*.txt)
        ↓
[01c_ingest_teacher_demographics.R] → teacher_staff_long.parquet
        ↓
[18_merge_teacher_student.R] → susp_v6_teacher_features.parquet
```

---

## Development Workflows

### Initial Setup

1. **Clone and activate R environment**:
   ```r
   # In R console
   renv::activate()
   renv::restore()  # Install exact package versions from renv.lock
   ```

2. **Install Python dependencies**:
   ```bash
   pip install -r graph_scripts/requirements.txt
   ```

3. **Configure environment variables** (optional, creates `.Renviron`):
   ```bash
   # Copy template
   cp .Renviron.example .Renviron

   # Edit to set paths if needed
   # REACH_PROJECT_ROOT=/path/to/reach-suspensions
   # REACH_DATA_DIR=/custom/data-stage
   # RAW_PATH=/custom/path/to/raw_file.xlsx
   ```

4. **Place raw data files** in `data-raw/`:
   - `copy_CDE_suspensions_1718-2324_sc_race.xlsx`
   - `copy_CDE_suspensions_1718-2324_sc_oth.xlsx`
   - Teacher TXT files: `stre1718.txt`, `stre1819.txt`, etc.

### Running the Pipeline

**Option 1: Full Pipeline** (recommended for comprehensive updates)
```r
source("run_all.R")
```
Executes:
1. Teacher demographic ingestion
2. Core pipeline (01-06, 22)
3. Canonical analyses
4. Tail concentration reports
5. Teacher-student merge

**Option 2: Core Pipeline Only**
```r
source("run_pipeline.R")
```
Executes: 01-06, 22 (student data only)

**Option 3: Individual Scripts**
```r
source("R/00_paths.R")  # ALWAYS source first
source("R/utils_keys_filters.R")  # If using canonical definitions
source("Analysis/02_black_rates_by_quartiles.R")  # Specific analysis
```

**Option 4: Python Visualizations**
```bash
cd graph_scripts
python 06_statewide_trends.py --diagnostics-only
```

### Making Changes to the Pipeline

#### Adding a New Feature to the Data

1. **Decide insertion point**: Which data version (v0-v6) should this feature join?
2. **Create new script**: `R/0X_feature_your_feature.R`
3. **Follow the pattern**:
   ```r
   # Guard against re-running in same session
   if (!exists(".ran_0X_feature_your_feature", envir = .GlobalEnv)) {
     message("=== 0X: Adding your_feature ===")

     source("R/00_paths.R")
     suppressPackageStartupMessages({
       library(dplyr); library(arrow)
     })

     # Read previous stage
     df <- read_parquet(file.path(dp_stage, "susp_vX.parquet"))

     # Add your feature
     df <- df %>%
       mutate(your_feature = ...)

     # Validate
     message(">>> Rows: ", nrow(df))
     message(">>> Feature distribution:")
     print(table(df$your_feature, useNA = "always"))

     # Write next stage
     write_parquet(df, file.path(dp_stage, "susp_vY.parquet"))

     assign(".ran_0X_feature_your_feature", TRUE, envir = .GlobalEnv)
   }
   ```
4. **Update downstream scripts**: Adjust input versions in subsequent scripts
5. **Update `run_pipeline.R`**: Add your script to the execution sequence
6. **Test thoroughly**: Run full pipeline, check diagnostics

#### Adding a New Analysis

1. **Create script in `Analysis/`**: Use descriptive name (e.g., `20_your_analysis.R`)
2. **Source required utilities**:
   ```r
   source("R/00_paths.R")
   source("R/utils_keys_filters.R")  # For canonical definitions
   ```
3. **Read final datasets**:
   ```r
   library(arrow)
   df <- read_parquet(file.path(dp_stage, "susp_v6_long.parquet"))
   ```
4. **Output to `outputs/`**:
   ```r
   ggsave(file.path(dp_out, "graphs", "your_plot.png"), plot, width = 10, height = 6)
   ```
5. **Document in analysis script**: Add comments explaining purpose, methods, outputs

### Adding Tests

1. **Create test file**: `tests/testthat/test_your_function.R`
2. **Follow testthat conventions**:
   ```r
   test_that("your_function handles edge cases", {
     expect_equal(your_function(input), expected_output)
     expect_error(your_function(invalid_input))
   })
   ```
3. **Run tests**:
   ```r
   testthat::test_dir("tests/testthat")
   ```

### Version Control Workflow

This repository follows standard Git practices:

1. **Create feature branch**:
   ```bash
   git checkout -b your-feature-branch
   ```

2. **Make changes and commit**:
   ```bash
   git add <files>
   git commit -m "Description of changes"
   ```

3. **Push changes**:
   ```bash
   git push -u origin your-feature-branch
   ```

4. **Data files**: `.gitignore` excludes large data files from version control

---

## Code Conventions & Standards

### General Principles

1. **Source utilities first**: Every script starts with `source("R/00_paths.R")`
2. **Pipeline re-run guards**: Use `.GlobalEnv` flags to prevent duplicate execution
3. **Progressive enrichment**: Each stage adds features without modifying prior stages
4. **Validation at each step**: Print diagnostics (row counts, distributions, ranges)
5. **Canonical definitions**: Use centralized constants from `utils_keys_filters.R`

### Naming Conventions

**Variables**: snake_case
- `cumulative_enrollment`, `total_suspensions`, `academic_year`
- `black_prop_q`, `white_share`, `is_traditional`

**Functions**: verb_noun pattern
- `filter_campus_only()`, `build_keys()`, `canon_race_label()`
- `safe_div()`, `safe_max()`, `drop_impossible()`

**Files**: lowercase with underscores
- `data-stage/susp_v6_long.parquet`
- `R/utils_keys_filters.R`

**Scripts**: Numbered prefixes indicate execution order
- `01-10`: Core data processing
- `15-20`: Analysis and merging
- `20-25`: Advanced analytics
- `90+`: Testing and validation utilities

### Canonical Definitions (CRITICAL)

**ALWAYS use definitions from `R/utils_keys_filters.R`**:

```r
# School levels
LEVEL_LABELS <- c("Elementary", "Middle", "High", "Other", "Alternative")
pal_level <- c(Elementary = "#1b9e77", Middle = "#d95f02", ...)

# Locales
locale_levels <- c("City", "Suburban", "Town", "Rural", "Unknown")
pal_locale <- c(City = "#0072B2", Suburban = "#009E73", ...)

# Suspension reasons
reason_labels  # tibble with reason codes and display labels
pal_reason     # color palette for reasons

# Race labels
ALLOWED_RACES  # canonical race/ethnicity labels
canon_race_label()  # function to map CDE codes to canonical labels

# Special school codes
SPECIAL_SCHOOL_CODES <- c("0000000", "0000001")  # Exclude from analyses
```

**DO NOT create ad-hoc labels or colors**. Always extend these canonical definitions.

### Safe Calculation Utilities

```r
# Division with zero-protection
safe_div <- function(num, denom, replace_na_with = NA_real_) {
  ifelse(denom == 0 | is.na(denom), replace_na_with, num / denom)
}

# Handle suppressed CDE values (asterisks)
parse_supp <- function(x, replace_na_with = NA_real_) {
  # Converts "*" to replace_na_with, parses numeric strings
}
```

### Data Quality Standards

1. **Suppression handling**: Flag asterisks before numeric parsing
2. **Minimum thresholds**: Typically 10 students for rate calculations
3. **Impossible values**: Drop suspensions > enrollment
4. **Uniqueness assertions**: Use `assert_unique_campus()`, `assert_unique_district()`
5. **Range validation**: Rates must be in [0, 1]
6. **NaN/Inf sanitization**: Replace with NA before writing outputs

### Code Structure Template

```r
# Script: R/XX_descriptive_name.R
# Purpose: Brief description of what this script does
# Input: susp_vX.parquet
# Output: susp_vY.parquet

# Guard against re-running
if (!exists(".ran_XX_descriptive_name", envir = .GlobalEnv)) {
  message("=== XX: Descriptive Name ===")

  # Source dependencies
  source("R/00_paths.R")
  source("R/utils_keys_filters.R")  # If using canonical definitions

  # Load packages
  suppressPackageStartupMessages({
    library(dplyr)
    library(arrow)
    # ... other packages
  })

  # Read input
  df <- read_parquet(file.path(dp_stage, "susp_vX.parquet"))
  message(">>> Input rows: ", nrow(df))

  # Process data
  df <- df %>%
    filter(...) %>%
    mutate(...)

  # Validate
  message(">>> Output rows: ", nrow(df))
  message(">>> Check key distributions:")
  print(summary(df$key_variable))

  # Write output
  write_parquet(df, file.path(dp_stage, "susp_vY.parquet"))
  message(">>> Wrote: ", file.path(dp_stage, "susp_vY.parquet"))

  # Set guard flag
  assign(".ran_XX_descriptive_name", TRUE, envir = .GlobalEnv)
}
```

### Documentation Standards

1. **Inline comments**: Explain **why**, not what (code shows what)
2. **Script headers**: Include purpose, inputs, outputs
3. **Diagnostic messages**: Use `message()` for pipeline progress
4. **Audit trails**: Document data transformations in analysis markdown files
5. **Citation standard**: Follow `CITATION_STANDARD.md` for all outputs

---

## Data Pipeline

### Pipeline Stages (Detailed)

#### Stage 0: Raw Ingestion (`01_ingest_v0.R`)
- **Input**: `data-raw/copy_CDE_suspensions_1718-2324_sc_race.xlsx`
- **Operations**:
  - Clean column names (`janitor::clean_names()`)
  - Parse numeric fields, preserve suppression asterisks
  - Build 14-digit CDS codes (county-district-school)
  - Standardize academic year format
- **Output**: `susp_v0.parquet`
- **Validation**: Check row counts, missing values, CDS code format

#### Stage 1: Locale Classification (`02_feature_locale_simple.R`)
- **Input**: `susp_v0.parquet`
- **Operations**:
  - Map NCES locale codes to categories: City, Suburban, Town, Rural, Unknown
  - Use `locale_levels` from `utils_keys_filters.R`
- **Output**: `susp_v1.parquet`
- **Validation**: Locale distribution, NA handling

#### Stage 1b: Charter Filtering (`02b_drop_charter_all.R`)
- **Input**: `susp_v1.parquet`
- **Operations**:
  - Remove aggregate charter school records (avoid double-counting)
  - Filter to campus-level data only
- **Output**: `susp_v1_noall.parquet`
- **Validation**: Row count reduction, no duplicate CDS codes

#### Stage 2: Enrollment Quartiles (`03_feature_size_quartiles_TA.R`)
- **Input**: `susp_v1_noall.parquet`
- **Operations**:
  - Calculate year-specific enrollment quartiles
  - Separate quartiles for Traditional vs. Alternative schools
  - Use cumulative enrollment for grouping
- **Output**: `susp_v2.parquet`
- **Validation**: Quartile boundaries, distribution balance

#### Stage 3: Racial Composition Quartiles (`04_feature_black_prop_quartiles.R`)
- **Input**: `susp_v2.parquet`
- **Operations**:
  - Calculate Black student proportion: `black_share = black_enrollment / total_enrollment`
  - Compute year-specific quartiles of Black student concentration
  - Label quartiles: Q1 (lowest), Q2, Q3, Q4 (highest)
- **Output**: `susp_v3.parquet`
- **Validation**: Quartile ranges, edge cases (all-Black schools, no Black students)

#### Stage 4: School Level Classification (`05_feature_school_level.R`)
- **Input**: `susp_v3.parquet`
- **Operations**:
  - Use `span_label()` to classify schools by grade span
  - Elementary (K-5), Middle (6-8), High (9-12), Other, Alternative
  - Check `is_alt()` for alternative program keywords
- **Output**: `susp_v4.parquet`
- **Validation**: Level distribution, Alternative school detection

#### Stage 5: Suspension Reason Shares (`06_feature_reason_shares.R`)
- **Input**: `susp_v4.parquet`
- **Operations**:
  - Calculate proportion of suspensions for each reason
  - Reshape to long format with one row per school-year-reason
  - Add `reason_lab` using `add_reason_label()`
- **Output**: `susp_v5.parquet`, `susp_v5_long.parquet`
- **Validation**: Shares sum to 1.0, reason labels match canonical definitions

#### Stage 6: Final Dataset Assembly (`22_build_v6_features.R`)
- **Input**: `susp_v5.parquet`, `oth_long.parquet` (other demographics)
- **Operations**:
  - LEFT JOIN demographic data (SPED, EL, SED, etc.)
  - Create wide-format feature matrix
  - Generate long-format analytic dataset
  - Apply `is_traditional` flag from features file
  - Sanitize NaN/Inf values
- **Output**: `susp_v6_features.parquet`, `susp_v6_long.parquet`
- **Validation**: Row retention, join coverage, missing data patterns

### Teacher Demographics Pipeline

#### Ingestion (`01c_ingest_teacher_demographics.R`)
- **Input**: `data-raw/stre*.txt` (CDE teacher staff files)
- **Operations**:
  - Read fixed-width TXT files
  - Standardize race/ethnicity codes (9 CDE categories)
  - Parse gender codes: GF (Female), GM (Male), GX (Non-Binary), GZ (Missing), ALL
  - Parse staff types: TCH (Teachers), ADM (Administrators), PSV (Pupil Services), OTH (Other), ALL
  - Aggregate by campus-year-race-gender-staff_type
  - Filter to school-level data (`aggregate_level = "S"`)
- **Output**: `teacher_staff_long.parquet`
- **Validation**: Staff type preservation, zero-value retention

#### Summarization (`teacher_processing.R::teacher_summarise_long()`)
- **Input**: `teacher_staff_long.parquet`
- **Operations**:
  - Aggregate to one row per school-year
  - Calculate totals, racial shares, gender shares, staff type breakdowns
  - Create wide-format columns: `teacher_staff_count_total`, `teacher_staff_count_african_american_share`, etc.
- **Output**: Wide-format summary (used in merge)
- **Validation**: Share ranges [0, 1], zero handling

#### Merging (`18_merge_teacher_student.R`)
- **Input**: `susp_v6_long.parquet`, teacher summaries
- **Join Keys**: `academic_year` + `cds_school` (14-digit code)
- **Join Type**: LEFT JOIN (preserve all student data)
- **Operations**:
  - Validate key uniqueness
  - Sanitize NaN/Inf in teacher data
  - Report coverage statistics
- **Output**: `susp_v6_teacher_features.parquet`
- **Validation**: 100% student data retention, teacher coverage report

### Data Versioning

| Version | Features Added | Use Cases |
|---------|----------------|-----------|
| v0 | Raw CDE data | Data quality checks |
| v1 | + Locale | Geographic analyses |
| v2 | + Enrollment quartiles | Size-stratified analyses |
| v3 | + Racial composition quartiles | Segregation analyses |
| v4 | + School level | Grade-level analyses |
| v5 | + Reason shares | Suspension reason analyses |
| v6 | + Other demographics | Intersectional analyses |

**When to use which version**:
- **Analysis-specific features**: Use earliest version with required features
- **Publication outputs**: Use v6 (most complete)
- **Quick prototypes**: v0 or v1 (faster to load)

---

## Testing & Validation

### Unit Tests

**Location**: `tests/testthat/`

**Run tests**:
```r
testthat::test_dir("tests/testthat")
```

**Coverage**:
- `test_utils_keys_filters.R`: Palette definitions, reason labels, uniqueness assertions
- `test_teacher_processing.R`: Teacher demographic processing
- `test_demographic_labels.R`: Label mapping functions
- `test_statewide_school_type.R`: School type classification

### Validation Checkpoints

**Every pipeline script should include**:
1. **Row count checks**:
   ```r
   message(">>> Input rows: ", nrow(df_in))
   message(">>> Output rows: ", nrow(df_out))
   ```

2. **Distribution summaries**:
   ```r
   print(table(df$key_variable, useNA = "always"))
   summary(df$numeric_variable)
   ```

3. **Range validation**:
   ```r
   stopifnot(all(df$rate >= 0 & df$rate <= 1, na.rm = TRUE))
   ```

4. **Uniqueness assertions** (from `utils_keys_filters.R`):
   ```r
   assert_unique_campus(df)  # One row per campus-year-race
   assert_unique_district(df)  # One row per district-year-race
   ```

### Cross-Validation

**R vs. Python Pipeline**:
```bash
# Generate diagnostics from Python
python graph_scripts/06_statewide_trends.py --diagnostics-only

# Output: outputs/graphs/diagnostics/statewide_elementary_rates.csv
```

**Compare in R**:
```r
# Read both outputs
r_rates <- ...
py_rates <- read_csv(file.path(dp_out, "graphs/diagnostics/statewide_elementary_rates.csv"))

# Check alignment (within rounding tolerance)
all.equal(r_rates$rate, py_rates$rate, tolerance = 1e-4)
```

### Data Retention Validation

```r
source("R/validate_data_retention.R")
# Generates detailed report of data retention across pipeline stages
```

### Audit Reports

The repository includes comprehensive audit documentation:
- `AUDIT_REPORT_DATA_CONSISTENCY.md`: Data consistency checks
- `COMPREHENSIVE_AUDIT_REPORT.md`: Full pipeline audit
- `TEACHER_DEMOGRAPHIC_INTEGRATION_AUDIT.md`: Teacher merge validation

---

## Common Tasks

### Task 1: Add a New School-Level Feature

**Scenario**: Add a new binary flag `is_high_poverty` based on SED proportion.

```r
# Create R/07_feature_high_poverty.R
if (!exists(".ran_07_feature_high_poverty", envir = .GlobalEnv)) {
  message("=== 07: Add High Poverty Flag ===")

  source("R/00_paths.R")
  suppressPackageStartupMessages({
    library(dplyr); library(arrow)
  })

  df <- read_parquet(file.path(dp_stage, "susp_v6_long.parquet"))

  df <- df %>%
    mutate(
      # You'll need to join or calculate SED share first
      is_high_poverty = sed_share >= 0.75
    )

  write_parquet(df, file.path(dp_stage, "susp_v7_long.parquet"))

  assign(".ran_07_feature_high_poverty", TRUE, envir = .GlobalEnv)
}
```

### Task 2: Create a Custom Visualization

```r
# Analysis/21_custom_plot.R
source("R/00_paths.R")
source("R/utils_keys_filters.R")

suppressPackageStartupMessages({
  library(dplyr); library(arrow); library(ggplot2)
})

# Read data
df <- read_parquet(file.path(dp_stage, "susp_v6_long.parquet"))

# Filter and prepare
plot_data <- df %>%
  filter(academic_year == "2023-24", !is.na(black_share)) %>%
  mutate(black_prop_q = factor(black_prop_q, levels = 1:4))

# Create plot using canonical colors
p <- ggplot(plot_data, aes(x = black_prop_q, y = suspension_rate)) +
  geom_boxplot(aes(fill = school_level)) +
  scale_fill_manual(values = pal_level) +  # Canonical palette
  labs(
    title = "Suspension Rates by Black Proportion Quartile",
    x = "Black Student Proportion Quartile",
    y = "Suspension Rate"
  ) +
  theme_minimal()

# Save to outputs
ggsave(
  file.path(dp_out, "graphs", "custom_plot.png"),
  p, width = 10, height = 6, dpi = 300
)
```

### Task 3: Merge External Dataset

```r
# Analysis/22_merge_external_data.R
source("R/00_paths.R")
suppressPackageStartupMessages({
  library(dplyr); library(arrow)
})

# Read suspension data
susp <- read_parquet(file.path(dp_stage, "susp_v6_long.parquet"))

# Read external data
external <- read_csv("path/to/external_data.csv")

# Prepare join keys (14-digit CDS codes, academic year)
external <- external %>%
  mutate(
    cds_school = str_pad(cds_code, 14, pad = "0"),  # Ensure 14 digits
    academic_year = paste0(year_start, "-", str_sub(year_end, 3, 4))
  )

# Validate uniqueness
stopifnot(!any(duplicated(external[c("cds_school", "academic_year")])))

# LEFT JOIN to preserve suspension data
merged <- susp %>%
  left_join(
    external,
    by = c("cds_school", "academic_year")
  )

# Validate retention
message(">>> Suspension rows: ", nrow(susp))
message(">>> Merged rows: ", nrow(merged))
stopifnot(nrow(merged) == nrow(susp))  # Should be equal

# Report coverage
coverage <- merged %>%
  summarise(
    external_coverage = mean(!is.na(external_variable))
  )
print(coverage)

# Write output
write_parquet(merged, file.path(dp_stage, "susp_v7_external.parquet"))
```

### Task 4: Calculate Aggregate Statistics

```r
# Calculate statewide suspension rates by race and year
source("R/00_paths.R")
source("R/utils_keys_filters.R")

library(dplyr); library(arrow)

df <- read_parquet(file.path(dp_stage, "susp_v6_long.parquet"))

statewide_rates <- df %>%
  filter(
    race %in% ALLOWED_RACES,  # Use canonical race list
    race != "Not Reported"
  ) %>%
  group_by(academic_year, race) %>%
  summarise(
    total_enrollment = sum(cumulative_enrollment, na.rm = TRUE),
    total_suspensions = sum(total_suspensions, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    suspension_rate = safe_div(total_suspensions, total_enrollment)
  )

# Write to outputs
write_csv(
  statewide_rates,
  file.path(dp_out, "tables", "statewide_rates_by_race.csv")
)
```

### Task 5: Debugging Pipeline Failures

```r
# If a pipeline script fails, debug interactively:

# 1. Source dependencies manually
source("R/00_paths.R")
source("R/utils_keys_filters.R")
library(dplyr); library(arrow)

# 2. Read the input file
df <- read_parquet(file.path(dp_stage, "susp_vX.parquet"))

# 3. Run transformations line by line
df <- df %>%
  filter(...) %>%  # Check output at each step
  mutate(...)

# 4. Inspect intermediate results
glimpse(df)
summary(df$problematic_column)
table(df$categorical_column, useNA = "always")

# 5. Check for common issues:
# - Missing values: sum(is.na(df$column))
# - Out-of-range values: range(df$numeric_column, na.rm = TRUE)
# - Duplicate keys: df %>% count(key_column) %>% filter(n > 1)
```

---

## Key Files Reference

### Must-Read Files for New Contributors

1. **README.md**: Setup instructions, package management, key concepts
2. **Analysis/data_processing_overview.md**: 660-line comprehensive pipeline documentation
3. **R/utils_keys_filters.R**: Canonical definitions (CRITICAL)
4. **R/00_paths.R**: Path configuration
5. **CITATION_STANDARD.md**: Citation requirements for outputs

### Configuration Files

- **.Renviron.example**: Environment variable template
- **renv.lock**: R package versions (DO NOT edit manually)
- **graph_scripts/requirements.txt**: Python dependencies
- **.gitignore**: Excludes data files from version control

### Documentation Files

| File | Purpose |
|------|---------|
| `README.md` | Quick start, setup, key concepts |
| `Analysis/data_processing_overview.md` | Complete pipeline documentation |
| `AUDIT_REPORT_DATA_CONSISTENCY.md` | Data consistency audit |
| `COMPREHENSIVE_AUDIT_REPORT.md` | Full pipeline audit |
| `TEACHER_DEMOGRAPHIC_INTEGRATION_AUDIT.md` | Teacher merge validation |
| `CITATION_STANDARD.md` | Standard citation for outputs |
| `UCLA-Brand-Colors.md` | Color palette documentation |

---

## Troubleshooting

### Common Errors

#### Error: "Raw file not found"
```
!! Could not find the raw Excel file in any of these locations:
   - /path/to/file.xlsx
Raw file not found. Set RAW_PATH env var or place the file in data-raw/.
```

**Solution**:
1. Place raw file in `data-raw/copy_CDE_suspensions_1718-2324_sc_race.xlsx`, OR
2. Set environment variable: `RAW_PATH=/custom/path/to/file.xlsx` in `.Renviron`

#### Error: "object 'dp_stage' not found"
```
Error: object 'dp_stage' not found
```

**Solution**: Source `R/00_paths.R` at the beginning of your script:
```r
source("R/00_paths.R")
```

#### Error: Package not found
```
Error: package 'dplyr' not found
```

**Solution**: Restore renv environment:
```r
renv::restore()
```

#### Error: "Unexpected reason codes"
```
Error in add_reason_label(): Unexpected reason codes: custom_reason
```

**Solution**: Add new reason codes to `reason_labels` in `R/utils_keys_filters.R`:
```r
reason_labels <- dplyr::tibble(
  reason = c(
    "violent_injury", "violent_no_injury", "weapons_possession",
    "illicit_drug", "defiance_only", "other_reasons",
    "custom_reason"  # Add your new code
  ),
  reason_lab = c(
    "Violent (Injury)", "Violent (No Injury)", "Weapons",
    "Illicit Drugs", "Willful Defiance", "Other",
    "Custom Reason"  # Add display label
  )
)
```

#### Error: Join produces more rows than expected
```
# After left_join, nrow(result) > nrow(left_df)
```

**Solution**: Check for duplicate keys in right dataframe:
```r
right_df %>%
  count(key_column1, key_column2) %>%
  filter(n > 1)
```

### Performance Issues

#### Slow parquet reads
```r
# Use column selection to read only needed columns
df <- read_parquet(
  file.path(dp_stage, "susp_v6_long.parquet"),
  col_select = c("academic_year", "cds_school", "suspension_rate")
)
```

#### Memory issues with large datasets
```r
# Process in chunks by academic year
years <- unique(df$academic_year)
results <- lapply(years, function(yr) {
  df %>%
    filter(academic_year == yr) %>%
    # ... process ...
})
combined <- bind_rows(results)
```

### Python-R Interoperability

#### Parquet files not compatible
**Solution**: Ensure consistent pyarrow and arrow versions:
```bash
pip install pyarrow==21.0.0  # Match R arrow expectations
```

#### Character encoding issues
**Solution**: Use UTF-8 consistently:
```r
# R
write_csv(df, file, fileEncoding = "UTF-8")

# Python
df.to_csv(file, encoding="utf-8")
```

---

## Best Practices for AI Assistants

### When Working on This Codebase

1. **Always start by understanding the data version**: Which `susp_vX.parquet` file is appropriate?
2. **Source R/00_paths.R first**: Ensures consistent path handling
3. **Use canonical definitions**: Don't create new labels/colors; extend existing ones
4. **Validate at every step**: Print row counts, distributions, ranges
5. **Preserve data retention**: Use LEFT JOINs when merging to preserve primary data
6. **Document transformations**: Add comments explaining "why", not "what"
7. **Test incrementally**: Run transformations line-by-line before full pipeline
8. **Check audit trails**: Review existing audit reports before making breaking changes
9. **Respect the staged architecture**: Don't skip stages; add new stages cleanly
10. **Cross-validate**: When possible, verify R outputs against Python outputs

### When Suggesting Changes

1. **Propose insertion points**: "This feature should join at v3 stage because..."
2. **Show full context**: Include sourcing, validation, and guard flags
3. **Explain impacts**: "This will affect downstream scripts: 22, Analysis/02, Analysis/18"
4. **Provide rollback plan**: "If issues arise, revert by removing line X and re-running from stage Y"
5. **Reference documentation**: Link to relevant sections in data_processing_overview.md

### When Debugging

1. **Check pipeline sequence**: Has a prior stage failed?
2. **Verify inputs exist**: Do the expected .parquet files exist in data-stage/?
3. **Inspect intermediate outputs**: Read .parquet files at each stage
4. **Compare with documentation**: Does behavior match data_processing_overview.md?
5. **Review audit reports**: Are there known issues documented?

---

## Contact and Resources

- **Repository**: This repository (reach-suspensions)
- **R Documentation**: https://www.rdocumentation.org/
- **Arrow (Parquet)**: https://arrow.apache.org/docs/r/
- **dplyr**: https://dplyr.tidyverse.org/
- **ggplot2**: https://ggplot2.tidyverse.org/
- **renv**: https://rstudio.github.io/renv/

---

**End of CLAUDE.md**

*This document should be updated whenever major architectural changes occur, new conventions are established, or additional workflows are introduced.*
