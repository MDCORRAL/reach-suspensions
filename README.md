## R Project Setup with renv

This project uses [`renv`](https://rstudio.github.io/renv/) to manage R package versions.

### First time setup
```r
# Activate renv in this project
renv::activate()

# Install the exact package versions from renv.lock
renv::restore()

# After installing/removing packages
renv::snapshot()

# Just restore to sync packages with renv.lock
renv::restore()
```

### Canonical race labels

`R/utils_keys_filters.R` defines `canon_race_label()` and `ALLOWED_RACES` used across analyses. The function maps CRDC code `RD` and strings such as "Not Reported" to the canonical label "Not Reported." This category is tracked for completeness but omitted from plots to avoid conflating missing data with student populations.

## Python prerequisites for graph scripts

Homebrew-managed Python installs block `pip install` outside a virtual environment (PEP 668). Use the helper script to set up an isolated venv and avoid the "externally-managed-environment" error:

```bash
bash scripts/utilities/setup_python_env.sh
# then, in new shells:
source .venv/bin/activate
```

The script will create `.venv/` if it does not exist and install the packages listed in `graph_scripts/requirements.txt`. From RStudio, point `reticulate` at the same interpreter so Python chunks use the venv:

```r
Sys.setenv(RETICULATE_PYTHON = "<repo-path>/.venv/bin/python")
```

To run the scripts without the helper, create your own venv and install dependencies manually:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r graph_scripts/requirements.txt
```

## Statewide analytic dataset

Statewide scripts—including the Python trends generator and the comprehensive R
analysis—now share the long-form staged file `data-stage/susp_v6_long.parquet`
and join `data-stage/susp_v6_features.parquet` for the `is_traditional` flag.
Both pipelines filter to campus-level records, remove placeholder school codes
(`0000000`, `0000001`), default missing traditional flags to `TRUE`, and focus
on Traditional schools unless the loader configuration is changed explicitly.

Running `python graph_scripts/06_statewide_trends.py --diagnostics-only`
produces `outputs/graphs/diagnostics/statewide_elementary_rates.csv`, which the
comprehensive R workflow reads to verify that pooled rates (e.g., American
Indian/Alaska Native elementary suspension rates) match across languages within
rounding tolerance.

## Analysis scripts

The canonical analysis of Black student suspension rates by school racial composition lives at
`Analysis/02_black_rates_by_quartiles.R`.

## Teacher Demographics Integration

The repository includes a complete pipeline for integrating teacher demographic data with student suspension data, enabling analyses of teacher-student demographic matching and staffing composition.

### Data Source

Teacher demographics come from CDE Teacher Staff Demographics TXT files following the pattern `stre{YYZZ}.txt` (e.g., `stre1920.txt` for the 2019-20 academic year). These files contain school-level data on staff by:

- **Race/Ethnicity**: 9 CDE categories (African American, American Indian/Alaska Native, Asian, Filipino, Hispanic/Latino, Native Hawaiian/Pacific Islander, White, Two or More Races, Not Reported)
- **Gender**: Female (GF), Male (GM), Non-Binary (GX), Missing (GZ), All Staff (ALL)
- **Staff Type** (`reporting_category`):
  - `TCH` = Teachers (classroom teachers, instructional staff)
  - `ADM` = Administrators (principals, assistant principals)
  - `PSV` = Pupil Services (counselors, psychologists, social workers, nurses)
  - `OTH` = Other Non-Instructional Staff (clerical, custodial, etc.)
  - `ALL` = All Staff (aggregate across all types)

### Processing Pipeline

**Step 1: Ingestion** (`R/01c_ingest_teacher_demographics.R`)
- Reads TXT files, standardizes columns, validates against CDE specifications
- Filters to school-level data (`aggregate_level = "S"`)
- Aggregates by campus-year-race-gender-staff_type
- Outputs: `data-stage/teacher_staff_long.parquet` (long format)

**Step 2: Summarization** (`R/teacher_processing.R`)
- `teacher_summarise_long()` aggregates to one row per school-year
- Calculates totals, shares by race, shares by gender, shares by staff type
- Outputs wide-format summary with columns like:
  - `teacher_staff_count_total`
  - `teacher_staff_count_african_american`, `teacher_staff_count_african_american_share`
  - `teacher_staff_count_by_gender_female`, `teacher_staff_count_by_gender_female_share`
  - `teacher_staff_count_by_type_teachers` (staff type breakdowns)

**Step 3: Merging** (`Analysis/18_merge_teacher_student.R`)
- LEFT JOIN to preserve all student suspension data
- Join keys: `academic_year` + `cds_school` (14-digit CDS code)
- Validates uniqueness, sanitizes NaN/Inf, reports coverage
- Outputs: `data-stage/susp_v6_teacher_features.parquet`

### Example Analysis

```r
# Load merged data
library(arrow)
library(dplyr)

data <- read_parquet("data-stage/susp_v6_teacher_features.parquet")

# Calculate teacher-student racial match rates
data %>%
  filter(!is.na(teacher_staff_count_african_american)) %>%
  mutate(
    black_student_share = black_share,
    black_teacher_share = teacher_staff_count_african_american_share
  ) %>%
  select(academic_year, school_name, black_student_share, black_teacher_share)
```

### Key Features

- **Staff Type Disaggregation**: Analyze teachers separately from administrators
- **Zero-Value Retention**: Keeps schools with 0 staff in specific categories (meaningful for equity analysis)
- **Comprehensive Validation**: Multiple checkpoints ensure staff type data is preserved throughout pipeline
- **Audit Trails**: Data lineage tracking, parsing issue logs, outlier flagging

### Data Retention

The teacher merge uses a LEFT JOIN to ensure **100% of student suspension data is preserved**. Teacher coverage varies by school and year based on CDE reporting. Run `R/validate_data_retention.R` to generate a detailed retention report.

## Environment variables

These optional environment variables allow the project to run without hard-coded paths. Set them in your shell or `.Renviron`.

- `REACH_PROJECT_ROOT`: path to the project root for R utilities. Defaults to the current working directory if unset.
- `REACH_SUSPENSIONS_ROOT`: optional override for Python graph scripts. Defaults to the auto-detected repository root.
- `REACH_DATA_DIR`: directory for staged data files. Defaults to `data-stage/` under the project root.
- `RAW_PATH`: full path to the raw Excel file `copy_CDE_suspensions_1718-2324_sc_race.xlsx`. Defaults to `data-raw/` under the project root.
- `TEACHER_RAW_DIR`: path to directory containing teacher TXT files (`stre*.txt`). Defaults to `data-raw/`.
- `OTH_RAW_PATH`: path to the other demographics Excel file. Defaults to `data-raw/copy_CDE_suspensions_1718-2324_sc_oth.xlsx`.

