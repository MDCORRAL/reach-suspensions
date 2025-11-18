# Teacher Demographics Script - Audit Trail Enhancements

## Overview
Enhanced the teacher demographics ingestion script (`R/01c_ingest_teacher_demographics.R`) with comprehensive audit trails and data lineage tracking to provide production-quality transparency and reproducibility.

## Enhancements Added

### 1. Parsing Issues Log (`teacher_parsing_log.csv`)
**Purpose:** Document all parsing problems encountered when reading raw teacher data files.

**Implementation:**
- Captures `readr::problems()` immediately after reading each file
- Stores problems as attributes on the raw data frame
- Aggregates all parsing issues across files
- Writes summary to `data-stage/teacher_parsing_log.csv`

**Output columns:**
- `source_file`: Name of the file with issues
- `n_problems`: Number of parsing problems detected
- `problem_rows`: Sample of affected row numbers (first 5)

**Value:** Provides traceable record of data quality issues (embedded tabs, column mismatches, etc.) for downstream investigation.

---

### 2. Data Lineage Summary (`teacher_data_lineage.csv`)
**Purpose:** Track row counts through each major transformation step.

**Implementation:**
- Captures row counts at 6 critical transformation points:
  1. Raw files loaded (after reading all files)
  2. After combining files (`list_rbind`)
  3. After campus-level filtering
  4. After aggregation by key columns
  5. After pivoting to long format
  6. Final dataset (after removing zeros)
- Calculates percentage retained at each step
- Writes to `data-stage/teacher_data_lineage.csv`

**Output columns:**
- `step`: Description of transformation step
- `n_rows`: Row count after this step
- `pct_retained`: Percentage of original rows retained

**Value:** Provides complete data provenance, showing exactly where data reduction occurs (e.g., 60% sparsity from zero filtering is expected behavior).

---

### 3. Large Schools Verification (`teacher_large_schools_to_verify.csv`)
**Purpose:** Flag unusually large staff counts (>1000) that warrant manual verification.

**Implementation:**
- Filters final dataset for staff counts exceeding 1000
- Captures school, year, gender, race, and count details
- Sorts by staff count (descending)
- Writes to `data-stage/teacher_large_schools_to_verify.csv`

**Output columns:**
- `cds_school`: School identifier
- `academic_year`: School year
- `staff_gender_code`: Gender code (M, F, ALL)
- `race_ethnicity`: Race/ethnicity category
- `staff_count`: Number of staff (>1000)

**Value:** Enables validation of extreme values (e.g., schools with 2,952 White staff) by cross-referencing with CDE's public school directory.

---

## Textbook Principles Applied

From **"R Programming for Data Science"** (§10.4):
> "Keep audit trails of data quality issues for reproducibility."

From **"Advanced Data Analysis"** (§1.3):
> "The goal of exploratory data analysis is not just to produce clean data, but to understand the data generation process deeply enough to make informed analytical decisions."

## Files Modified
- `R/01c_ingest_teacher_demographics.R`

## Files Generated (when script runs)
- `data-stage/teacher_parsing_log.csv`
- `data-stage/teacher_data_lineage.csv`
- `data-stage/teacher_large_schools_to_verify.csv`

## Usage
Run the enhanced script as normal:
```bash
Rscript R/01c_ingest_teacher_demographics.R
```

The three audit trail CSV files will be automatically generated in `data-stage/` directory.

## Next Steps
1. Review `teacher_large_schools_to_verify.csv` and cross-reference flagged CDS codes with CDE public school directory
2. Monitor `teacher_parsing_log.csv` for recurring parsing issues that may need upstream fixes
3. Use `teacher_data_lineage.csv` to document and justify data reduction decisions in reports

---

**Date:** 2025-11-06
**Script Version:** 01c_ingest_teacher_demographics.R (with audit trail enhancements)
