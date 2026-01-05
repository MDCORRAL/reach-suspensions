# REACH Data Provenance Audit Report

**Generated**: 2026-01-05
**Repository**: MDCORRAL/reach-suspensions
**Auditor**: Claude Code (Automated Provenance Audit)

---

## Executive Summary

This report documents the data provenance for the REACH Suspensions Analysis pipeline, tracing all data elements from their raw sources through transformation scripts to final analytic datasets in `data-stage/`.

### Key Findings

1. **All data originates from California Department of Education (CDE)** - No external or third-party data sources are used
2. **Three primary raw data sources** power the analysis:
   - Suspension data by race/ethnicity (Excel)
   - Suspension data by other demographics (Excel)
   - Teacher/Staff demographics (TXT files)
3. **Locale classification is derived from CDE raw data** - The `locale_simple` field is created by parsing the `school_locale` field from the raw CDE suspension Excel file, NOT from an external SchoolSites file
4. **Complete data lineage is traceable** for all variables in the final datasets

---

## 1. Raw Data Inventory

### 1.1 Primary Suspension Data (Race/Ethnicity)

| Attribute | Value |
|-----------|-------|
| **File** | `copy_CDE_suspensions_1718-2324_sc_race.xlsx` |
| **Location** | `data-raw/` |
| **Source** | California Department of Education (CDE) |
| **Years Covered** | 2017-18 through 2023-24 |
| **Aggregation Level** | School-level (S), District (D), County (C), State (T) |
| **Key Variables** | `cumulative_enrollment`, `total_suspensions`, suspension reason counts, race/ethnicity codes |
| **Ingestion Script** | `R/01_ingest_v0.R` |
| **Output** | `data-stage/susp_v0.parquet` |

### 1.2 Other Demographics Data

| Attribute | Value |
|-----------|-------|
| **File** | `copy_CDE_suspensions_1718-2324_sc_oth.xlsx` |
| **Location** | `data-raw/` |
| **Source** | California Department of Education (CDE) |
| **Demographics** | Special Education (SPED), English Learners (EL), Foster Youth, Homeless, Migrant, Socioeconomically Disadvantaged (SED), Gender |
| **Ingestion Script** | `R/01b_ingest_demographics.R` |
| **Output** | `data-stage/oth_long.parquet` |

### 1.3 Teacher/Staff Demographics

| Attribute | Value |
|-----------|-------|
| **Files** | `stre{YYZZ}.txt` (e.g., `stre1920.txt` for 2019-20) |
| **Location** | `data-raw/` |
| **Source** | California Department of Education (CDE) Staff Demographics |
| **Format** | Tab-separated values (TSV) |
| **Years Covered** | 2019-20 through 2024-25 |
| **Dimensions** | Race/Ethnicity (9 categories), Gender (4 codes), Staff Type (TCH/ADM/PSV/OTH/ALL), Grade Span |
| **Ingestion Script** | `R/01c_ingest_teacher_demographics.R` |
| **Output** | `data-stage/teacher_staff_long.parquet` |

### 1.4 External Data Sources

**IMPORTANT FINDING**: The user's prompt mentioned an external locale classification file (`SchoolSites2425.csv`). **This file is NOT used in the current codebase.**

The `locale_simple` variable is derived entirely from the `school_locale` field that already exists in the raw CDE suspension data (`copy_CDE_suspensions_1718-2324_sc_race.xlsx`). The derivation occurs in `R/02_feature_locale_simple.R` using regex pattern matching:

```r
locale_simple = case_when(
  str_detect(str_to_lower(school_locale), "city")     ~ "City",
  str_detect(str_to_lower(school_locale), "suburban") ~ "Suburban",
  str_detect(str_to_lower(school_locale), "rural")    ~ "Rural",
  str_detect(str_to_lower(school_locale), "town")     ~ "Town",
  TRUE ~ "Unknown"
)
```

---

## 2. Data Pipeline Flow

### 2.1 Core Pipeline (Student Suspension Data)

```
Raw CDE Excel File (data-raw/copy_CDE_suspensions_1718-2324_sc_race.xlsx)
        │
        ▼
[R/01_ingest_v0.R] ─────────────────────────────► susp_v0.parquet
        │  • Clean column names (janitor)
        │  • Parse numerics, preserve suppression (*) as NA
        │  • Build 14-digit CDS codes
        │  • Standardize academic_year format
        ▼
[R/02_feature_locale_simple.R] ─────────────────► susp_v1.parquet
        │  • Derive locale_simple from school_locale
        │  • Categories: City, Suburban, Town, Rural, Unknown
        ▼
[R/02b_drop_charter_all.R] ─────────────────────► susp_v1_noall.parquet
        │  • Remove charter_yn = "All" aggregate rows
        │  • Filter to campus-level data only
        ▼
[R/03_feature_size_quartiles_TA.R] ─────────────► susp_v2.parquet
        │  • Calculate year-specific enrollment quartiles
        │  • Based on All Students cumulative enrollment
        │  • Adds: all_enroll, enroll_q, enroll_q_label
        ▼
[R/04_feature_black_prop_quartiles.R] ──────────► susp_v3.parquet
        │  • Calculate racial composition proportions
        │  • Year-specific quartiles for Black, White, Hispanic
        │  • Adds: prop_black, prop_white, prop_hispanic,
        │         black_prop_q, white_prop_q, hispanic_prop_q (+ labels)
        ▼
[R/05_feature_school_level.R] ──────────────────► susp_v4.parquet
        │  • Classify schools by grade span
        │  • Parse grades_served field
        │  • Categories: Elementary, Middle, High, Other, Alternative
        │  • Alternative override based on school_type keywords
        ▼
[R/06_feature_reason_shares.R] ─────────────────► susp_v5.parquet
        │                                         susp_v5_long.parquet
        │  • Calculate suspension reason proportions
        │  • 6 reason categories: violent (injury/no injury),
        │    weapons, drugs, defiance, other
        │  • Generate long-format with reason labels
        ▼
[R/22_build_v6_features.R] ─────────────────────► susp_v6_features.parquet
                                                  susp_v6_long.parquet
           • LEFT JOIN with oth_long.parquet (other demographics)
           • Merge SPED, EL, Foster, Homeless, Migrant, SED, Gender
           • Create is_traditional flag
           • Sanitize NaN/Inf values
           • One row per campus-year (features) / per campus-year-race (long)
```

### 2.2 Teacher Demographics Pipeline

```
Raw CDE Teacher TXT Files (data-raw/stre*.txt)
        │
        ▼
[R/01c_ingest_teacher_demographics.R] ──────────► teacher_staff_long.parquet
        │  • Read tab-delimited TXT files
        │  • Standardize column names
        │  • Validate CDE codes (staff type, gender, race)
        │  • Filter to school-level (aggregate_level = "S")
        │  • Remove charter = "ALL" aggregates
        │  • Pivot race columns to long format
        │  • Map race slugs to CDE labels
        ▼
[Analysis/18_merge_teacher_student.R] ──────────► susp_v6_teacher_long.parquet
        │  • LEFT JOIN teacher summary to v6_long
        │  • Join keys: academic_year + cds_school
        │  • Preserves all student data
        │  • Reports coverage statistics
        ▼
[Analysis/22_build_teacher_race_shares.R] ──────► susp_v6_teacher_features.parquet
           • Summarize teacher demographics via teacher_summarise_long()
           • Calculate race shares, gender shares
           • Merge with student suspension long format
           • Final dataset for regression analyses
```

### 2.3 Other Demographics Pipeline

```
Raw CDE Other Demographics Excel (data-raw/copy_CDE_suspensions_1718-2324_sc_oth.xlsx)
        │
        ▼
[R/01b_ingest_demographics.R] ──────────────────► oth_long.parquet
        │  • Read school-level sheet
        │  • Canonicalize demographic codes via demographic_labels.R
        │  • Map aliases (GF→SF, GM→SM, etc.)
        │  • Categories: Special Education, English Learner, Sex,
        │                Foster, Homeless, Migrant, Socioeconomic
        │
        │  (Merged into v6 via R/22_build_v6_features.R)
        ▼
[R/22_build_v6_features.R]
        │  • Extract SPED, EL, Migrant, Foster, Homeless, SED
        │  • Calculate rates (num/den)
        │  • Extract gender-specific rates
        │  • LEFT JOIN to roster by school_code + academic_year
```

---

## 3. Variable-Level Provenance by Category

### 3.1 Core Identifiers

| Variable | Source | Script | Notes |
|----------|--------|--------|-------|
| `cds_school` | Derived | `R/utils_keys_filters.R::build_keys()` | 14-digit concatenation: county_code + district_code + school_code |
| `cds_district` | Derived | `R/utils_keys_filters.R::build_keys()` | 7-digit: county_code + district_code |
| `county_code` | CDE Race Excel | `R/01_ingest_v0.R` | 2-digit, zero-padded |
| `district_code` | CDE Race Excel | `R/01_ingest_v0.R` | 5-digit, zero-padded |
| `school_code` | CDE Race Excel | `R/01_ingest_v0.R` | 7-digit, zero-padded |
| `academic_year` | CDE Race Excel | `R/01_ingest_v0.R` | Format: "YYYY-YY" (e.g., "2023-24") |

### 3.2 Suspension Metrics

| Variable | Source | Script | Notes |
|----------|--------|--------|-------|
| `cumulative_enrollment` | CDE Race Excel | `R/01_ingest_v0.R` | Direct import; suppressed values (*) → NA |
| `total_suspensions` | CDE Race Excel | `R/01_ingest_v0.R` | Direct import |
| `suspension_count_*` (6 reasons) | CDE Race Excel | `R/01_ingest_v0.R` | violent_injury, violent_no_injury, weapons, drugs, defiance, other |
| `prop_susp_*` | Derived | `R/06_feature_reason_shares.R` | reason_count / total_suspensions |

### 3.3 Locale Classification

| Variable | Source | Script | Notes |
|----------|--------|--------|-------|
| `school_locale` | CDE Race Excel | `R/01_ingest_v0.R` | NCES locale classification from raw CDE data |
| `locale_simple` | Derived | `R/02_feature_locale_simple.R` | Simplified to City/Suburban/Town/Rural/Unknown via regex on school_locale |

**CRITICAL NOTE**: The locale classification does NOT use an external SchoolSites file. It is entirely derived from the `school_locale` field in the raw CDE suspension data.

### 3.4 Enrollment Quartiles

| Variable | Source | Script | Notes |
|----------|--------|--------|-------|
| `all_enroll` | Derived | `R/03_feature_size_quartiles_TA.R` | Total enrollment from "All Students" rows |
| `enroll_q` | Derived | `R/03_feature_size_quartiles_TA.R` | Year-specific ntile(4) on all_enroll |
| `enroll_q_label` | Derived | `R/03_feature_size_quartiles_TA.R` | Q1 (Smallest) through Q4 (Largest) |

### 3.5 Racial Composition Quartiles

| Variable | Source | Script | Notes |
|----------|--------|--------|-------|
| `prop_black` / `black_share` | Derived | `R/04_feature_black_prop_quartiles.R` | Black enrollment / Total enrollment |
| `prop_white` / `white_share` | Derived | `R/04_feature_black_prop_quartiles.R` | White enrollment / Total enrollment |
| `prop_hispanic` / `hispanic_share` | Derived | `R/04_feature_black_prop_quartiles.R` | Hispanic enrollment / Total enrollment |
| `black_prop_q` | Derived | `R/04_feature_black_prop_quartiles.R` | Year-specific ntile(4) on prop_black |
| `white_prop_q` | Derived | `R/04_feature_black_prop_quartiles.R` | Year-specific ntile(4) on prop_white |
| `hispanic_prop_q` | Derived | `R/04_feature_black_prop_quartiles.R` | Year-specific ntile(4) on prop_hispanic |

### 3.6 School Level Classification

| Variable | Source | Script | Notes |
|----------|--------|--------|-------|
| `grades_served` | CDE Race Excel | `R/01_ingest_v0.R` | Grade span string (e.g., "K-8") |
| `school_type` | CDE Race Excel | `R/01_ingest_v0.R` | CDE school type classification |
| `school_level` | Derived | `R/05_feature_school_level.R` | Elementary/Middle/High/Other/Alternative based on grade parsing and school_type |
| `is_traditional` | Derived | `R/22_build_v6_features.R` | Boolean flag based on school_type patterns |

### 3.7 Other Demographics (from OTH Excel)

| Variable | Source | Script | Notes |
|----------|--------|--------|-------|
| `sped_num`, `sped_den`, `sped_rate` | CDE OTH Excel | `R/01b_ingest_demographics.R` + `R/22_build_v6_features.R` | Students with Disabilities |
| `ell_num`, `ell_den`, `ell_rate` | CDE OTH Excel | `R/01b_ingest_demographics.R` + `R/22_build_v6_features.R` | English Learners (current EL) |
| `foster_num`, `foster_den`, `foster_rate` | CDE OTH Excel | `R/01b_ingest_demographics.R` + `R/22_build_v6_features.R` | Foster Youth |
| `homeless_num`, `homeless_den`, `homeless_rate` | CDE OTH Excel | `R/01b_ingest_demographics.R` + `R/22_build_v6_features.R` | Homeless students |
| `migrant_num`, `migrant_den`, `migrant_rate` | CDE OTH Excel | `R/01b_ingest_demographics.R` + `R/22_build_v6_features.R` | Migrant students |
| `sed_num`, `sed_den`, `sed_rate` | CDE OTH Excel | `R/01b_ingest_demographics.R` + `R/22_build_v6_features.R` | Socioeconomically Disadvantaged |
| `sex_male_rate`, `sex_female_rate` | CDE OTH Excel | `R/01b_ingest_demographics.R` + `R/22_build_v6_features.R` | Gender-specific rates |

### 3.8 Teacher Demographics

| Variable | Source | Script | Notes |
|----------|--------|--------|-------|
| `teacher_staff_count_total` | CDE Teacher TXT | `R/01c_ingest_teacher_demographics.R` | Total staff count |
| `teacher_staff_count_{race}` | CDE Teacher TXT | `R/01c_ingest_teacher_demographics.R` | Staff count by race (9 categories) |
| `teacher_staff_count_{race}_share` | Derived | `R/teacher_processing.R` | Race proportion: count / total |
| `teacher_staff_count_by_gender_{gender}` | CDE Teacher TXT | `R/01c_ingest_teacher_demographics.R` | Staff count by gender |
| `teacher_staff_count_by_type_{type}_{race}` | CDE Teacher TXT | `R/teacher_processing.R` | Staff by type (TCH/ADM/etc.) and race |

---

## 4. Join Keys and Merge Logic

### 4.1 Primary Join Key

The primary join key throughout the pipeline is:
- **`cds_school`** (14-digit) + **`academic_year`** (YYYY-YY format)

This uniquely identifies a school-year observation.

### 4.2 Key Merges

| Merge | Left Side | Right Side | Join Keys | Type |
|-------|-----------|------------|-----------|------|
| v6 features | susp_v5 (roster) | sped_wide, ell_wide, etc. | school_code + academic_year | LEFT JOIN |
| Teacher merge | susp_v6_long | teacher_summary | cds_school + academic_year | LEFT JOIN (many-to-one) |

### 4.3 Data Retention Guarantees

- All LEFT JOINs preserve 100% of student suspension data
- Teacher merge coverage varies by year (~70-95% of schools)
- Missing teacher data results in NA values, not dropped rows

---

## 5. Identified Provenance Gaps and Unresolved Questions

### 5.1 Confirmed Items

1. **Locale classification source**: Confirmed that `locale_simple` derives from CDE raw data (`school_locale` field), NOT from external SchoolSites file

2. **Teacher data coverage**: Teacher TXT files cover 2019-20 through 2024-25, but student suspension data covers 2017-18 through 2023-24. Years 2017-18 and 2018-19 will have no teacher data.

3. **Suppression handling**: CDE suppresses small cell counts with asterisks (*). These are converted to NA and flagged with `sup_*` columns.

### 5.2 Items Requiring User Verification

1. **Raw file acquisition dates**: The audit cannot determine when raw CDE files were downloaded. User should document download dates and CDE data portal URLs used.

2. **CDE data version**: CDE occasionally republishes data with corrections. User should verify whether the most recent CDE data release is being used.

3. **Teacher file completeness**: User should verify that all expected `stre*.txt` files are present in `data-raw/`. See `data-raw/README_TEACHER_DATA.md` for acquisition instructions.

4. **External SchoolSites file**: The prompt mentioned `SchoolSites2425.csv` but this file is NOT used in the codebase. If locale classification should be enhanced with this external file, that would require pipeline modifications.

### 5.3 Potential Data Quality Considerations

1. **Year-specific quartiles**: Quartile boundaries vary by year, which affects cross-year comparisons
2. **Alternative school classification**: Relies on regex pattern matching of school_type field
3. **Teacher-student merge coverage**: Not all schools have teacher demographic data

---

## 6. Python Script Data Access

Python scripts in `graph_scripts/` and `dashboard/` access data via:

### 6.1 Data Loading Functions

| Function | File | Source Data |
|----------|------|-------------|
| `load_susp_v6_long()` | `graph_scripts/data_sources.py` | `data-stage/susp_v6_long.parquet` |
| `load_susp_v6_features()` | `graph_scripts/data_sources.py` | `data-stage/susp_v6_features.parquet` |

### 6.2 Python Pipeline Characteristics

- All Python scripts are **read-only** consumers of parquet files
- No Python scripts create or modify staged data
- Python handles visualization and dashboard generation only
- Data validation occurs in `graph_scripts/data_validations.py`

---

## 7. Outputs Generated

This audit produced:

1. **`outputs/data_audit/data_provenance.csv`** - Structured CSV with column-by-column provenance
2. **`outputs/data_audit/data_provenance_report.md`** - This report

---

## 8. Recommendations

### 8.1 Documentation Improvements

1. Add CDE data portal URLs and download dates to `data-raw/README.md`
2. Document specific CDE file versions used (if available from CDE metadata)
3. Add data dictionary linking CDE variable names to analytic variable names

### 8.2 Pipeline Enhancements

1. Consider adding checksum validation for raw files
2. Add automated tests for join key uniqueness
3. Implement data lineage logging in pipeline scripts

### 8.3 External Data Integration

If the external `SchoolSites2425.csv` file should be incorporated:
1. Place file in `data-raw/`
2. Create ingestion script `R/01d_ingest_locale_sites.R`
3. Modify `R/02_feature_locale_simple.R` to merge or validate against external source
4. Document the external source provenance

---

## Appendix A: File Inventory

### Raw Data Files (data-raw/)

| File | Description | Status |
|------|-------------|--------|
| `copy_CDE_suspensions_1718-2324_sc_race.xlsx` | Suspension data by race/ethnicity | Required |
| `copy_CDE_suspensions_1718-2324_sc_oth.xlsx` | Suspension data by other demographics | Required |
| `stre1718.txt` through `stre2425.txt` | Teacher demographics by year | Required for teacher analysis |
| `README_TEACHER_DATA.md` | Instructions for obtaining teacher files | Present |

### Staged Data Files (data-stage/)

| File | Description | Created By |
|------|-------------|------------|
| `susp_v0.parquet` | Raw ingestion | `R/01_ingest_v0.R` |
| `susp_v1.parquet` | + locale | `R/02_feature_locale_simple.R` |
| `susp_v1_noall.parquet` | Charter filtered | `R/02b_drop_charter_all.R` |
| `susp_v2.parquet` | + enrollment quartiles | `R/03_feature_size_quartiles_TA.R` |
| `susp_v3.parquet` | + racial composition quartiles | `R/04_feature_black_prop_quartiles.R` |
| `susp_v4.parquet` | + school level | `R/05_feature_school_level.R` |
| `susp_v5.parquet` | + reason shares (wide) | `R/06_feature_reason_shares.R` |
| `susp_v5_long.parquet` | + reason shares (long) | `R/06_feature_reason_shares.R` |
| `susp_v6_features.parquet` | Final features (wide) | `R/22_build_v6_features.R` |
| `susp_v6_long.parquet` | Final (long, by race) | `R/22_build_v6_features.R` |
| `oth_long.parquet` | Other demographics | `R/01b_ingest_demographics.R` |
| `teacher_staff_long.parquet` | Teacher demographics | `R/01c_ingest_teacher_demographics.R` |
| `susp_v6_teacher_features.parquet` | Student + teacher merged | `Analysis/22_build_teacher_race_shares.R` |

---

## Appendix B: Script Reference

### Core Pipeline Scripts

| Script | Input | Output | Purpose |
|--------|-------|--------|---------|
| `R/00_paths.R` | N/A | Environment vars | Path configuration |
| `R/01_ingest_v0.R` | Excel | v0.parquet | Raw ingestion |
| `R/01b_ingest_demographics.R` | Excel | oth_long.parquet | Other demographics |
| `R/01c_ingest_teacher_demographics.R` | TXT | teacher_staff_long.parquet | Teacher demographics |
| `R/02_feature_locale_simple.R` | v0 | v1 | Locale classification |
| `R/02b_drop_charter_all.R` | v1 | v1_noall | Charter filtering |
| `R/03_feature_size_quartiles_TA.R` | v1_noall | v2 | Enrollment quartiles |
| `R/04_feature_black_prop_quartiles.R` | v2 | v3 | Racial composition quartiles |
| `R/05_feature_school_level.R` | v3 | v4 | School level classification |
| `R/06_feature_reason_shares.R` | v4 | v5, v5_long | Reason proportions |
| `R/22_build_v6_features.R` | v5, oth_long | v6_features, v6_long | Final assembly |

### Utility Scripts

| Script | Purpose |
|--------|---------|
| `R/utils_keys_filters.R` | CDS key building, canonical labels, filters |
| `R/ingest_helpers.R` | Column picking, year derivation |
| `R/demographic_labels.R` | Demographic code canonicalization |
| `R/teacher_processing.R` | Teacher data summarization |

---

*End of Data Provenance Audit Report*
