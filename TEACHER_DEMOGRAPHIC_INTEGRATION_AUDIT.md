# Teacher Demographic Data Integration Audit Report

**Date:** November 5, 2025
**Repository:** REACH Suspensions Analysis
**Auditor:** Claude Code
**Purpose:** Comprehensive review of teacher demographic data integration with student suspension and enrollment data

---

## Executive Summary

This audit reviewed the integration of teacher demographic data into the REACH suspension analysis pipeline. The integration was recently implemented (November 4-5, 2025) and includes a well-designed data ingestion, processing, and merging workflow. The codebase demonstrates strong engineering practices with proper error handling, data validation, and testing coverage.

### Key Findings

✅ **STRENGTHS:**
- Well-structured, modular pipeline design
- Comprehensive error handling and validation
- Recent bug fixes addressing race metadata preservation
- Good test coverage for core processing functions
- Consistent naming conventions and data structures
- Clear separation of concerns between ingestion, processing, and merging

⚠️ **AREAS FOR ATTENTION:**
- Teacher data pipeline has not yet been executed (no output files exist)
- Missing integration with downstream Python dashboard scripts
- Limited documentation of teacher-specific column names in merged dataset
- No cross-validation between teacher and student race category labels
- Potential inconsistencies in how "Not Reported" race category is handled

---

## 1. Data Pipeline Architecture Review

### 1.1 Overall Structure

The teacher demographic integration follows a three-stage pipeline:

```
┌─────────────────────────────────────────────────────────────┐
│ STAGE 1: INGESTION (R/01c_ingest_teacher_demographics.R)  │
├─────────────────────────────────────────────────────────────┤
│ Input:  data-raw/stre*.txt (Raw CDE teacher TXT files)    │
│ Output: data-stage/teacher_staff_long.parquet              │
│ Actions:                                                    │
│  • Read and normalize column names                         │
│  • Parse numeric data, validate gender/grade codes         │
│  • Pivot race columns from wide to long format             │
│  • Preserve provenance (source file tracking)              │
│  • Aggregate by campus-year-race-gender                    │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ STAGE 2: PROCESSING (R/teacher_processing.R)              │
├─────────────────────────────────────────────────────────────┤
│ Helper Functions:                                           │
│  • teacher_summarise_long() - Aggregates to campus-year    │
│  • teacher_longify_wide_counts() - Pivot transformations   │
│  • teacher_gender_label() - Gender code mapping            │
│  • teacher_slugify() - Safe column name creation           │
│ Output Structure:                                           │
│  • teacher_*_total (aggregate metrics)                     │
│  • teacher_*_<race_slug> (race-specific metrics)           │
│  • teacher_*_by_gender_<gender> (gender breakdowns)        │
│  • teacher_*_<race>_share (proportion metrics)             │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ STAGE 3: MERGING (Analysis/18_merge_teacher_student.R)    │
├─────────────────────────────────────────────────────────────┤
│ Input:  teacher_staff_long.parquet + susp_v6_features.parquet │
│ Output: susp_v6_teacher_features.parquet                   │
│ Join Keys: academic_year + cds_school                      │
│ Join Type: LEFT JOIN (preserves all student data)         │
│ Validation:                                                 │
│  • Uniqueness assertion (one row per campus-year)          │
│  • NaN/Inf handling                                         │
│  • Coverage reporting                                       │
└─────────────────────────────────────────────────────────────┘
```

**ASSESSMENT:** ✅ **EXCELLENT** - The pipeline follows best practices with clear stage separation, proper validation, and comprehensive error handling.

---

## 2. Code Quality Review

### 2.1 Ingestion Script (R/01c_ingest_teacher_demographics.R)

**Strengths:**
- ✅ Flexible file location handling via `TEACHER_RAW_DIR` environment variable
- ✅ Fallback parsing strategy (readr → utils::read.delim) for robustness
- ✅ Parsing issue detection and logging (lines 94-99)
- ✅ Defensive filtering of header leakage (lines 107-110)
- ✅ Year derivation from both file content and filename
- ✅ Charter flag normalization with "All" category handling
- ✅ Grade span validation against CDE-allowed codes
- ✅ Suppression of duplicate total_staff/total_staff_count columns
- ✅ Backfilling of totals from race component sums (lines 305-320)

**Issues Identified:**

**Issue #1: Missing Raw Data Files**
```
Location: Lines 27-35
Severity: HIGH
Status: BLOCKING

The script expects raw teacher TXT files in data-raw/ directory, but:
- Directory does not exist in current environment
- No stre*.txt files found
- Pipeline cannot execute without source data
```

**Recommendation:** Document the expected location and naming convention for teacher source files. Add instructions for obtaining or generating these files from CDE data sources.

**Issue #2: Incomplete Data Quality Validation**
```
Location: Lines 381-459
Severity: MEDIUM
Status: ADVISORY

Data quality checks at end of script (lines 369-459) are exploratory code
that should be refactored into formal validation functions.

Current issues:
- Checks run unconditionally (may error if data structures change)
- Results not captured for reporting
- No thresholds defined for acceptable discrepancies
```

**Recommendation:** Move data quality checks into separate validation module with:
- Clear pass/fail thresholds
- Structured reporting output
- Option to skip for production runs

### 2.2 Processing Module (R/teacher_processing.R)

**Strengths:**
- ✅ Well-documented functions with roxygen-style comments
- ✅ Defensive column existence checks (`teacher_pull_lower`, `teacher_pull_upper`)
- ✅ Comprehensive gender label mapping including Non-Binary (GX) and Missing (GZ)
- ✅ Race suffix lookup table with CDE reporting codes
- ✅ Safe division function (`teacher_safe_div`) prevents division-by-zero errors
- ✅ Recent fix (commit 0bdd739) properly guards missing race metadata columns

**Issue Identified:**

**Issue #3: Race Category Alignment Gap**
```
Location: Lines 81-107 (teacher_race_suffix_lookup)
Severity: MEDIUM
Status: ADVISORY

Teacher race categories may not align with student race categories:

Teacher categories (R/teacher_processing.R:82-106):
- african_american → "Black/African American"
- american_indian_or_alaska_native → "American Indian/Alaska Native"
- asian → "Asian"
- filipino → "Filipino"
- hispanic_or_latino → "Hispanic/Latino"
- pacific_islander → "Native Hawaiian/Pacific Islander"
- white → "White"
- two_or_more_races → "Two or More Races"
- not_reported → "Not Reported"

Student categories would need verification from student data processing.
No cross-reference validation exists.
```

**Recommendation:** Create a shared race category definition module that both teacher and student pipelines reference. Add validation to assert category alignment during merge operations.

### 2.3 Merge Script (Analysis/18_merge_teacher_student.R)

**Strengths:**
- ✅ Clear dependency checking (lines 22-27)
- ✅ Proper key building using shared `build_keys()` utility
- ✅ NaN and Inf value sanitization before join (lines 43-47)
- ✅ Uniqueness assertions on both input datasets
- ✅ One-to-one join relationship enforcement
- ✅ Coverage reporting for teacher data availability

**Issue Identified:**

**Issue #4: No Output File Exists**
```
Location: Line 76 (write_parquet)
Severity: HIGH
Status: BLOCKING

Expected output: data-stage/susp_v6_teacher_features.parquet
Actual status: FILE DOES NOT EXIST

The merge script has not been executed. Current susp_v6_features.parquet
contains 0 teacher columns (verified via inspection).
```

**Recommendation:** Execute the full pipeline via `run_all.R` to generate teacher-integrated datasets. Verify output file creation and column presence.

---

## 3. Recent Bug Fixes Analysis

### 3.1 Commit 0bdd739: "Guard race metadata columns in teacher longify"

**Change Summary:**
```r
# BEFORE (caused errors):
df %>%
  tidyr::pivot_longer(...) %>%
  dplyr::left_join(lookup, by = "race_suffix") %>%
  dplyr::mutate(
    reporting_category = dplyr::coalesce(reporting_category, reporting_category_code),
    # ^^^ ERROR: 'reporting_category' may not exist if not in original df
  )

# AFTER (fixed):
long_df <- df %>%
  tidyr::pivot_longer(...) %>%
  dplyr::left_join(lookup, by = "race_suffix")

missing_cols <- setdiff(c("reporting_category", "reporting_category_description",
                           "race_ethnicity"), names(long_df))
for (col in missing_cols) {
  long_df[[col]] <- rep(NA_character_, nrow(long_df))
}

long_df %>%
  dplyr::mutate(
    reporting_category = dplyr::coalesce(reporting_category, reporting_category_code),
    # ^^^ NOW SAFE: column guaranteed to exist
  )
```

**Assessment:** ✅ **CORRECT FIX** - This properly handles cases where input data frames lack expected metadata columns, preventing runtime errors during coalesce operations.

### 3.2 Commit e260f16: "Normalize teacher ingestion race/gender structure"

**Change Summary:** This commit created the entire `R/teacher_processing.R` module, establishing the standardized processing functions for teacher demographic data.

**Assessment:** ✅ **WELL DESIGNED** - The module provides a clean API for teacher data transformations with consistent naming and behavior.

---

## 4. Test Coverage Review

### 4.1 Existing Tests (tests/testthat/test_teacher_processing.R)

**Coverage:**
- ✅ `teacher_slugify()` - 2 test cases
- ✅ `teacher_longify_wide_counts()` - 2 test cases (plain and suffixed metrics)
- ✅ `teacher_summarise_long()` - 1 comprehensive test case

**Test Quality:**
- All tests use realistic data structures
- Tests verify both data transformations and column naming conventions
- Share calculations properly tested (proportions vs. totals)

**Gaps:**

**Issue #5: Missing Test Coverage**
```
Severity: MEDIUM
Status: ADVISORY

Untested functions:
- teacher_gender_label() - No tests for GF/GM/GX/GZ/ALL mapping
- teacher_is_total_row() - No tests for total row detection logic
- teacher_value_columns() - No tests for numeric column identification
- teacher_race_suffix_lookup() - No validation of lookup table consistency

Integration tests:
- No end-to-end test from raw TXT → long parquet
- No test of merge operation with actual suspension data
- No validation of output column counts/names
```

**Recommendation:** Add unit tests for all public functions. Create integration test with synthetic data to verify full pipeline execution.

---

## 5. Data Consistency and Accuracy Analysis

### 5.1 Join Key Validation

**Join Keys Used:** `academic_year` + `cds_school`

**Verification:**
- ✅ Both keys properly padded and standardized via `build_keys()` (R/utils_keys_filters.R)
- ✅ Uniqueness assertions applied before join (Analysis/18_merge_teacher_student.R:49, 56)
- ✅ One-to-one relationship enforced (line 61)
- ✅ Left join preserves all student data even when teacher data missing

**Assessment:** ✅ **CORRECT** - Join strategy is appropriate and well-validated.

### 5.2 Data Type Consistency

**Teacher Columns Generated:**
```
teacher_staff_count_total
teacher_staff_count_african_american
teacher_staff_count_american_indian_or_alaska_native
teacher_staff_count_asian
teacher_staff_count_filipino
teacher_staff_count_hispanic_or_latino
teacher_staff_count_native_hawaiian_pacific_islander
teacher_staff_count_white
teacher_staff_count_two_or_more_races
teacher_staff_count_not_reported
teacher_staff_count_by_gender_female
teacher_staff_count_by_gender_male
teacher_staff_count_by_gender_non_binary
teacher_staff_count_by_gender_gender_missing
teacher_staff_count_african_american_share
teacher_staff_count_american_indian_or_alaska_native_share
[... additional share columns ...]
```

**Issue Identified:**

**Issue #6: Column Name Inconsistency with Student Data**
```
Severity: LOW
Status: ADVISORY

Teacher columns use snake_case race names:
  • african_american
  • american_indian_or_alaska_native
  • native_hawaiian_pacific_islander

Student suspension data likely uses different conventions.
No shared constant or enum ensures consistency.
```

**Recommendation:** Define canonical race category names in shared module (similar to `R/demographic_labels.R` for other demographics). Use these constants across both teacher and student processing.

### 5.3 Missing Data Handling

**Teacher Data Coverage Reporting:**
The merge script reports teacher data coverage:
```r
coverage <- combined %>%
  mutate(has_teacher = if_any(all_of(teacher_cols), ~ !is.na(.x))) %>%
  summarise(
    total_campuses = n(),
    with_teacher   = sum(has_teacher, na.rm = TRUE)
  )
```

**Assessment:** ✅ **GOOD** - Proper handling of missing teacher data via left join and coverage reporting. NA values preserved for downstream filtering decisions.

**Issue Identified:**

**Issue #7: No Guidance on Missing Data Thresholds**
```
Severity: LOW
Status: ADVISORY

The script reports coverage but provides no guidance on acceptable thresholds.
Questions:
- What % of schools should have teacher data?
- Are certain years/regions expected to have gaps?
- Should analyses exclude campus-years without teacher data?
```

**Recommendation:** Document expected coverage rates by year. Add warnings if coverage falls below expected thresholds (e.g., <80% of traditional schools).

---

## 6. Integration with Downstream Systems

### 6.1 R Analysis Scripts

**Current Integration:**
- ✅ `run_all.R` includes teacher pipeline at line 8
- ✅ Teacher merge runs after demographic integration (line 27)
- ✅ Proper sequencing ensures dependencies met

**Assessment:** ✅ **CORRECT PLACEMENT** in pipeline execution order.

### 6.2 Python Dashboard Scripts

**Gap Identified:**

**Issue #8: No Python Dashboard Integration**
```
Location: dashboard/*.py, graph_scripts/*.py
Severity: MEDIUM
Status: INCOMPLETE

Python scripts reviewed:
- dashboard/build_dashboard_data.py (495 lines)
- dashboard/build_rates_by_race_year.py (271 lines)
- dashboard/build_suspension_overview.py (210 lines)
- graph_scripts/06_statewide_trends.py (1,360 lines)

None of these scripts:
- Read susp_v6_teacher_features.parquet
- Reference any teacher_* columns
- Include teacher demographics in visualizations
- Filter or stratify by teacher racial composition
```

**Recommendation:** Extend dashboard scripts to:
1. Read teacher-integrated dataset
2. Add teacher demographic filters/stratifications
3. Create visualizations showing relationships between teacher demographics and suspension patterns
4. Document which teacher metrics are most relevant for analysis

### 6.3 Documentation Gaps

**Issue #9: Missing Teacher Documentation**
```
Severity: MEDIUM
Status: INCOMPLETE

Reviewed documentation files:
- README.md - No mention of teacher demographics
- Analysis/data_processing_overview.md - Extensive, but no teacher section
- Analysis/susp_v6_data_explanation.md - No teacher columns documented

The 660-line data processing overview document describes all v0-v6 stages
in detail but does not include teacher demographic integration.
```

**Recommendation:** Update documentation to include:
1. Teacher data sources and file formats
2. Teacher demographic processing stages
3. Teacher column definitions in v6_teacher_features dataset
4. Example analyses using teacher demographics
5. Coverage statistics and data quality notes

---

## 7. Race/Demographic Metadata Handling

### 7.1 Race Category Definitions

**Teacher Categories (from teacher_race_suffix_lookup()):**
```r
race_suffix = c(
  "african_american",                 # → RB: "Black/African American"
  "american_indian_or_alaska_native", # → RI: "American Indian/Alaska Native"
  "asian",                            # → RA: "Asian"
  "filipino",                         # → RF: "Filipino"
  "hispanic_or_latino",               # → RH: "Hispanic/Latino"
  "pacific_islander",                 # → RP: "Native Hawaiian/Pacific Islander"
  "white",                            # → RW: "White"
  "two_or_more_races",                # → RT: "Two or More Races"
  "not_reported"                      # → RD: "Not Reported"
)
```

**CDE Reporting Codes:** RB, RI, RA, RF, RH, RP, RW, RT, RD

**Assessment:** ✅ **COMPLETE** - All standard California race/ethnicity categories included.

### 7.2 Gender Category Handling

**Teacher Gender Codes (from teacher_gender_label()):**
```r
code_upper %in% c("GF", "F")   → "Female"
code_upper %in% c("GM", "M")   → "Male"
code_upper == "GX"             → "Non-Binary"
code_upper == "GZ"             → "Gender Missing"
code_upper == "ALL"            → "All Staff"
```

**Assessment:** ✅ **INCLUSIVE** - Proper handling of non-binary and missing gender categories, consistent with modern demographic reporting standards.

### 7.3 Cross-System Consistency Check

**Issue #10: Race Category Mapping Not Validated Across Systems**
```
Severity: MEDIUM
Status: ADVISORY

Teacher race labels (R/teacher_processing.R) and student race labels
(location TBD) are defined independently.

Potential inconsistencies:
1. Label text differences (e.g., "pacific_islander" vs "native_hawaiian_pacific_islander")
2. Category inclusion/exclusion (teacher has "not_reported", verify student has same)
3. Reporting code mappings (both should use RB, RI, RA, etc.)

No automated validation ensures these stay aligned.
```

**Recommendation:**
1. Create shared `R/race_ethnicity_categories.R` module
2. Define canonical list of race categories with:
   - CDE reporting codes
   - Display labels
   - Slug names for columns
   - Sort order for visualizations
3. Reference this module from both teacher and student processing
4. Add test to verify alignment

---

## 8. Recommendations Summary

### Priority 1: Critical (Execute Before Analysis)

1. **Execute Full Pipeline**
   - Run `run_all.R` to generate teacher-integrated datasets
   - Verify `susp_v6_teacher_features.parquet` creation
   - Confirm teacher columns appear in output
   - Validate row counts and coverage statistics

2. **Document Teacher Data Sources**
   - Specify location and naming of raw teacher TXT files
   - Document CDE source and download process
   - Add data dictionary for raw teacher file columns
   - Include expected file format and example rows

3. **Validate Data Integration**
   - Inspect merged output for expected columns
   - Verify join keys matched correctly
   - Check for unexpected NA patterns
   - Compare teacher coverage across years

### Priority 2: Important (Implement Soon)

4. **Create Shared Race Category Module**
   - Define canonical race/ethnicity categories
   - Use same definitions for teacher and student data
   - Add validation test for category alignment
   - Update both pipelines to reference shared module

5. **Extend Dashboard Integration**
   - Update Python scripts to read teacher-integrated dataset
   - Add teacher demographic filters to dashboards
   - Create visualizations showing teacher-student relationships
   - Document teacher metrics in dashboard help text

6. **Update Documentation**
   - Add teacher demographic section to data processing overview
   - Document all teacher_* columns in data dictionary
   - Include example analyses using teacher data
   - Add FAQ section for teacher data questions

### Priority 3: Enhancements (Nice to Have)

7. **Expand Test Coverage**
   - Add unit tests for untested functions
   - Create integration tests with synthetic data
   - Add regression tests for bug fixes
   - Implement continuous validation checks

8. **Refactor Data Quality Checks**
   - Move exploratory code to validation module
   - Define pass/fail thresholds
   - Create structured validation reports
   - Add option for production vs. development modes

9. **Improve Missing Data Reporting**
   - Define expected teacher coverage thresholds
   - Add warnings for low coverage
   - Document known gaps by year/region
   - Provide guidance on handling missing teacher data

---

## 9. Audit Conclusion

### Overall Assessment: ✅ WELL-DESIGNED, IMPLEMENTATION INCOMPLETE

The teacher demographic integration codebase demonstrates **strong engineering practices** with:
- Clear modular design
- Comprehensive error handling
- Good test coverage for core functions
- Recent bug fixes properly addressing edge cases
- Consistent naming conventions
- Proper validation and reporting

However, the integration is **not yet operational** because:
- Raw teacher data files are not present
- Pipeline has not been executed
- Output files do not exist
- Downstream systems not yet updated
- Documentation not yet complete

### Immediate Action Items

Before conducting any analysis using teacher demographics:

1. ✅ **Verify data availability:** Confirm raw teacher TXT files exist
2. ✅ **Execute pipeline:** Run `run_all.R` and verify successful completion
3. ✅ **Inspect outputs:** Check `susp_v6_teacher_features.parquet` for expected columns
4. ✅ **Validate coverage:** Review teacher data coverage statistics by year
5. ✅ **Update dashboards:** Integrate teacher metrics into visualization scripts
6. ✅ **Document process:** Add teacher section to data processing documentation

### Long-Term Improvements

For sustainable, accurate teacher demographic analysis:

1. Create shared race/ethnicity category definitions
2. Implement automated validation between teacher and student categories
3. Expand test coverage with integration tests
4. Add continuous validation to detect data quality issues
5. Document expected coverage patterns and acceptable thresholds

---

## Appendix A: File Inventory

### Teacher Demographics Processing Files

| File | Lines | Purpose | Status |
|------|-------|---------|--------|
| R/01c_ingest_teacher_demographics.R | 459 | TXT ingestion → long parquet | ✅ Implemented |
| R/teacher_processing.R | 268 | Processing helpers | ✅ Implemented |
| Analysis/18_merge_teacher_student.R | 80 | Merge teacher + student | ✅ Implemented |
| tests/testthat/test_teacher_processing.R | 74 | Unit tests | ✅ Partial coverage |

### Expected Data Files

| File | Expected Rows | Status |
|------|---------------|--------|
| data-raw/stre*.txt | Variable | ❌ Not present |
| data-stage/teacher_staff_long.parquet | ~200,000-500,000 | ❌ Not generated |
| data-stage/susp_v6_teacher_features.parquet | ~60,000 | ❌ Not generated |

### Downstream Integration Points

| System | Status | Action Required |
|--------|--------|-----------------|
| run_all.R pipeline | ✅ Integrated | Execute pipeline |
| Python dashboards | ❌ Not integrated | Add teacher metrics |
| Documentation | ❌ Incomplete | Add teacher section |
| Tests | ⚠️ Partial | Expand coverage |

---

## Appendix B: Column Naming Conventions

### Teacher Column Naming Pattern

```
teacher_{metric}_{stratification}[_share]

Examples:
teacher_staff_count_total
teacher_staff_count_african_american
teacher_staff_count_by_gender_female
teacher_staff_count_african_american_share
teacher_staff_count_by_gender_female_share
```

**Metric:** Usually `staff_count` (but extensible to `fte`, `headcount`, etc.)
**Stratification:** Race slug (e.g., `african_american`) or gender slug (e.g., `by_gender_female`)
**Share:** Optional suffix for proportion columns (stratification value / total)

### Naming Consistency Guidelines

1. Use snake_case for all column names
2. Prefix all teacher columns with `teacher_`
3. Use race slugs from `teacher_race_suffix_lookup()`
4. Use gender slugs from `teacher_slugify(teacher_gender_label())`
5. Append `_share` for proportions, not absolute counts
6. Append `_total` for aggregated metrics across all categories

---

**End of Audit Report**
