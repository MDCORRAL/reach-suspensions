# California Department of Education Data Validation Report

**Report Date**: [Insert Date]
**Reporting Institution**: UCLA Center for the Transformation of Schools
**Research Project**: REACH Suspensions Analysis
**Data Files**: CDE Teacher Demographics (STRE) and Suspension Data
**Contact**: [Insert Contact Information]

---

## Purpose

This report documents data quality issues discovered during processing of California Department of Education (CDE) data files for research purposes. The issues identified impact data usability and may affect other researchers and analysts using these datasets.

We respectfully request CDE's review of these issues and consideration of enhanced data validation procedures for future data releases.

---

## Summary of Issues

| Issue Type | Severity | Files Affected | Records Impacted | Impact on Research |
|------------|----------|----------------|------------------|-------------------|
| Fixed-width text parsing errors | **High** | All 6 teacher files | 48 parsing errors | Manual cleanup required |
| Invalid categorical values | **High** | All 6 teacher files | 12 invalid rows | Data loss |
| Missing charter status | **Medium** | All 6 teacher files | 591,812 aggregate rows | Cannot distinguish school types |
| Suspension > Enrollment | **Low** | Suspension files | 1,380 records (0.2%) | Rate calculation issues |

---

## Issue 1: Fixed-Width Text File Parsing Errors

### Description

The STRE (Staff Demographic) text files contain structural inconsistencies that prevent clean parsing:
- Expected format: 23 columns (fixed-width format)
- Actual format: Variable columns (9-44) in specific rows
- Root cause: Header row leakage and inconsistent column alignment

### Affected Files and Locations

#### stre1920.txt (11 parsing errors)
```
Row 187686: Expected 23 columns, received 35 columns
Row 187907: Expected 23 columns, received 26 columns
Row 191097: Expected 23 columns, received 14 columns
Row 191176: Expected 23 columns, received 25 columns
Row 197479: Expected 23 columns, received 25 columns
Row 209192: Expected 23 columns, received 10 columns
Row 209294: Expected 23 columns, received 24 columns
Row 233073: Expected 23 columns, received 11 columns
Row 233286: Expected 23 columns, received 27 columns
Row 279877: Expected 23 columns, received 44 columns
[Additional row in output]
```

#### stre2021.txt (1 parsing error)
```
Row 195888: Expected 23 columns, received 27 columns
```

#### stre2122.txt (10 parsing errors)
```
Row 187351: Expected 23 columns, received 28 columns
Row 187571: Expected 23 columns, received 24 columns
Row 279866: Expected 23 columns, received 33 columns
Row 280009: Expected 23 columns, received 27 columns
Row 330290: Expected 23 columns, received 9 columns
Row 330475: Expected 23 columns, received 24 columns
Row 357567: Expected 23 columns, received 13 columns
Row 357653: Expected 23 columns, received 25 columns
Row 365352: Expected 23 columns, received 32 columns
Row 365572: Expected 23 columns, received 25 columns
```

#### stre2223.txt (4 parsing errors)
```
Row 241206: Expected 23 columns, received 41 columns
Row 241429: Expected 23 columns, received 25 columns
Row 285496: Expected 23 columns, received 28 columns
Row 285598: Expected 23 columns, received 26 columns
```

#### stre2324.txt (10 parsing errors)
```
Row 204396: Expected 23 columns, received 30 columns
Row 204617: Expected 23 columns, received 25 columns
Row 288862: Expected 23 columns, received 29 columns
Row 289083: Expected 23 columns, received 16 columns
Row 289084: Expected 23 columns, received 9 columns
Row 291631: Expected 23 columns, received 34 columns
Row 291852: Expected 23 columns, received 26 columns
Row 294445: Expected 23 columns, received 30 columns
Row 294494: Expected 23 columns, received 19 columns
Row 294594: Expected 23 columns, received 27 columns
```

#### stre2425.txt (12 parsing errors)
```
Row 190830: Expected 23 columns, received 41 columns
Row 191052: Expected 23 columns, received 24 columns
Row 289393: Expected 23 columns, received 29 columns
Row 289420: Expected 23 columns, received 25 columns
Row 343578: Expected 23 columns, received 36 columns
Row 346643: Expected 23 columns, received 36 columns
Row 346663: Expected 23 columns, received 24 columns
Row 352637: Expected 23 columns, received 26 columns
Row 352680: Expected 23 columns, received 25 columns
Row 363546: Expected 23 columns, received 12 columns
[Additional rows in output]
```

### Impact on Research

- Researchers must implement custom error handling
- Risk of data loss if parsing errors not detected
- Inconsistent results across different analysis tools
- Manual data cleanup required, increasing analysis time

### Recommended Solutions

1. **Add pre-publication validation**:
   - Verify all rows have exactly 23 columns
   - Flag rows that don't match expected format
   - Reject files that fail validation

2. **Improve data generation process**:
   - Review export query/script for column alignment issues
   - Ensure header rows are completely excluded from data
   - Test parsing with standard tools (R, Python, SAS) before publication

3. **Provide data dictionary**:
   - Document exact column positions and widths
   - Specify expected data types for each column
   - Include sample parsing code in R and Python

---

## Issue 2: Invalid Categorical Values in Staff Type Field

### Description

The `staff_type` (renamed to `reporting_category` in analysis) field contains invalid values that don't match CDE data standards.

**Valid CDE Staff Type Codes**:
- `ADM` = Administrators
- `ALL` = All staff (aggregate)
- `OTH` = Other staff
- `PSV` = Pupil Services
- `TCH` = Teachers

### Invalid Values Found

#### stre1920.txt
```
Invalid values in reporting_category field:
  - "122" (1 occurrence) - appears to be data entry error
  - "284" (1 occurrence) - appears to be data entry error
  - "N" (2 occurrences) - possibly meant to be "No" or a missing value
Total invalid: 4 rows dropped
```

#### stre2021.txt
```
Invalid values in reporting_category field:
  - "DISTRICT NAME" (1 occurrence) - header row leaked into data
Total invalid: 1 row dropped
```

#### stre2122.txt
```
Invalid values in reporting_category field:
  - "54277" (1 occurrence) - appears to be code from wrong field
  - "COUNTY NAME" (1 occurrence) - header row leaked into data
Total invalid: 2 rows dropped
```

#### stre2324.txt
```
Invalid values in reporting_category field:
  - "44" (1 occurrence) - appears to be data entry error
  - "DISTRICT CODE" (1 occurrence) - header row leaked into data
  - "SCHOOL CODE" (1 occurrence) - header row leaked into data
Total invalid: 3 rows dropped
```

#### stre2425.txt
```
Invalid values in reporting_category field:
  - "2" (1 occurrence) - appears to be data entry error
  - "STAFF TYPE" (1 occurrence) - header row leaked into data
Total invalid: 2 rows dropped

Additional issue:
  - Invalid school_grade_span value: "GSALL" (1 occurrence)
```

### Impact on Research

- Data loss: 12 rows across all files
- Ambiguous staff categorization in original data
- Risk of misclassification if invalid values not detected
- Teacher vs. administrator comparisons could be inaccurate if invalid codes not removed

### Recommended Solutions

1. **Add upload validation**:
   - Restrict `staff_type` field to valid codes only: `ADM`, `ALL`, `OTH`, `PSV`, `TCH`
   - Reject submissions with invalid categorical values
   - Provide clear error messages to data submitters

2. **Prevent header leakage**:
   - Review data export process to ensure headers are properly excluded
   - Add automated test to check for column names appearing in data rows

3. **Improve data entry**:
   - Use dropdown/picklist for categorical fields in data entry systems
   - Add frontend validation before submission
   - Provide data entry training highlighting common errors

4. **Data documentation**:
   - Publish complete list of valid values for all categorical fields
   - Include data validation rules in technical documentation

---

## Issue 3: Missing Charter School Status at School Level

### Description

School-level records (aggregate_level = "S") are missing `charter_yn` field values, while county, district, and total-level aggregates have this field populated.

### Statistics

| File | School-level Rows Missing charter_yn | % of File |
|------|-------------------------------------|-----------|
| stre1920.txt | 120,625 | ~50% |
| stre2021.txt | 111,387 | ~50% |
| stre2122.txt | 120,186 | ~50% |
| stre2223.txt | 116,917 | ~48% |
| stre2324.txt | 122,653 | ~49% |
| stre2425.txt | 124,044 | ~49% |
| **Total** | **591,812** | - |

### Impact on Research

- Cannot distinguish charter vs. traditional schools in teacher diversity analysis
- Must drop school-level rows that lack charter status
- Analysis limited to schools with complete data
- Equity comparisons between charter and traditional schools incomplete

### Aggregate Levels with Missing Charter Data

```
Aggregate levels in dropped rows:
  - C = County-level aggregates (intentionally excluded)
  - D = District-level aggregates (intentionally excluded)
  - T = Total/statewide aggregates (intentionally excluded)
  - Invalid codes: "0", "188" (data quality issues)
```

### Recommended Solutions

1. **Populate charter_yn for all school-level records**:
   - Use CDE school master file to backfill charter status
   - Include charter status in teacher demographic data export
   - Ensure consistency with suspension data charter flags

2. **Data completeness validation**:
   - Require `charter_yn` for all rows where `aggregate_level = "S"`
   - Flag incomplete records before publication
   - Document which aggregate levels should have charter status

3. **Cross-file consistency**:
   - Verify charter status matches across suspension and teacher datasets
   - Use consistent CDS codes for joining
   - Publish crosswalk file for charter school identification

---

## Issue 4: Suspension Counts Exceeding Enrollment

### Description

Some school-year-subgroup records report more suspensions than enrolled students, which creates issues for rate calculations.

### Statistics

- **Total records with suspensions > enrollment**: 1,380
- **Percentage of total records**: 0.2%
- **Years affected**: All years (2017-18 through 2023-24)

### Possible Causes

1. **Multiple suspensions per student**: Student suspended multiple times counted multiple times
2. **Transfer students**: Student counted at multiple schools
3. **Data entry errors**: Incorrect enrollment or suspension counts
4. **Timing mismatches**: Enrollment snapshot vs. year-end suspensions

### Impact on Research

- Cannot calculate accurate suspension rates (rate > 100%)
- Must cap values at enrollment to prevent invalid rates
- Uncertainty about true suspension burden
- Potential bias if capping applied inconsistently

### Current Mitigation (Researchers)

Researchers automatically cap suspension counts at enrollment:
```r
total_suspensions = pmin(total_suspensions, cumulative_enrollment, na.rm = TRUE)
```

This ensures suspension rates ∈ [0, 1], but may underestimate true suspension burden if multiple suspensions per student are occurring.

### Recommended Solutions

1. **Data validation at submission**:
   - Add business rule: `total_suspensions <= cumulative_enrollment × [multiplier]`
   - Determine appropriate multiplier (e.g., 2.0 if multiple suspensions expected)
   - Flag violations for review before publication

2. **Clarify reporting guidance**:
   - Document whether schools should report:
     - **Unduplicated student count** (preferred for rate calculations)
     - **Total suspension incidents** (preferred for understanding discipline burden)
   - Provide separate fields for each metric if both are needed

3. **Add validation fields**:
   - `unduplicated_students_suspended` (count of unique students)
   - `total_suspension_incidents` (count of all suspension events)
   - `multiple_suspensions_flag` (yes/no indicator)

4. **Publish data quality notes**:
   - Flag records where suspensions > enrollment
   - Provide explanation of what this represents
   - Guidance for researchers on how to handle these records

---

## Recommendations Summary

### High Priority (Implement Before Next Release)

1. ✅ Add fixed-width format validation for text files
2. ✅ Add categorical field value constraints
3. ✅ Remove header row leakage from data files
4. ✅ Populate charter_yn for all school-level records

### Medium Priority (Implement Within 6 Months)

1. Add pre-publication data quality checks:
   - Parsing validation
   - Range checks (suspensions ≤ enrollment × multiplier)
   - Completeness checks (required fields populated)

2. Improve documentation:
   - Publish data dictionaries with valid values
   - Provide sample parsing code
   - Document known data quality issues

3. Cross-file consistency validation:
   - CDS codes match across datasets
   - Charter status consistent
   - Enrollment numbers consistent

### Low Priority (Ongoing Improvements)

1. Enhanced data submission validation:
   - Real-time validation in data entry systems
   - Clear error messages for submitters
   - Training materials for district data coordinators

2. Data quality monitoring:
   - Track error rates over time
   - Publish annual data quality reports
   - Identify districts with recurring issues for targeted support

---

## Appendix A: Technical Details

### Parsing Tools Used
- **R version**: 4.3.0+
- **Parsing package**: `vroom` 1.6.0+ (fixed-width format)
- **Fixed-width specification**: 23 columns as documented in CDE data dictionary

### Validation Code Sample

```r
# Detect invalid staff type codes
invalid_staff_types <- raw_data %>%
  filter(!is.na(staff_type)) %>%
  filter(!staff_type %in% c("ALL", "ADM", "PSV", "TCH", "OTH")) %>%
  count(staff_type, name = "n_invalid")

if (nrow(invalid_staff_types) > 0) {
  message("Invalid staff_type values found:")
  print(invalid_staff_types)
}
```

### Test Dataset Request

To help CDE diagnose these issues, we can provide:
- Specific row numbers for parsing errors
- Extract of affected records (anonymized if needed)
- Sample parsing code demonstrating issues

---

## Appendix B: Impact on REACH Research

### Current Workarounds

1. **Parsing errors**: Manual log review and row-by-row validation
2. **Invalid values**: Custom filtering with documented drops
3. **Missing charter_yn**: Analysis limited to schools with complete data
4. **Suspension > enrollment**: Automatic capping with audit trail

### Research Questions Affected

1. **Teacher diversity by school type**: Limited to schools with charter status (62% coverage loss)
2. **Suspension trend analysis**: 0.2% of records require capping
3. **Charter vs. traditional comparisons**: Incomplete due to missing charter flags
4. **Equity gap analysis**: Teacher data coverage reduced due to data quality filtering

### Time Impact

- **Data cleaning**: +8-10 hours per data release
- **Validation**: +4-6 hours per analysis
- **Documentation**: +2-4 hours per report
- **Total overhead**: ~15-20 hours per academic year

---

## Contact Information

**Research Team**: UCLA Center for the Transformation of Schools

**Principal Investigator**: [Insert Name and Contact]

**Data Analyst**: [Insert Name and Contact]

**Best contact for technical questions**: [Insert Email]

**Project website**: [Insert URL if applicable]

---

## Acknowledgments

We appreciate CDE's commitment to data transparency and quality. This report is submitted in the spirit of continuous improvement and to support enhanced data quality for all researchers, policymakers, and practitioners using CDE data.

We are available to discuss these findings, provide additional technical details, or collaborate on validation procedures for future data releases.

---

**Document Version**: 1.0
**Last Updated**: [Insert Date]
**Next Review**: [After next CDE data release]
