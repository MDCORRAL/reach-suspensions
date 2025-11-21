# Audit Implementation Testing Guide
## Date: 2025-11-21
## Implementation: All 4 Audit Recommendations

---

## ✅ Implementation Complete

All 4 audit recommendations have been implemented:

1. ✅ **Version Pinning** (Scripts 16, 17, 17a)
2. ✅ **Reason Reconciliation** (Script 02)
3. ✅ **Teacher Coverage Persistence** (Script 18)
4. ✅ **Environment Documentation** (21_QUICKSTART.md)

---

## 🧪 Testing Protocol

### Prerequisites

Ensure you have:
- ✅ RStudio or R console open
- ✅ Working directory set to repository root
- ✅ Data files in `data-stage/` (run `run_pipeline.R` if needed)

---

## Test 1: Version Pinning (Recommendation #1)

### Test 1A: Normal Operation (Should Pass)

```r
# Run script with matching versions
source("Analysis/16_tail_concentration_analysis.R")
```

**Expected Output**:
```
Using suspension data: susp_v6_long.parquet
Using features: susp_v6_features.parquet
✓ Version check passed: Both files are v6
[... analysis continues ...]
```

**Status**: ✅ **PASS** if message shows version check passed

---

### Test 1B: Version Mismatch Detection (Should Error)

```r
# Simulate version mismatch
file.rename(
  "data-stage/susp_v6_features.parquet",
  "data-stage/susp_v5_features_TEMP.parquet"
)

# Try to run script - should ERROR
tryCatch(
  source("Analysis/16_tail_concentration_analysis.R"),
  error = function(e) {
    message("✅ TEST PASSED: Error detected as expected")
    message("Error message:\n", e$message)
  }
)

# Restore file
file.rename(
  "data-stage/susp_v5_features_TEMP.parquet",
  "data-stage/susp_v6_features.parquet"
)
```

**Expected Error**:
```
Error: VERSION MISMATCH DETECTED:
  Suspension data: susp_v6_long.parquet (v6)
  Features data: susp_v5_features.parquet (v5)

These files must use the same version to ensure schema compatibility.

To fix:
  1. Check data-stage/ for matching versions
  2. Re-run pipeline if needed: source('run_pipeline.R')
  3. Ensure 22_build_v6_features.R completed successfully
```

**Status**: ✅ **PASS** if error is thrown with clear message

---

### Test 1C: All Three Scripts

Repeat Tests 1A-1B for:

```r
# Script 16
source("Analysis/16_tail_concentration_analysis.R")

# Script 17
source("Analysis/17_tail_by_grade-school_concentration_analysis.R")

# Script 17a
source("Analysis/17_tail_concentration_by_level.R")
```

Each should show `✓ Version check passed: v6` message.

**Status**: ✅ **PASS** if all 3 scripts pass version check

---

## Test 2: Reason Reconciliation (Recommendation #2)

### Test 2A: Normal Operation

```r
# Run script
source("Analysis/02_black_rates_by_quartiles.R")
```

**Expected Output** (if data is clean):
```
Generating plots...
✓ Reason reconciliation check passed: All totals within 1% of expected
[... plots generated ...]
```

**OR** (if discrepancies exist):
```
Generating plots...
Warning: REASON RECONCILIATION ISSUES DETECTED:
  8 year × quartile groups have reason totals differing from total_suspensions by >1%
  Max discrepancy: 7.2%
  Audit file: outputs/data_audit/reason_reconciliation_issues.csv

Possible causes:
  - Rounding errors in proportion calculations
  - Missing suspension reason categories
  - Data suppression (asterisks converted to NA)

Review audit file before using these rates in publications.
  Wrote 8 discrepancy records to audit file
```

**Status**: ✅ **PASS** if either message appears (no silent failure)

---

### Test 2B: Check Audit File (If Warning Shown)

```r
# Read audit file
library(readr)
issues <- read_csv("outputs/data_audit/reason_reconciliation_issues.csv")

# Inspect
print(issues)

# Verify structure
stopifnot(all(c("academic_year", "quartile", "original_total", "derived_total", "pct_diff_label") %in% names(issues)))

message("✅ Audit file structure validated")
```

**Expected Columns**:
- `academic_year`
- `quartile`
- `original_total`
- `derived_total`
- `abs_diff_label`
- `pct_diff_label`
- `n_reasons`

**Status**: ✅ **PASS** if audit file has correct structure

---

## Test 3: Teacher Coverage Persistence (Recommendation #3)

### Test 3A: Run Merge Script

```r
# Run teacher-student merge
source("Analysis/18_merge_teacher_student.R")
```

**Expected Output**:
```
[18] Teacher coverage: 45632 of 48901 student subgroup rows.
[18] Unique school coverage: 8234 of 9012 campus-years.
[18] Generating coverage audit report by year...
[18] Coverage audit saved: /path/to/outputs/data_audit/teacher_coverage_by_year.csv
[18] Coverage summary:
# A tibble: 7 × 4
  academic_year unique_schools coverage_pct_schools coverage_tier
  <chr>                  <int>                <dbl> <chr>
1 2017-18                 1234                  23.4 Low (20-49%)
2 2018-19                 1289                  67.8 Moderate (50-79%)
...

Warning: LOW TEACHER COVERAGE (<50%) in years: 2017-18
Use caution when analyzing teacher diversity metrics for these years.
```

**Status**: ✅ **PASS** if coverage report is generated and saved

---

### Test 3B: Verify Coverage File

```r
# Check file exists
stopifnot(file.exists("outputs/data_audit/teacher_coverage_by_year.csv"))

# Read and validate
library(readr)
coverage <- read_csv("outputs/data_audit/teacher_coverage_by_year.csv")

# Check structure
required_cols <- c("academic_year", "unique_schools", "schools_with_teacher",
                   "coverage_pct_schools", "coverage_tier")
stopifnot(all(required_cols %in% names(coverage)))

# Check coverage tiers are valid
valid_tiers <- c("High (≥80%)", "Moderate (50-79%)", "Low (20-49%)", "Very Low (<20%)")
stopifnot(all(coverage$coverage_tier %in% valid_tiers))

message("✅ Teacher coverage file validated")
```

**Expected Columns**:
- `academic_year`
- `unique_schools`
- `schools_with_teacher`
- `schools_without_teacher`
- `coverage_pct_schools`
- `coverage_tier`

**Status**: ✅ **PASS** if file structure validated

---

### Test 3C: Coverage Trend Visualization (Optional)

```r
library(ggplot2)

coverage <- read_csv("outputs/data_audit/teacher_coverage_by_year.csv")

ggplot(coverage, aes(x = academic_year, y = coverage_pct_schools)) +
  geom_line(group = 1, linewidth = 1, color = "steelblue") +
  geom_point(size = 3) +
  geom_hline(yintercept = 50, linetype = "dashed", color = "red") +
  labs(
    title = "Teacher Data Coverage Over Time",
    subtitle = "Percentage of schools with any teacher demographic data",
    y = "Coverage (%)",
    x = "Academic Year"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

ggsave("outputs/data_audit/teacher_coverage_trend.png", width = 10, height = 6, dpi = 300)
message("✅ Coverage trend plot saved")
```

---

## Test 4: Environment Documentation (Recommendation #4)

### Test 4A: Visual Inspection

```bash
# View documentation
cat Analysis/21_QUICKSTART.md | grep -A 20 "Environment variables"
```

**Expected Content**:
```markdown
✅ **Environment variables** (optional):
\`\`\`bash
# For custom data paths, copy and edit .Renviron:
cp .Renviron.example .Renviron

# Then edit .Renviron to set:
# RAW_PATH=/path/to/copy_CDE_suspensions_1718-2324_sc_race.xlsx
# OTH_RAW_PATH=/path/to/copy_CDE_suspensions_1718-2324_sc_oth.xlsx
# REACH_DATA_DIR=/custom/path/to/data-stage
\`\`\`
```

**Status**: ✅ **PASS** if documentation is present and correctly formatted

---

### Test 4B: Markdown Rendering

Open `Analysis/21_QUICKSTART.md` in:
- RStudio markdown viewer
- GitHub web interface
- VS Code markdown preview

**Verify**:
- ✅ Code blocks render correctly
- ✅ Bullet points formatted properly
- ✅ Section headers display correctly

**Status**: ✅ **PASS** if markdown renders without errors

---

## Integration Test: Full Pipeline

### Test 5: Run Complete Pipeline

```r
# Run full pipeline
source("run_all.R")
```

**Expected Behavior**:
1. ✅ Teacher demographics ingested
2. ✅ Core pipeline runs (01-06, 22)
3. ✅ Version checks pass for all tail analyses
4. ✅ Reason reconciliation runs (pass or warn)
5. ✅ Teacher coverage saved to audit file
6. ✅ All outputs generated

**Check These Files After Completion**:

```r
# Version check logs
# Should see "✓ Version check passed: v6" in console output

# Reason reconciliation
file.exists("outputs/data_audit/reason_reconciliation_issues.csv")  # May not exist if all passed

# Teacher coverage (MUST exist)
stopifnot(file.exists("outputs/data_audit/teacher_coverage_by_year.csv"))

# Regular outputs
list.files("outputs/graphs", pattern = "02_black", full.names = TRUE)
list.files("outputs", pattern = "tail_concentration_", full.names = TRUE)
```

**Status**: ✅ **PASS** if all scripts complete and audit files created

---

## Validation Checklist

After running all tests, verify:

### Critical Checks (Must Pass)

- [ ] **Version pinning**: All 3 tail scripts show version check message
- [ ] **Version error**: Mismatch test throws informative error
- [ ] **Reason reconciliation**: Script 02 runs without silent failures
- [ ] **Teacher coverage**: CSV file created in `outputs/data_audit/`
- [ ] **Documentation**: Environment variables section visible in QUICKSTART.md

### Data Quality Checks

- [ ] **Coverage file**: Contains all expected academic years
- [ ] **Coverage tiers**: Correctly categorized (High/Moderate/Low)
- [ ] **Reason audit**: If discrepancies exist, they're documented in CSV
- [ ] **No regressions**: Existing outputs still generate correctly

### Integration Checks

- [ ] **Full pipeline**: `run_all.R` completes without errors
- [ ] **Audit trail**: Both audit files exist in `outputs/data_audit/`
- [ ] **Console output**: Clear messages at each validation point
- [ ] **No silent failures**: All checks either pass with ✓ or warn/error

---

## Expected Audit Files

After full implementation and testing, you should have:

```bash
outputs/data_audit/
├── teacher_coverage_by_year.csv          # From Rec #3 (ALWAYS created)
└── reason_reconciliation_issues.csv      # From Rec #2 (ONLY if issues exist)
```

**Additional optional files**:
```bash
outputs/data_audit/
└── teacher_coverage_trend.png            # From Test 3C (optional visualization)
```

---

## Troubleshooting

### Issue: Version check doesn't run

**Symptom**: No "✓ Version check passed" message

**Fix**:
```r
# Verify code was added correctly
readLines("Analysis/16_tail_concentration_analysis.R")[128:165]
# Should show version checking code
```

### Issue: Reason reconciliation silent

**Symptom**: No message about reconciliation

**Fix**:
```r
# Check if validation function exists
exists("validate_reason_totals")  # Should be FALSE (function is local to script)

# Re-source script and watch for messages
source("Analysis/02_black_rates_by_quartiles.R")
```

### Issue: Teacher coverage file not created

**Symptom**: File missing in `outputs/data_audit/`

**Fix**:
```r
# Check if directory exists
dir.exists("outputs/data_audit")  # Should be TRUE

# Re-run merge script
source("Analysis/18_merge_teacher_student.R")

# Check for error messages
```

### Issue: Documentation not rendering

**Symptom**: Markdown looks broken

**Fix**:
- Check for balanced backticks (\`\`\`)
- Verify code block language tags (bash, r)
- Restart markdown viewer

---

## Test Results Template

Copy and fill out after running tests:

```
=== AUDIT IMPLEMENTATION TEST RESULTS ===
Date: _______________
Tester: _______________

Test 1A: Version Pinning (Normal)     [ ] PASS  [ ] FAIL
Test 1B: Version Mismatch Detection   [ ] PASS  [ ] FAIL
Test 1C: All Three Scripts            [ ] PASS  [ ] FAIL

Test 2A: Reason Reconciliation        [ ] PASS  [ ] FAIL
Test 2B: Audit File Structure         [ ] PASS  [ ] FAIL  [ ] N/A

Test 3A: Coverage Report Generated    [ ] PASS  [ ] FAIL
Test 3B: Coverage File Validated      [ ] PASS  [ ] FAIL
Test 3C: Trend Visualization          [ ] PASS  [ ] FAIL  [ ] SKIP

Test 4A: Documentation Present        [ ] PASS  [ ] FAIL
Test 4B: Markdown Rendering           [ ] PASS  [ ] FAIL

Test 5: Full Pipeline Integration     [ ] PASS  [ ] FAIL

OVERALL STATUS:  [ ] ALL PASS  [ ] SOME FAIL

Notes:
_________________________________________________________________
_________________________________________________________________
_________________________________________________________________
```

---

## Success Criteria

Implementation is **COMPLETE and VALIDATED** when:

1. ✅ All test cases pass (or fail as expected for error tests)
2. ✅ Audit files are created in `outputs/data_audit/`
3. ✅ Version mismatches are caught with clear errors
4. ✅ Reason reconciliation reports discrepancies (if any)
5. ✅ Teacher coverage tracked year-over-year
6. ✅ Documentation is complete and renders correctly
7. ✅ Full pipeline runs end-to-end successfully

---

## Next Steps After Testing

Once all tests pass:

1. **Commit changes** with comprehensive message
2. **Update changelog** or release notes
3. **Document in README** that audit recommendations implemented
4. **Share audit files** with collaborators/reviewers
5. **Archive test results** for reproducibility

---

**Testing Guide Created**: 2025-11-21
**Implementation Version**: All 4 recommendations
**Estimated Testing Time**: 30-45 minutes

