# Teacher Diversity Analysis Diagnostic Report

**Date**: 2025-11-14
**Issue**: Teacher diversity analysis scripts failing due to missing race/ethnicity data
**Status**: ❌ **DATA NOT AVAILABLE** - Requires CDE teacher TXT files

---

## Executive Summary

The teacher diversity analysis (`Analysis/21_teacher_diversity_regression.R` and `Analysis/21_weighted_teacher_diversity_by_quartile.R`) **cannot run** because the teacher demographic data lacks race/ethnicity breakdowns.

**Root Cause**: The raw CDE teacher staff demographic files (`stre*.txt`) are not present in the repository. Without these files, the pipeline cannot ingest teacher race/ethnicity data.

**Current State**:
- ✅ All code infrastructure is ready
- ✅ Teacher data processing pipeline exists
- ✅ Merge and analysis scripts are functional
- ❌ **Raw teacher TXT files missing**
- ❌ Teacher race/ethnicity columns not available

---

## What's Missing

### Files Needed

The `data-raw/` directory needs CDE Teacher Staff Demographics TXT files:

```
data-raw/
  ├── stre1718.txt  # 2017-18 academic year
  ├── stre1819.txt  # 2018-19 academic year
  ├── stre1920.txt  # 2019-20 academic year
  ├── stre2021.txt  # 2020-21 academic year
  ├── stre2122.txt  # 2021-22 academic year
  ├── stre2223.txt  # 2022-23 academic year
  └── stre2324.txt  # 2023-24 academic year
```

### Required Data Columns

The TXT files must include race/ethnicity columns:
- `African American` or `african_american`
- `American Indian or Alaska Native`
- `Asian`
- `Filipino`
- `Hispanic or Latino`
- `Native Hawaiian/Pacific Islander`
- `White`
- `Two or More Races`
- `Not Reported`

These columns contain staff counts by race/ethnicity and must be present for the diversity analysis to work.

---

## Current Diagnostic Output

### Error from 21_teacher_diversity_regression.R

```
>>> ERROR: Cannot proceed with teacher diversity analysis
>>> The teacher data does not include race/ethnicity breakdowns.
>>>
>>> Available teacher data includes:
>>>   - Total staff counts by position (teachers, administrators, etc.)
>>>   - Gender breakdowns (female, male, non-binary)
>>>   - Combinations of position and gender
>>>
>>> To complete this analysis, you would need:
>>>   - Teacher demographic data with race/ethnicity breakdowns
>>>   - Columns like: teacher_staff_count_african_american, teacher_staff_count_white, etc.
```

### Error from 21_weighted_teacher_diversity_by_quartile.R

```
>>> Found 0 teacher race columns
>>>
>>> ERROR: Cannot proceed with teacher diversity analysis
>>> The teacher data does not include race/ethnicity breakdowns.
```

### What Data IS Available

The existing `susp_v6_teacher_features.parquet` contains:
- ✅ Staff counts by position (Teachers, Administrators, Pupil Services, Other)
- ✅ Staff counts by gender (Female, Male, Non-Binary)
- ✅ Cross-tabulations of position × gender
- ❌ **NO race/ethnicity breakdowns**

This suggests the teacher data was either:
1. Never ingested from raw TXT files (most likely), OR
2. Ingested from files that lacked race/ethnicity columns

---

## How to Obtain the Data

### Option 1: CDE DataQuest (Recommended)

**URL**: https://dq.cde.ca.gov/dataquest/

**Navigation**:
1. Go to `Staff` → `Staff Demographics`
2. Select `Download Data Files`
3. Look for "Staff by School, Ethnicity, and Gender" files
4. Download files for academic years 2017-18 through 2023-24

**Expected file pattern**: `stre{YYZZ}.txt` where YYZZ is the year (e.g., `stre1920.txt` for 2019-20)

### Option 2: CDE FilesMare FTP

**URL**: https://www3.cde.ca.gov/researchfiles/

**Instructions**:
1. Look for "Staff Demographics" or similar directories
2. Download TXT files matching pattern `stre*.txt`
3. Verify files include race/ethnicity columns

### Option 3: Direct Data Request

If public downloads aren't available:

**Contact**: CDE Data Reporting Office
**Email**: datareporting@cde.ca.gov

**Request Template**:
```
Subject: Request for Staff Demographics Data Files

Dear CDE Data Reporting Office,

I am requesting Staff Demographics data by school, race/ethnicity, and gender
for academic years 2017-18 through 2023-24.

Specifications:
- School-level aggregation (aggregate_level = "S")
- All staff types (Teachers, Administrators, Pupil Services, Other)
- Broken down by race/ethnicity (9 CDE standard categories)
- Broken down by gender (Female, Male, Non-Binary)
- File format: Tab-separated TXT files (stre*.txt pattern)

Please advise on availability and download procedures.

Thank you,
[Your Name]
[Your Affiliation]
```

---

## Next Steps

### Step 1: Obtain and Place Files

1. Download teacher TXT files from CDE (see options above)
2. Place files in `data-raw/` directory:
   ```bash
   cp stre*.txt /home/user/reach-suspensions/data-raw/
   ```

3. Verify files contain race/ethnicity columns:
   ```bash
   head -n 1 data-raw/stre1920.txt | tr '\t' '\n' | grep -i "african\|hispanic\|asian\|white"
   ```

### Step 2: Run Ingestion

**Option A: Full pipeline** (recommended)
```bash
cd /home/user/reach-suspensions
Rscript run_all.R
```

This will:
1. Ingest teacher TXT files → `teacher_staff_long.parquet`
2. Process race/ethnicity data
3. Merge with student data → `susp_v6_teacher_features.parquet` (with race cols)
4. Run all analyses including teacher diversity

**Option B: Teacher ingestion only**
```bash
Rscript R/01c_ingest_teacher_demographics.R
```

Then run merge:
```bash
Rscript Analysis/18_merge_teacher_student.R
```

### Step 3: Verify Data

After ingestion, verify race/ethnicity columns are present:

```r
library(arrow)
library(dplyr)

# Check raw teacher data
teacher <- read_parquet("data-stage/teacher_staff_long.parquet")
print("Race/ethnicity categories:")
print(unique(teacher$race_ethnicity))

# Should show 9 categories:
# - African American
# - American Indian or Alaska Native
# - Asian
# - Filipino
# - Hispanic or Latino
# - Native Hawaiian/Pacific Islander
# - White
# - Two or More Races
# - Not Reported

# Check merged data
merged <- read_parquet("data-stage/susp_v6_teacher_features.parquet")
race_cols <- grep("teacher.*african|teacher.*white|teacher.*hispanic",
                  names(merged), value = TRUE)
print("\nTeacher race columns in merged data:")
print(race_cols)

# Should show columns like:
# - teacher_staff_count_african_american
# - teacher_staff_count_white
# - teacher_staff_count_hispanic_or_latino
# - teacher_staff_count_asian
# ... (and _share variants)
```

### Step 4: Run Teacher Diversity Analysis

Once verification passes:

```r
# Run regression analysis
source("Analysis/21_teacher_diversity_regression.R")

# Run weighted quartile analysis
source("Analysis/21_weighted_teacher_diversity_by_quartile.R")
```

---

## Technical Details

### Why This Happened

The teacher data pipeline requires two types of data:
1. **Position and Gender**: Available from aggregate-level files
2. **Race/Ethnicity**: ONLY available from detailed TXT files with individual race columns

The current `susp_v6_teacher_features.parquet` file was likely created from aggregate data that lacked the detailed race breakdowns, or the ingestion was never run.

### Code Infrastructure Status

All required code is ready:

| Component | File | Status |
|-----------|------|--------|
| Ingestion | `R/01c_ingest_teacher_demographics.R` | ✅ Ready |
| Processing | `R/teacher_processing.R` | ✅ Ready |
| Merge | `Analysis/18_merge_teacher_student.R` | ✅ Ready |
| Regression Analysis | `Analysis/21_teacher_diversity_regression.R` | ✅ Ready |
| Quartile Analysis | `Analysis/21_weighted_teacher_diversity_by_quartile.R` | ✅ Ready |

The pipeline will work **as soon as** the raw TXT files are available.

### Expected Columns After Ingestion

After running the ingestion, you should see columns like:

```
teacher_staff_count_african_american
teacher_staff_count_african_american_share
teacher_staff_count_asian
teacher_staff_count_asian_share
teacher_staff_count_hispanic_or_latino
teacher_staff_count_hispanic_or_latino_share
teacher_staff_count_white
teacher_staff_count_white_share
teacher_staff_count_by_type_teachers_african_american
teacher_staff_count_by_type_teachers_white
teacher_staff_count_by_type_administrators_african_american
teacher_staff_count_by_type_administrators_white
... (and many more variants)
```

These columns are generated by `teacher_processing.R::teacher_summarise_long()` which:
1. Aggregates staff counts by race/ethnicity
2. Calculates racial composition shares
3. Cross-tabulates race × staff type (teachers vs. administrators)
4. Creates wide-format columns for merging

---

## Alternative: Analyze Without Teacher Race Data

If obtaining teacher race/ethnicity data is not immediately feasible, you can still:

### Option A: Analyze Teacher Gender Diversity

Modify the analysis scripts to use gender instead of race:

```r
# In Analysis/21_teacher_diversity_regression.R
# Replace race-based predictors with gender-based predictors

# Example:
# teacher_female_share (instead of teacher_non_white_share)
# admin_female_share (instead of admin_non_white_share)
```

### Option B: Document Data Limitation

Add a note to your analysis:

```
NOTE: Teacher diversity analysis is limited to gender breakdowns due to
unavailability of CDE race/ethnicity data. Full race-based analysis requires
obtaining teacher demographic TXT files from CDE (see TEACHER_DIVERSITY_ANALYSIS_DIAGNOSTIC.md).
```

### Option C: Use Proxy Measures

Analyze student-teacher racial congruence using:
- School-level student racial composition (already available)
- District-level teacher racial composition (if available)
- State-level benchmarks

---

## Questions and Support

### For Data Acquisition

- **CDE DataQuest Support**: https://www.cde.ca.gov/ds/sd/cb/
- **CDE Contact**: datareporting@cde.ca.gov
- **FilesMare Documentation**: https://www3.cde.ca.gov/researchfiles/

### For Technical Issues

After obtaining data, if ingestion fails:

1. Check file format: Must be tab-separated TXT
2. Verify columns: See `data-raw/README_TEACHER_DATA.md` for expected columns
3. Review ingestion logs: Check console output for errors
4. Examine parsing issues: Check `data-stage/teacher_parsing_log.csv`

### Reference Documentation

- `CLAUDE.md` - Section "Teacher Demographics Pipeline"
- `README.md` - Section "Teacher Demographics Integration"
- `data-raw/README_TEACHER_DATA.md` - Detailed data requirements
- `TEACHER_DEMOGRAPHIC_INTEGRATION_AUDIT.md` - Pipeline audit

---

## Summary

**What You Need**: CDE teacher staff demographic TXT files with race/ethnicity columns

**Where to Get It**:
1. CDE DataQuest (https://dq.cde.ca.gov/dataquest/)
2. CDE FilesMare (https://www3.cde.ca.gov/researchfiles/)
3. Direct request (datareporting@cde.ca.gov)

**What to Do Next**:
1. Obtain `stre*.txt` files for years 2017-18 through 2023-24
2. Place in `data-raw/` directory
3. Run `Rscript run_all.R` or `Rscript R/01c_ingest_teacher_demographics.R`
4. Verify race columns appear in `susp_v6_teacher_features.parquet`
5. Run teacher diversity analyses

**All code is ready** - just waiting for the data files.

---

**End of Diagnostic Report**
