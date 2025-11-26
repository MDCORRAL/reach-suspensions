# Teacher Race/Ethnicity Data - Setup Guide

**Issue**: Analysis/21 (weighted teacher diversity by quartile) cannot run because teacher demographic data with race/ethnicity breakdowns is not available.

**Branch**: `claude/add-teacher-race-ethnicity-data-01MGndxCD3ZjNEYnQasx25Qn`

**Status**: ⚠️ **ACTION REQUIRED** - Raw CDE teacher data files need to be obtained and processed

---

## Problem Summary

When running `Analysis/21_weighted_teacher_diversity_by_quartile.R`, the script fails with:

```
>>> ERROR: Cannot proceed with teacher diversity analysis
>>> The teacher data does not include race/ethnicity breakdowns.
```

### Root Cause

1. **No raw teacher data files**: The CDE teacher demographic TXT files (`stre*.txt`) are not present in the repository
2. **Teacher pipeline not executed**: `R/01c_ingest_teacher_demographics.R` has not been run
3. **Missing output file**: `data-stage/teacher_staff_long.parquet` does not exist

### Current State

- ✅ **Code is ready**: All scripts for teacher data processing exist and are well-tested
  - `R/01c_ingest_teacher_demographics.R` - Ingestion with full race/ethnicity support
  - `R/teacher_processing.R` - Processing utilities with race summarization
  - `Analysis/18_merge_teacher_student.R` - Merge teacher + student data
  - `Analysis/21_weighted_teacher_diversity_by_quartile.R` - Diversity analysis

- ❌ **Data is missing**: Raw CDE files are not present
  - Need: `data-raw/stre1718.txt`, `stre1819.txt`, ..., `stre2324.txt`
  - Currently: `data-raw/` directory is empty

- ⚠️ **Partial merge exists**: `data-stage/susp_v6_teacher_features.parquet` exists but likely contains only student data (no teacher race/ethnicity columns)

---

## Solution: Obtain and Process Teacher Data

### Step 1: Obtain Raw Teacher Data Files

You need to download CDE Teacher Staff Demographics files for academic years 2017-18 through 2023-24.

#### Option A: CDE DataQuest Portal (Recommended)

1. Visit **CDE DataQuest**: https://dq.cde.ca.gov/dataquest/
2. Navigate to: `Staff` → `Staff Demographics` → `Download Data Files`
3. Download TXT files for each year:
   - 2017-18: `stre1718.txt`
   - 2018-19: `stre1819.txt`
   - 2019-20: `stre1920.txt`
   - 2020-21: `stre2021.txt`
   - 2021-22: `stre2122.txt`
   - 2022-23: `stre2223.txt`
   - 2023-24: `stre2324.txt`

#### Option B: CDE FilesMare FTP

1. Visit **CDE FilesMare**: https://www3.cde.ca.gov/researchfiles/
2. Look for teacher/staff demographic files in the research files section
3. Download all available years

#### Option C: Data Request

If files are not publicly available, contact CDE:
- **Email**: datareporting@cde.ca.gov
- **Request**: "Staff Demographics by School, Race/Ethnicity, Gender, and Staff Type for academic years 2017-18 through 2023-24"
- **Specify**: School-level aggregation (`aggregate_level = "S"`), including race/ethnicity breakdowns

### Step 2: Place Files in Repository

```bash
# Files should be placed here:
/home/user/reach-suspensions/data-raw/stre1718.txt
/home/user/reach-suspensions/data-raw/stre1819.txt
/home/user/reach-suspensions/data-raw/stre1920.txt
/home/user/reach-suspensions/data-raw/stre2021.txt
/home/user/reach-suspensions/data-raw/stre2122.txt
/home/user/reach-suspensions/data-raw/stre2223.txt
/home/user/reach-suspensions/data-raw/stre2324.txt
```

**Note**: These files are excluded from git (in `.gitignore`) because they are large and contain raw data.

### Step 3: Run the Teacher Data Pipeline

Once files are in place, run the full pipeline:

```bash
cd /home/user/reach-suspensions

# Option 1: Run full pipeline (recommended)
Rscript run_all.R

# Option 2: Run just teacher ingestion
Rscript R/01c_ingest_teacher_demographics.R

# Option 3: Run teacher ingestion + merge
Rscript R/01c_ingest_teacher_demographics.R
Rscript Analysis/18_merge_teacher_student.R
```

### Step 4: Verify Race/Ethnicity Data

After ingestion, run the automated diagnostic to confirm that the staged parquet
files expose the race columns required by `Analysis/21`:

```bash
Rscript R/check_teacher_race_columns.R
```

Sample success output:

```
[teacher-check] Reading /.../data-stage/teacher_staff_long.parquet
[teacher-check] Summarising teacher data to wide format ...
[teacher-check] ✔ Found 9 race count columns in teacher summary.
                 Examples: teacher_staff_count_african_american, ...
[teacher-check] ✔ Merged dataset includes 9 race columns.
```

If the script stops with an error, re-run `R/01c_ingest_teacher_demographics.R`
after confirming that the `stre*.txt` files include race/ethnicity detail. When
the teacher summary contains the columns but the merged features do not, re-run
`Analysis/18_merge_teacher_student.R` so the race fields flow into
`susp_v6_teacher_features.parquet`.

### Step 5: Run Analysis/21

Now the diversity analysis should work:

```r
source("Analysis/21_weighted_teacher_diversity_by_quartile.R")
```

Expected output:
- Tables in `outputs/tables/21_teacher_diversity_by_quartile_*.csv`
- Visualizations in `outputs/graphs/21_teacher_diversity_by_quartile_*.png`

---

## What the Teacher Data Contains

The CDE teacher demographic files include:

### Critical Dimensions for Analysis/21

1. **Race/Ethnicity** (9 CDE categories):
   - African American
   - American Indian or Alaska Native
   - Asian
   - Filipino
   - Hispanic or Latino
   - Native Hawaiian/Pacific Islander
   - White
   - Two or More Races
   - Not Reported

2. **Staff Type** (`reporting_category`):
   - `TCH` = Teachers (classroom teachers, instructional staff)
   - `ADM` = Administrators (principals, assistant principals)
   - `PSV` = Pupil Services (counselors, psychologists, social workers, nurses)
   - `OTH` = Other Non-Instructional Staff (clerical, custodial, etc.)
   - `ALL` = All Staff (aggregate across all types)

3. **Gender**:
   - `GF` = Female
   - `GM` = Male
   - `GX` = Non-Binary
   - `GZ` = Gender Missing
   - `ALL` = All Staff

### Data Processing

The pipeline creates these outputs:

1. **`teacher_staff_long.parquet`**: Long-format teacher data
   - One row per school-year-race-gender-staff_type
   - Columns: `cds_school`, `academic_year`, `race_ethnicity`, `staff_gender_code`, `reporting_category`, `staff_count`

2. **`susp_v6_teacher_features.parquet`**: Merged student + teacher data
   - Wide-format with columns like:
     - `teacher_staff_count_total` - Total staff count
     - `teacher_staff_count_african_american` - Count of African American staff
     - `teacher_staff_count_african_american_share` - % African American
     - `teacher_staff_count_by_type_teachers_african_american` - African American teachers only
     - `teacher_staff_count_by_type_administrators_african_american` - African American administrators only
     - Similar columns for all 9 race/ethnicity categories
     - Gender breakdowns: `teacher_staff_count_by_gender_female`, etc.

---

## Expected File Structure After Setup

```
reach-suspensions/
├── data-raw/
│   ├── stre1718.txt          # CDE teacher data 2017-18
│   ├── stre1819.txt          # CDE teacher data 2018-19
│   ├── stre1920.txt          # CDE teacher data 2019-20
│   ├── stre2021.txt          # CDE teacher data 2020-21
│   ├── stre2122.txt          # CDE teacher data 2021-22
│   ├── stre2223.txt          # CDE teacher data 2022-23
│   ├── stre2324.txt          # CDE teacher data 2023-24
│   └── README_TEACHER_DATA.md # Documentation
│
├── data-stage/
│   ├── teacher_staff_long.parquet         # NEW: Teacher demographics (long)
│   ├── teacher_data_lineage.csv           # NEW: Data lineage audit trail
│   ├── teacher_parsing_log.csv            # NEW: Parsing issues log
│   ├── susp_v6_teacher_features.parquet   # UPDATED: Now with race/ethnicity
│   └── [other existing files]
│
├── outputs/
│   ├── tables/
│   │   └── 21_teacher_diversity_by_quartile_*.csv  # NEW: Analysis results
│   └── graphs/
│       └── 21_teacher_diversity_by_quartile_*.png  # NEW: Visualizations
```

---

## Validation Checklist

Before considering the task complete, verify:

- [ ] Raw teacher TXT files are in `data-raw/` (7 files: stre1718.txt through stre2324.txt)
- [ ] `data-stage/teacher_staff_long.parquet` exists and is not empty
- [ ] `teacher_staff_long.parquet` contains `race_ethnicity` column with 9 categories
- [ ] `teacher_staff_long.parquet` contains `reporting_category` column with 5 staff types (TCH, ADM, PSV, OTH, ALL)
- [ ] `data-stage/susp_v6_teacher_features.parquet` contains columns like `teacher_staff_count_african_american`
- [ ] Analysis/21 runs without errors
- [ ] Analysis/21 produces output tables and graphs in `outputs/`
- [ ] Output tables show teacher diversity metrics by Black enrollment quartile

---

## Troubleshooting

### "No stre*.txt files found"

**Cause**: Raw teacher files are not in `data-raw/`

**Fix**: Download files from CDE (see Step 1 above) and place in `data-raw/` directory

### "Invalid staff type codes found"

**Cause**: Raw files may have data quality issues or unexpected formats

**Fix**: Check `data-stage/teacher_parsing_log.csv` for details. The ingestion script filters invalid codes automatically.

### "Teacher coverage very low (<20%)"

**Cause**: Some years may have incomplete CDE reporting

**Expected**: Teacher data coverage varies by year. Recent years (2019-20+) typically have better coverage.

### "Race columns still not found after ingestion"

**Cause**: Raw TXT files may not contain race/ethnicity breakdowns

**Fix**:
1. Open one of the TXT files and verify it has columns like `African American`, `White`, `Hispanic or Latino`, etc.
2. If columns are named differently, update the column mapping in `R/01c_ingest_teacher_demographics.R` lines 216-229
3. Contact CDE to confirm race/ethnicity data availability for your requested years

---

## Additional Resources

- **Detailed field documentation**: `R/01c_ingest_teacher_demographics.R` (lines 1-96)
- **Processing pipeline**: `CLAUDE.md` → "Teacher Demographics Pipeline" section
- **Integration audit**: `TEACHER_DEMOGRAPHIC_INTEGRATION_AUDIT.md`
- **Data lineage**: Generated at `data-stage/teacher_data_lineage.csv` after ingestion
- **CDE contact**: datareporting@cde.ca.gov

---

## Next Steps

1. **Obtain CDE teacher data files** from DataQuest or FilesMare
2. **Place files in `data-raw/` directory**
3. **Run the pipeline**: `Rscript run_all.R`
4. **Verify outputs** using the checklist above
5. **Run Analysis/21** to generate teacher diversity analysis
6. **Commit and push results** (excluding raw TXT files, per `.gitignore`)

---

**Last Updated**: 2025-11-14
**Author**: Claude (AI Assistant)
**Related Issue**: Adding teacher race/ethnicity data to enable diversity analysis
