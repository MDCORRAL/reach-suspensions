# Teacher Demographic Data Files

## Required Files

This directory should contain CDE Teacher Staff Demographics TXT files following the pattern:

```
stre{YYZZ}.txt
```

Where `YYZZ` represents the academic year (e.g., `1920` for 2019-20).

## Expected Files for Analysis

Based on the project's academic year coverage (2017-18 through 2023-24), you need:

- `stre1718.txt` - 2017-18 academic year
- `stre1819.txt` - 2018-19 academic year
- `stre1920.txt` - 2019-20 academic year
- `stre2021.txt` - 2020-21 academic year
- `stre2122.txt` - 2021-22 academic year
- `stre2223.txt` - 2022-23 academic year
- `stre2324.txt` - 2023-24 academic year

## Data Source

These files come from the **California Department of Education (CDE) Staff Demographics** data collection.

### Official CDE Data Portal

**Primary Source**: CDE DataQuest - Staff Demographics
- URL: https://dq.cde.ca.gov/dataquest/
- Navigate to: `Staff` → `Staff Demographics` → `Download Data Files`

### Alternative: CDE FilesMare

**Direct File Downloads**: CDE FilesMare FTP
- URL: https://www3.cde.ca.gov/researchfiles/
- Look for files in the format: `stre{YYZZ}.txt` or similar teacher/staff demographic files

### Data Request (if needed)

If files are not publicly available:
- **CDE Data Reporting Office**: datareporting@cde.ca.gov
- **Request**: "Staff Demographics by School, Race/Ethnicity, and Gender for academic years 2017-18 through 2023-24"
- **Specify**: School-level aggregation (aggregate_level = "S")

## File Format

Expected TXT file format:
- **Delimiter**: Tab-separated values (TSV)
- **Encoding**: UTF-8
- **Header row**: Column names in first row

### Expected Columns

The ingestion script (`R/01c_ingest_teacher_demographics.R`) expects the following dimensions:

#### Geographic Identifiers
- `County Code` / `COUNTY CODE`
- `District Code` / `DISTRICT CODE`
- `School Code` / `SCHOOL CODE`
- `County Name`, `District Name`, `School Name`

#### Temporal
- `Academic Year` (format: "2019-20" or derivable from filename)

#### Aggregation Level
- `Aggregate Level`: Must be "S" for school-level (not T/C/D for state/county/district)

#### Demographics
- `Staff Type` / `reporting_category`:
  - `TCH` = Teachers
  - `ADM` = Administrators
  - `PSV` = Pupil Services
  - `OTH` = Other Staff
  - `ALL` = All Staff (aggregate)

- `Staff Gender Code` / `staff_gender_code`:
  - `GF` = Female
  - `GM` = Male
  - `GX` = Non-Binary
  - `GZ` = Gender Missing
  - `ALL` = All Staff

- **Race/Ethnicity Columns** (CRITICAL for Analysis/21):
  - `African American` or `african_american`
  - `American Indian or Alaska Native` or `american_indian_or_alaska_native`
  - `Asian`
  - `Filipino`
  - `Hispanic or Latino` or `hispanic_or_latino`
  - `Native Hawaiian/Pacific Islander` or `pacific_islander`
  - `White`
  - `Two or More Races` or `two_or_more_races`
  - `Not Reported` or `not_reported`

#### School Characteristics
- `Charter` / `charter_yn`: "Yes", "No", or "ALL"
- `School Grade Span`: "GS_K6", "GS_69", "GS_912", "GS_K12", "ALL"

#### Staff Counts
- `Total Staff Count` or individual race columns with numeric values

## Data Quality Requirements

The ingestion script validates:

1. ✅ **School-level only**: Filters to `aggregate_level = "S"`
2. ✅ **Valid charter codes**: Removes "ALL" charter at school level
3. ✅ **Valid grade spans**: Removes "ALL" at school level
4. ✅ **Staff type codes**: Must be TCH/ADM/PSV/OTH/ALL
5. ✅ **Gender codes**: Must be GF/GM/GX/GZ/ALL
6. ✅ **Race categories**: Must match CDE 9-category standard

## After Obtaining Files

Once you've placed the files in this directory:

```bash
# Navigate to project root
cd /home/user/reach-suspensions

# Run the ingestion script (option 1: individual script)
Rscript R/01c_ingest_teacher_demographics.R

# OR run the full pipeline (option 2: comprehensive)
Rscript run_all.R
```

This will:
1. Ingest all `stre*.txt` files
2. Create `data-stage/teacher_staff_long.parquet` with race/ethnicity data
3. Run Analysis/18 to merge with student suspension data
4. Generate `data-stage/susp_v6_teacher_features.parquet` with full teacher demographics

## Verification

After ingestion, verify race/ethnicity data is present:

```r
library(arrow)
library(dplyr)

# Check teacher data
teacher <- read_parquet("data-stage/teacher_staff_long.parquet")

# Should show 9 race/ethnicity categories
print(unique(teacher$race_ethnicity))

# Should show race columns in merged data
merged <- read_parquet("data-stage/susp_v6_teacher_features.parquet")
grep("teacher.*african|teacher.*white|teacher.*hispanic", names(merged), value = TRUE)
```

## Current Status

❌ **No teacher TXT files present** - Files need to be obtained from CDE

Once files are added:
- ✅ Ingestion script ready: `R/01c_ingest_teacher_demographics.R`
- ✅ Processing utilities ready: `R/teacher_processing.R`
- ✅ Merge script ready: `Analysis/18_merge_teacher_student.R`
- ✅ Analysis script ready: `Analysis/21_weighted_teacher_diversity_by_quartile.R`

## Questions?

See:
- `CLAUDE.md` - Section "Teacher Demographics Pipeline"
- `README.md` - Section "Teacher Demographics Integration"
- `TEACHER_DEMOGRAPHIC_INTEGRATION_AUDIT.md` - Comprehensive audit report
- `R/01c_ingest_teacher_demographics.R` - Lines 1-96 for detailed field documentation
