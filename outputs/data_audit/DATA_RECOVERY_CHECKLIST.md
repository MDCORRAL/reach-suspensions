# Data Recovery Implementation Checklist

Use this checklist to systematically recover excluded data and expand dashboard coverage.

---

## Phase 1: Race/Ethnicity Access (1-2 weeks)

### Audit Current State
- [ ] List all existing dashboards:
  ```bash
  ls *.html
  ```
- [ ] For each dashboard, check if race/ethnicity filter exists
- [ ] Document which dashboards have it, which don't

### Dashboard Files to Check
- [ ] `suspension_dashboard.html`
- [ ] `race_year_rates_dashboard.html` (likely already has it)
- [ ] `quartile_suspension_dashboard.html`
- [ ] `tail_concentration_dashboard.html`
- [ ] `suspension_categories_dashboard.html`

### Implementation
- [ ] For each dashboard lacking race filter, add:
  ```html
  <select id="raceEthnicityFilter">
    <option value="all">All Students</option>
    <option value="Black/African American">Black/African American</option>
    <option value="Hispanic/Latino">Hispanic/Latino</option>
    <option value="White">White</option>
    <option value="Asian">Asian</option>
    <option value="Filipino">Filipino</option>
    <option value="American Indian/Alaska Native">American Indian/Alaska Native</option>
    <option value="Native Hawaiian/Pacific Islander">Native Hawaiian/Pacific Islander</option>
    <option value="Two or More Races">Two or More Races</option>
  </select>
  ```

- [ ] Update corresponding JSON builder scripts to include race breakdowns:
  - `dashboard/build_dashboard_data.py`
  - `dashboard/build_suspension_overview.py`
  - Others as needed

- [ ] Test each updated dashboard

### Validation
- [ ] Verify all race groups appear in dropdown
- [ ] Verify data updates when race filter changes
- [ ] Verify "All Students" matches previous totals

---

## Phase 2: Non-Traditional Schools (2-4 weeks)

### Step 1: Modify Data Processing
- [ ] Create new parameter in config or at top of scripts:
  ```r
  # R/config.R or add to individual scripts
  INCLUDE_NON_TRADITIONAL <- TRUE  # or FALSE to match current behavior
  ```

- [ ] Update filtering in key scripts:

**R/22_build_v6_features.R (lines 398-400):**
```r
# BEFORE:
v6_clean <- v6 %>%
  filter(
    is_traditional %in% TRUE,
    ...
  )

# AFTER:
v6_clean <- v6 %>%
  filter(
    if (INCLUDE_NON_TRADITIONAL) TRUE else is_traditional %in% TRUE,
    ...
  )
```

**graph_scripts/06_statewide_trends.py (line 358):**
```python
# BEFORE:
base = base[base["is_traditional"]]

# AFTER:
if not INCLUDE_NON_TRADITIONAL:
    base = base[base["is_traditional"]]
```

**dashboard/data_sources.py:**
- [ ] Add parameter to `prepare_analysis_frame()` allowing non-traditional inclusion

### Step 2: Create Setting-Specific Analyses
- [ ] Create `R/Analysis/24_traditional_vs_nontraditional_comparison.R`
  ```r
  # Compare suspension rates:
  # - Traditional schools
  # - Non-traditional schools
  # - Side-by-side charts
  # - Export to Excel workbook
  ```

- [ ] Generate summary statistics:
  - [ ] % of schools that are non-traditional
  - [ ] % of students in non-traditional schools
  - [ ] Average suspension rates by setting
  - [ ] Race gaps by setting

### Step 3: Update Dashboards
- [ ] Add school setting filter to each dashboard:
  ```html
  <select id="schoolSettingFilter">
    <option value="all">All Schools</option>
    <option value="traditional" selected>Traditional Schools</option>
    <option value="nontraditional">Non-Traditional Schools</option>
  </select>
  ```

- [ ] Update JSON builders to include setting breakdowns:
  - [ ] `dashboard/build_dashboard_data.py`
  - [ ] `dashboard/build_rates_by_race_year.py`
  - [ ] Others

- [ ] Add documentation panel to dashboards:
  ```html
  <div class="info-panel">
    <strong>About Non-Traditional Schools:</strong>
    Non-traditional schools include alternative schools, continuation schools,
    community day schools, and juvenile court schools. These schools serve
    students with different needs and challenges, so suspension rates may
    differ from traditional schools due to population characteristics rather
    than school practices.
  </div>
  ```

### Validation
- [ ] Verify filter toggles between traditional/non-traditional/all
- [ ] Verify non-traditional counts match expectations
- [ ] Verify documentation is clear and prominent

---

## Phase 3: Intersectional Data (3-6 weeks)

### Step 1: Investigate Source Data
- [ ] Check raw Excel files for race × demographic cross-tabs
  - [ ] Open `copy_CDE_suspensions_1718-2324_sc_oth.xlsx`
  - [ ] Look for subgroups like "Black Students with Disabilities"
  - [ ] Document what intersections are available

- [ ] If intersections NOT in raw data:
  - [ ] Document limitation
  - [ ] Consider requesting from CDE
  - [ ] Explore statistical imputation methods (advanced)

- [ ] If intersections ARE in raw data:
  - [ ] Proceed with full integration below

### Step 2: Build Intersectional Dataset
- [ ] Create `R/23_build_v7_intersectional.R`:
  ```r
  # Read race data
  race_long <- read_parquet(here("data-stage", "susp_v6_long.parquet"))

  # Read demographic data
  demo_long <- read_parquet(here("data-stage", "oth_long.parquet"))

  # Combine (method depends on source data structure)
  # If CDE provides race × demo cross-tabs:
  v7 <- bind_rows(
    race_long %>% mutate(intersection_type = "race_only"),
    demo_long %>% mutate(intersection_type = "demographic_only"),
    intersectional_data %>% mutate(intersection_type = "race_x_demographic")
  )

  write_parquet(v7, here("data-stage", "susp_v7_intersectional.parquet"))
  ```

- [ ] Add to pipeline: Update `run_pipeline.R` to include Step 23

### Step 3: Create Intersectional Analyses
- [ ] Create `R/Analysis/25_intersectional_disparities.R`:
  - [ ] Compare Black SPED vs. White SPED suspension rates
  - [ ] Compare Hispanic ELL vs. White ELL suspension rates
  - [ ] Compare by school level (Elementary Black SPED, etc.)
  - [ ] Export to Excel

- [ ] Create visualizations showing within-subgroup disparities

### Step 4: Update Dashboards
- [ ] Add intersectional filters:
  ```html
  <div class="filter-group">
    <label>Race/Ethnicity:</label>
    <select id="raceFilter">
      <option value="all">All Students</option>
      <option value="Black/African American">Black/African American</option>
      <!-- ... -->
    </select>

    <label>Student Group:</label>
    <select id="demographicFilter">
      <option value="all">All Students</option>
      <option value="SPED">Students with Disabilities</option>
      <option value="ELL">English Learners</option>
      <option value="Foster">Foster Youth</option>
      <option value="Homeless">Homeless Students</option>
      <option value="SED">Socioeconomically Disadvantaged</option>
    </select>
  </div>
  ```

- [ ] Update JSON builders to support intersectional queries

### Validation
- [ ] Verify totals: Black SPED + Black non-SPED = Total Black students
- [ ] Verify rates make sense (SPED rates often higher than all-student rates)
- [ ] Check for suppressed cells (small intersections may be suppressed)

---

## Phase 4: District/County Aggregates (4-8 weeks)

### Step 1: Preserve Aggregate Data
- [ ] Create `R/02c_preserve_aggregates.R`:
  ```r
  v1 <- read_parquet(here("data-stage", "susp_v1.parquet"))

  # Preserve districts
  v1_districts <- v1 %>%
    filter(
      tolower(aggregate_level) == "d",
      is.na(charter_yn) | charter_yn != "All"
    )
  write_parquet(v1_districts, here("data-stage", "susp_v1_districts.parquet"))

  # Preserve counties
  v1_counties <- v1 %>%
    filter(
      tolower(aggregate_level) == "c",
      is.na(charter_yn) | charter_yn != "All"
    )
  write_parquet(v1_counties, here("data-stage", "susp_v1_counties.parquet"))
  ```

- [ ] Add to pipeline: Update `run_pipeline.R` to include Step 02c

### Step 2: Build District Features
- [ ] Create `R/23_build_v6_district_features.R` (modeled after campus version):
  ```r
  # Similar to R/22_build_v6_features.R but for districts
  # One row per district-year
  # District-level demographics
  # District-level quartiles (by district enrollment composition)
  ```

- [ ] Output: `susp_v6_districts.parquet`

### Step 3: Create District Dashboards
- [ ] Create `district_dashboard.html`:
  - [ ] District suspension rate trends
  - [ ] District rankings
  - [ ] District-level race gaps
  - [ ] Comparison to state average

- [ ] Create `dashboard/build_district_data.py` to generate JSON

### Step 4: Repeat for Counties
- [ ] Create `R/24_build_v6_county_features.R`
- [ ] Create `county_dashboard.html`
- [ ] Create `dashboard/build_county_data.py`

### Validation
- [ ] Verify district totals sum to state totals
- [ ] Verify county totals sum to state totals
- [ ] Check for district/county name consistency

---

## Phase 5: Suspension Reasons (1-2 weeks)

### Step 1: Verify Reason Data Exists
- [ ] Confirm `susp_v5_long.parquet` has reason columns:
  - `reason` (code)
  - `reason_lab` (label)
  - Reason shares: `share_violent_injury`, `share_violent_no_injury`, etc.

### Step 2: Add Reason Filter to Dashboards
- [ ] Add reason filter:
  ```html
  <select id="suspensionReasonFilter">
    <option value="all">All Reasons</option>
    <option value="violent_injury">Violent (Injury)</option>
    <option value="violent_no_injury">Violent (No Injury)</option>
    <option value="weapons_possession">Weapons Possession</option>
    <option value="illicit_drug">Illicit Drugs</option>
    <option value="defiance_only">Willful Defiance</option>
    <option value="other_reasons">Other Reasons</option>
  </select>
  ```

- [ ] Update JSON builders to include reason-specific rates

### Step 3: Create Reason-Focused Analyses
- [ ] Expand `graph_scripts/20_suspension_reason_trends_by_level_and_locale.py`:
  - [ ] Currently exists but may not be in all dashboards
  - [ ] Integrate into main dashboard

- [ ] Create "Willful Defiance Trends" special report (policy-relevant)

- [ ] Add race × reason analysis: "Are Black students suspended for defiance more than White students?"

### Validation
- [ ] Verify reason totals sum to overall suspensions
- [ ] Check for missing reason data (some years may not have reason breakdowns)

---

## Phase 6: Data Quality & Documentation (Ongoing)

### Data Quality Dashboard
- [ ] Create `data_quality_dashboard.html`:
  - [ ] % records suppressed by year
  - [ ] % records with missing enrollment
  - [ ] % records with missing suspension counts
  - [ ] Trends over time

- [ ] Create `dashboard/build_data_quality.py`

### Documentation
- [ ] Create user guide: `docs/USER_GUIDE.md`
  - [ ] Explain all filters
  - [ ] Explain what's included/excluded
  - [ ] Provide interpretation guidelines

- [ ] Create data dictionary: `docs/DATA_DICTIONARY.md`
  - [ ] List all fields
  - [ ] Explain suppression policy
  - [ ] Document filtering decisions

- [ ] Add FAQ section to dashboards:
  - [ ] "Why don't I see my district?"
  - [ ] "Why are non-traditional schools separate?"
  - [ ] "What does 'suppressed' mean?"

### Downloadable Datasets
- [ ] Create CSV exports for each dashboard view
- [ ] Add "Download Data" button to dashboards
- [ ] Include data documentation with downloads

---

## Testing & Validation

### For Each Phase:
- [ ] Unit test: Individual components work
- [ ] Integration test: Components work together
- [ ] User test: Stakeholders can use dashboards
- [ ] Performance test: Dashboards load in <5 seconds
- [ ] Accuracy test: Spot-check numbers against known totals

### Regression Testing:
- [ ] Verify existing analyses still work
- [ ] Verify existing dashboard numbers unchanged (when using same filters)
- [ ] Verify pipeline can still run end-to-end

---

## Rollout Strategy

### Pilot Phase
- [ ] Share Phase 1 (race/ethnicity) with small group
- [ ] Collect feedback
- [ ] Iterate based on feedback

### Staged Rollout
- [ ] Release Phase 1 (race/ethnicity access)
- [ ] Wait 2 weeks, collect feedback
- [ ] Release Phase 2 (non-traditional schools)
- [ ] Wait 2 weeks, collect feedback
- [ ] Release Phases 3-5 together

### Communication
- [ ] Announce each release with:
  - [ ] What's new
  - [ ] How to use it
  - [ ] Known limitations
  - [ ] Feedback mechanism

---

## Success Metrics

Track these to measure progress:

- [ ] % dashboards with race/ethnicity filter: ___% → 100%
- [ ] % schools included in public dashboards: ___% → ~95%
- [ ] # intersectional analyses available: 0 → 6+
- [ ] # district-level dashboards: 0 → 3
- [ ] # dashboards with reason breakdown: 1 → 5+
- [ ] User satisfaction score: ___ → >80%
- [ ] Dashboard usage (page views): ___ → +50%

---

## Resources Needed

### Skills
- [ ] R programming (for pipeline modifications)
- [ ] Python programming (for dashboard builders)
- [ ] JavaScript (for dashboard interactivity)
- [ ] Data analysis (for validation)

### Time Estimates
- Phase 1 (Race/Ethnicity): 1-2 weeks
- Phase 2 (Non-Traditional): 2-4 weeks
- Phase 3 (Intersectional): 3-6 weeks (depends on data availability)
- Phase 4 (District/County): 4-8 weeks
- Phase 5 (Reasons): 1-2 weeks
- Phase 6 (Documentation): Ongoing

**Total:** 11-23 weeks depending on parallelization

### Budget
- [ ] Developer time: ___ hours @ $___/hour
- [ ] QA/testing time: ___ hours @ $___/hour
- [ ] Stakeholder review time: ___ hours
- [ ] Server/hosting costs: $___/month (if increased load)

---

## Blockers & Dependencies

### Potential Blockers
- [ ] R/Python environment not set up → Set up development environment
- [ ] Raw data doesn't have intersections → Request from CDE or document limitation
- [ ] Performance issues with larger datasets → Optimize queries, pre-aggregate
- [ ] Stakeholder disagreement on priorities → Facilitate prioritization meeting

### Dependencies
- [ ] Access to production server for deployment
- [ ] Approval from stakeholders for each phase
- [ ] CDE cooperation if requesting additional data
- [ ] User testing group availability

---

## Contact & Support

**For Questions:**
- Technical: Review `COMPREHENSIVE_DATA_AUDIT_REPORT.md` for detailed explanations
- Priority decisions: Schedule stakeholder meeting
- CDE data requests: Contact [CDE contact person]

**Key Files:**
- Pipeline: `run_pipeline.R`
- Campus features: `R/22_build_v6_features.R`
- Dashboard builders: `dashboard/build_*.py`
- Visualizations: `graph_scripts/*.py`

---

**Remember:** Start with quick wins (Phase 1), build momentum, tackle bigger pieces systematically. Test thoroughly before rollout. Communicate clearly with users about what's changing and why.
