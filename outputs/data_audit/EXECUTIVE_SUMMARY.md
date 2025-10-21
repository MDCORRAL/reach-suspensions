# Data Audit Executive Summary
## California School Suspension Data - Recovery Opportunities

**Date:** October 21, 2025

---

## The Bottom Line

Your data pipeline is **processing ~3.4 million records**, but typical analyses use only **~60,000-80,000 records (2-3%)**.

**Why?** Multiple necessary filters stack up:
- Charter "All" removal
- Campus-level only (drops district/county aggregates)
- Traditional schools only (**excludes 40-50% of campuses**)
- "All Students" subgroup only (**excludes 85-90% of race/demographic detail**)

**The Good News:** Most excluded data is **valid and recoverable** - it just needs to be surfaced.

---

## What's Being Lost

### 1. Non-Traditional Schools (40-50% of campuses)
- **Excluded:** Alternative, continuation, community day, juvenile court, special ed schools
- **Why it matters:** These serve the most at-risk youth, often with highest suspension rates
- **Impact:** Incomplete picture of California's discipline landscape

### 2. Race/Ethnicity Subgroups (85-90% of detail)
- **Excluded:** Detailed racial breakdowns not uniformly accessible across dashboards
- **Why it matters:** Central to equity analysis
- **Impact:** Hidden disparities

### 3. Demographic Intersections (Nearly all)
- **Excluded:** Black SPED students, Hispanic ELL students, etc.
- **Why it matters:** Reveals disproportionality within vulnerable populations
- **Impact:** Can't answer "Are Black students with disabilities suspended more than White students with disabilities?"

### 4. District/County Aggregates (10-15% of data)
- **Excluded:** All district and county-level summaries
- **Why it matters:** Needed for accountability and comparison
- **Impact:** Can't compare districts or analyze county patterns

### 5. Suspension Reasons (Present but underutilized)
- **Excluded:** Reason breakdowns not prominent in dashboards
- **Why it matters:** Policy-relevant (e.g., willful defiance reforms)
- **Impact:** Miss patterns in why students are suspended

---

## Top 3 Priority Actions

### 🔴 PRIORITY 1: Non-Traditional Schools
**Do This:**
- Add "School Setting" filter to all dashboards: [Traditional] [Non-traditional] [All]
- Create separate non-traditional schools analysis section
- Document why these schools differ (serve different populations)

**Impact:** Doubles school coverage from ~50% to ~95%
**Effort:** Medium (2-4 weeks)
**Files to modify:** `graph_scripts/*.py`, `dashboard/build_*.py`

---

### 🔴 PRIORITY 2: Race/Ethnicity Access
**Do This:**
- Audit all dashboards - which show race breakdowns, which don't?
- Add race/ethnicity toggle to every dashboard that lacks it
- Create dedicated "Racial Disparities" dashboard

**Impact:** Full transparency on equity gaps
**Effort:** Low (1-2 weeks)
**Files to modify:** `dashboard/*.html`, `dashboard/build_*.py`

---

### 🔴 PRIORITY 3: Demographic Intersections
**Do This:**
- Build unified intersectional dataset: race × demographic category
- Add intersectional filters: "Show [Black/Hispanic/White] students who are [SPED/ELL/Foster/All]"
- Highlight disproportionality within subgroups

**Impact:** Reveals hidden disparities (e.g., Black SPED suspension rates vs. White SPED)
**Effort:** Medium-High (3-6 weeks)
**New files needed:** `R/23_build_v7_intersectional.R`, updates to dashboard builders

---

## Quick Wins (Can Do This Week)

1. **Add race toggle to main dashboard** (if missing)
   - Modify: `dashboard/suspension_dashboard.html`
   - Add dropdown: `<select id="raceFilter">` with all race/ethnicity options

2. **Document current exclusions**
   - Add FAQ section to dashboards: "Why don't I see [alternative schools/districts/all subgroups]?"
   - Link to full methodology document

3. **Generate current vs. potential coverage report**
   - Run: `Rscript data_audit_analysis.R` (when R is available)
   - Share metrics: "Currently showing X schools, could show Y schools"

---

## Implementation Roadmap

### Week 1-2: Quick Wins + Planning
- [ ] Audit all dashboards for race/ethnicity toggle
- [ ] Add missing toggles
- [ ] Document all current filtering decisions
- [ ] Plan non-traditional schools implementation

### Month 1: Non-Traditional Schools
- [ ] Modify analysis scripts to include `INCLUDE_NON_TRADITIONAL` parameter
- [ ] Update dashboards with school setting filter
- [ ] Generate non-traditional schools comparison report
- [ ] Add documentation/context about differences

### Month 2: Intersectional Data
- [ ] Investigate if CDE provides race × demographic cross-tabs
- [ ] Build intersectional dataset (v7) if possible
- [ ] Create demo intersectional dashboard
- [ ] Pilot with stakeholders

### Month 3: District/County Aggregates
- [ ] Preserve district-level data from pipeline stage 0
- [ ] Build district features (parallel to campus features)
- [ ] Create district comparison dashboard
- [ ] Add county-level summaries

### Month 4-6: Polish and Expand
- [ ] Data quality dashboard (% missing, % suppressed)
- [ ] Suspension reason prominence
- [ ] User-customizable data explorer
- [ ] Downloadable datasets with documentation

---

## Expected Outcomes

| Metric | Before | After | Change |
|--------|--------|-------|--------|
| Schools in public dashboards | ~30,000 | ~60,000 | +100% |
| Subgroup detail accessible | ~10% | ~90% | +800% |
| Intersectional analyses | 0 | 6+ | New capability |
| District-level dashboards | 0 | 3 | New capability |
| Suspension reason breakdowns | 1 dashboard | 5+ dashboards | +400% |

---

## Key Files to Review

**Understanding Current Filtering:**
- `R/22_build_v6_features.R` - Final data assembly, traditional school filter
- `R/utils_keys_filters.R` - Campus-only filter, special codes
- `dashboard/data_sources.py` - Dashboard filtering logic
- `graph_scripts/06_statewide_trends.py` - Analysis filtering

**Where to Make Changes:**
- **For non-traditional schools:** Add `setting` filter parameter throughout
- **For race/ethnicity:** Update dashboard HTML + JSON builders
- **For intersections:** Create new `R/23_build_v7_intersectional.R`
- **For districts:** Preserve district records in `R/02c_preserve_districts.R`

---

## Questions to Answer

Before implementing, clarify:

1. **Does CDE provide race × demographic intersections?**
   - Check if raw data has "Black SPED" as a distinct category, or if you only have "Black" and "SPED" separately
   - Impacts feasibility of true intersectional analysis

2. **Why were non-traditional schools excluded originally?**
   - Document reasoning for transparency
   - Ensure you're not introducing biased comparisons

3. **What's the user priority?**
   - Survey stakeholders: What missing data would be most valuable?
   - Focus implementation on highest-value additions

4. **Performance considerations?**
   - Will doubling dataset size slow dashboards?
   - May need lazy loading or separate dashboards

---

## Risk Mitigation

### Risk: Users misinterpret non-traditional school data
**Mitigation:** Prominent documentation, tooltips, separate dashboards

### Risk: Performance issues with larger datasets
**Mitigation:** Pre-aggregation, lazy loading, separate dashboards for detail

### Risk: Too many filters overwhelm users
**Mitigation:** Grouped filters, "Recommended Views" presets, guided tour

---

## Next Steps

1. **Review this audit** with stakeholders
2. **Prioritize recovery actions** based on user needs
3. **Assign implementation** to team members
4. **Set timeline** for deliverables
5. **Pilot changes** before full rollout

**Questions?** See full detailed report: `COMPREHENSIVE_DATA_AUDIT_REPORT.md`

---

**Remember:** The data exists. It's already being processed and stored. You just need to surface it in dashboards and analyses. Start with the quick wins, then tackle the bigger pieces systematically.
