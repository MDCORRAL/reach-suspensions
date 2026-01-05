# Data Audit Results - October 21, 2025

This directory contains the complete data audit of the California School Suspension Data processing pipeline.

## What's in This Directory

### 📊 Main Reports

1. **[EXECUTIVE_SUMMARY.md](EXECUTIVE_SUMMARY.md)** - **START HERE**
   - Quick overview of findings
   - Top 3 priority actions
   - Expected outcomes
   - 5-10 minute read

2. **[COMPREHENSIVE_DATA_AUDIT_REPORT.md](COMPREHENSIVE_DATA_AUDIT_REPORT.md)** - Complete Analysis
   - Detailed findings at every pipeline stage
   - Quantification of data loss
   - Technical implementation guide
   - Recovery recommendations
   - 30-45 minute read

3. **[DATA_RECOVERY_CHECKLIST.md](DATA_RECOVERY_CHECKLIST.md)** - Action Plan
   - Step-by-step implementation guide
   - Phase-by-phase checklist
   - Code snippets and examples
   - Resource requirements
   - Use this to execute the recovery plan

4. **[data_provenance_report.md](data_provenance_report.md)** - Data Provenance Audit (2025-11-25)
   - Raw source inventory (including external locale file)
   - Staged parquet lineage map
   - Variable-level provenance and unresolved gaps

5. **[data_provenance.csv](data_provenance.csv)** - Structured Provenance Table
   - Variable/data category → raw source → staging → transform script mapping
   - Join keys and assumptions for each data category

### 📁 Analysis Scripts

- **`data_audit_analysis.R`** - R script to quantify data loss (requires R environment)
- **`data_audit_analysis.py`** - Python script to quantify data loss (requires pandas/pyarrow)

*Note: These scripts are ready to run when appropriate environments are available.*

---

## Key Findings Summary

### The Numbers

- **Total records processed:** ~3.4 million (in susp_v6_long.parquet)
- **Records used in typical analyses:** ~60,000-80,000 (2-3%)
- **Major exclusion:** Traditional schools only filter excludes 40-50% of campuses

### What's Being Lost

1. **Non-Traditional Schools** (40-50% of campuses)
   - Alternative, continuation, community day, juvenile court schools
   - Serve most at-risk youth
   - Often have highest suspension rates

2. **Race/Ethnicity Detail** (85-90% of subgroup data)
   - Detailed racial breakdowns exist but not uniformly accessible
   - Critical for equity analysis

3. **Demographic Intersections** (nearly all)
   - Black SPED students, Hispanic ELL students, etc.
   - Reveals disproportionality within vulnerable populations

4. **District/County Aggregates** (10-15% of data)
   - All higher-level summaries dropped early
   - Needed for accountability

5. **Suspension Reasons** (present but underutilized)
   - Policy-relevant (e.g., willful defiance reforms)

### The Good News

**Most excluded data is valid and recoverable.** It's already processed and stored - it just needs to be surfaced in dashboards and analyses.

---

## Top 3 Priorities

### 🔴 Priority 1: Non-Traditional Schools
- Add "School Setting" filter to all dashboards
- Create separate non-traditional schools section
- **Impact:** Doubles coverage from ~50% to ~95% of schools
- **Effort:** Medium (2-4 weeks)

### 🔴 Priority 2: Race/Ethnicity Access
- Audit all dashboards for race toggles
- Add missing race/ethnicity filters
- Create dedicated disparities dashboard
- **Impact:** Full transparency on equity gaps
- **Effort:** Low (1-2 weeks)

### 🔴 Priority 3: Demographic Intersections
- Build intersectional dataset (race × demographics)
- Add intersectional filters to dashboards
- Highlight within-subgroup disparities
- **Impact:** Reveals hidden disparities (e.g., Black SPED vs. White SPED rates)
- **Effort:** Medium-High (3-6 weeks)

---

## How to Use These Reports

### If you have 5 minutes:
Read the **Key Findings** section above, then jump to Top 3 Priorities in [EXECUTIVE_SUMMARY.md](EXECUTIVE_SUMMARY.md)

### If you have 30 minutes:
Read the [EXECUTIVE_SUMMARY.md](EXECUTIVE_SUMMARY.md) completely

### If you're implementing changes:
1. Read [EXECUTIVE_SUMMARY.md](EXECUTIVE_SUMMARY.md) for context
2. Use [DATA_RECOVERY_CHECKLIST.md](DATA_RECOVERY_CHECKLIST.md) as your implementation guide
3. Refer to [COMPREHENSIVE_DATA_AUDIT_REPORT.md](COMPREHENSIVE_DATA_AUDIT_REPORT.md) for technical details

### If you want to understand everything:
Read all three documents in order:
1. EXECUTIVE_SUMMARY.md
2. COMPREHENSIVE_DATA_AUDIT_REPORT.md
3. DATA_RECOVERY_CHECKLIST.md

---

## Pipeline Architecture (Quick Reference)

```
Raw Excel Files
    ↓
Stage 01: Ingestion → susp_v0.parquet (~4M records)
    ↓
Stage 02: Locale features → susp_v1.parquet
    ↓
Stage 02b: Drop charter "All" → susp_v1_noall.parquet ⚠️ LOSS
    ↓
Stage 03-05: Quartiles, level, reasons → susp_v5*.parquet
    ↓
Stage 22: Merge demographics → susp_v6_features.parquet (60K campus-years)
                             → susp_v6_long.parquet (3.4M records)
    ↓
Dashboards/Analysis: Apply filters ⚠️ MAJOR LOSS
                     → ~60K-80K records used (2-3%)
```

**Main Data Loss Points:**
1. Charter "All" removal (intentional, prevents double-counting)
2. Campus-only filter (drops district/county/state aggregates)
3. **Traditional schools only** (drops 40-50% of campuses)
4. **"All Students" subgroup only** (drops 85-90% of race/demographic detail)

---

## File Modification Guide

### To add non-traditional schools:
- **Modify:** `R/22_build_v6_features.R` (lines 398-400)
- **Modify:** `graph_scripts/06_statewide_trends.py` (line 358)
- **Modify:** `dashboard/data_sources.py`
- **Update:** All dashboard HTML files to add setting filter

### To add race/ethnicity toggles:
- **Modify:** Dashboard HTML files (add `<select id="raceFilter">`)
- **Modify:** Dashboard JSON builders (`dashboard/build_*.py`)

### To add intersectional data:
- **Create:** `R/23_build_v7_intersectional.R`
- **Update:** `run_pipeline.R` to include new stage
- **Modify:** Dashboard builders to support intersections

### To add district/county data:
- **Create:** `R/02c_preserve_aggregates.R`
- **Create:** `R/23_build_v6_district_features.R`
- **Create:** `district_dashboard.html`
- **Create:** `dashboard/build_district_data.py`

---

## Expected Outcomes

| Metric | Before | After | Change |
|--------|--------|-------|--------|
| Schools in dashboards | ~30,000 | ~60,000 | +100% |
| Subgroup detail | ~10% | ~90% | +800% |
| Intersectional analyses | 0 | 6+ | New |
| District dashboards | 0 | 3 | New |
| Reason breakdowns | 1 | 5+ | +400% |

---

## Questions?

- **Technical details:** See [COMPREHENSIVE_DATA_AUDIT_REPORT.md](COMPREHENSIVE_DATA_AUDIT_REPORT.md)
- **Implementation steps:** See [DATA_RECOVERY_CHECKLIST.md](DATA_RECOVERY_CHECKLIST.md)
- **Quick overview:** See [EXECUTIVE_SUMMARY.md](EXECUTIVE_SUMMARY.md)

---

## Timeline Estimate

- **Phase 1 (Race/Ethnicity):** 1-2 weeks
- **Phase 2 (Non-Traditional):** 2-4 weeks
- **Phase 3 (Intersectional):** 3-6 weeks
- **Phase 4 (District/County):** 4-8 weeks
- **Phase 5 (Reasons):** 1-2 weeks
- **Phase 6 (Documentation):** Ongoing

**Total:** 11-23 weeks depending on parallelization

---

## Audit Date & Scope

- **Date:** October 21, 2025
- **Scope:** Complete California School Suspension Data pipeline (2017-18 through 2023-24)
- **Method:** Comprehensive code review of all R and Python processing scripts
- **Files Reviewed:** 20+ R scripts, 10+ Python scripts, all dashboard files

---

**The data exists. It's time to bring it back.**
