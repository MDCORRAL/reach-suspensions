# Documentation Directory

This directory contains all documentation for the REACH Suspensions Analysis Pipeline, organized into logical subdirectories for easy navigation.

**Last Updated**: 2025-11-18

---

## Directory Structure

```
docs/
├── audits/                 # Audit reports and data quality assessments
├── guides/                 # Setup and usage guides
├── protocols/              # Standard protocols and conventions
├── fixes/                  # Fix summaries and diagnostic reports
├── data-explanations/      # Data documentation (DOCX files)
└── archive/                # Deprecated/old documentation
```

---

## 📊 Audits

Comprehensive audit reports documenting data quality, pipeline validation, and analysis verification.

| File | Description |
|------|-------------|
| `AUDIT_REPORT_DATA_CONSISTENCY.md` | Data consistency checks across pipeline stages |
| `AUDIT_TEACHER_DIVERSITY_REGRESSION.md` | Teacher diversity regression analysis audit |
| `AUDIT_TRAIL_ENHANCEMENTS.md` | Documentation of audit trail enhancements |
| `COMPREHENSIVE_AUDIT_REPORT.md` | Full pipeline audit covering all stages |
| `TEACHER_DEMOGRAPHIC_INTEGRATION_AUDIT.md` | Teacher-student data merge validation |
| `TEACHER_DIVERSITY_ANALYSIS_DIAGNOSTIC.md` | Teacher diversity analysis diagnostics |

---

## 📖 Guides

Step-by-step guides for setup, configuration, and analysis.

| File | Description |
|------|-------------|
| `POWER_ANALYSIS_GUIDE.md` | Guide for conducting and interpreting power analyses |
| `POWER_ANALYSIS_RESULTS_SUMMARY.md` | **NEW** Summary of completed power analysis (2025-11-21) |
| `TEACHER_DATA_SETUP_GUIDE.md` | Guide for obtaining and preparing CDE teacher data |
| `GitHub Workflow in RStudio.Rmd` | Git workflow guide for RStudio users |
| `reach_suspensions.Rmd` | Project overview and analysis walkthrough |
| `session_startup_guide.rmd` | Guide for setting up analysis sessions |

---

## 📋 Protocols

Standard protocols, conventions, and reference documentation used throughout the project.

| File | Description |
|------|-------------|
| `CITATION_STANDARD.md` | Standard citation format for all outputs |
| `PROTOCOL_SCRIPT_REQUEST_REMINDER.md` | Checklist and response reminder to reference CLAUDE.md for any new/updated scripts |
| `PROTOCOL_TEACHER_DATA_MERGE.md` | Protocol for merging teacher and student data |
| `UCLA-Brand-Colors.md` | UCLA-branded color palette documentation |

**Key Usage**:
- Always follow `CITATION_STANDARD.md` when publishing outputs
- Use `UCLA-Brand-Colors.md` color palettes for all visualizations
- Follow `PROTOCOL_TEACHER_DATA_MERGE.md` when working with teacher data

---

## 🔧 Fixes

Fix summaries, diagnostic reports, and issue resolutions.

| File | Description |
|------|-------------|
| `DIAGNOSIS_PIPELINE_FAILURE.md` | Pipeline failure diagnostics |
| `ENHANCEMENTS_SUMMARY.md` | Summary of pipeline enhancements |
| `FIXES_SUMMARY_SCRIPT_23.md` | Fixes for script 23 |
| `FIX_EXPLANATION_Q4_SUSPENSION_DATA.md` | Q4 suspension data fix explanation |
| `FIX_REGRESSION_SCRIPT.md` | Regression script fixes |
| `ISSUE_TEACHER_SHARES_NOT_SUMMING.md` | Teacher share calculation issue resolution |
| `REPAIR_SUMMARY_23_teacher_demographics.md` | Script 23 teacher demographics repair |
| `SCRIPT_18_FIX_SUMMARY.md` | Script 18 fix summary |
| `TEACHER_RACE_DATA_FIX.md` | Teacher race data fixes |

---

## 📄 Data Explanations

Detailed documentation of data structures, processing steps, and variable definitions (DOCX format).

| File | Description |
|------|-------------|
| `data_processing_overview.docx` | Comprehensive pipeline documentation |
| `quartile_alignment_plan (1).docx` | Quartile calculation alignment plan |
| `susp_v6_data_explanation.docx` | Final dataset (v6) documentation |

**Note**: These are legacy DOCX files. Markdown versions are available in `Analysis/data_processing_overview.md` and related files.

---

## 🗄️ Archive

Deprecated scripts, old documentation, and legacy files kept for historical reference.

| File | Description |
|------|-------------|
| `02_black_rates_by_quartiles.R` | Legacy quartile analysis script |
| `02b_black_rates_by_quartiles.R` | Legacy quartile analysis variant |
| `05z_feature_school_levels.R` | Old school level classification script |
| `indexupdates.rhtml` | Old HTML generation script |
| `legacy_html_script.rhtml` | Legacy HTML dashboard script |
| `v34_suspension_dashboard.html` | Old dashboard version |

**Note**: Files in this directory are not actively maintained and may be outdated. Refer to current scripts in `R/` and `Analysis/` directories instead.

---

## Quick Reference

### Most Frequently Used Documents

1. **For new contributors**: Start with `../README.md` and `../CLAUDE.md`
2. **For data quality checks**: See `audits/COMPREHENSIVE_AUDIT_REPORT.md`
3. **For teacher data**: See `guides/TEACHER_DATA_SETUP_GUIDE.md` and `protocols/PROTOCOL_TEACHER_DATA_MERGE.md`
4. **For citation requirements**: See `protocols/CITATION_STANDARD.md`
5. **For visualization colors**: See `protocols/UCLA-Brand-Colors.md`
6. **For troubleshooting**: Check `fixes/` directory for relevant issue

### Related Documentation

- **Main README**: `../README.md` - Quick start and setup
- **CLAUDE.md**: `../CLAUDE.md` - Comprehensive AI assistant guide
- **Analysis Documentation**: `../Analysis/data_processing_overview.md` - Detailed pipeline documentation
- **Analysis Guides**: `../Analysis/` directory - Analysis-specific guides and documentation

---

## Contributing to Documentation

When adding new documentation:

1. **Place in appropriate subdirectory**:
   - Audits → `audits/`
   - Setup/usage guides → `guides/`
   - Standards/protocols → `protocols/`
   - Bug fixes/diagnostics → `fixes/`
   - Data documentation → `data-explanations/`

2. **Use descriptive filenames**:
   - Good: `AUDIT_TEACHER_DIVERSITY_REGRESSION.md`
   - Bad: `report1.md`

3. **Update this README**: Add new files to the appropriate table above

4. **Cross-reference**: Link to related documentation using relative paths
   - Example: `See docs/protocols/CITATION_STANDARD.md for citation requirements`

5. **Date stamp**: Include "Last Updated" date in document headers

---

**Questions?** See `../CLAUDE.md` for comprehensive project documentation.
