# Analysis 27: Power Diagnostics for Teacher Diversity Studies (Analyses 21-25)

**Purpose**: Extend the Analysis 26 power framework to the other flagship teacher-diversity analyses so each has transparent, reproducible power diagnostics.

**Scope Covered**:
- Analysis 21: Teacher & administrator diversity regressions
- Analysis 22: Black suspension rates by enrollment quartile with teacher demographics
- Analysis 23: Teacher demographics in the highest Black-enrollment quartile (Q4)
- Analysis 24: Quartile slope comparisons over time
- Analysis 25: Interaction-term regressions between teacher diversity and Black enrollment quartiles

**How to Run**
1. Ensure `data-stage/susp_v6_teacher_features.parquet` (or the `..._teacher_long` fallback) exists.
2. From the repo root, run:
   ```bash
   Rscript Analysis/27_power_analysis_multiscript.R
   ```
3. Outputs are written to `outputs/tables/27_power_analysis_by_group.csv` (group-level results) and `outputs/tables/27_power_analysis_overview.csv` (analysis-level medians).

**Method Highlights**
- Mirrors Analysis 26 structure: Kish effective N (enrollment weights), sensitivity analysis via `pwr::pwr.f2.test`, and Bonferroni adjustments sized to each analysis’ multiple comparisons.
- Normalizes race labels and Black-enrollment quartiles from whichever columns are available (`student_group`/`subgroup`; `black_prop_q`/`black_quartile`).
- Uses predictor/control counts aligned with the documented model specifications in Analyses 21–25 (see `analysis_plan` in the script).

**Integration Guidance**
- After running, pull the relevant rows into each analysis’ summary doc (e.g., filter `analysis_id == "21_teacher_diversity_regression"`).
- The overview file surfaces median minimum-detectable R² values to quickly flag any low-powered subsets before interpreting null results.

**File Location**
- Script: `Analysis/27_power_analysis_multiscript.R`
- Primary outputs: `outputs/tables/27_power_analysis_by_group.csv`, `outputs/tables/27_power_analysis_overview.csv`
