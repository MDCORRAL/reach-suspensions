# Teacher Diversity Regression Diagnostics

This note documents the R workflow that explores school-level suspension
rates as a function of teacher and administrator diversity. The
analysis now relies on `21_teacher_diversity_regression.R`, which uses
base R plus a lightweight Python bridge (via the system `python` or
`python3` executable) to load parquet files, run data-quality diagnostics, and fit
weighted linear models when staff diversity inputs exist. When Python lacks
`pyarrow`, the script now falls back to an existing CSV export (for example
`data-stage/susp_v6_teacher_features.csv`) so analysts can still run the
workflow after manually converting the parquet file.

## Data availability

The repository snapshot still lacks
`data-stage/susp_v6_teacher_features.parquet`, so the script falls back
to `data-stage/susp_v6_features.parquet`. That file contains student
suspension features but no teacher or administrator race columns,
preventing the requested regression from running. Other parquet files in
`data-stage/` remain in the GitHub version because they were committed
before the current ignore rules were adopted; Git continues to track
their existing history even though new parquet artifacts are now blocked
unless explicitly whitelisted.

Key diagnostics from the fallback dataset:

- 60,188 school-year rows across 38 columns
- No duplicate `cds_school` + `academic_year` combinations
- Missingness concentrated in auxiliary program counts (e.g., migrant,
  foster, homeless fields)

Because staff diversity data are absent, the script halts before model
fitting and reports the missing columns. Once the teacher merge parquet
is generated, rerunning the script will automatically compute non-White
shares, construct the modeling frame (including charter status,
grade-level dummies, economic disadvantage, and school size weights),
and fit a weighted least squares model.

## How to rerun

1. Execute `Rscript Analysis/21_teacher_diversity_regression.R` from the
   repository root. Set `RENV_CONFIG_AUTOLOADER_ENABLED=false` if the
   project autoloads renv in your environment.
2. To regenerate the teacher merge parquet, run
   `R/01c_ingest_teacher_demographics.R` followed by
   `Analysis/18_merge_teacher_student.R` once the raw CDE teacher TXT
   files are available under `data-raw/`.
   - If your machine only exposes `python3`, the script detects it
     automatically. Should neither `python` nor `python3` be available,
     or if Python lacks the `pyarrow` package, manually convert the
     parquet file to CSV (e.g., `data-stage/susp_v6_teacher_features.csv`)
     before rerunning the R analysis.
3. Review small-enrollment schools flagged by the script to decide
   whether to pool, trim, or interpret with caution.

Until the merged teacher file is present, regression estimates linking
staff diversity to suspension outcomes cannot be produced, but the
script now completes all diagnostics with base R and clearly reports the
missing inputs.
