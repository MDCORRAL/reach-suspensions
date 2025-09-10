# Tail Concentration by School Level and Setting

This analysis quantifies how suspensions are concentrated among schools and
breaks out results by **school level** (elementary, middle, high, other) and
**setting** (traditional vs. non‑traditional).  It produces Pareto share
summaries so that, for example, you can identify what share of suspensions are
accounted for by the top 5%, 10% or 20% of schools within each level/setting
combination.

## Data sources

The script automatically scans the `data-stage/` directory and uses the first
`susp_v*.parquet` file that contains the following fields:

- `school_code`
- `academic_year`
- `cumulative_enrollment`
- `total_suspensions`
- `unduplicated_count_of_students_suspended_total`

It also requires `susp_v6_features.parquet`, which supplies `is_traditional`
and `school_type` so that schools can be tagged by setting and level.

## Data cleaning

1. Filter to "Total/All Students" records only.
2. Convert counts and enrollment to numeric and drop rows with missing or
   zero enrollment.
3. Derive `year_num` from the academic year string.
4. Join on `susp_v6_features.parquet` and map:
   - `setting` = Traditional or Non‑traditional based on `is_traditional`
   - `level`   = school type from `school_type` (elementary, middle, etc.)

## Methodology

For each year × level × setting group the script:

1. Aggregates suspension counts per school.
2. Sorts schools by the selected measure (`total_susp` by default).
3. Computes the share of suspensions attributable to the top 5%, 10%, and 20%
   of schools (Pareto shares).
4. Emits both a raw dataset and a "slide‑ready" version with formatted
   percentages.

## Outputs

Results are written to `outputs/tail_concentration_by_level_<timestamp>/`:

- `pareto_shares_by_year_level_setting_raw.csv`
- `pareto_shares_by_year_level_setting_slide_ready.csv`

These files let you isolate concentration patterns for elementary, middle, and
high schools in traditional and non‑traditional settings.

