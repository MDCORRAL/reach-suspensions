# Suspension data explanation sheet (susp_v6_long + susp_v6_features)

This note documents how the statewide suspension data were staged, cleaned, and filtered before any analysis that relies on the paired files `data-stage/susp_v6_long.parquet` and `data-stage/susp_v6_features.parquet`. It follows the flow from the raw race and demographic workbooks through the campus-year roster used for joining demographic rates and highlights how many records remain after each major filtering step.

## 1. Source extracts and base counts

| File | Description | Rows | Distinct campus-years |
| --- | --- | ---: | ---: |
| `susp_v5_long.parquet` | Race/ethnicity suspension counts in long form (campus × subgroup × reason) | 3,402,282 | 60,188 |
| `susp_v5.parquet` | Race/ethnicity suspension counts in wide form (one record per campus-year-subgroup) | 567,047 | 60,188 |
| `oth_long.parquet` | Other demographic suspension counts (Special Education, EL, Foster, etc.) | 558,431 | 60,200 |
| `susp_v6_features.parquet` | Final campus-year roster with demographic rates and quartile labels | 60,188 | 60,188 |
| `susp_v6_long.parquet` | Long-form race records reused for analysis-ready joins | 3,402,282 | 60,188 |

The v5 race files and the OTH demographic file are read from `data-stage/`, school codes are padded, and repository helpers (`build_keys()`, `filter_campus_only()`) are applied to keep only school-level rows before any joins occur.【e3165c†L49-L91】 The long race file is trimmed to the columns required for downstream merges at this stage.【e3165c†L84-L91】

## 2. Building the campus-year roster (`susp_v6_features.parquet`)

1. **Roster initialization (60,188 campus-years).** The script extracts “All Students” rows from `susp_v5.parquet`, standardizes the Black/White/Hispanic share quartile labels, and retains one record per campus-year along with school names and types.【e3165c†L96-L150】 This produces a deduplicated roster keyed by `school_code` and `academic_year` with 60,188 rows.
2. **Demographic aggregation.** For each demographic category (Students with Disabilities, English Learners, Migrant, Foster, Homeless, Socioeconomically Disadvantaged, Sex), the script keeps only the topline subgroup, drops impossible numerators/denominators (negative counts, denominators ≤ 0, numerators above denominators), then sums numerators, carries forward the largest denominator, and calculates a rate per campus-year.【e3165c†L151-L199】【b29797†L201-L312】 Missing or suppressed source records leave `NA` rates in the roster—for example, the staged file shows 2,028 traditional campus-years without English Learner rates and 27,557 without Migrant rates before any quartile filtering (see §3).
3. **Joining and quality checks.** The demographic summaries are left-joined onto the roster, a traditional-school flag is derived from `school_type`, and `assert_unique_campus()` confirms a single row per campus-year. Any missing school names are backfilled from the original v5 race table before the roster is written to `susp_v6_features.parquet`.【b29797†L295-L360】【49da0c†L373-L377】 The resulting file contains 60,188 records, matching the count of unique campus-year pairs.

## 3. Understanding roster coverage and missingness

The table below shows how many campus-year records remain after applying the key filters that most analyses require. Counts are calculated from the staged `susp_v6_features.parquet` file.

| Step | Criteria | Campus-years remaining | Rows removed at this step | Primary reason for removals |
| --- | --- | ---: | ---: | --- |
| Baseline roster | All rows | 60,188 | — | All campus-level schools with “All Students” race data |
| Restrict to traditional schools | `is_traditional == TRUE` | 31,429 | 28,759 | Alternative settings (community day, juvenile, continuation, etc.) flagged during roster build【b29797†L317-L331】 |
| Require known Black quartile | `black_prop_q_label` in {Q1–Q4} | 18,242 | 13,187 | Missing Black share data keep quartile as “Unknown” and are dropped for quartile analyses |
| Require Students with Disabilities denominator | `sped_den > 0` | 18,106 | 136 | Schools with zero or missing SWD enrollment cannot produce a suspension rate |

Within the final 18,106-campus sample commonly used for quartile comparisons, auxiliary demographic rates still contain gaps because the source OTH file lacks coverage for certain subgroups at many schools:

- English Learner rates are missing for 286 campus-years.
- Migrant rates are missing for 16,458 campus-years.
- Foster rates are missing for 12,969 campus-years.
- Homeless rates are missing for 7,916 campus-years.
- Socioeconomically Disadvantaged rates are complete (no missing values after filtering).
- Sex-specific rates are missing for 11 campus-years (male) and 7 campus-years (female).

These missing rates reflect suppressed or absent subgroup rows in `oth_long.parquet`; the roster retains `NA` values rather than imputing replacements.

## 4. Long-form analysis file (`susp_v6_long.parquet`)

After the roster is assembled, the cleaned race long file (`race_long`) is written unchanged to `susp_v6_long.parquet` to provide subgroup-by-reason metrics for plotting and aggregation.【e3165c†L62-L91】【49da0c†L373-L377】 The staged file contains 3,402,282 rows—the same as `susp_v5_long.parquet`—and inherits the same 60,188 unique campus-year pairs. Analyses that need school attributes (e.g., `is_traditional`, quartiles, demographic rates) join `susp_v6_long.parquet` to `susp_v6_features.parquet` on `school_code`/`academic_year`.

## 5. How sample sizes shrink in practice

The SPED-by-Black-quartile workflow embedded in `R/22_build_v6_features.R` is representative of downstream analyses. It:

1. Restricts the roster to traditional schools (`31,429` rows) and reports the quartile distribution across Q1–Q4 plus Unknown.【49da0c†L388-L409】 The 13,187 Unknown rows are excluded from quartile reporting because the raw race workbook lacks the enrollment detail needed to compute Black student shares.
2. Drops records without positive SWD enrollment, leaving `18,106` schools (100% of these retain non-missing SWD rates).【49da0c†L395-L410】
3. Aggregates by quartile to compute weighted and unweighted suspension rates, using the numerator and denominator sums derived during the demographic aggregation step.【49da0c†L423-L440】

Any further reductions in n-size for other analyses stem from analogous filters (e.g., requiring available EL rates or focusing on a subset of years). Because the staged files retain rows with suppressed numerators/denominators as `NA`, analysts can transparently justify each exclusion when constructing study-specific cohorts.

