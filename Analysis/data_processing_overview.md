# Data processing and analysis overview

## Pipeline execution status
- Attempted to rerun the scripted pipeline via `run_pipeline.R`, but the `renv` bootstrap could not download packages from Posit Package Manager (HTTP 403). As a result, the existing staged Parquet files in `data-stage/` were reused for documentation and validation.【391f54†L1-L8】
- Current staged datasets contain 567,047 school-race rows in `susp_v5.parquet`, 3,402,282 rows in `susp_v5_long.parquet`, 558,431 demographic rows in `oth_long.parquet`, 60,188 school-year feature rows in `susp_v6_features.parquet`, and 3,402,282 long-form rows in `susp_v6_long.parquet`.【1a8d07†L1-L1】【a9d7bc†L1-L1】【2ec817†L1-L1】【2ec817†L1-L1】【2b981e†L1-L1】

## Path configuration and raw data expectations
- `R/00_paths.R` centralizes directory handling. `REACH_PROJECT_ROOT` can override the project root; otherwise `getwd()` is used. `REACH_DATA_DIR` can redirect staged outputs, falling back to `<project>/data-stage/`. The script also defines raw (`data-raw/`) and output (`outputs/`) directories and ensures they exist.【6d57b1†L13-L36】
- The suspension raw Excel file is resolved by prioritizing a `RAW_PATH` environment variable, then `data-raw/copy_CDE_suspensions_1718-2324_sc_race.xlsx`. The script halts if no candidate is found, printing the locations it tried.【6d57b1†L38-L56】
- Similar logic applies to demographics: `R/01b_ingest_demographics.R` reads `OTH_RAW_PATH` when present, otherwise defaults to `data-raw/copy_CDE_suspensions_1718-2324_sc_oth.xlsx` and stops if the file is missing.【66e29c†L17-L36】

## Stage-by-stage pipeline summary
### 01 – Base ingestion (`R/01_ingest_v0.R`)
- Reads the raw suspension workbook, normalizes column names, derives `year`/`academic_year`, and identifies numeric columns via `numeric_cols()` from `R/ingest_helpers.R`.【3902e8†L24-L33】
- Creates suppression flags for all numeric-like fields before parsing numeric strings, trims charter flags, and records whether any suppression indicator is present. It prints row/column counts plus `academic_year` and `charter_yn` distributions for auditing.【3902e8†L52-L80】
- Outputs `data-stage/susp_v0.parquet` and `column_dictionary_v0.csv` for downstream scripts.【3902e8†L83-L90】

### 02 – Locale tagging (`R/02_feature_locale_simple.R`)
- Loads `susp_v0.parquet`, then standardizes locales into `City`, `Suburban`, `Rural`, `Town`, or `Unknown` by string matching; the accepted level order comes from `locale_levels` in `R/utils_keys_filters.R`.【685a13†L15-L27】【b997cd†L41-L55】
- Writes `susp_v1.parquet` and prints counts by `locale_simple` to confirm coverage.【685a13†L29-L36】

### 02b – Charter “All” filter (`R/02b_drop_charter_all.R`)
- Removes `charter_yn == "All"` rows from `susp_v1.parquet`, producing `susp_v1_noall.parquet` to prevent double-counting state-wide charter totals.【d8a26a†L16-L33】

### 03 – Total-enrollment quartiles (`R/03_feature_size_quartiles_TA.R`)
- Builds canonical keys via `build_keys()` and restricts to campus-level records using `filter_campus_only()` from `R/utils_keys_filters.R`. For each campus-year it keeps the first non-missing total enrollment for the “All Students” subgroup, then assigns year-specific quartiles (`Q1`–`Q4`, `Unknown`) through `ntile()` when enrollment is positive.【3fee71†L21-L55】【b997cd†L130-L150】
- Summary counts show a near-uniform quartile split with small numbers of Unknown records (Table 1).【7987b5†L1-L28】
- Writes `susp_v2.parquet` with the `enroll_q` indicators for all race rows.【3fee71†L58-L70】

**Table 1. Enrollment quartile counts by year**【7987b5†L1-L28】
| Academic year | Enrollment quartile | Campuses |
|---|---|---|
| 2017-18 | Q1 (Smallest) | 2487 |
| 2017-18 | Q2 | 2487 |
| 2017-18 | Q3 | 2487 |
| 2017-18 | Q4 (Largest) | 2486 |
| 2017-18 | Unknown | 106 |
| 2018-19 | Q1 (Smallest) | 2491 |
| 2018-19 | Q2 | 2490 |
| 2018-19 | Q3 | 2490 |
| 2018-19 | Q4 (Largest) | 2490 |
| 2018-19 | Unknown | 70 |
| 2019-20 | Q1 (Smallest) | 2516 |
| 2019-20 | Q2 | 2516 |
| 2019-20 | Q3 | 2516 |
| 2019-20 | Q4 (Largest) | 2516 |
| 2021-22 | Q1 (Smallest) | 2504 |
| 2021-22 | Q2 | 2503 |
| 2021-22 | Q3 | 2503 |
| 2021-22 | Q4 (Largest) | 2503 |
| 2022-23 | Q1 (Smallest) | 2506 |
| 2022-23 | Q2 | 2506 |
| 2022-23 | Q3 | 2506 |
| 2022-23 | Q4 (Largest) | 2506 |
| 2023-24 | Q1 (Smallest) | 2500 |
| 2023-24 | Q2 | 2501 |
| 2023-24 | Q3 | 2501 |
| 2023-24 | Q4 (Largest) | 2500 |
| 2023-24 | Unknown | 1 |

### 04 – Race-share quartiles (`R/04_feature_black_prop_quartiles.R`)
- Derives campus-year enrollment for Black, White, Hispanic/Latino, and All Students, using `prop_black = enroll_Black / enroll_All`, `prop_white = enroll_White / enroll_All`, and `prop_hispanic = enroll_Hispanic / enroll_All` when denominators are positive.【e43be4†L42-L66】
- Assigns quartiles per year (`black_prop_q`, `white_prop_q`, `hispanic_prop_q`) with labeled factors, guarding against duplicate campus-year keys and out-of-bounds proportions.【e43be4†L68-L149】
- Writes `susp_v3.parquet` that appends the share metrics to every race row.【e43be4†L82-L153】
- In the staged data, Black quartile coverage remains heavily “Unknown” because the current `susp_v5.parquet` lacks the Hispanic quartile columns; see the distribution in Table 2 (derived after filtering to unique campus-years).【eebe29†L4-L36】

**Table 2. Black enrollment quartile coverage (campus-year deduplicated)**【eebe29†L4-L36】
| Academic year | Quartile label | Campuses |
|---|---|---|
| 2017-18 | Q1 (Lowest % Black) | 1373 |
| 2017-18 | Q2 | 1373 |
| 2017-18 | Q3 | 1373 |
| 2017-18 | Q4 (Highest % Black) | 1372 |
| 2017-18 | Unknown | 4562 |
| 2018-19 | Q1 (Lowest % Black) | 1353 |
| 2018-19 | Q2 | 1353 |
| 2018-19 | Q3 | 1352 |
| 2018-19 | Q4 (Highest % Black) | 1352 |
| 2018-19 | Unknown | 4621 |
| 2019-20 | Q1 (Lowest % Black) | 1342 |
| 2019-20 | Q2 | 1342 |
| 2019-20 | Q3 | 1342 |
| 2019-20 | Q4 (Highest % Black) | 1341 |
| 2019-20 | Unknown | 4697 |
| 2021-22 | Q1 (Lowest % Black) | 1302 |
| 2021-22 | Q2 | 1302 |
| 2021-22 | Q3 | 1302 |
| 2021-22 | Q4 (Highest % Black) | 1301 |
| 2021-22 | Unknown | 4806 |
| 2022-23 | Q1 (Lowest % Black) | 1300 |
| 2022-23 | Q2 | 1299 |
| 2022-23 | Q3 | 1299 |
| 2022-23 | Q4 (Highest % Black) | 1299 |
| 2022-23 | Unknown | 4827 |
| 2023-24 | Q1 (Lowest % Black) | 1283 |
| 2023-24 | Q2 | 1282 |
| 2023-24 | Q3 | 1282 |
| 2023-24 | Q4 (Highest % Black) | 1282 |
| 2023-24 | Unknown | 4874 |

### 05 – School-level classification (`R/05_feature_school_level.R`)
- Extracts minimum/maximum grade tokens (treating PK/TK/K as 0), maps them through `span_label()` to Elementary/Middle/High/Other, and overrides to “Alternative” when `is_alt()` matches phrases such as “juvenile court”, “community day”, “continuation”, or “alternative”.【5f215d†L26-L63】【b997cd†L26-L39】
- Outputs `susp_v4.parquet` containing `school_level`, `grade_min_num`, and `grade_max_num`, with optional diagnostics for unique campus/year counts by level.【5f215d†L46-L89】

### 06 – Suspension-reason shares (`R/06_feature_reason_shares.R`)
- Calculates `prop_raw_pos(numer, denom)` for each reason, emitting a proportion only when both totals exist and are strictly positive; otherwise the share is `NA`. Reasons include violent (injury/no injury), weapons, illicit drugs, willful defiance, and other causes.【0511d3†L21-L45】
- Generates both wide (`susp_v5.parquet`) and long (`susp_v5_long.parquet`) formats, validating that proportions lie in (0,1] and summarizing how many schools have non-missing shares per year (Table 3).【0511d3†L50-L94】【b1ce56†L1-L7】

**Table 3. Non-missing reason-share counts (campus rows)**【b1ce56†L1-L7】
| academic_year | n_with_tot_raw_pos | n_vi | n_vn | n_wp | n_id | n_def | n_oth |
|:----------------|---------------------:|-------:|-------:|-------:|-------:|--------:|--------:|
| 2017-18 | 31720 | 15285 | 27106 | 9892 | 12248 | 13629 | 10643 |
| 2018-19 | 31401 | 15081 | 26826 | 9223 | 13063 | 12331 | 9465 |
| 2019-20 | 27678 | 13008 | 22893 | 7036 | 11296 | 8139 | 7352 |
| 2021-22 | 28734 | 14563 | 24279 | 9757 | 11987 | 6585 | 8822 |
| 2022-23 | 30443 | 15853 | 26086 | 9748 | 13883 | 6179 | 8872 |
| 2023-24 | 30384 | 16163 | 26385 | 8257 | 12093 | 4993 | 8028 |

## Demographic ingestion and labeling helpers
- `R/01b_ingest_demographics.R` standardizes “Other” (OTH) demographic data: normalizes subgroup labels, canonicalizes them with `canonicalize_demo()` (which draws on `desc_to_canon()` and `code_alias_to_canon()`), sums enrollment/suspension totals per campus-year-subgroup, applies `safe_rate()` with a minimum enrollment threshold of 10, and writes `oth_long.parquet`. Console diagnostics list available category types and sample subgroup names.【66e29c†L28-L107】【aab81b†L1-L133】
- `R/demographic_labels.R` supplies the codebook for demographic subgroup codes, canonical labels, and category assignments used during ingestion and later merges.【aab81b†L1-L133】
- `R/utils_keys_filters.R` provides cross-cutting helpers:
  - `filter_campus_only()` keeps `aggregate_level` values S/“school” and removes special codes `0000000` and `0000001` before campus-level analyses.【b997cd†L130-L150】
  - `span_label()` and `is_alt()` underpin school-level tagging as described above.【b997cd†L26-L39】
  - `canon_race_label()` harmonizes race labels, used in both quartile construction and analysis scripts.【b997cd†L208-L220】
  - Key builders (`build_keys()`, `ensure_keys()`) guarantee padded CDS codes for consistent joins.【b997cd†L108-L141】

## Building version 6 features (`R/22_build_v6_features.R`)
- Reads race (`susp_v5.parquet`), demographic (`oth_long.parquet`), and long race (`susp_v5_long.parquet`) data, padding school codes and reapplying `build_keys()`/`filter_campus_only()` when available.【886424†L49-L83】
- Extracts All Students rows to form `v5_core`, cleans quartile labels, and selects race share fields (`black_share`, `black_prop_q`, etc.) for the roster. The staged `susp_v6_features.parquet` currently retains only Black share/quartile fields along with school names and types.【886424†L96-L136】【bd7bf5†L1-L1】
- For each demographic domain (Students with Disabilities, English Learners, Migrant, Foster, Homeless, Socioeconomically Disadvantaged, Sex), the script filters `oth_long`, keeps topline subgroup records via regex patterns, caps impossible numerators/denominators with `drop_impossible()`, and summarizes to campus-year numerators, denominators, and rates using `safe_div()`/`safe_max()`.【886424†L30-L217】
- Joins all demographics onto the roster and flags traditional schools. Schools are tagged as traditional when their `school_type` contains terms such as “traditional” or grade-level descriptors and does **not** match any of the alternative patterns (community day, juvenile, continuation, alternative, opportunity, independent study, etc.).【886424†L318-L331】
- Calls `assert_unique_campus()` to ensure one row per campus-year, enforces rate bounds via `rng_ok()`, backfills missing name columns, and writes both `susp_v6_features.parquet` and `susp_v6_long.parquet`.【886424†L333-L377】
- When the analysis section runs, it restricts to traditional schools with known Black quartiles and positive SPED enrollment, reporting the sample size (18,106 rows out of 31,429 traditional campus-years), enrollment quartiles, and both pooled (weighted) and mean (unweighted) SPED suspension rates (Tables 4 and 5).【7138bf†L1-L19】【c0a17a†L1-L29】【1f5f5a†L1-L12】

**Table 4. Weighted SPED suspension rates by Black enrollment quartile**【1f5f5a†L1-L7】
| black_prop_q_label | n_schools | events | denom | weighted_rate | unweighted_rate | median_rate |
|:-------------------|----------:|-------:|------:|--------------:|----------------:|------------:|
| Q1 | 4531 | 42,985 | 804,403 | 0.05344 | 0.04560 | 0.02941 |
| Q2 | 4670 | 34,347 | 710,785 | 0.04832 | 0.03926 | 0.02133 |
| Q3 | 4368 | 41,247 | 695,968 | 0.05927 | 0.04784 | 0.02778 |
| Q4 | 4537 | 48,001 | 639,452 | 0.07507 | 0.06019 | 0.03448 |

**Table 5. Unweighted SPED suspension rates with 95% t-based confidence intervals**【1f5f5a†L8-L12】
| black_prop_q_label | n_schools | mean_rate | sd_rate | se_rate | ci_low | ci_high | median_rate |
|:-------------------|----------:|----------:|--------:|--------:|-------:|--------:|------------:|
| Q1 | 4531 | 0.04560 | 0.04907 | 0.00073 | 0.04417 | 0.04703 | 0.02941 |
| Q2 | 4670 | 0.03926 | 0.04871 | 0.00071 | 0.03787 | 0.04066 | 0.02133 |
| Q3 | 4368 | 0.04784 | 0.05651 | 0.00086 | 0.04616 | 0.04951 | 0.02778 |
| Q4 | 4537 | 0.06019 | 0.07200 | 0.00107 | 0.05810 | 0.06229 | 0.03448 |

Enrollment quartiles show lower SPED enrollment medians as Black share rises (min=11 students across quartiles, median drops from 142 in Q1 to 103 in Q4).【fad982†L1-L9】

## Downstream analyses
### Analysis 02 – Black suspension rates by racial composition
- Loads `susp_v6_long.parquet`, rebuilds keys, and filters to campus-level rows. It verifies needed columns (including quartiles) exist and generates quartile labels with `get_quartile_label()` when missing.【ca2bee†L31-L47】
- Totals are aggregated by academic year × quartile to compute pooled suspension rates (`sum(total_suspensions)/sum(cumulative_enrollment)`). Reason-specific plots either use provided `_count` columns or derive counts by multiplying `prop_susp_*` shares by total suspensions before aggregating and labeling via `add_reason_label()`.【ca2bee†L68-L188】

### Analysis 15 – Demographic merges for intersectional EDA
- Cleans both race and OTH parquet files, enforces canonical keys with `build_keys()`, validates that required demographic categories (Sex, Special Education, Socioeconomic, English Learner, Foster, Migrant, Homeless, Total) are present, and canonicalizes subgroup text using `canonicalize_demo()`.【2558ac†L31-L105】
- Caps impossible suspension counts greater than enrollment, then joins campus attributes from race data using either `cds_school` or district-level fallbacks (e.g., modal `school_level`/`locale_simple` within a district-year) to backfill missing classifications.【2558ac†L113-L220】
- Outputs Excel summaries (`15_demographic_*.xlsx`) and CSV flags describing intersectional combinations, ensuring consistent naming for Traditional vs. Non-traditional settings based on `ed_ops_name`.【2558ac†L157-L219】

## Key staged outputs and usage
- `susp_v5.parquet` (race-level metrics + quartiles + reason shares) feeds Stage 22 and long-form analysis. `susp_v5_long.parquet` provides reason shares in long format for plots and merges.【886424†L54-L66】【0511d3†L50-L63】
- `oth_long.parquet` supplies demographic subgroup rates that Stage 22 aggregates into `sped_rate`, `ell_rate`, `migrant_rate`, `foster_rate`, `homeless_rate`, `sed_rate`, and `sex_*_rate`.【66e29c†L76-L94】【886424†L129-L217】
- `susp_v6_features.parquet` is the campus-year analytic dataset (wide form), while `susp_v6_long.parquet` preserves race-long records with appended school attributes for cross-tab analyses.【886424†L336-L377】

