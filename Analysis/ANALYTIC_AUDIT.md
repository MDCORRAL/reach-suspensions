# Analytic Script Audit (Analysis/ directory)

## Scope and approach
- Reviewed core Analysis scripts and pipeline runners to confirm data lineage, guardrails, and statistical conventions.
- Cross-referenced the documented ingestion/feature pipeline to verify that analytics pull from the expected staged parquet assets and apply campus-level filters/keys consistently.
- Flagged improvement actions to tighten reproducibility, versioning, and validation coverage across downstream analyses.

## Data lineage and staging alignment
- The pipeline orchestrator (`run_all.R`) sequences ingestion, feature engineering, and analysis scripts so analytic outputs are always built on the latest staged data products (e.g., `susp_v6_long.parquet`).【F:run_all.R†L3-L32】
- Upstream processing is documented with explicit raw sources, path controls, and staged dataset inventory, giving a clear contract for analytic inputs (e.g., `susp_v6_long.parquet`, `susp_v6_features.parquet`).【F:Analysis/data_processing_overview.md†L17-L193】
- Analytic scripts that rely on race-specific suspension detail load the canonical `data-stage/susp_v6_long.parquet` and immediately harmonize campus keys/filters before aggregation, reducing the risk of duplicate or aggregate rows in rate calculations.【F:Analysis/02_black_rates_by_quartiles.R†L31-L55】

## Script-level checks
- **02_black_rates_by_quartiles.R**: Enforces required columns (`subgroup`, enrollment, quartile fields), applies campus-only filter, and branches between count and proportion inputs for reason-level rates, ensuring robustness to schema differences in `susp_v6_long` versions.【F:Analysis/02_black_rates_by_quartiles.R†L31-L170】
- **16_tail_concentration_analysis.R**: Dynamically selects the newest `susp_v*_long.parquet` that satisfies required columns and aligns with matching `susp_v*_features.parquet`, adding explicit fallbacks and logging when versions are skipped for missing fields; this guards against silent schema drift in tail metrics.【F:Analysis/16_tail_concentration_analysis.R†L32-L127】
- **18_merge_teacher_student.R**: Verifies presence of teacher and suspension parquet assets, cleans keys, asserts campus-year uniqueness on the teacher summary, and joins with race-specific suspension rows using many-to-one semantics to avoid duplication of teacher metrics across subgroups.【F:Analysis/18_merge_teacher_student.R†L18-L87】

## Risk/gap assessment and recommendations
- **Version pinning**: The dynamic file selection in `16_tail_concentration_analysis.R` improves resiliency but can silently mix feature/suspension vintages if multiple versions coexist; add an explicit version check (e.g., warn/error when `susp_v6_long` pairs with `susp_v5_features`).【F:Analysis/16_tail_concentration_analysis.R†L75-L127】
- **Reason-data completeness**: `02_black_rates_by_quartiles.R` currently infers reason counts from proportions when count columns are absent; add a validation summary that compares derived totals vs. `total_suspensions` to detect rounding or suppression-induced undercounts before plotting.【F:Analysis/02_black_rates_by_quartiles.R†L56-L170】
- **Teacher coverage transparency**: `18_merge_teacher_student.R` reports coverage counts but does not persist them; consider writing a small diagnostics table (school-year coverage percentages) alongside the merged parquet to keep audit trails of teacher data completeness over time.【F:Analysis/18_merge_teacher_student.R†L63-L87】
- **End-to-end reproducibility**: Document recommended environment toggles (e.g., `RAW_PATH`, `OTH_RAW_PATH`, `REACH_DATA_DIR`) alongside `run_all.R` usage so analysts running individual scripts outside the orchestrator use consistent staged assets and avoid stale intermediates.【F:run_all.R†L3-L32】【F:Analysis/data_processing_overview.md†L43-L51】

## Next-step checklist
1. Implement version-consistency assertions between suspension and feature parquet files in tail analyses.
2. Add reason-rate reconciliation diagnostics to rate-by-quartile outputs and surface in generated figures or logs.
3. Emit teacher-data coverage tables during merges and include them in outputs/ for transparency.
4. Extend the Analysis README/quickstart to state environment-variable expectations when running scripts ad hoc.
