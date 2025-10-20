# Quartile Suspension Dashboard Alignment Plan

## Objective
Establish a repeatable workflow to ensure that `quartile_suspension_dashboard.html` and the supporting R/Python scripts (`graph_scripts/02_statewide_quartiles.R`, `graph_scripts/07_quartile_enrollment_comparison.R`, `graph_scripts/21_black_quartile_suspension_trends.R`, and any related utilities) load identical data, apply consistent transformations, and produce harmonized outputs.

## Phase 1 – Repository Survey & Baseline Validation
1. **Inventory data sources**
   - Enumerate all datasets consumed by the dashboard HTML and the three R scripts.
   - Capture file paths, data dictionaries, and refresh cadence.
2. **Baseline reproduction**
   - Re-run each R script in isolation, confirming the current outputs and saving intermediate tables.
   - Render `quartile_suspension_dashboard.html` (or rebuild from source if HTML is generated) to capture existing visual outputs for comparison.
3. **Document discrepancies**
   - Compare summary metrics, totals, and trends across the outputs.
   - Log mismatches in a centralized checklist.

## Phase 2 – Data Pipeline Harmonization
1. **Source of truth selection**
   - Decide on a single canonical dataset (or reproducible pipeline) shared across scripts and the HTML dashboard.
   - Verify schema parity (column names, types, filters, aggregation levels).
2. **Refactor shared data loading**
   - Extract shared data preparation steps into a common script/module.
   - Update each R script to call the shared logic, minimizing divergence.
3. **Audit transformation steps**
   - Align filtering, grouping, and calculation logic across scripts.
   - Add assertions/tests to guard against divergent totals or missing categories.

## Phase 3 – Output Synchronization
1. **Standardize intermediate outputs**
   - Define a structured directory (e.g., `data-stage/quartile/`) for intermediate CSV/RDS outputs consumed by both scripts and the HTML.
   - Ensure filenames encode version or timestamp metadata for traceability.
2. **HTML dashboard refresh**
   - Identify how the HTML obtains its data (embedded CSV/JSON, API call, etc.).
   - Replace ad-hoc data blocks with exports generated in Phase 2.
3. **Visual and metric QA**
   - Cross-validate key metrics (enrollment totals, suspension counts, rates) between the HTML and R outputs.
   - Create a sign-off checklist that pairs each visualization/table with its upstream dataset.

## Phase 4 – Automation & Monitoring
1. **Script execution orchestration**
   - Integrate the refactored scripts into `run_pipeline.R` or an equivalent workflow manager.
   - Parameterize execution so that all artifacts are rebuilt together.
2. **Regression checks**
   - Add unit/integration tests (e.g., via `testthat`) to validate known totals.
   - Incorporate data freshness checks and schema validation.
3. **Documentation updates**
   - Update `README` or a dedicated `docs/` page summarizing the unified workflow.
   - Provide runbooks for regenerating outputs and troubleshooting data mismatches.

## Phase 5 – Stakeholder Review & Sign-off
1. **Stakeholder walkthrough**
   - Demo the harmonized outputs, highlighting resolved discrepancies.
   - Gather feedback on any remaining edge cases.
2. **Finalize governance**
   - Define ownership for data updates and dashboard refreshes.
   - Schedule periodic audits to ensure ongoing alignment.

## Deliverables Checklist
- [ ] Inventory of data sources with owners and refresh schedules.
- [ ] Shared data preparation script/module adopted by all R scripts.
- [ ] Consistent intermediate datasets powering scripts and HTML.
- [ ] QA checklist documenting cross-validation results.
- [ ] Updated documentation and runbooks.
- [ ] Regression testing suite integrated into the pipeline.
- [ ] Sign-off notes from stakeholders.
