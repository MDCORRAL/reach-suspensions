# Analysis/RUN_TEACHER_DIVERSITY_ANALYSIS.R
# Master script to run complete teacher racial diversity analysis
#
# This script executes the full analysis pipeline:
#   1. Build teacher race share features
#   2. Run regression analysis
#   3. Generate visualizations
#
# PREREQUISITES:
#   - Teacher demographics ingested (teacher_staff_long.parquet exists)
#   - Student suspension data processed (susp_v6_long.parquet exists)
#   - R packages: dplyr, tidyr, arrow, ggplot2, scales
#
# USAGE:
#   source("Analysis/RUN_TEACHER_DIVERSITY_ANALYSIS.R")

message("\n")
message("╔════════════════════════════════════════════════════════════════╗")
message("║                                                                ║")
message("║           TEACHER RACIAL DIVERSITY ANALYSIS                   ║")
message("║           Complete Analysis Pipeline                          ║")
message("║                                                                ║")
message("╚════════════════════════════════════════════════════════════════╝\n")

start_time <- Sys.time()

# =============================================================================
# STEP 0: Verify Prerequisites
# =============================================================================

message("Step 0: Checking prerequisites...")

required_files <- c(
  "data-stage/teacher_staff_long.parquet",
  "data-stage/susp_v6_long.parquet"
)

missing_files <- required_files[!file.exists(required_files)]

if (length(missing_files)) {
  stop("\n",
       "════════════════════════════════════════════════════════════════\n",
       "❌ MISSING REQUIRED FILES:\n",
       "════════════════════════════════════════════════════════════════\n\n",
       paste("  •", missing_files, collapse = "\n"), "\n\n",
       "Please run:\n",
       "  1. R/01c_ingest_teacher_demographics.R  (creates teacher_staff_long.parquet)\n",
       "  2. run_pipeline.R                       (creates susp_v6_long.parquet)\n\n",
       "════════════════════════════════════════════════════════════════\n")
}

message("  ✓ All required files present\n")

# =============================================================================
# STEP 1: Build Teacher Race Share Features
# =============================================================================

message("\n╔════════════════════════════════════════════════════════════════╗")
message("║  STEP 1: Building Teacher Race Share Features                 ║")
message("╚════════════════════════════════════════════════════════════════╝\n")

tryCatch({
  source("Analysis/22_build_teacher_race_shares.R")
  message("\n✓ Step 1 complete: susp_v6_teacher_features.parquet created\n")
}, error = function(e) {
  stop("\n❌ Step 1 FAILED:\n", conditionMessage(e), "\n")
})

# =============================================================================
# STEP 2: Run Regression Analysis
# =============================================================================

message("\n╔════════════════════════════════════════════════════════════════╗")
message("║  STEP 2: Running Regression Analysis                          ║")
message("╚════════════════════════════════════════════════════════════════╝\n")

tryCatch({
  source("Analysis/21_teacher_diversity_regression_FIXED.R")
  message("\n✓ Step 2 complete: Regressions executed\n")
}, error = function(e) {
  stop("\n❌ Step 2 FAILED:\n", conditionMessage(e), "\n")
})

# =============================================================================
# STEP 3: Generate Visualizations
# =============================================================================

message("\n╔════════════════════════════════════════════════════════════════╗")
message("║  STEP 3: Generating Visualizations                            ║")
message("╚════════════════════════════════════════════════════════════════╝\n")

tryCatch({
  source("Analysis/23_visualize_teacher_diversity.R")
  message("\n✓ Step 3 complete: Visualizations created\n")
}, error = function(e) {
  stop("\n❌ Step 3 FAILED:\n", conditionMessage(e), "\n")
})

# =============================================================================
# COMPLETION SUMMARY
# =============================================================================

end_time <- Sys.time()
elapsed <- as.numeric(difftime(end_time, start_time, units = "secs"))

message("\n")
message("╔════════════════════════════════════════════════════════════════╗")
message("║                                                                ║")
message("║                    ANALYSIS COMPLETE ✓                        ║")
message("║                                                                ║")
message("╚════════════════════════════════════════════════════════════════╝\n")

message(sprintf("Total time: %.1f seconds (%.1f minutes)\n", elapsed, elapsed / 60))

message("📁 OUTPUT LOCATIONS:")
message("  • Data: data-stage/susp_v6_teacher_features.parquet")
message("  • Visualizations: outputs/graphs/teacher_diversity/")
message("  • Regression output: displayed above")

message("\n📊 NEXT STEPS:")
message("  1. Review regression results above")
message("  2. Examine visualizations in outputs/graphs/teacher_diversity/")
message("  3. Check summary statistics in summary_statistics.csv")
message("  4. Interpret findings (remember: associations, not causal effects!)")

message("\n══════════════════════════════════════════════════════════════════\n")

invisible(TRUE)
