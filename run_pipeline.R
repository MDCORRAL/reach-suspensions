# run_pipeline.R  — REACH suspensions pipeline runner
# Runs the numbered scripts in order, skipping the archived 05 "levels" file.
# --- pipeline re-run guard (put this at the top) ---
if (isTRUE(getOption("pipeline_running"))) {
  stop("Pipeline is already running in this session. Aborting duplicate call.")
}
options(pipeline_running = TRUE)
on.exit(options(pipeline_running = FALSE), add = TRUE)
# --- setup: clear environment and load packages ------------------------------

# --- toggle: pick which 03 to run -------------------------------------------
USE_TA <- TRUE   # TRUE = R/03_feature_size_quartiles_TA.R, FALSE = R/03_feature_size_quartiles.R

# --- helper: source with progress + basic error handling --------------------
run <- function(f) {
  message("\n=== Running: ", f, " ===")
  tryCatch(
    {
      source(f, chdir = FALSE, local = new.env(parent = globalenv()))
      message("✓ Finished: ", f)
    },
    error = function(e) {
      message("✗ Error in ", f, "\n", conditionMessage(e))
      stop(e)
    }
  )
}

# --- assemble the script list (no archived [levels] variant) ----------------
scripts <- c(
  "R/00_paths.R",
  "R/01_ingest_v0.R",
  "R/02_feature_locale_simple.R",
  if (USE_TA) "R/02b_drop_charter_all.R",
  if (USE_TA) "R/03_feature_size_quartiles_TA.R" else "R/03_feature_size_quartiles.R",
  "R/04_feature_black_prop_quartiles.R",
  "R/05_feature_school_level.R",   # merged version; NOT the archived 'school_levels'
  "R/06_feature_reason_shares.R",
  "R/22_build_v6_features.R"
)

message("Pipeline start: ", format(Sys.time(), usetz = TRUE))
for (f in scripts) run(f)
message("\nAll done: ", format(Sys.time(), usetz = TRUE))

# End of file -----------------------------------------------------------------
