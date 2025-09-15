# run_all.R  — full pipeline runner (root-level)

message("=== REACH: full pipeline start @ ", format(Sys.time(), usetz = TRUE), " ===")

source("R/run_helper.R")

# 1) Core build (00–06, with USE_TA toggle inside run_pipeline.R)
run("run_pipeline.R")

# 2) Black student suspension rates by quartiles
run("Analysis/02_black_rates_by_quartiles.R")

# 3) EDA + merge steps that produce non-intersectional exports used by tail analysis
#    (these paths match what you showed; adjust if your files live elsewhere)
run("Analysis/15_merge_demographic_categories.R")
run("Analysis/15a_emit_nonintersectional_exports.R")   # new companion script we discussed

# 4) Tail concentration analysis (reads outputs/data-merged/school_year_allstudents.parquet)
#    name this file however you like; keep it under analysis/
run("Analysis/16_tail_concentration_analysis.R")
run("Analysis/17_tail_concentration_by_level.R")

message("\n=== All done @ ", format(Sys.time(), usetz = TRUE), " ===")
# End of file ----------------------------------------------------------------- 