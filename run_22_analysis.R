# run_22_analysis.R
# Runner script for Black suspension rates with teacher demographics analysis
#
# Usage: In R/RStudio, run:
#   source("run_22_analysis.R")

message("=== Running Analysis 22: Black Suspension Rates with Teacher Demographics ===")
message("Start time: ", format(Sys.time(), usetz = TRUE))

# Load helper if available
if (file.exists("R/run_helper.R")) {
  source("R/run_helper.R")
  run("Analysis/22_black_suspension_rates_teacher_demographics.R")
} else {
  # Direct execution
  source("Analysis/22_black_suspension_rates_teacher_demographics.R")
}

message("\n=== Analysis 22 Complete @ ", format(Sys.time(), usetz = TRUE), " ===")
