# Consolidate regression scripts
# This replaces the original with the fixed version and removes the _FIXED file

message("Consolidating regression scripts...")

# Replace original with fixed version
file.copy(
  "Analysis/21_teacher_diversity_regression_FIXED.R",
  "Analysis/21_teacher_diversity_regression.R",
  overwrite = TRUE
)

message("✓ Replaced Analysis/21_teacher_diversity_regression.R with fixed version")

# Remove the _FIXED version
file.remove("Analysis/21_teacher_diversity_regression_FIXED.R")

message("✓ Removed Analysis/21_teacher_diversity_regression_FIXED.R")
message("\nConsolidation complete! You now have a single regression script.")
message("Run: source('Analysis/21_teacher_diversity_regression.R')")
