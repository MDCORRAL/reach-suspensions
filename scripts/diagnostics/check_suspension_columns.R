# Quick diagnostic: Check what suspension columns exist in the data

library(arrow)
library(dplyr)

message("=== Checking suspension columns ===\n")

# Read the data
df <- read_parquet("data-stage/susp_v6_teacher_features.parquet")

# Check what columns match the current pattern
pattern <- "^total_suspensions|^suspension_count"
susp_cols <- grep(pattern, names(df), value = TRUE)

message(">>> Columns matched by pattern: '", pattern, "'\n")
message(">>> Number of columns: ", length(susp_cols))
message("\nColumn names:")
print(susp_cols)

# Get sample values from first row
if (length(susp_cols) > 0) {
  message("\n>>> Sample values from first school-year:")
  first_row <- df[1, susp_cols]
  print(as.data.frame(first_row))

  # Check if we're double-counting
  if ("total_suspensions" %in% susp_cols && length(susp_cols) > 1) {
    message("\n⚠️  WARNING: Pattern matches BOTH total_suspensions AND individual columns!")
    message("    This will cause double-counting when summing.")
    message("\n    Individual suspension columns:")
    individual_cols <- setdiff(susp_cols, "total_suspensions")
    print(individual_cols)
  }
}

# Show correct pattern to use
message("\n=== RECOMMENDATION ===")
message("\nTo avoid double-counting, use ONLY total_suspensions:")
message("  susp_cols <- grep('^total_suspensions$', names(df), value = TRUE)")
message("\nOR use only individual suspension_count columns:")
message("  susp_cols <- grep('^suspension_count_', names(df), value = TRUE)")
