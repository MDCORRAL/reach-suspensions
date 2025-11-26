# Quick diagnostic: Check if "race" column exists and what data structure we have

library(arrow)
library(dplyr)

message("=== Checking data structure ===\n")

# Check the teacher-merged file
merged_file <- "data-stage/susp_v6_teacher_features.parquet"

if (file.exists(merged_file)) {
  message("Reading: ", merged_file)
  df <- read_parquet(merged_file)

  message("\n>>> Total rows: ", format(nrow(df), big.mark = ","))
  message(">>> Total columns: ", ncol(df))

  # Check for race column
  if ("race" %in% names(df)) {
    message("\n✓ 'race' column EXISTS")
    message("\nUnique race values:")
    print(unique(df$race))

    # Check if "All Students" exists
    if ("All Students" %in% unique(df$race)) {
      message("\n✓✓ 'All Students' total EXISTS!")
      message("   --> We should use filtering approach")

      # Count observations
      all_students_count <- sum(df$race == "All Students")
      message("\n   'All Students' rows: ", format(all_students_count, big.mark = ","))
      message("   Other race rows: ", format(nrow(df) - all_students_count, big.mark = ","))
    } else {
      message("\n✗ 'All Students' total NOT FOUND")
      message("   --> Current max() approach is best")
    }
  } else {
    message("\n✗ 'race' column DOES NOT EXIST")
    message("\nAvailable columns (first 30):")
    print(head(names(df), 30))

    # Check if this might be already aggregated
    message("\n>>> Checking if data is already aggregated...")

    # Look for patterns suggesting school-year level
    school_year_check <- df %>%
      group_by(cds_school, academic_year) %>%
      summarise(n = n(), .groups = "drop")

    avg_obs_per_school_year <- mean(school_year_check$n)
    message(">>> Average observations per school-year: ", round(avg_obs_per_school_year, 1))

    if (avg_obs_per_school_year > 10) {
      message("   --> Data appears to be at granular level (race × reason)")
      message("   --> 'race' column may have different name")
    } else {
      message("   --> Data appears to be already aggregated to school-year")
      message("   --> 'race' was dropped during aggregation")
    }
  }
} else {
  message("✗ File not found: ", merged_file)
}

# Also check v6_long (before teacher merge)
message("\n\n=== Checking v6_long (before teacher merge) ===\n")

v6_long_file <- "data-stage/susp_v6_long.parquet"

if (file.exists(v6_long_file)) {
  message("Reading: ", v6_long_file)
  df_v6 <- read_parquet(v6_long_file)

  message("\n>>> Total rows: ", format(nrow(df_v6), big.mark = ","))

  if ("race" %in% names(df_v6)) {
    message("\n✓ 'race' column EXISTS in v6_long")
    message("\nUnique race values:")
    print(unique(df_v6$race))

    if ("All Students" %in% unique(df_v6$race)) {
      message("\n✓✓ 'All Students' total EXISTS in v6_long!")
    }
  } else {
    message("\n✗ 'race' column not in v6_long either")
    message("\nAvailable columns (first 30):")
    print(head(names(df_v6), 30))
  }
} else {
  message("✗ File not found: ", v6_long_file)
}

message("\n\n=== RECOMMENDATION ===")
message("\nBased on the above, we can determine:")
message("1. Whether 'All Students' category exists")
message("2. Whether we should update scripts to use filtering approach")
message("3. Or whether current max() approach is optimal")
