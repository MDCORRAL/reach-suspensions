# Data Audit Analysis Script
# Quantifies data loss through the processing pipeline and identifies recovery opportunities.

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(tibble)
  library(here)
  library(jsonlite)
})

# Paths
DATA_STAGE <- here("data-stage")
AUDIT_OUTPUT <- here("outputs", "data_audit")
dir.create(AUDIT_OUTPUT, showWarnings = FALSE, recursive = TRUE)

# Helper function
safe_count <- function(df, description) {
  count <- if (!is.null(df)) nrow(df) else 0
  message(sprintf("%s: %s records", description, format(count, big.mark = ",")))
  count
}

cat("================================================================================\n")
cat("DATA PIPELINE AUDIT - QUANTIFYING RECORDS AT EACH STAGE\n")
cat("================================================================================\n\n")

results <- list()

# Stage 0: Initial ingestion
cat("### STAGE 0: Initial Ingestion (susp_v0.parquet)\n")
v0_path <- file.path(DATA_STAGE, "susp_v0.parquet")
if (file.exists(v0_path)) {
  v0 <- read_parquet(v0_path)
  results$v0_total <- safe_count(v0, "  Total records after ingestion")

  # Charter_yn distribution
  cat("\n  Charter_yn distribution:\n")
  charter_counts <- v0 %>% count(charter_yn, sort = TRUE)
  print(charter_counts)
  results$v0_charter_all <- sum(charter_counts$n[charter_counts$charter_yn == "All"], 0)

  # Aggregate level distribution
  if ("aggregate_level" %in% names(v0)) {
    cat("\n  Aggregate_level distribution:\n")
    agg_counts <- v0 %>% count(aggregate_level, sort = TRUE)
    print(agg_counts)
    results$v0_school_level <- v0 %>%
      filter(tolower(aggregate_level) %in% c("s", "school")) %>%
      nrow()
  }

  # Special school codes
  if ("school_code" %in% names(v0)) {
    special_codes <- v0 %>%
      filter(school_code %in% c("0000000", "0000001"))
    results$v0_special_codes <- nrow(special_codes)
    cat(sprintf("\n  Special school codes (0000000, 0000001): %s\n",
                format(results$v0_special_codes, big.mark = ",")))
  }

  rm(v0)
  gc(verbose = FALSE)
} else {
  cat("  FILE NOT FOUND\n")
}

# Stage 1: After locale features
cat("\n### STAGE 1: After Locale Features (susp_v1.parquet)\n")
v1_path <- file.path(DATA_STAGE, "susp_v1.parquet")
if (file.exists(v1_path)) {
  v1 <- read_parquet(v1_path)
  results$v1_total <- safe_count(v1, "  Total records")
  rm(v1)
  gc(verbose = FALSE)
}

# Stage 1-noall: After dropping charter "All"
cat("\n### STAGE 1-NOALL: After Dropping Charter 'All' (susp_v1_noall.parquet)\n")
v1_noall_path <- file.path(DATA_STAGE, "susp_v1_noall.parquet")
if (file.exists(v1_noall_path)) {
  v1_noall <- read_parquet(v1_noall_path)
  results$v1_noall_total <- safe_count(v1_noall, "  Total records")
  if (!is.null(results$v1_total)) {
    lost <- results$v1_total - results$v1_noall_total
    cat(sprintf("  Records LOST by dropping charter 'All': %s\n", format(lost, big.mark = ",")))
    results$lost_charter_all <- lost
  }
  rm(v1_noall)
  gc(verbose = FALSE)
}

# Stage 5: After reason shares
cat("\n### STAGE 5: After Reason Shares (susp_v5.parquet and susp_v5_long.parquet)\n")
v5_path <- file.path(DATA_STAGE, "susp_v5.parquet")
v5_long_path <- file.path(DATA_STAGE, "susp_v5_long.parquet")

if (file.exists(v5_path)) {
  v5 <- read_parquet(v5_path)
  results$v5_wide_total <- safe_count(v5, "  v5 (wide) total records")
  rm(v5)
  gc(verbose = FALSE)
}

if (file.exists(v5_long_path)) {
  v5_long <- read_parquet(v5_long_path)
  results$v5_long_total <- safe_count(v5_long, "  v5_long total records")

  if ("subgroup" %in% names(v5_long)) {
    cat("\n  Subgroup distribution in v5_long (top 15):\n")
    subgroup_counts <- v5_long %>% count(subgroup, sort = TRUE) %>% head(15)
    print(subgroup_counts)
  }
  rm(v5_long)
  gc(verbose = FALSE)
}

# Demographics
cat("\n### DEMOGRAPHICS: Other Demographic Data (oth_long.parquet)\n")
oth_path <- file.path(DATA_STAGE, "oth_long.parquet")
if (file.exists(oth_path)) {
  oth <- read_parquet(oth_path)
  results$oth_total <- safe_count(oth, "  Total demographic records")

  if ("category_type" %in% names(oth)) {
    cat("\n  Category type distribution:\n")
    cat_counts <- oth %>% count(category_type, sort = TRUE)
    print(cat_counts)
  }

  # Check for impossible values
  if (all(c("unduplicated_suspensions", "cumulative_enrollment") %in% names(oth))) {
    impossible <- oth %>%
      filter(
        !is.na(unduplicated_suspensions),
        !is.na(cumulative_enrollment),
        (unduplicated_suspensions < 0 |
         cumulative_enrollment <= 0 |
         unduplicated_suspensions > cumulative_enrollment)
      )
    results$oth_impossible <- nrow(impossible)
    cat(sprintf("\n  Records with impossible num/den: %s\n",
                format(results$oth_impossible, big.mark = ",")))
  }

  rm(oth)
  gc(verbose = FALSE)
}

# Stage 6: Final v6 features
cat("\n### STAGE 6: Final v6 Features (susp_v6_features.parquet)\n")
v6_feat_path <- file.path(DATA_STAGE, "susp_v6_features.parquet")
if (file.exists(v6_feat_path)) {
  v6_feat <- read_parquet(v6_feat_path)
  results$v6_features_total <- safe_count(v6_feat, "  Total campus-year records")

  if ("is_traditional" %in% names(v6_feat)) {
    cat("\n  Traditional status:\n")
    trad_counts <- v6_feat %>% count(is_traditional, sort = TRUE)
    print(trad_counts)
    results$v6_traditional <- sum(trad_counts$n[trad_counts$is_traditional == TRUE], 0)
    results$v6_nontraditional <- sum(trad_counts$n[trad_counts$is_traditional == FALSE], 0)
  }

  rm(v6_feat)
  gc(verbose = FALSE)
}

# Stage 6: Final v6 long
cat("\n### STAGE 6: Final v6 Long (susp_v6_long.parquet)\n")
v6_long_path <- file.path(DATA_STAGE, "susp_v6_long.parquet")
if (file.exists(v6_long_path)) {
  v6_long <- read_parquet(v6_long_path)
  results$v6_long_total <- safe_count(v6_long, "  Total records")

  # Subgroups
  if ("subgroup" %in% names(v6_long)) {
    cat("\n  Subgroup distribution (top 15):\n")
    subgroup_counts <- v6_long %>% count(subgroup, sort = TRUE) %>% head(15)
    print(subgroup_counts)
  }

  # School level
  if ("school_level" %in% names(v6_long)) {
    cat("\n  School level distribution:\n")
    level_counts <- v6_long %>% count(school_level, sort = TRUE)
    print(level_counts)
  }

  # Aggregate level
  if ("aggregate_level" %in% names(v6_long)) {
    cat("\n  Aggregate level distribution:\n")
    agg_counts <- v6_long %>% count(aggregate_level, sort = TRUE)
    print(agg_counts)

    campus_only <- v6_long %>%
      filter(tolower(aggregate_level) %in% c("s", "school"))
    results$v6_long_campus_only <- nrow(campus_only)
    cat(sprintf("  Campus-level only: %s\n", format(results$v6_long_campus_only, big.mark = ",")))
  }

  # Filtering impact analysis
  cat("\n================================================================================\n")
  cat("FILTERING IMPACT ANALYSIS\n")
  cat("================================================================================\n\n")

  total <- nrow(v6_long)
  results$filtering_total <- total
  cat(sprintf("Starting with v6_long: %s records\n", format(total, big.mark = ",")))

  # Campus-only filter
  cat("\n### Impact of Campus-Only Filter\n")
  if ("aggregate_level" %in% names(v6_long)) {
    campus_count <- v6_long %>%
      filter(tolower(aggregate_level) %in% c("s", "school")) %>%
      nrow()
    non_campus <- total - campus_count
    results$filtering_campus_only <- campus_count
    results$filtering_lost_non_campus <- non_campus
    cat(sprintf("  Campus-level records: %s\n", format(campus_count, big.mark = ",")))
    cat(sprintf("  Non-campus records (would be excluded): %s (%.1f%%)\n",
                format(non_campus, big.mark = ","), 100*non_campus/total))
  }

  # Special school codes
  cat("\n### Impact of Special School Codes Filter\n")
  if ("school_code" %in% names(v6_long)) {
    special_count <- v6_long %>%
      filter(school_code %in% c("0000000", "0000001")) %>%
      nrow()
    results$filtering_lost_special_codes <- special_count
    cat(sprintf("  Special school code records (would be excluded): %s (%.1f%%)\n",
                format(special_count, big.mark = ","), 100*special_count/total))
  }

  # All Students filter
  cat("\n### Impact of 'All Students'/'Total' Subgroup Filter\n")
  if ("subgroup" %in% names(v6_long)) {
    all_students_count <- v6_long %>%
      filter(tolower(subgroup) %in% c("total", "all students", "ta")) %>%
      nrow()
    other_subgroups <- total - all_students_count
    results$filtering_all_students_only <- all_students_count
    results$filtering_other_subgroups <- other_subgroups
    cat(sprintf("  'All Students'/'Total' records: %s\n", format(all_students_count, big.mark = ",")))
    cat(sprintf("  Other subgroup records (excluded by dashboard/graphs): %s (%.1f%%)\n",
                format(other_subgroups, big.mark = ","), 100*other_subgroups/total))
  }

  # Missing data
  cat("\n### Impact of Missing Data Filter\n")
  missing_enrollment <- sum(is.na(v6_long$cumulative_enrollment))
  missing_suspensions <- sum(is.na(v6_long$total_suspensions))
  zero_enrollment <- sum(v6_long$cumulative_enrollment == 0, na.rm = TRUE)
  negative_suspensions <- sum(v6_long$total_suspensions < 0, na.rm = TRUE)

  results$filtering_missing_enrollment <- missing_enrollment
  results$filtering_missing_suspensions <- missing_suspensions
  results$filtering_zero_enrollment <- zero_enrollment
  results$filtering_negative_suspensions <- negative_suspensions

  cat(sprintf("  Missing enrollment: %s (%.1f%%)\n", format(missing_enrollment, big.mark = ","), 100*missing_enrollment/total))
  cat(sprintf("  Missing suspensions: %s (%.1f%%)\n", format(missing_suspensions, big.mark = ","), 100*missing_suspensions/total))
  cat(sprintf("  Zero enrollment: %s (%.1f%%)\n", format(zero_enrollment, big.mark = ","), 100*zero_enrollment/total))
  cat(sprintf("  Negative suspensions: %s (%.1f%%)\n", format(negative_suspensions, big.mark = ","), 100*negative_suspensions/total))

  # Quartile filters
  cat("\n### Impact of Unknown Quartile Filters\n")
  for (col_info in list(
    list(col = "black_prop_q_label", name = "Black"),
    list(col = "white_prop_q_label", name = "White"),
    list(col = "hispanic_prop_q_label", name = "Hispanic/Latino")
  )) {
    if (col_info$col %in% names(v6_long)) {
      unknown_count <- v6_long %>%
        filter(is.na(.data[[col_info$col]]) | .data[[col_info$col]] == "Unknown") %>%
        nrow()
      results[[paste0("filtering_unknown_", tolower(gsub("/", "_", col_info$name)), "_quartile")]] <- unknown_count
      cat(sprintf("  Unknown %s quartile: %s (%.1f%%)\n",
                  col_info$name, format(unknown_count, big.mark = ","), 100*unknown_count/total))
    }
  }

  # Combined typical analysis filter
  cat("\n### Combined 'Typical Analysis' Filter Impact\n")
  cat("  (Campus-only + 'All Students' + valid enrollment/suspensions + Traditional)\n")

  if (file.exists(v6_feat_path)) {
    v6_feat <- read_parquet(v6_feat_path, col_select = c("school_code", "academic_year", "is_traditional"))

    v6_with_trad <- v6_long %>%
      left_join(v6_feat, by = c("school_code", "academic_year"))

    typical_count <- v6_with_trad %>%
      filter(
        tolower(aggregate_level) %in% c("s", "school"),
        !school_code %in% c("0000000", "0000001"),
        tolower(subgroup) %in% c("total", "all students", "ta"),
        !is.na(cumulative_enrollment),
        !is.na(total_suspensions),
        cumulative_enrollment > 0,
        total_suspensions >= 0,
        is_traditional == TRUE
      ) %>%
      nrow()

    typical_excluded <- total - typical_count
    results$filtering_typical_analysis_included <- typical_count
    results$filtering_typical_analysis_excluded <- typical_excluded

    cat(sprintf("  Records INCLUDED in typical analysis: %s (%.1f%%)\n",
                format(typical_count, big.mark = ","), 100*typical_count/total))
    cat(sprintf("  Records EXCLUDED from typical analysis: %s (%.1f%%)\n",
                format(typical_excluded, big.mark = ","), 100*typical_excluded/total))
  }

  rm(v6_long)
  gc(verbose = FALSE)
}

# Save results
cat("\n================================================================================\n")
cat("SAVING AUDIT REPORT\n")
cat("================================================================================\n\n")

# Create summary
summary <- list(
  total_v0_records = results$v0_total %||% 0,
  final_v6_long_records = results$v6_long_total %||% 0,
  final_v6_features_records = results$v6_features_total %||% 0,
  typical_analysis_records = results$filtering_typical_analysis_included %||% 0,
  total_excluded_from_typical_analysis = results$filtering_typical_analysis_excluded %||% 0,
  percentage_used_in_typical_analysis = if (!is.null(results$v6_long_total) && results$v6_long_total > 0) {
    round(100 * (results$filtering_typical_analysis_included %||% 0) / results$v6_long_total, 1)
  } else { 0 }
)

# Save JSON
json_path <- file.path(AUDIT_OUTPUT, "data_audit_report.json")
write_json(list(
  pipeline_stages = results,
  summary = summary
), json_path, pretty = TRUE, auto_unbox = TRUE)
cat(sprintf("Saved JSON report: %s\n", json_path))

# Save text summary
summary_path <- file.path(AUDIT_OUTPUT, "data_audit_summary.txt")
sink(summary_path)
cat("DATA AUDIT SUMMARY\n")
cat(strrep("=", 80), "\n\n")

cat("PIPELINE OVERVIEW:\n")
cat(sprintf("  Initial ingestion (v0): %s records\n", format(results$v0_total %||% 0, big.mark = ",")))
cat(sprintf("  Final v6_long: %s records\n", format(results$v6_long_total %||% 0, big.mark = ",")))
cat(sprintf("  Final v6_features (campus-years): %s records\n", format(results$v6_features_total %||% 0, big.mark = ",")))
cat(sprintf("  Demographic data (oth_long): %s records\n\n", format(results$oth_total %||% 0, big.mark = ",")))

cat("KEY DATA EXCLUSIONS:\n")
cat(sprintf("  Charter 'All' rows dropped: %s\n", format(results$lost_charter_all %||% 0, big.mark = ",")))
cat(sprintf("  Special school codes (0000000, 0000001): %s\n", format(results$v0_special_codes %||% 0, big.mark = ",")))
cat(sprintf("  Non-traditional schools: %s\n", format(results$v6_nontraditional %||% 0, big.mark = ",")))
cat(sprintf("  Records excluded from typical analysis: %s (%.1f%% of v6_long)\n\n",
            format(results$filtering_typical_analysis_excluded %||% 0, big.mark = ","),
            if (!is.null(results$v6_long_total) && results$v6_long_total > 0) {
              100 * (results$filtering_typical_analysis_excluded %||% 0) / results$v6_long_total
            } else { 0 }))

cat("TYPICAL ANALYSIS USES:\n")
cat(sprintf("  Records INCLUDED: %s (%.1f%% of v6_long)\n",
            format(results$filtering_typical_analysis_included %||% 0, big.mark = ","),
            summary$percentage_used_in_typical_analysis))
cat("  Filters applied:\n")
cat("    - Campus-level only (aggregate_level = 'S'/'school')\n")
cat("    - Exclude special codes (0000000, 0000001)\n")
cat("    - 'All Students'/'Total' subgroup only\n")
cat("    - Valid enrollment and suspensions (enrollment > 0, suspensions >= 0)\n")
cat("    - Traditional schools only\n\n")

cat("DATA CURRENTLY EXCLUDED BUT POTENTIALLY RECOVERABLE:\n\n")

cat("1. NON-TRADITIONAL SCHOOLS\n")
cat(sprintf("   Records: %s\n", format(results$v6_nontraditional %||% 0, big.mark = ",")))
cat("   Impact: HIGH - represents distinct student population\n")
cat("   Recovery: Create separate analysis track for alternative/continuation/community day schools\n\n")

cat("2. RACE/ETHNICITY SUBGROUPS (beyond 'All Students')\n")
cat(sprintf("   Records: %s\n", format(results$filtering_other_subgroups %||% 0, big.mark = ",")))
cat("   Impact: HIGH - critical for equity analysis\n")
cat("   Recovery: Already used in some analyses; ensure all dashboards offer breakdown\n\n")

cat("3. DEMOGRAPHIC SUBGROUPS (SPED, ELL, etc.)\n")
cat(sprintf("   Records: %s\n", format(results$oth_total %||% 0, big.mark = ",")))
cat("   Impact: HIGH - critical for understanding disproportionality\n")
cat("   Recovery: Partially integrated in v6; expand dashboard for intersectional analyses\n\n")

cat("4. NON-CAMPUS AGGREGATES (District/County/State)\n")
cat(sprintf("   Records: %s\n", format(results$filtering_lost_non_campus %||% 0, big.mark = ",")))
cat("   Impact: MEDIUM - useful for higher-level summaries\n")
cat("   Recovery: Create separate dashboards for district/county-level trends\n\n")

cat("5. UNKNOWN QUARTILE SCHOOLS\n")
unknown_total <- sum(
  results$filtering_unknown_black_quartile %||% 0,
  results$filtering_unknown_white_quartile %||% 0,
  results$filtering_unknown_hispanic_latino_quartile %||% 0
)
cat(sprintf("   Records: %s (across all quartile fields)\n", format(unknown_total, big.mark = ",")))
cat("   Impact: MEDIUM - affects quartile-based analyses only\n")
cat("   Recovery: Investigate why quartiles missing; recalculate if possible\n\n")

cat("RECOMMENDATIONS:\n\n")
cat("1. IMMEDIATE (High Priority):\n")
cat("   - Verify all race/ethnicity subgroup data is accessible in dashboards\n")
cat("   - Create dedicated non-traditional schools analysis section\n")
cat("   - Expand demographic intersectional analyses (SPED x Race, ELL x Race, etc.)\n\n")

cat("2. SHORT-TERM:\n")
cat("   - Investigate missing quartile assignments\n")
cat("   - Add district/county-level dashboards for aggregate trends\n")
cat("   - Document filtering decisions in user-facing materials\n\n")

cat("3. LONG-TERM:\n")
cat("   - Create comprehensive data dictionary explaining all exclusions\n")
cat("   - Build 'data explorer' allowing users to customize filters\n")
cat("   - Add downloadable datasets with documentation\n")

sink()
cat(sprintf("Saved summary report: %s\n", summary_path))

cat("\n================================================================================\n")
cat("AUDIT COMPLETE\n")
cat("================================================================================\n\n")
cat(sprintf("Reports saved to: %s/\n", AUDIT_OUTPUT))
cat("  - data_audit_report.json (detailed JSON)\n")
cat("  - data_audit_summary.txt (human-readable summary)\n")
