# R/validate_data_retention.R
# Validate that no school-years are unnecessarily lost across the data pipeline
#
# Purpose:
#   - Track unique school-year combinations through all pipeline stages
#   - Identify any unexpected data loss
#   - Report retention percentages
#   - Flag specific schools that are lost
#
# Outputs:
#   - Console report of retention statistics
#   - CSV file with detailed retention tracking
#   - CSV file listing lost school-years (if any)
#
# Usage:
#   source("R/validate_data_retention.R")

suppressPackageStartupMessages({
  library(dplyr)
  library(arrow)
  library(here)
  library(readr)
  library(tibble)
})

message("[VALIDATE] Starting data retention validation...")

# ---- Helper Functions -------------------------------------------------------

get_school_years <- function(df, stage_name) {
  if (!file.exists(df) && !is.data.frame(df)) {
    warning("File not found: ", df)
    return(tibble(
      stage = stage_name,
      school_years = 0,
      note = "File not found"
    ))
  }

  data <- if (is.character(df)) {
    if (grepl("\\.parquet$", df)) {
      arrow::read_parquet(df)
    } else if (grepl("\\.csv$", df)) {
      readr::read_csv(df, show_col_types = FALSE)
    } else {
      warning("Unknown file type: ", df)
      return(tibble(stage = stage_name, school_years = 0, note = "Unknown file type"))
    }
  } else {
    df
  }

  # Identify key columns
  cds_col <- if ("cds_school" %in% names(data)) {
    "cds_school"
  } else if (all(c("county_code", "district_code", "school_code") %in% names(data))) {
    # Build cds_school
    data <- data %>%
      mutate(cds_school = paste0(
        str_pad(as.character(county_code), 2, pad = "0"),
        str_pad(as.character(district_code), 5, pad = "0"),
        str_pad(as.character(school_code), 7, pad = "0")
      ))
    "cds_school"
  } else {
    warning("No CDS school identifier found in ", stage_name)
    return(tibble(stage = stage_name, school_years = 0, note = "No CDS identifier"))
  }

  year_col <- if ("academic_year" %in% names(data)) {
    "academic_year"
  } else if ("year" %in% names(data)) {
    "year"
  } else {
    warning("No year identifier found in ", stage_name)
    return(tibble(stage = stage_name, school_years = 0, note = "No year identifier"))
  }

  # Count unique school-years
  school_years <- data %>%
    distinct(!!sym(cds_col), !!sym(year_col)) %>%
    nrow()

  tibble(
    stage = stage_name,
    school_years = school_years,
    note = "OK"
  )
}

# ---- Define Pipeline Stages -------------------------------------------------

pipeline_stages <- tribble(
  ~stage_name,                    ~file_path,

  # Student Suspension Pipeline
  "01. Raw Suspension Data (v0)",  here("data-stage", "susp_v0.parquet"),
  "02. + Locale (v1)",             here("data-stage", "susp_v1.parquet"),
  "03. - Charter All (v1_noall)", here("data-stage", "susp_v1_noall.parquet"),
  "04. + Size Quartiles (v2)",     here("data-stage", "susp_v2.parquet"),
  "05. + Black % Quartiles (v3)",  here("data-stage", "susp_v3.parquet"),
  "06. + School Level (v4)",       here("data-stage", "susp_v4.parquet"),
  "07. + Reason Shares (v5)",      here("data-stage", "susp_v5.parquet"),
  "08. CANONICAL v6 Features",     here("data-stage", "susp_v6_features.parquet"),
  "09. CANONICAL v6 Long",         here("data-stage", "susp_v6_long.parquet"),

  # Other Demographics
  "10. Other Demographics (oth)",  here("data-stage", "oth_long.parquet"),

  # Teacher Demographics
  "11. Teacher Demographics",      here("data-stage", "teacher_staff_long.parquet"),

  # Merged Data
  "12. FINAL: v6 + Teacher",       here("data-stage", "susp_v6_teacher_features.parquet")
)

# ---- Validate Retention -----------------------------------------------------

message("[VALIDATE] Checking school-year counts across ", nrow(pipeline_stages), " stages...")

retention_summary <- pipeline_stages %>%
  rowwise() %>%
  mutate(
    school_years = list(get_school_years(file_path, stage_name))
  ) %>%
  unnest(school_years) %>%
  ungroup() %>%
  select(stage = stage_name, school_years, note)

# Calculate retention percentages
retention_summary <- retention_summary %>%
  mutate(
    pct_of_baseline = round(100 * school_years / first(school_years), 1),
    change_from_prev = school_years - lag(school_years),
    pct_change = round(100 * change_from_prev / lag(school_years), 1)
  )

# ---- Report Results ---------------------------------------------------------

message("\n" ,rep("=", 80), "\n")
message("DATA RETENTION VALIDATION REPORT\n")
message(rep("=", 80), "\n\n")

# Summary table
print(retention_summary, n = Inf)

# Highlight any significant drops
large_drops <- retention_summary %>%
  filter(abs(change_from_prev) > 100 | abs(pct_change) > 5, !is.na(pct_change))

if (nrow(large_drops) > 0) {
  message("\n", rep("-", 80))
  message("\n⚠️  SIGNIFICANT DATA CHANGES DETECTED:\n")
  print(large_drops, n = Inf)
  message("\n", rep("-", 80), "\n")
} else {
  message("\n✅ No significant unexpected data loss detected.\n")
}

# ---- Detailed Analysis: Identify Lost School-Years -------------------------

# Compare v0 to v6_features to find any lost school-years
v0_path <- here("data-stage", "susp_v0.parquet")
v6_path <- here("data-stage", "susp_v6_features.parquet")

if (file.exists(v0_path) && file.exists(v6_path)) {
  message("[VALIDATE] Comparing v0 (raw) to v6 (final) to identify lost schools...")

  v0 <- arrow::read_parquet(v0_path) %>%
    mutate(
      cds_school = if ("cds_school" %in% names(.)) {
        cds_school
      } else {
        paste0(
          str_pad(as.character(county_code), 2, pad = "0"),
          str_pad(as.character(district_code), 5, pad = "0"),
          str_pad(as.character(school_code), 7, pad = "0")
        )
      },
      academic_year = if ("academic_year" %in% names(.)) {
        academic_year
      } else {
        paste0(as.integer(year) - 1, "-", substr(as.integer(year), 3, 4))
      }
    )

  v6 <- arrow::read_parquet(v6_path) %>%
    mutate(
      cds_school = if ("cds_school" %in% names(.)) {
        cds_school
      } else {
        paste0(
          str_pad(as.character(county_code), 2, pad = "0"),
          str_pad(as.character(district_code), 5, pad = "0"),
          str_pad(as.character(school_code), 7, pad = "0")
        )
      }
    )

  v0_schools <- v0 %>% distinct(cds_school, academic_year)
  v6_schools <- v6 %>% distinct(cds_school, academic_year)

  lost_schools <- v0_schools %>%
    anti_join(v6_schools, by = c("cds_school", "academic_year"))

  if (nrow(lost_schools) > 0) {
    message("\n⚠️  ", nrow(lost_schools), " school-year combinations were lost from v0 to v6:\n")

    # Add school names if available
    if (all(c("school_name", "school_code") %in% names(v0))) {
      lost_with_names <- lost_schools %>%
        left_join(
          v0 %>% distinct(cds_school, academic_year,
                         school_name = school_name,
                         school_code = school_code),
          by = c("cds_school", "academic_year")
        )
    } else {
      lost_with_names <- lost_schools %>%
        mutate(
          school_code = substr(cds_school, 8, 14),
          school_name = NA_character_
        )
    }

    print(lost_with_names %>% head(20), n = 20)

    # Save to file
    lost_path <- here("data-stage", "validation_lost_school_years.csv")
    write_csv(lost_with_names, lost_path)
    message("\nFull list saved to: ", lost_path)

    # Analyze why schools were lost
    message("\nAnalyzing reasons for data loss...")

    # Check if lost schools are special codes
    special_codes <- c("0000000", "0000001")
    special_code_count <- lost_with_names %>%
      filter(substr(cds_school, 8, 14) %in% special_codes) %>%
      nrow()

    if (special_code_count > 0) {
      message("  - ", special_code_count, " are special aggregate codes (0000000, 0000001) - EXPECTED")
    }

    # Check if lost schools are non-school aggregates
    if ("aggregate_level" %in% names(v0)) {
      non_school <- v0 %>%
        semi_join(lost_schools, by = c("cds_school", "academic_year")) %>%
        filter(tolower(aggregate_level) != "s") %>%
        distinct(cds_school, academic_year) %>%
        nrow()

      if (non_school > 0) {
        message("  - ", non_school, " are non-school aggregates (county/district/state) - EXPECTED")
      }
    }

  } else {
    message("\n✅ No school-year combinations lost from v0 to v6!\n")
  }
} else {
  message("\n⚠️  Cannot compare v0 to v6 - files not found")
}

# ---- Validate Teacher Data Retention ----------------------------------------

message("\n", rep("-", 80))
message("\nVALIDATING TEACHER DATA RETENTION\n")
message(rep("-", 80), "\n")

teacher_path <- here("data-stage", "teacher_staff_long.parquet")
v6_teacher_path <- here("data-stage", "susp_v6_teacher_features.parquet")

if (file.exists(teacher_path) && file.exists(v6_teacher_path)) {
  teacher <- arrow::read_parquet(teacher_path)
  v6_teacher <- arrow::read_parquet(v6_teacher_path)

  # Count schools in teacher data
  teacher_schools <- teacher %>%
    distinct(cds_school, academic_year) %>%
    nrow()

  # Count schools in merged data
  merged_schools <- v6_teacher %>%
    distinct(cds_school, academic_year) %>%
    nrow()

  # Count schools with teacher data in merged file
  teacher_cols <- grep("^teacher_", names(v6_teacher), value = TRUE)

  if (length(teacher_cols) > 0) {
    schools_with_teacher <- v6_teacher %>%
      filter(if_any(all_of(teacher_cols), ~ !is.na(.x))) %>%
      distinct(cds_school, academic_year) %>%
      nrow()

    coverage_pct <- round(100 * schools_with_teacher / merged_schools, 1)

    message("Teacher data statistics:")
    message("  - Unique school-years in teacher_staff_long.parquet: ", teacher_schools)
    message("  - Unique school-years in susp_v6_teacher_features.parquet: ", merged_schools)
    message("  - School-years with teacher data after merge: ", schools_with_teacher)
    message("  - Teacher data coverage: ", coverage_pct, "%")

    if (coverage_pct < 80) {
      message("\n⚠️  WARNING: Teacher data coverage is below 80%")
      message("    This may be expected if not all schools report teacher demographics.")
      message("    Review teacher_staff_long.parquet to verify expected coverage.")
    } else {
      message("\n✅ Teacher data coverage is good (", coverage_pct, "%)")
    }

    # Verify LEFT JOIN preserved all student data
    if (merged_schools >= max(retention_summary$school_years, na.rm = TRUE) * 0.99) {
      message("\n✅ LEFT JOIN confirmed: All student suspension data preserved")
    } else {
      message("\n⚠️  WARNING: Some student suspension data may have been lost in teacher merge")
      message("    Expected: ~", max(retention_summary$school_years, na.rm = TRUE))
      message("    Actual: ", merged_schools)
    }

  } else {
    message("\n⚠️  No teacher_* columns found in merged file")
  }

} else {
  message("\n⚠️  Cannot validate teacher data - files not found")
  if (!file.exists(teacher_path)) message("  Missing: ", teacher_path)
  if (!file.exists(v6_teacher_path)) message("  Missing: ", v6_teacher_path)
}

# ---- Save Summary Report ----------------------------------------------------

output_path <- here("data-stage", "validation_data_retention_summary.csv")
write_csv(retention_summary, output_path)
message("\n", rep("=", 80))
message("\nRetention summary saved to: ", output_path)
message("\n", rep("=", 80), "\n")

# ---- Final Assessment -------------------------------------------------------

baseline <- retention_summary$school_years[1]
final <- retention_summary %>%
  filter(grepl("FINAL", stage)) %>%
  pull(school_years) %>%
  first()

if (!is.na(baseline) && !is.na(final) && baseline > 0) {
  final_retention <- round(100 * final / baseline, 1)

  message("\n📊 FINAL ASSESSMENT:")
  message("  - Baseline school-years (v0): ", baseline)
  message("  - Final school-years (v6+teacher): ", final)
  message("  - Overall retention: ", final_retention, "%")

  if (final_retention >= 95) {
    message("\n✅ PASS: Excellent data retention (≥95%)\n")
  } else if (final_retention >= 90) {
    message("\n✓  PASS: Good data retention (≥90%)\n")
  } else if (final_retention >= 80) {
    message("\n⚠️  WARNING: Moderate data retention (80-90%)\n")
    message("   Review pipeline for unexpected data loss.\n")
  } else {
    message("\n❌ FAIL: Low data retention (<80%)\n")
    message("   Investigate pipeline stages for data loss.\n")
  }
}

message("\n[VALIDATE] Data retention validation complete.\n")

invisible(TRUE)
