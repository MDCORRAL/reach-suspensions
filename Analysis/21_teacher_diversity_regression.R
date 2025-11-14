# Analysis/21_teacher_diversity_regression.R
# Explore associations between staff diversity and suspension rates using
# base R utilities. The script avoids optional packages so it can run in
# restricted environments while still honouring the original diagnostics
# and modeling intent.

TEACHER_PATH <- file.path("data-stage", "susp_v6_teacher_features.parquet")
TEACHER_CSV_PATH <- file.path("data-stage", "susp_v6_teacher_features.csv")
FALLBACK_PATH <- file.path("data-stage", "susp_v6_features.parquet")
FALLBACK_CSV_PATH <- file.path("data-stage", "susp_v6_features.csv")
LONG_PATH <- file.path("data-stage", "susp_v6_long.parquet")
LONG_CSV_PATH <- file.path("data-stage", "susp_v6_long.csv")

format_number <- function(x) {
  format(x, big.mark = ",", scientific = FALSE, trim = TRUE)
}

`%||%` <- function(x, y) {
  if (!is.null(x)) x else y
}

clean_names <- function(x) {
  lower <- tolower(x)
  cleaned <- gsub("[^a-z0-9]+", "_", lower)
  cleaned <- gsub("(^_|_$)", "", cleaned)
  cleaned <- gsub("__+", "_", cleaned)
  cleaned[nchar(cleaned) == 0] <- "col"
  cleaned
}

canonicalize_race_label <- function(x) {
  labels <- rep(NA_character_, length(x))
  clean <- tolower(trimws(as.character(x)))

  assign_label <- function(indices, value) {
    labels[indices & is.na(labels)] <<- value
  }

  assign_label(clean %in% c("ta", "total", "all students", "all_students"), "All Students")
  assign_label(clean %in% c("ra", "asian"), "Asian")
  assign_label(clean %in% c("rb", "black", "african american", "black/african american", "african_american"), "Black/African American")
  assign_label(clean %in% c("rf", "filipino"), "Filipino")
  assign_label(clean %in% c("rh", "rl", "hispanic", "latino", "hispanic/latino", "hispanic_latino"), "Hispanic/Latino")
  assign_label(clean %in% c("ri", "american indian", "alaska native", "american indian/alaska native", "native american"), "American Indian/Alaska Native")
  assign_label(clean %in% c("rp", "pacific islander", "native hawaiian"), "Native Hawaiian/Pacific Islander")
  assign_label(clean %in% c("rt", "two or more", "two or more races", "multirace", "multiple"), "Two or More Races")
  assign_label(clean %in% c("rw", "white"), "White")
  assign_label(clean %in% c("rd", "not reported", "not_reported", "notreported"), "Not Reported")

  labels
}

ALLOWED_RACE_GROUPS <- c(
  "Black/African American",
  "White",
  "Hispanic/Latino",
  "American Indian/Alaska Native",
  "Asian",
  "Filipino",
  "Native Hawaiian/Pacific Islander",
  "Two or More Races"
)

TEACHER_RACE_PATTERNS <- c(
  "african_american",
  "black_african_american",
  "american_indian_or_alaska_native",
  "american_indian_alaska_native",
  "asian",
  "filipino",
  "hispanic_or_latino",
  "latino",
  "pacific_islander",
  "native_hawaiian",
  "hawaiian",
  "two_or_more_races",
  "two_or_more",
  "all_other_races",
  "other_races",
  "not_reported",
  "unknown",
  "white",
  "white_not_hispanic"
)

PYTHON_BIN <- NULL

python_has_pyarrow <- function(python_bin) {
  script_path <- tempfile(fileext = ".py")
  on.exit(unlink(script_path), add = TRUE)
  writeLines(
    c(
      "import importlib.util",
      "import sys",
      "sys.exit(0 if importlib.util.find_spec(\"pyarrow\") else 1)"
    ),
    con = script_path
  )
  status <- suppressWarnings(
    system2(
      python_bin,
      args = script_path,
      stdout = FALSE,
      stderr = FALSE
    )
  )
  identical(status, 0L)
}

detect_python <- function(require_pyarrow = FALSE) {
  if (!is.null(PYTHON_BIN)) {
    if (!require_pyarrow || python_has_pyarrow(PYTHON_BIN)) {
      return(PYTHON_BIN)
    }
  }

  candidates <- Sys.which(c("python", "python3"))
  candidates <- unique(candidates[nzchar(candidates)])

  if (!length(candidates)) {
    stop(
      "Python interpreter not found. Install Python 3 with the pyarrow package ",
      "or convert the parquet file to CSV manually before running this script."
    )
  }

  if (!require_pyarrow) {
    PYTHON_BIN <<- candidates[1]
    return(PYTHON_BIN)
  }

  for (candidate in candidates) {
    if (python_has_pyarrow(candidate)) {
      PYTHON_BIN <<- candidate
      return(candidate)
    }
  }

  stop(
    "Python interpreter(s) found (",
    paste(basename(candidates), collapse = ", "),
    ") are missing the pyarrow package."
  )
}

convert_parquet_to_csv <- function(parquet_path, csv_path, csv_hint = NULL) {
  script_path <- tempfile(fileext = ".py")
  on.exit(unlink(script_path), add = TRUE)
  python_bin <- tryCatch(
    detect_python(require_pyarrow = TRUE),
    error = function(err) {
      manual_msg <- if (is.null(csv_hint)) {
        paste0(dirname(parquet_path), "/", tools::file_path_sans_ext(basename(parquet_path)), ".csv")
      } else {
        csv_hint
      }
      stop(
        conditionMessage(err),
        " Install pyarrow (e.g., `pip install pyarrow`) or supply a CSV copy at ",
        manual_msg,
        " before rerunning.",
        call. = FALSE
      )
    }
  )

  writeLines(
    c(
      "import sys",
      "from pathlib import Path",
      "import pyarrow.parquet as pq",
      "import pyarrow.csv as pc",
      "source, dest = Path(sys.argv[1]), Path(sys.argv[2])",
      "if not source.exists():",
      "    raise FileNotFoundError(f'{source} not found')",
      "table = pq.read_table(source)",
      "with dest.open('wb') as output:",
      "    pc.write_csv(table, output)"
    ),
    con = script_path
  )
  output <- suppressWarnings(
    system2(
      python_bin,
      args = c(script_path, parquet_path, csv_path),
      stdout = TRUE,
      stderr = TRUE
    )
  )
  status <- attr(output, "status")
  if (is.null(status)) {
    status <- 0L
  }
  if (!identical(status, 0L)) {
    if (length(output)) {
      message(paste(output, collapse = "\n"))
    }
    stop(
      "Failed to convert ", parquet_path, " using ", python_bin,
      " (exit status ", status, "). Ensure pyarrow is installed.",
      call. = FALSE
    )
  }
}

read_parquet <- function(path, csv_hint = NULL) {
  if (file.exists(path) && requireNamespace("arrow", quietly = TRUE)) {
    df <- as.data.frame(arrow::read_parquet(path))
    attr(df, "source_path") <- path
    return(df)
  }

  csv_tmp <- tempfile(fileext = ".csv")
  on.exit(unlink(csv_tmp), add = TRUE)
  tryCatch({
    if (!file.exists(path)) {
      stop(path, " not found.")
    }
    convert_parquet_to_csv(path, csv_tmp, csv_hint)
    df <- utils::read.csv(csv_tmp, stringsAsFactors = FALSE, check.names = FALSE)
    attr(df, "source_path") <- path
    df
  }, error = function(err) {
    if (!is.null(csv_hint) && file.exists(csv_hint)) {
      message(conditionMessage(err))
      message("Falling back to existing CSV file: ", basename(csv_hint))
      df <- utils::read.csv(csv_hint, stringsAsFactors = FALSE, check.names = FALSE)
      attr(df, "source_path") <- csv_hint
      return(df)
    }
    stop(conditionMessage(err), call. = FALSE)
  })
}

load_features <- function() {
  teacher_available <- file.exists(TEACHER_PATH) || file.exists(TEACHER_CSV_PATH)
  fallback_available <- file.exists(FALLBACK_PATH) || file.exists(FALLBACK_CSV_PATH)

  if (teacher_available) {
    df <- read_parquet(TEACHER_PATH, TEACHER_CSV_PATH)
    source_path <- attr(df, "source_path") %||% TEACHER_PATH
    meta <- list(source = basename(source_path), teacher_columns = "present")
  } else if (fallback_available) {
    df <- read_parquet(FALLBACK_PATH, FALLBACK_CSV_PATH)
    source_path <- attr(df, "source_path") %||% FALLBACK_PATH
    meta <- list(
      source = basename(source_path),
      teacher_columns = "missing",
      note = "Teacher merge parquet not found; loaded student features only."
    )
  } else {
    stop("Neither ", basename(TEACHER_PATH), " nor ", basename(FALLBACK_PATH), " is available.")
  }

  names(df) <- clean_names(names(df))
  attach <- attach_suspension_outcomes(df)
  df <- attach$data
  if (!is.null(attach$note)) {
    meta$note <- if (is.null(meta$note)) attach$note else paste(meta$note, attach$note, sep = " | ")
  }

  list(data = df, meta = meta)
}

attach_suspension_outcomes <- function(df) {
  outcome_cols <- intersect(
    c("suspension_rate_percent_total", "susp_all_rate", "susp_all", "susp_rate"),
    names(df)
  )
  enrollment_cols <- intersect(
    c("cumulative_enrollment", "sup_cumulative_enrollment", "all_enroll", "enroll_all"),
    names(df)
  )

  if (length(outcome_cols) && length(enrollment_cols)) {
    return(list(data = df, note = NULL))
  }

  if (!file.exists(LONG_PATH) && !file.exists(LONG_CSV_PATH)) {
    message(
      "Suspension outcomes unavailable in the main dataset and ",
      basename(LONG_PATH),
      " is not present."
    )
    return(list(
      data = df,
      note = "Suspension outcomes missing; susp_v6_long.parquet unavailable for join."
    ))
  }

  long_df <- read_parquet(LONG_PATH, LONG_CSV_PATH)
  names(long_df) <- clean_names(names(long_df))
  source_path <- attr(long_df, "source_path") %||% LONG_PATH

  key_cols <- c("cds_school", "academic_year")
  if (!all(key_cols %in% names(long_df))) {
    message("susp_v6_long.parquet missing keys cds_school/academic_year; cannot attach suspension rates.")
    return(list(
      data = df,
      note = "Suspension outcomes missing; susp_v6_long.parquet lacks school-year keys."
    ))
  }

  filtered <- long_df
  if ("aggregate_level" %in% names(filtered)) {
    agg <- tolower(as.character(filtered$aggregate_level))
    filtered <- filtered[!is.na(agg) & agg %in% c("s", "school"), , drop = FALSE]
  }
  if ("category_type" %in% names(filtered)) {
    cat_type <- tolower(as.character(filtered$category_type))
    filtered <- filtered[!is.na(cat_type) & cat_type %in% c("race/ethnicity", "race_ethnicity"), , drop = FALSE]
  }

  if (!nrow(filtered)) {
    message("susp_v6_long.parquet has no race-specific school rows after filtering.")
    return(list(
      data = df,
      note = "Suspension outcomes missing; no race-specific school rows in susp_v6_long.parquet."
    ))
  }

  subgroup_values <- if ("subgroup" %in% names(filtered)) {
    filtered$subgroup
  } else {
    rep(NA_character_, nrow(filtered))
  }
  if ("reporting_category" %in% names(filtered)) {
    missing_subgroup <- is.na(subgroup_values) | !nzchar(trimws(as.character(subgroup_values)))
    subgroup_values[missing_subgroup] <- filtered$reporting_category[missing_subgroup]
  }
  group_labels <- canonicalize_race_label(subgroup_values)

  filtered$student_group <- group_labels
  keep_groups <- !is.na(filtered$student_group) &
    filtered$student_group %in% ALLOWED_RACE_GROUPS
  filtered <- filtered[keep_groups, , drop = FALSE]

  if (!nrow(filtered)) {
    message("susp_v6_long.parquet does not contain race-specific student groups needed for the join.")
    return(list(
      data = df,
      note = "Suspension outcomes missing; race-specific student groups unavailable in susp_v6_long.parquet."
    ))
  }

  if (!"suspension_rate_percent_total" %in% names(filtered)) {
    message("susp_v6_long.parquet lacks suspension_rate_percent_total after filtering; suspension outcomes remain missing.")
    return(list(
      data = df,
      note = "Suspension outcomes missing; suspension_rate_percent_total absent in susp_v6_long.parquet."
    ))
  }

  value_cols <- intersect(
    c("total_suspensions", "cumulative_enrollment", "suspension_rate_percent_total"),
    names(filtered)
  )
  new_cols <- setdiff(value_cols, names(df))
  if (!"student_group" %in% names(df)) {
    new_cols <- c(new_cols, "student_group")
  }
  new_cols <- unique(new_cols)
  if (!length(new_cols)) {
    return(list(data = df, note = NULL))
  }
  filtered <- filtered[, unique(c(key_cols, new_cols)), drop = FALSE]

  if (!nrow(filtered)) {
    message("susp_v6_long.parquet does not contain usable race-specific rows after selecting columns.")
    return(list(
      data = df,
      note = "Suspension outcomes missing; no race-specific data remained after column selection."
    ))
  }

  ord <- do.call(order, filtered[c(key_cols, "student_group")])
  filtered <- filtered[ord, , drop = FALSE]
  dup <- duplicated(filtered[, c(key_cols, "student_group"), drop = FALSE])
  filtered <- filtered[!dup, , drop = FALSE]

  df$.__rowid <- seq_len(nrow(df))
  merged <- merge(df, filtered, by = key_cols, all.x = TRUE, sort = FALSE)
  if ("student_group" %in% names(merged)) {
    order_group <- ifelse(is.na(merged$student_group), "\uFFFF", merged$student_group)
    merged <- merged[order(merged$.__rowid, order_group), , drop = FALSE]
  } else {
    merged <- merged[order(merged$.__rowid), , drop = FALSE]
  }
  merged$.__rowid <- NULL

  message("Attached race-specific suspension outcomes from ", basename(source_path), ".")
  list(
    data = merged,
    note = "Attached race-specific suspension outcomes from susp_v6_long.parquet."
  )
}

summarise_data <- function(df, meta) {
  message("=== Data Overview ===")
  message("Source file: ", meta$source)
  message("Rows: ", format_number(nrow(df)), "; Columns: ", format_number(ncol(df)))

  key_cols <- intersect(c("cds_school", "academic_year"), names(df))
  if (length(key_cols) == 2) {
    dup_count <- sum(duplicated(df[, key_cols, drop = FALSE]))
    if (dup_count > 0 && "student_group" %in% names(df)) {
      message(
        "Duplicate school-year rows: ",
        format_number(dup_count),
        " (expected when student_group is present)."
      )
    } else {
      message("Duplicate school-year rows: ", format_number(dup_count))
    }
  } else {
    message("Key columns cds_school/academic_year unavailable for duplicate check.")
  }

  missing_rates <- colMeans(is.na(df))
  missing_rates <- missing_rates[missing_rates > 0]
  if (length(missing_rates)) {
    ord <- order(missing_rates, decreasing = TRUE)
    top <- head(ord, 10)
    message("\nTop missingness rates:")
    for (idx in top) {
      pct <- sprintf("%.1f%%", missing_rates[idx] * 100)
      message("  ", names(missing_rates)[idx], ": ", pct)
    }
  } else {
    message("\nNo missing values detected.")
  }

  invisible(df)
}

extract_race_nonwhite_share <- function(df, cols) {
  race_cols <- cols[!grepl("gender", cols, ignore.case = TRUE)]
  if (!length(race_cols)) {
    return(NULL)
  }

  race_pattern <- paste0("(", paste(TEACHER_RACE_PATTERNS, collapse = "|"), ")")
  race_cols <- race_cols[grepl(race_pattern, race_cols, ignore.case = TRUE)]
  if (!length(race_cols)) {
    return(NULL)
  }

  to_numeric <- function(column) suppressWarnings(as.numeric(df[[column]]))
  build_matrix <- function(columns) {
    if (!length(columns)) {
      return(NULL)
    }
    values <- lapply(columns, to_numeric)
    names(values) <- columns
    mat <- do.call(cbind, values)
    if (is.null(mat)) {
      return(NULL)
    }
    if (!is.matrix(mat)) {
      mat <- matrix(mat, ncol = 1)
      colnames(mat) <- columns[1]
    } else {
      colnames(mat) <- columns
    }
    mat
  }

  share_cols <- race_cols[grepl("_share$", race_cols)]
  direct <- share_cols[grepl("non_white|nonwhite", share_cols, ignore.case = TRUE)]
  if (length(direct)) {
    return(list(
      values = to_numeric(direct[1]),
      meta = list(type = "race_share", columns = direct[1], method = "direct_non_white_share")
    ))
  }

  race_share_cols <- share_cols[grepl(race_pattern, share_cols, ignore.case = TRUE)]
  non_white_share_cols <- race_share_cols[
    !grepl("white", race_share_cols, ignore.case = TRUE) &
      !grepl("not_reported|unknown", race_share_cols, ignore.case = TRUE)
  ]
  if (length(non_white_share_cols)) {
    mat <- build_matrix(non_white_share_cols)
    if (!is.null(mat)) {
      values <- rowSums(mat, na.rm = TRUE)
      all_missing <- apply(is.na(mat), 1, all)
      values[all_missing] <- NA_real_
      return(list(
        values = values,
        meta = list(
          type = "race_share",
          columns = non_white_share_cols,
          method = "sum_of_race_shares"
        )
      ))
    }
  }

  for (suffix in c("_white_share", "_white_not_hispanic_share", "_white_not_reported_share")) {
    candidate <- race_share_cols[grepl(paste0(suffix, "$"), race_share_cols, ignore.case = TRUE)]
    if (length(candidate)) {
      values <- 1 - to_numeric(candidate[1])
      return(list(
        values = values,
        meta = list(type = "race_share", columns = candidate[1], method = "derived_from_white_share")
      ))
    }
  }

  white_like <- race_share_cols[grepl("white", race_share_cols, ignore.case = TRUE)]
  if (length(white_like)) {
    white <- to_numeric(white_like[1])
    values <- 1 - white
    nr_cols <- race_share_cols[grepl("not_reported|unknown", race_share_cols, ignore.case = TRUE)]
    used_columns <- white_like[1]
    if (length(nr_cols)) {
      not_reported <- to_numeric(nr_cols[1])
      values <- values - not_reported
      used_columns <- c(used_columns, nr_cols[1])
    }
    values[!is.finite(values)] <- NA_real_
    return(list(
      values = values,
      meta = list(
        type = "race_share",
        columns = used_columns,
        method = if (length(nr_cols)) {
          "derived_from_white_and_not_reported_shares"
        } else {
          "derived_from_white_share"
        }
      )
    ))
  }

  count_cols <- race_cols[grepl("_(count|fte|total)$", race_cols)]
  count_cols <- count_cols[grepl(race_pattern, count_cols, ignore.case = TRUE)]
  if (length(count_cols)) {
    non_white_counts <- count_cols[
      !grepl("white", count_cols, ignore.case = TRUE) &
        !grepl("not_reported|unknown", count_cols, ignore.case = TRUE)
    ]
    if (length(non_white_counts)) {
      mat <- build_matrix(non_white_counts)
      if (!is.null(mat)) {
        numerator <- rowSums(mat, na.rm = TRUE)
        all_missing <- apply(is.na(mat), 1, all)
        numerator[all_missing] <- NA_real_
        total_candidates <- intersect(
          c(
            "teacher_total_staff_count_total",
            "teacher_staff_count_total",
            "teacher_total_staff_count_total_by_type_all_staff",
            "teacher_staff_count_total_by_type_all_staff"
          ),
          names(df)
        )
        if (!length(total_candidates)) {
          total_candidates <- count_cols[grepl("total$", count_cols, ignore.case = TRUE)]
        }
        if (length(total_candidates)) {
          total <- to_numeric(total_candidates[1])
          share <- suppressWarnings(numerator / total)
          share[!is.finite(share)] <- NA_real_
          return(list(
            values = share,
            meta = list(
              type = "race_share",
              columns = non_white_counts,
              method = "derived_from_race_counts",
              total_column = total_candidates[1]
            )
          ))
        }
      }
    }
  }

  NULL
}

extract_gender_non_male_share <- function(df, cols) {
  gender_cols <- cols[grepl("gender", cols, ignore.case = TRUE)]
  if (!length(gender_cols)) {
    return(NULL)
  }

  share_cols <- gender_cols[grepl("_share$", gender_cols)]
  male_share <- share_cols[
    grepl("male_share$", share_cols, ignore.case = TRUE) &
      !grepl("female", share_cols, ignore.case = TRUE)
  ]
  if (length(male_share)) {
    values <- 1 - suppressWarnings(as.numeric(df[[male_share[1]]]))
    return(list(
      values = values,
      meta = list(type = "gender_share", columns = male_share[1], method = "1_minus_male_share")
    ))
  }

  female_share <- share_cols[grepl("gender.*female_share$", share_cols, ignore.case = TRUE)]
  nb_share <- share_cols[grepl("gender.*non_binary_share$", share_cols, ignore.case = TRUE)]
  has_female <- length(female_share) > 0
  has_nb <- length(nb_share) > 0
  if (has_female || has_nb) {
    female <- if (has_female) suppressWarnings(as.numeric(df[[female_share[1]]])) else rep(NA_real_, nrow(df))
    nb <- if (has_nb) suppressWarnings(as.numeric(df[[nb_share[1]]])) else rep(NA_real_, nrow(df))
    components <- cbind(female, nb)
    values <- rowSums(components, na.rm = TRUE)
    all_missing <- apply(is.na(components), 1, all)
    values[all_missing] <- NA_real_
    used_columns <- c(if (has_female) female_share[1] else NA_character_, if (has_nb) nb_share[1] else NA_character_)
    used_columns <- used_columns[!is.na(used_columns)]
    if (!length(used_columns)) {
      used_columns <- NA_character_
    }
    return(list(
      values = values,
      meta = list(
        type = "gender_share",
        columns = used_columns,
        method = "female_plus_non_binary_share"
      )
    ))
  }

  count_cols <- gender_cols[!grepl("_share$", gender_cols)]
  female_counts <- count_cols[grepl("gender_female$", count_cols, ignore.case = TRUE)]
  nb_counts <- count_cols[grepl("gender_non_binary$", count_cols, ignore.case = TRUE)]
  male_counts <- count_cols[
    grepl("gender_male$", count_cols, ignore.case = TRUE) &
      !grepl("female", count_cols, ignore.case = TRUE)
  ]
  has_female_counts <- length(female_counts) > 0
  has_nb_counts <- length(nb_counts) > 0
  if (has_female_counts || has_nb_counts) {
    n <- nrow(df)
    female <- if (has_female_counts) suppressWarnings(as.numeric(df[[female_counts[1]]])) else rep(NA_real_, n)
    nb <- if (has_nb_counts) suppressWarnings(as.numeric(df[[nb_counts[1]]])) else rep(NA_real_, n)
    male <- if (length(male_counts)) suppressWarnings(as.numeric(df[[male_counts[1]]])) else rep(NA_real_, n)
    components <- cbind(female, nb, male)
    total_candidates <- cols[!grepl("gender", cols, ignore.case = TRUE) & grepl("(total$|all_staff$|administrators$)", cols)]
    if (length(total_candidates)) {
      total <- suppressWarnings(as.numeric(df[[total_candidates[1]]]))
    } else {
      total <- rowSums(components, na.rm = TRUE)
      total[apply(is.na(components), 1, all)] <- NA_real_
    }
    numerator <- rowSums(cbind(female, nb), na.rm = TRUE)
    numerator[apply(is.na(cbind(female, nb)), 1, all)] <- NA_real_
    share <- suppressWarnings(numerator / total)
    share[!is.finite(share)] <- NA_real_
    used_columns <- c(
      if (has_female_counts) female_counts[1] else NA_character_,
      if (has_nb_counts) nb_counts[1] else NA_character_
    )
    used_columns <- used_columns[!is.na(used_columns)]
    if (!length(used_columns)) {
      used_columns <- NA_character_
    }
    return(list(
      values = share,
      meta = list(
        type = "gender_share",
        columns = used_columns,
        method = "derived_from_gender_counts",
        total_column = if (length(total_candidates)) total_candidates[1] else NA_character_
      )
    ))
  }

  NULL
}

find_diversity_share <- function(df, patterns) {
  for (pattern in patterns) {
    cols <- grep(pattern, names(df), value = TRUE)
    if (!length(cols)) {
      next
    }

    race <- extract_race_nonwhite_share(df, cols)
    if (!is.null(race)) {
      return(race)
    }
  }

  for (pattern in patterns) {
    cols <- grep(pattern, names(df), value = TRUE)
    if (!length(cols)) {
      next
    }

    gender <- extract_gender_non_male_share(df, cols)
    if (!is.null(gender)) {
      return(gender)
    }
  }

  NULL
}

describe_share_source <- function(meta, label) {
  if (is.null(meta)) {
    return(paste(label, "diversity source: unknown"))
  }

  collapse_columns <- function(columns, fallback_label) {
    cols <- columns
    if (is.null(cols)) {
      cols <- character()
    }
    cols <- cols[!is.na(cols)]
    if (!length(cols)) {
      return(fallback_label)
    }
    paste0("`", paste(cols, collapse = "`, `"), "`")
  }

  if (identical(meta$type, "race_share")) {
    column_desc <- collapse_columns(c(meta$columns, meta$column), "race columns")
    if (!is.null(meta$total_column) && !is.na(meta$total_column)) {
      column_desc <- paste0(column_desc, "; total reference `", meta$total_column, "`")
    }
    return(paste0(label, " diversity derived from ", column_desc, " (", meta$method, ")"))
  }

  if (identical(meta$type, "gender_share")) {
    column_desc <- collapse_columns(meta$columns, "gender columns")
    if (!is.null(meta$total_column) && !is.na(meta$total_column)) {
      column_desc <- paste0(column_desc, "; total reference `", meta$total_column, "`")
    }
    return(paste0(label, " diversity derived from ", column_desc, " (", meta$method, ")"))
  }

  paste(label, "diversity source: unspecified")
}

prepare_regression_frame_single <- function(df, student_group = NULL) {
  if (!is.null(student_group)) {
    if (!"student_group" %in% names(df)) {
      return(NULL)
    }
    df <- df[df$student_group == student_group, , drop = FALSE]
    if (!nrow(df)) {
      return(NULL)
    }
    message("\n--- Student group: ", student_group, " ---")
  }

  teacher_info <- find_diversity_share(df, c("^teacher"))
  admin_info <- find_diversity_share(
    df,
    c("teacher.*administrators", "^administrators?", "^admin\\b")
  )

  if (is.null(teacher_info) || is.null(admin_info)) {
    message("\nTeacher or administrator diversity columns not located; regression skipped.")
    return(NULL)
  }

  message(describe_share_source(teacher_info$meta, "Teacher"))
  message(describe_share_source(admin_info$meta, "Administrator"))

  enrollment_candidates <- intersect(
    c("cumulative_enrollment", "sup_cumulative_enrollment", "all_enroll", "enroll_all"),
    names(df)
  )
  enrollment_col <- if (length(enrollment_candidates)) enrollment_candidates[1] else NA_character_

  econ_cols <- names(df)[grepl("sed_rate|economic", names(df))]
  sed_col <- if (length(econ_cols)) econ_cols[1] else NA_character_

  grade_span_cols <- intersect(
    c("level_strict3", "school_level_final", "school_type", "grades_served"),
    names(df)
  )
  grade_col <- if (length(grade_span_cols)) grade_span_cols[1] else NA_character_

  charter_cols <- intersect(c("charter_yn_std", "charter_yn", "is_traditional"), names(df))
  charter_col <- if (length(charter_cols)) charter_cols[1] else NA_character_

  outcome_candidates <- intersect(
    c("suspension_rate_percent_total", "susp_all_rate", "susp_all", "susp_rate"),
    names(df)
  )
  outcome_col <- if (length(outcome_candidates)) outcome_candidates[1] else NA_character_
  if (is.na(outcome_col)) {
    message("Suspension rate column not located; regression skipped.")
    return(NULL)
  }

  divisor <- if (grepl("percent", outcome_col)) 100 else 1
  suspension_rate <- suppressWarnings(as.numeric(df[[outcome_col]]) / divisor)

  model_df <- data.frame(
    suspension_rate = suspension_rate,
    teacher_non_white_share = suppressWarnings(as.numeric(teacher_info$values)),
    admin_non_white_share = suppressWarnings(as.numeric(admin_info$values)),
    stringsAsFactors = FALSE
  )

  if (!is.na(sed_col)) {
    model_df$sed_rate <- suppressWarnings(as.numeric(df[[sed_col]]))
  }
  if (!is.na(charter_col)) {
    charter_vec <- df[[charter_col]]
    if (is.logical(charter_vec)) {
      model_df$is_charter <- as.integer(charter_vec)
    } else {
      charter_str <- tolower(as.character(charter_vec))
      model_df$is_charter <- as.integer(charter_str %in% c("y", "yes", "charter", "true", "1"))
    }
  }
  if (!is.na(grade_col)) {
    model_df$grade_level <- factor(df[[grade_col]])
  }
  if (!is.na(enrollment_col)) {
    model_df$enrollment <- suppressWarnings(as.numeric(df[[enrollment_col]]))
  }

  keep <- !is.na(model_df$suspension_rate) &
    !is.na(model_df$teacher_non_white_share) &
    !is.na(model_df$admin_non_white_share)
  model_df <- model_df[keep, , drop = FALSE]

  if ("enrollment" %in% names(model_df)) {
    positive <- !is.na(model_df$enrollment) & model_df$enrollment > 0
    model_df <- model_df[positive, , drop = FALSE]
    model_df$weights <- model_df$enrollment
    small_n <- sum(model_df$enrollment < 50, na.rm = TRUE)
  } else {
    model_df$weights <- 1
    small_n <- 0
  }
  message("Small-enrollment rows (<50 students): ", format_number(small_n))

  if (!nrow(model_df)) {
    message("No rows available after filtering; regression skipped.")
    return(NULL)
  }

  list(
    data = model_df,
    enrollment_col = enrollment_col,
    sed_col = sed_col,
    charter_col = charter_col,
    grade_col = grade_col,
    diversity_meta = list(teacher = teacher_info$meta, administrator = admin_info$meta),
    student_group = student_group
  )
}

prepare_regression_frames <- function(df) {
  if ("student_group" %in% names(df)) {
    groups <- unique(df$student_group)
    groups <- groups[!is.na(groups)]
    ordered <- intersect(ALLOWED_RACE_GROUPS, groups)
    extras <- setdiff(groups, ordered)
    groups <- c(ordered, sort(extras))
    results <- vector("list", length = 0L)
    for (group in groups) {
      info <- prepare_regression_frame_single(df, student_group = group)
      if (!is.null(info)) {
        results[[length(results) + 1L]] <- info
      }
    }
    return(results)
  }

  info <- prepare_regression_frame_single(df, student_group = NULL)
  if (is.null(info)) {
    list()
  } else {
    list(info)
  }
}

format_coefficient_table <- function(fit) {
  coef_mat <- summary(fit)$coefficients
  conf <- suppressWarnings(confint(fit))
  combined <- cbind(coef_mat, conf)
  colnames(combined)[(ncol(combined) - 1):ncol(combined)] <- c("conf.low", "conf.high")
  df <- data.frame(term = rownames(combined), combined, row.names = NULL)
  df
}

format_glance <- function(fit) {
  s <- summary(fit)
  data.frame(
    r.squared = s$r.squared,
    adj.r.squared = s$adj.r.squared,
    sigma = s$sigma,
    statistic = s$fstatistic[1],
    df1 = s$fstatistic[2],
    df2 = s$fstatistic[3],
    p.value = pf(s$fstatistic[1], s$fstatistic[2], s$fstatistic[3], lower.tail = FALSE),
    nobs = stats::nobs(fit)
  )
}

run_regression <- function(model_info) {
  model_df <- model_info$data
  predictors <- c("teacher_non_white_share", "admin_non_white_share")
  optional <- intersect(c("sed_rate", "is_charter"), names(model_df))
  predictors <- c(predictors, optional)
  if ("grade_level" %in% names(model_df)) {
    predictors <- c(predictors, "grade_level")
  }

  formula <- stats::as.formula(paste("suspension_rate ~", paste(predictors, collapse = " + ")))
  fit <- stats::lm(formula, data = model_df, weights = model_df$weights)

  group_label <- model_info$student_group
  if (!is.null(group_label) && !is.na(group_label)) {
    message("\n=== Weighted Linear Regression: ", group_label, " ===")
  } else {
    message("\n=== Weighted Linear Regression ===")
  }
  print(summary(fit))

  message("\nCoefficient table (with 95% CI):")
  print(format_coefficient_table(fit))

  message("\nModel fit statistics:")
  print(format_glance(fit))

  invisible(fit)
}

main <- function() {
  features <- load_features()
  df <- features$data
  meta <- features$meta

  summarise_data(df, meta)
  model_infos <- prepare_regression_frames(df)
  if (!length(model_infos)) {
    message("Regression model not executed because required columns were unavailable.")
    return(invisible(NULL))
  }

  for (model_info in model_infos) {
    run_regression(model_info)
  }
  message("\nReminder: Associations are descriptive. Do not infer causality from these coefficients.")
}

if (identical(environment(), globalenv()) && !isTRUE(getOption("teacher_regression_skip_main"))) {
  main()
}
