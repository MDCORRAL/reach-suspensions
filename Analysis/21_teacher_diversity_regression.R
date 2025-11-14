# Analysis/21_teacher_diversity_regression.R
# Explore associations between staff diversity and suspension rates using
# base R utilities. The script avoids optional packages so it can run in
# restricted environments while still honouring the original diagnostics
# and modeling intent.

TEACHER_PATH <- file.path("data-stage", "susp_v6_teacher_features.parquet")
TEACHER_CSV_PATH <- file.path("data-stage", "susp_v6_teacher_features.csv")
FALLBACK_PATH <- file.path("data-stage", "susp_v6_features.parquet")
FALLBACK_CSV_PATH <- file.path("data-stage", "susp_v6_features.csv")

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

PYTHON_BIN <- NULL

python_has_pyarrow <- function(python_bin) {
  identical(
    suppressWarnings(
      system2(
        python_bin,
        args = c(
          "-c",
          "import importlib.util, sys; sys.exit(0 if importlib.util.find_spec('pyarrow') else 1)"
        ),
        stdout = FALSE,
        stderr = FALSE
      )
    ),
    0L
  )
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
  csv_tmp <- tempfile(fileext = ".csv")
  on.exit(unlink(csv_tmp), add = TRUE)
  tryCatch({
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
  list(data = df, meta = meta)
}

summarise_data <- function(df, meta) {
  message("=== Data Overview ===")
  message("Source file: ", meta$source)
  message("Rows: ", format_number(nrow(df)), "; Columns: ", format_number(ncol(df)))

  key_cols <- intersect(c("cds_school", "academic_year"), names(df))
  if (length(key_cols) == 2) {
    dup_count <- sum(duplicated(df[, key_cols, drop = FALSE]))
    message("Duplicate school-year rows: ", format_number(dup_count))
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

find_non_white_share <- function(df, prefix) {
  cols <- grep(paste0("^", prefix), names(df), value = TRUE)
  if (!length(cols)) {
    return(NULL)
  }

  share_cols <- cols[grepl("_share$", cols)]
  direct <- share_cols[grepl("non_white|nonwhite", share_cols)]
  if (length(direct)) {
    return(df[[direct[1]]])
  }

  for (suffix in c("_white_share", "_white_not_hispanic_share", "_white_not_reported_share")) {
    candidate <- paste0(prefix, suffix)
    if (candidate %in% share_cols) {
      return(1 - df[[candidate]])
    }
  }

  white_like <- share_cols[grepl("white", share_cols)]
  if (length(white_like)) {
    return(1 - df[[white_like[1]]])
  }

  count_cols <- cols[grepl("_(count|fte|total)$", cols)]
  if (length(count_cols)) {
    white_counts <- count_cols[grepl("white", count_cols)]
    total_candidates <- count_cols[grepl("total$", count_cols)]
    if (length(white_counts) && length(total_candidates)) {
      white <- suppressWarnings(as.numeric(df[[white_counts[1]]]))
      total <- suppressWarnings(as.numeric(df[[total_candidates[1]]]))
      share <- suppressWarnings(1 - (white / total))
      share[!is.finite(share)] <- NA_real_
      return(share)
    }
  }

  NULL
}

prepare_regression_frame <- function(df) {
  teacher_share <- find_non_white_share(df, "teacher")
  admin_share <- NULL
  for (prefix in c("administrator", "admin", "teacher_staff_count_by_type_adm", "adm")) {
    admin_share <- find_non_white_share(df, prefix)
    if (!is.null(admin_share)) break
  }

  if (is.null(teacher_share) || is.null(admin_share)) {
    message("\nTeacher or administrator diversity columns not located; regression skipped.")
    return(NULL)
  }

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
    teacher_non_white_share = suppressWarnings(as.numeric(teacher_share)),
    admin_non_white_share = suppressWarnings(as.numeric(admin_share)),
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
    grade_col = grade_col
  )
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

  message("\n=== Weighted Linear Regression ===")
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
  model_info <- prepare_regression_frame(df)
  if (is.null(model_info)) {
    message("Regression model not executed because required columns were unavailable.")
    return(invisible(NULL))
  }

  run_regression(model_info)
  message("\nReminder: Associations are descriptive. Do not infer causality from these coefficients.")
}

if (identical(environment(), globalenv()) && !isTRUE(getOption("teacher_regression_skip_main"))) {
  main()
}
