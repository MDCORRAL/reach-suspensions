# Analysis/23_teacher_diversity_visualizations.R
#
# Generates publication-ready visuals and summary tables for the
# teacher/administrator racial diversity regressions in
# Analysis/21_teacher_diversity_regression.R.
#
# Outputs (written to outputs/ by default):
#   - teacher_diversity_effects.png  : coefficient plot for teacher and administrator
#                                      non-white share across student groups
#   - teacher_diversity_effects.svg  : vector version of the coefficient plot
#   - teacher_diversity_summary.html : gt table summarizing effect sizes, CIs,
#                                      p-values, R^2, and sample sizes

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(purrr)
  library(ggplot2)
  library(forcats)
  library(scales)
  library(gt)
  library(glue)
  library(broom)
  library(here)
})

# Null-coalescing helper to match the regression script
`%||%` <- function(a, b) if (is.null(a) || length(a) == 0) b else a

# Reuse the data-loading and feature-extraction logic from the regression script.
options(teacher_regression_skip_main = TRUE)
source(here("Analysis", "21_teacher_diversity_regression.R"))

save_dir <- here("outputs")
dir.create(save_dir, showWarnings = FALSE, recursive = TRUE)

build_formula <- function(model_df) {
  predictors <- c("teacher_non_white_share", "admin_non_white_share")
  optional <- intersect(c("sed_rate", "is_charter"), names(model_df))
  predictors <- c(predictors, optional)
  if ("grade_level" %in% names(model_df)) {
    predictors <- c(predictors, "grade_level")
  }
  stats::as.formula(paste("suspension_rate ~", paste(predictors, collapse = " + ")))
}

fit_group_model <- function(df, student_group) {
  model_info <- prepare_regression_frame(df, student_group)
  if (is.null(model_info)) {
    return(NULL)
  }

  model_df <- model_info$data
  formula <- build_formula(model_df)
  fit <- stats::lm(formula, data = model_df, weights = model_df$weights)

  tidy_res <- broom::tidy(fit, conf.int = TRUE)
  glance_res <- broom::glance(fit)

  list(
    student_group = student_group %||% "All Students",
    model_df = model_df,
    tidy = tidy_res,
    glance = glance_res,
    weighting = if ("weights" %in% names(model_df) && length(unique(model_df$weights)) > 1) {
      "Enrollment-weighted"
    } else {
      "Unweighted"
    },
    n_small = if ("enrollment" %in% names(model_df)) sum(model_df$enrollment < 50, na.rm = TRUE) else NA_integer_,
    diversity_meta = model_info$diversity_meta
  )
}

summarize_coefficients <- function(models) {
  map_dfr(models, function(m) {
    m$tidy |> 
      filter(term %in% c("teacher_non_white_share", "admin_non_white_share")) |> 
      mutate(
        student_group = m$student_group,
        weighting = m$weighting,
        r.squared = m$glance$r.squared,
        adj.r.squared = m$glance$adj.r.squared,
        n_obs = m$glance$nobs,
        n_small = m$n_small,
        term_label = recode(term,
          teacher_non_white_share = "Teacher non-white share",
          admin_non_white_share = "Administrator non-white share"
        ),
        # Translate to suspension percentage-point change for a +10pp diversity increase
        pp_change_10 = estimate * 10,
        pp_low_10 = conf.low * 10,
        pp_high_10 = conf.high * 10,
        p_label = scales::pvalue(p.value, accuracy = 0.0001, add_p = TRUE)
      )
  })
}

make_coefficient_plot <- function(coef_df) {
  plot_df <- coef_df |
    mutate(
      student_group = fct_relevel(student_group, ALLOWED_RACE_GROUPS, after = Inf),
      student_group = fct_rev(fct_inorder(student_group)),
      term_label = factor(term_label, levels = c("Teacher non-white share", "Administrator non-white share"))
    )

  ggplot(plot_df, aes(x = pp_change_10, y = student_group, color = term_label)) +
    geom_vline(xintercept = 0, linewidth = 0.6, linetype = "dashed", color = "grey40") +
    geom_pointrange(aes(xmin = pp_low_10, xmax = pp_high_10),
                    position = position_dodge(width = 0.5),
                    linewidth = 0.5) +
    scale_x_continuous(
      name = "Suspension rate change (percentage points) for +10pp diversity",
      labels = label_number(accuracy = 0.0005)
    ) +
    scale_color_brewer(palette = "Dark2", name = NULL) +
    labs(
      y = "Student group",
      title = "Teacher and administrator racial diversity vs. suspension rates",
      subtitle = "Points show coefficient estimates; bars show 95% CIs (enrollment-weighted regressions)",
      caption = "Associations only; do not interpret as causal effects."
    ) +
    theme_minimal(base_size = 12) +
    theme(
      legend.position = "top",
      panel.grid.major.y = element_blank()
    )
}

make_summary_table <- function(coef_df) {
  effect_tbl <- coef_df |>
    transmute(
      student_group,
      term_label,
      effect = glue("{scales::number(pp_change_10, accuracy = 0.0001, prefix = ifelse(pp_change_10 >= 0, \"+\", \"\"))} ({scales::number(pp_low_10, accuracy = 0.0001)}, {scales::number(pp_high_10, accuracy = 0.0001)})"),
      p_display = p_label
    ) |>
    pivot_wider(
      names_from = term_label,
      values_from = c(effect, p_display),
      names_glue = "{term_label} ({.value})"
    )

  meta_tbl <- coef_df |>
    distinct(student_group, r.squared, adj.r.squared, n_obs, n_small, weighting)

  wide_tbl <- meta_tbl |>
    left_join(effect_tbl, by = "student_group") |>
    arrange(match(student_group, ALLOWED_RACE_GROUPS))

  gt_tbl <- wide_tbl |>
    gt(rowname_col = "student_group") |
    tab_header(
      title = "Effect of educator racial diversity on suspension rates",
      subtitle = "Change in suspension percentage points for a +10pp increase in non-white share"
    ) |
    fmt_number(columns = c(r.squared, adj.r.squared), decimals = 3) |
    fmt_number(columns = c(n_obs, n_small), decimals = 0, use_seps = TRUE) |
    cols_label(
      student_group = "Student group",
      `Teacher non-white share (effect)` = "Teacher effect (95% CI)",
      `Administrator non-white share (effect)` = "Admin effect (95% CI)",
      `Teacher non-white share (p_display)` = "Teacher p-value",
      `Administrator non-white share (p_display)` = "Admin p-value",
      r.squared = "R²",
      adj.r.squared = "Adj. R²",
      n_obs = "N (schools)",
      n_small = "N < 50 students",
      weighting = "Weights"
    ) |
    tab_spanner(
      label = "Coefficient estimates",
      columns = c(`Teacher non-white share (effect)`, `Administrator non-white share (effect)`,
                  `Teacher non-white share (p_display)`, `Administrator non-white share (p_display)`)
    ) |
    cols_align(everything(), align = "center") |
    tab_source_note("Associations from weighted linear models; not causal estimates.")

  gt_tbl
}

run_visualizations <- function() {
  message("\n════════════════════════════════════════════════════════════════")
  message("📊 Building visuals for teacher/administrator diversity regressions")
  message("════════════════════════════════════════════════════════════════\n")

  data_result <- load_features()
  df <- data_result$data

  groups <- if ("student_group" %in% names(df)) {
    intersect(ALLOWED_RACE_GROUPS, unique(df$student_group))
  } else {
    "All Students"
  }

  models <- map(groups, ~fit_group_model(df, .x)) |> compact()
  if (!length(models)) {
    stop("No models could be fit; check input data and diversity columns.")
  }

  coef_df <- summarize_coefficients(models)

  coef_plot <- make_coefficient_plot(coef_df)
  png_path <- file.path(save_dir, "teacher_diversity_effects.png")
  svg_path <- file.path(save_dir, "teacher_diversity_effects.svg")
  ggsave(png_path, coef_plot, width = 9, height = 6, dpi = 320)
  ggsave(svg_path, coef_plot, width = 9, height = 6)
  message("Saved coefficient plot to: ", png_path)
  message("Saved coefficient plot to: ", svg_path)

  summary_table <- make_summary_table(coef_df)
  html_path <- file.path(save_dir, "teacher_diversity_summary.html")
  gt::gtsave(summary_table, html_path, inline_css = TRUE)
  message("Saved summary table to: ", html_path)

  invisible(list(
    models = models,
    coefficients = coef_df,
    plot = coef_plot,
    table = summary_table
  ))
}

if (identical(environment(), globalenv())) {
  run_visualizations()
}
