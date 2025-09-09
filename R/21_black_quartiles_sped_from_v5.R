#####21_black_quartiles_sped_from_v5.R

suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
  library(forcats)
  library(janitor)
  library(arrow)
  library(here)
  library(ggplot2)
  library(openxlsx)
  library(scales)
})
source(here::here("R", "utils_keys_filters.R"))

if (!exists("v6_features")) {
  candidate <- here("data-stage", "susp_v6_features.parquet")
  if (file.exists(candidate)) {
    message(">>> Loading v6_features from: ", candidate)
    v6_features <- arrow::read_parquet(candidate)
  } else {
    stop("susp_v6_features.parquet not found in data-stage/")
  }
}

# Add validation checks immediately after loading data
if (!("sped_rate" %in% names(v6_features))) {
  stop("sped_rate column not found in v6_features")
}
stopifnot("sped_den" %in% names(v6_features))
stopifnot(all(c("is_traditional","black_q") %in% names(v6_features)))


# Check for data completeness
message("SPED rate coverage: ", 
        sum(!is.na(v6_features$sped_rate[v6_features$is_traditional])), 
        " of ", sum(v6_features$is_traditional), " traditional schools")

# Validate quartile distribution
message("Original quartile distribution:")
print(table(v6_features$black_q[v6_features$is_traditional], useNA = "always"))

# Clean and filter data
v6_clean <- v6_features %>%
  mutate(
    black_q = as.character(black_q),
    black_q = str_replace(black_q, "^QQ", "Q"),
    black_q = str_replace(black_q, "\\s*\\(.*\\)$", ""),
    black_q = ifelse(str_detect(black_q, "Unknown|NA"), NA_character_, black_q)
  ) %>%
  filter(is_traditional, 
         str_detect(black_q, "^Q[1-4]$"),  # Only valid quartiles
         !is.na(sped_rate),
         !is.na(sped_den),
         sped_den > 0) %>%  # Exclude schools with no SPED students
  mutate(black_q = fct_relevel(factor(black_q), "Q1","Q2","Q3","Q4"))

# Report sample size
total_traditional <- sum(v6_features$is_traditional, na.rm = TRUE)
final_sample <- nrow(v6_clean)
message("Analysis sample: ", final_sample, " of ", total_traditional, 
        " traditional schools (", round(100*final_sample/total_traditional, 1), "%)")

# Check enrollment distribution within quartiles
enrollment_summary <- v6_clean %>%
  group_by(black_q) %>%
  summarise(
    min_sped_den = min(sped_den, na.rm = TRUE),
    q25_sped_den = quantile(sped_den, 0.25, na.rm = TRUE),
    median_sped_den = median(sped_den, na.rm = TRUE),
    q75_sped_den = quantile(sped_den, 0.75, na.rm = TRUE),
    max_sped_den = max(sped_den, na.rm = TRUE),
    .groups = "drop"
  )

message("SPED enrollment distribution by quartile:")
print(enrollment_summary)

# Create both weighted and unweighted analyses
# --- Weighted: pooled binomial with Wilson CI
plot_df_weighted <- v6_clean %>%
  group_by(black_q) %>%
  summarise(
    n_schools = n(),
    events    = sum(sped_rate * sped_den, na.rm = TRUE),
    denom     = sum(sped_den,             na.rm = TRUE),
    weighted_rate = ifelse(denom > 0, events / denom, NA_real_),
    unweighted_rate = mean(sped_rate, na.rm = TRUE),
    median_rate     = median(sped_rate, na.rm = TRUE),
    sd_rate         = sd(sped_rate, na.rm = TRUE),
    median_enrollment = median(sped_den, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    z = 1.96, p_hat = weighted_rate, n_eff = denom,
    ci_low  = pmax(0, (p_hat + z^2/(2*n_eff) - z*sqrt(p_hat*(1-p_hat)/n_eff + z^2/(4*n_eff^2))) / (1 + z^2/n_eff)),
    ci_high = pmin(1, (p_hat + z^2/(2*n_eff) + z*sqrt(p_hat*(1-p_hat)/n_eff + z^2/(4*n_eff^2))) / (1 + z^2/n_eff))
  )


# Create unweighted plot with proper confidence intervals
# --- Unweighted: t-based CI on mean of school rates
plot_df_unweighted <- v6_clean %>%
  group_by(black_q) %>%
  summarise(
    n_schools = n(),
    mean_rate = mean(sped_rate, na.rm = TRUE),
    sd_rate   = sd(sped_rate,   na.rm = TRUE),
    se_rate   = sd_rate / sqrt(n_schools),
    tcrit     = qt(0.975, df = pmax(n_schools - 1, 1)),
    ci_low    = pmax(0, mean_rate - tcrit * se_rate),
    ci_high   = pmin(1, mean_rate + tcrit * se_rate),
    median_rate = median(sped_rate, na.rm = TRUE),
    .groups = "drop"
  )


# Print comparison of weighted vs unweighted rates
message("Weighted vs Unweighted Rate Comparison:")
comparison <- plot_df_weighted %>%
  select(black_q, weighted_rate, unweighted_rate) %>%
  mutate(
    difference = weighted_rate - unweighted_rate,
    pct_difference = 100 * difference / unweighted_rate
  )
print(comparison)

# Create plot using unweighted means (specify which approach you're using)
p_unweighted <- ggplot(plot_df_unweighted, aes(x = black_q, y = mean_rate)) +
  geom_point(size = 3) +
  geom_errorbar(aes(ymin = ci_low, ymax = ci_high), width = 0.15) +
  scale_y_continuous(labels = percent_format(accuracy = 0.1)) +
  labs(
    x = "Quartile of Black Student Enrollment (Q1 = lowest share)",
    y = "Special Education Suspension Rate",
    title = "SPED Suspension Rate by Black Enrollment Quartile (Unweighted)",
    caption = paste0("Traditional schools only; unweighted school-level means with 95% CI.\n",
                     "Sample sizes: Q1=", plot_df_unweighted$n_schools[1], 
                     ", Q2=", plot_df_unweighted$n_schools[2], 
                     ", Q3=", plot_df_unweighted$n_schools[3], 
                     ", Q4=", plot_df_unweighted$n_schools[4])
  ) +
  theme_minimal(base_size = 12)

# Create weighted plot for comparison
p_weighted <- ggplot(plot_df_weighted, aes(x = black_q, y = weighted_rate)) +
  geom_point(size = 3) +
  geom_errorbar(aes(ymin = ci_low, ymax = ci_high), width = 0.15) +  # ADD THIS LINE
  scale_y_continuous(labels = percent_format(accuracy = 0.1)) +
  labs(
    x = "Quartile of Black Student Enrollment (Q1 = lowest share)",
    y = "Special Education Suspension Rate",
    title = "SPED Suspension Rate by Black Enrollment Quartile (Enrollment-Weighted)",
    caption = paste0("Traditional schools only; enrollment-weighted rates.\n",
                     "Sample sizes: Q1=", plot_df_weighted$n_schools[1], 
                     ", Q2=", plot_df_weighted$n_schools[2], 
                     ", Q3=", plot_df_weighted$n_schools[3], 
                     ", Q4=", plot_df_weighted$n_schools[4])
  ) +
  theme_minimal(base_size = 12)

# Add after your main analysis to verify temporal stability
yearly_check <- v6_clean %>%
  group_by(year, black_q) %>%
  summarise(
    n_schools = n(),
    mean_rate = mean(sped_rate, na.rm = TRUE),
    .groups = "drop"
  )

print("Rates by year and quartile (checking temporal stability):")
print(yearly_check)

# Save outputs
dir.create(here("outputs"), showWarnings = FALSE)

ggsave(here("outputs","21_sped_rate_by_black_quartile_unweighted.png"), 
       p_unweighted, width = 8, height = 5.2, dpi = 300)

ggsave(here("outputs","21_sped_rate_by_black_quartile_weighted.png"), 
       p_weighted, width = 8, height = 5.2, dpi = 300)

# Excel output with both analyses
wb <- createWorkbook()

# Unweighted summary
addWorksheet(wb, "unweighted_summary")
writeData(
  wb, "unweighted_summary",
  plot_df_unweighted %>% 
    mutate(across(c(mean_rate, ci_low, ci_high, median_rate),
                  ~ percent(.x, accuracy = 0.001)))
)

# Weighted summary
addWorksheet(wb, "weighted_summary")
writeData(
  wb, "weighted_summary",
  plot_df_weighted %>% 
    mutate(across(c(weighted_rate, unweighted_rate, median_rate),
                  ~ percent(.x, accuracy = 0.001)))
)

# School-level detail
addWorksheet(wb, "school_level")
writeData(
  wb, "school_level",
  v6_clean %>% 
    transmute(
      school_code,
      year,
      black_share   = percent(black_share, accuracy = 0.1),
      black_quartile = as.character(black_q),
      sped_rate     = percent(sped_rate, accuracy = 0.1),
      sped_enrollment = sped_den,
      school_type
    )
)

# Enrollment distribution summary
addWorksheet(wb, "enrollment_distribution")
writeData(wb, "enrollment_distribution", enrollment_summary)

# Investigate excluded schools
excluded_schools <- v6_features %>%
  filter(is_traditional) %>%
  mutate(
    included = school_code %in% v6_clean$school_code,
    quartile_status = case_when(
      str_detect(black_q, "Unknown") ~ "Unknown quartile",
      is.na(sped_rate) ~ "Missing SPED rate", 
      is.na(sped_den) | sped_den == 0 ~ "No SPED enrollment",
      TRUE ~ "Included"
    )
  )

table(excluded_schools$quartile_status)
saveWorkbook(wb, here("outputs","21_sped_rate_by_black_quartile.xlsx"), overwrite = TRUE)

message("Analysis complete. Check outputs folder for plots and Excel file.")
message("Consider which approach (weighted vs unweighted) is most appropriate for your research question.")