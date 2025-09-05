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

# Ensure quartile ordering (works whether it's "Q1".."Q4" or 1..4)
v6 <- v6_features %>%
  mutate(
    black_q = as.character(black_q),
    black_q = ifelse(grepl("^Q\\d$", black_q), black_q, paste0("Q", black_q)),
    black_q = fct_relevel(factor(black_q), "Q1","Q2","Q3","Q4")
  )

# Filter: traditional schools, valid quartile & rate
plot_df <- v6 %>%
  filter(is_traditional, !is.na(black_q), !is.na(sped_rate)) %>%
  group_by(black_q) %>%
  summarise(
    n_schools   = n(),
    mean_rate   = mean(sped_rate, na.rm = TRUE),
    sd_rate     = sd(sped_rate, na.rm = TRUE),
    se_rate     = sd_rate / sqrt(n_schools),
    ci_low      = mean_rate - 1.96 * se_rate,
    ci_high     = mean_rate + 1.96 * se_rate,
    median_rate = median(sped_rate, na.rm = TRUE),
    .groups = "drop"
  )

# Plot
p <- ggplot(plot_df, aes(x = black_q, y = mean_rate)) +
  geom_point(size = 3) +
  geom_errorbar(aes(ymin = ci_low, ymax = ci_high), width = 0.15) +
  scale_y_continuous(labels = percent_format(accuracy = 0.1)) +
  labs(
    x = "Quartile of Black Student Enrollment (Q1 = lowest share)",
    y = "Special Education Suspension Rate",
    title = "SPED Suspension Rate by Black Enrollment Quartile",
    caption = "Traditional schools only; school-level means with 95% CI."
  ) +
  theme_minimal(base_size = 12)

dir.create(here("outputs"), showWarnings = FALSE)
ggsave(here("outputs","sped_rate_by_black_quartile.png"), p, width = 8, height = 5.2, dpi = 300)

# Excel (summary + school-level detail)
wb <- createWorkbook()
addWorksheet(wb, "by_quartile")
writeData(
  wb, "by_quartile",
  plot_df %>% mutate(across(c(mean_rate, ci_low, ci_high, median_rate),
                            ~ percent(.x, accuracy = 0.1)))
)

addWorksheet(wb, "school_level")
writeData(
  wb, "school_level",
  v6 %>% 
    filter(is_traditional, !is.na(black_q), !is.na(sped_rate)) %>%
    transmute(
      school_code,
      year,
      black_share   = percent(black_share, accuracy = 0.1),
      black_quartile = as.character(black_q),
      sped_rate     = percent(sped_rate, accuracy = 0.1),
      school_type
    )
)

saveWorkbook(wb, here("outputs","sped_rate_by_black_quartile.xlsx"), overwrite = TRUE)
