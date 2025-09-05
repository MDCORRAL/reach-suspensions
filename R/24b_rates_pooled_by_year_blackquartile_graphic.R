# expects `sum_by` built with pooled counts and labels as in v24 script,
# but now filtered to the two series we want (Total + SWD)

library(ggplot2)
library(scales)
library(forcats)
library(dplyr)
library(ggrepel)

# 1) Keep just Total and Students with Disabilities
sum_2 <- sum_by %>%
  dplyr::filter(subgroup %in% c("Total", "Students with Disabilities")) %>%
  dplyr::mutate(
    year    = fct_inorder(year),
    black_q = fct_relevel(black_q, "Q1","Q2","Q3","Q4"),
    series  = dplyr::recode(subgroup,
                            "Total" = "Total (All Students)",
                            "Students with Disabilities" = "Students with Disabilities")
  )

# 2) Build paired colors: light = Total, dark = SWD
# base quartile colors (like your example)
base_cols <- c("Q1"="#F8766D","Q2"="#7CAE00","Q3"="#00BFC4","Q4"="#C77CFF")

# helper to lighten/darken without extra deps
lighten <- function(col, f=0.5) grDevices::rgb(
  pmin(1, col2rgb(col)[1,]/255 + (1 - col2rgb(col)[1,]/255)*f),
  pmin(1, col2rgb(col)[2,]/255 + (1 - col2rgb(col)[2,]/255)*f),
  pmin(1, col2rgb(col)[3,]/255 + (1 - col2rgb(col)[3,]/255)*f)
)

darken <- function(col, f=0.3) grDevices::rgb(
  pmax(0, col2rgb(col)[1,]/255 * (1 - f)),
  pmax(0, col2rgb(col)[2,]/255 * (1 - f)),
  pmax(0, col2rgb(col)[3,]/255 * (1 - f))
)

# map to 8-color palette by (quartile, series)
keys <- with(sum_2, interaction(black_q, series, drop=TRUE, lex.order = TRUE))
levels_keys <- levels(keys)

pal <- setNames(
  vapply(levels_keys, function(k){
    parts <- strsplit(k, "\\.")[[1]]      # e.g., "Q1.Total (All Students)"
    q  <- parts[1]
    s  <- parts[2]
    if (grepl("^Total", s))  lighten(base_cols[q], 0.55) else darken(base_cols[q], 0.25)
  }, character(1)),
  levels_keys
)

# 3) Labels only at the last year, but points for all years
sum_2 <- sum_2 %>%
  dplyr::mutate(label_txt = scales::percent(pooled_rate, accuracy = 0.1))

# 4) Plot
# make sure ggrepel is installed
# install.packages("ggrepel")

library(ggrepel)

p <- ggplot(sum_2,
            aes(x = year, y = pooled_rate,
                group = interaction(black_q, series, drop=TRUE, lex.order = TRUE),
                color = interaction(black_q, series, drop=TRUE, lex.order = TRUE))) +
  geom_line(linewidth = 1.0) +
  geom_point(shape = 21, size = 2.8, stroke = 0.9, fill = "white") +
  # repel version of labels
  geom_text_repel(aes(label = label_txt),
                  size = 3,
                  max.overlaps = 20,   # tweak: higher = allow more labels
                  box.padding = 0.3,   # space around label
                  point.padding = 0.2, # space around point
                  segment.color = "grey70",
                  segment.size = 0.3,
                  show.legend = FALSE) +
  scale_color_manual(values = pal,
                     breaks = names(pal),
                     labels = sub("\\.", " — ", names(pal))) +
  scale_y_continuous(labels = scales::percent_format(0.1),
                     expand = expansion(mult = c(0.05, 0.15))) +
  coord_cartesian(clip = "off") +
  labs(
    title = "Suspension Rates by Year • Black-Enrollment Quartile\nTotal (lighter) vs Students with Disabilities (darker)",
    subtitle = "Pooled rates (Σ suspensions ÷ Σ enrollment) • Traditional schools only",
    x = NULL, y = "Rate", color = "Quartile — Series"
  ) +
  theme_minimal(base_size = 13) +
  theme(
    panel.background = element_rect(fill = "white", color = NA),
    plot.background  = element_rect(fill = "white", color = NA),
    panel.grid.major = element_line(linewidth = 0.3, color = "#D8D8D8"),
    panel.grid.minor = element_blank(),
    axis.text.x      = element_text(angle = 45, hjust = 1),
    legend.position  = "right",
    plot.title       = element_text(face = "bold", size = 16),
    plot.margin      = margin(10, 30, 10, 10)
  )

ggsave(here::here("outputs","rates_pooled_all_in_one.png"),
       p, width = 14, height = 7.5, dpi = 300)

# 5) Save
ggsave(here::here("outputs","rates_pooled_all_in_one.png"),
       p, width = 14, height = 7.5, dpi = 300)

