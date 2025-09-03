# analysis/05c_rates_by_locale_facet_race_chunked.R
# Corrected and improved script to analyze suspension rates by locale, faceted by race.

# --- 1) Setup -----------------------------------------------------------------
suppressPackageStartupMessages({
  library(here); library(arrow); library(dplyr); library(tidyr)
  library(stringr); library(ggplot2); library(scales); library(ggrepel)
})

# --- 2) Configuration ---------------------------------------------------------
MAX_FACETS_PER_IMAGE  <- 2      # Your requirement
DROP_ALL_STUDENTS     <- TRUE   # Don't facet "All Students" here
IMG_WIDTH             <- 12.5
IMG_HEIGHT            <- 6.0    # Adjusted for better aspect ratio
IMG_DPI               <- 300

set.seed(42)

# --- 3) Load & Prepare Data ----------------------------------------------------
message("Loading and preparing data...")
v5_path <- here::here("data-stage","susp_v5.parquet")
if (!file.exists(v5_path)) stop("Data file not found: ", v5_path)
v5 <- arrow::read_parquet(v5_path)

# Check for required columns
need <- c("reporting_category", "academic_year", "locale_simple",
          "total_suspensions", "cumulative_enrollment")
miss <- setdiff(need, names(v5))
if (length(miss)) stop("Missing required columns: ", paste(miss, collapse=", "))

# Get academic year order
year_levels <- v5 %>%
  filter(reporting_category == "TA") %>%
  distinct(academic_year) %>% arrange(academic_year) %>% pull(academic_year)

# Function to create clean race labels
race_label <- function(code) dplyr::recode(
  code,
  RB = "Black/African American", RW = "White",
  RH = "Hispanic/Latino", RL = "Hispanic/Latino",
  RI = "American Indian/Alaska Native", RA = "Asian",
  RF = "Filipino", RP = "Pacific Islander",
  RT = "Two or More Races", TA = "All Students",
  .default = NA_character_
)

# Aggregate data to get pooled rates
df_all <- v5 %>%
  mutate(race = race_label(reporting_category)) %>%
  filter(!is.na(race)) %>%
  group_by(academic_year, locale_simple, race) %>%
  summarise(
    susp = sum(total_suspensions, na.rm = TRUE),
    enroll = sum(cumulative_enrollment, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    rate      = if_else(enroll > 0, susp/enroll, NA_real_),
    year_fct  = factor(academic_year, levels = year_levels),
    label_txt = percent(rate, accuracy = 0.1)
  )

if (DROP_ALL_STUDENTS) df_all <- df_all %>% filter(race != "All Students")
if (!nrow(df_all)) stop("After filtering, there is no data left to plot.")

# --- 4) Chunk Races for Plotting ----------------------------------------------
# Order races by average suspension rate to group them logically
race_order <- df_all %>%
  group_by(race) %>%
  summarise(avg_rate = mean(rate, na.rm = TRUE), .groups = "drop") %>%
  arrange(desc(avg_rate)) %>%
  pull(race)

# Split the ordered races into a list of chunks
chunk_id <- ceiling(seq_along(race_order) / MAX_FACETS_PER_IMAGE)
race_chunks <- split(race_order, chunk_id)
message("Data will be split into ", length(race_chunks), " plot files.")

# --- 5) Plotting Function -----------------------------------------------------
# Reusable function to create a plot for a given set of races
plot_race_chunk <- function(races, plot_number) {
  
  dat <- df_all %>%
    filter(race %in% races) %>%
    mutate(
      race = factor(race, levels = races),
      locale_simple = factor(locale_simple, levels = c("City","Suburban","Town","Rural"))
    )
  
  plot_title <- paste0("Suspension Rates by Locale (Set ", plot_number, " of ", length(race_chunks), ")")
  
  ggplot(dat, aes(x = year_fct, y = rate, color = locale_simple, group = locale_simple)) +
    geom_line(linewidth = 1.05) +
    geom_point(size = 1.8) +
    ggrepel::geom_text_repel(
      aes(label = label_txt),
      size = 2.8,
      fontface = 'bold',
      color = "black",
      bg.color = "white",
      bg.r = 0.15,
      segment.color = 'grey50',
      box.padding = 0.4,
      point.padding = 0.2,
      min.segment.length = 0,
      show.legend = FALSE
    ) +
    scale_color_manual(
      name = "School Locale",
      values = c("City" = "#0072B2", "Suburban"= "#009E73", "Town" = "#E69F00", "Rural" = "#D55E00")
    ) +
    scale_y_continuous(labels = percent_format(accuracy = 0.1), limits = c(0, NA),
                       expand = expansion(mult = c(0.05, 0.15))) +
    scale_x_discrete(expand = expansion(mult = c(0.05, 0.15))) +
    labs(
      title = plot_title,
      subtitle = "Suspension rate per 100 students, faceted by race/ethnicity",
      x = NULL, y = "Suspension Rate (%)"
    ) +
    facet_wrap(~ race, ncol = MAX_FACETS_PER_IMAGE, scales = "free_y") +
    theme_minimal(base_size = 12) +
    theme(
      axis.text.x = element_text(angle = 35, hjust = 1),
      legend.position = "bottom",
      plot.title = element_text(face = "bold", size = 16),
      strip.text = element_text(face = "bold", size = 12),
      plot.margin = margin(12, 12, 12, 12)
    )
}

# --- 6) Loop to Generate and Save All Plots -----------------------------------
outdir <- here::here("outputs")
dir.create(outdir, showWarnings = FALSE)

for (i in seq_along(race_chunks)) {
  races_in_chunk <- race_chunks[[i]]
  
  message(sprintf("Generating plot %d for: %s", i, paste(races_in_chunk, collapse = " & ")))
  
  # Generate the plot
  p <- plot_race_chunk(races_in_chunk, i)
  print(p)
  
  # Save the plot with a unique filename
  file_name <- paste0("rates_by_locale_facet_race_set_", i, ".png")
  ggsave(here::here(outdir, file_name), p,
         width = ifelse(length(races_in_chunk) > 1, IMG_WIDTH, 7), # Use a narrower width for single plots
         height = IMG_HEIGHT, dpi = IMG_DPI, bg = "white")
}

message("\n✓ Analysis complete! Check the 'outputs' folder for the separated plot files.")