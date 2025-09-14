# R/02b_drop_charter_all.R

# Quiet core libs
suppressPackageStartupMessages({
  library(here)     # project-root paths
  library(arrow)    # parquet I/O
  library(dplyr)    # data wrangling
})

source("R/00_paths.R")
message(">>> Running from project root: ", here::here())

# -------------------------
# 1) Load v1
# -------------------------
v1 <- arrow::read_parquet(here::here("data-stage", "susp_v1.parquet"))

# -------------------------
# 2) Drop "All" rows
# -------------------------
v1_noall <- v1 %>%
  filter(is.na(charter_yn) | charter_yn != "All")

# sanity check
stopifnot(!any(v1_noall$charter_yn %in% "All", na.rm = TRUE))

# -------------------------
# 3) Write out
# -------------------------
arrow::write_parquet(v1_noall, here::here("data-stage", "susp_v1_noall.parquet"))

message(">>> 02b_drop_charter_all: wrote susp_v1_noall.parquet")

invisible(TRUE)
