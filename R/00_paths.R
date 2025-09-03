# 00_paths.R  — minimal, portable paths with fallbacks (no new deps)
# Safe, cross-machine path handling for this project.

# helper: null-coalesce
`%||%` <- function(a, b) if (is.null(a) || identical(a, "") ) b else a

# safe normalizePath wrapper: returns NA_character_ for empty/NA inputs
safe_norm <- function(x, mustWork = TRUE) {
  if (is.null(x) || identical(x, "") || is.na(x)) return(NA_character_)
  normalizePath(x, winslash = "/", mustWork = mustWork)
}

# --- Project / data dirs (prefer env vars; fall back to repo-relative) -----
proj_env <- Sys.getenv("REACH_PROJECT_ROOT", unset = "")
PROJ_ROOT <- if (nzchar(proj_env)) {
  safe_norm(proj_env, mustWork = TRUE)
} else {
  # fallback to current working directory (useful when opening project from repo root)
  safe_norm(getwd(), mustWork = FALSE)
}

data_env <- Sys.getenv("REACH_DATA_DIR", unset = "")
dp_stage <- if (nzchar(data_env)) {
  safe_norm(data_env, mustWork = TRUE)
} else {
  file.path(PROJ_ROOT %||% getwd(), "data-stage")
}

# repo-relative defaults for raw and outputs (use PROJ_ROOT as base)
dp_raw   <- file.path(PROJ_ROOT %||% getwd(), "data-raw")
dp_out   <- file.path(PROJ_ROOT %||% getwd(), "outputs")

# Ensure key dirs exist when scripts write to them
dir.create(dp_stage, showWarnings = FALSE, recursive = TRUE)
dir.create(dp_out,   showWarnings = FALSE, recursive = TRUE)
dir.create(dp_raw,   showWarnings = FALSE, recursive = TRUE)

# --- Candidates for the main raw Excel file (first existing wins) ----------
raw_candidates <- c(
  # 1) Optional override via environment variable RAW_PATH (set in ~/.Renviron or shell)
  Sys.getenv("RAW_PATH", unset = ""),
  # 2) Repo-relative default (portable across machines)
  file.path(dp_raw, "copy_CDE_suspensions_1718-2324_sc_race.xlsx"),
  # 3) Your original absolute path (kept as a fallback for this machine)
  "/Users/michaelcorral/Library/CloudStorage/GoogleDrive-mdcorral@g.ucla.edu/.shortcut-targets-by-id/1qNAOKIg0UjuT3XWFlk4dkDLN6UPWJVGx/Center for the Transformation of Schools/Research/CA Race Education And Community Healing (REACH)/2. REACH Network (INTERNAL)/15. REACH Baseline Report_Summer 2025/6. R Data Analysis Project Folders/1. 250828_R Folder/copy_CDE_suspensions_1718-2324_sc_race.xlsx"
)

# drop blank strings so file.exists() won't get empty values
raw_candidates <- raw_candidates[nzchar(raw_candidates)]

# choose the first candidate that exists
raw_path_idx <- which(file.exists(raw_candidates))[1]

if (is.na(raw_path_idx) || length(raw_path_idx) == 0) {
  message("!! Could not find the raw Excel file in any of these locations:")
  for (p in raw_candidates) message("   - ", p)
  stop("Raw file not found. Set RAW_PATH env var or place the file in data-raw/.")
}

raw_path <- safe_norm(raw_candidates[raw_path_idx], mustWork = TRUE)
message(">>> Using raw_path: ", raw_path)

# Quick sanity: ensure at least one of (data-raw dir OR raw_path) exists
# (some pipelines expect the dp_raw dir; other scripts read the raw_path file)
if (!dir.exists(dp_raw) && !file.exists(raw_path)) {
  stop("Neither data-raw directory nor raw file exists. Check RAW_PATH or create data-raw/.")
}

# Print what we will use (helpful when switching machines)
message(">>> PROJ_ROOT: ", PROJ_ROOT)
message(">>> DATA_DIR (dp_stage): ", dp_stage)
message(">>> RAW_DIR (dp_raw): ", dp_raw)
message(">>> OUTPUTS_DIR: ", dp_out)
