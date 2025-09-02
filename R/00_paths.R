# 00_paths.R  — minimal, portable paths with fallbacks (no new deps)

# Helper: build repo-relative paths
dp_raw   <- file.path("data-raw")
dp_stage <- file.path("data-stage")
dp_out   <- file.path("outputs")

# Ensure key dirs exist when scripts write to them
dir.create(dp_stage, showWarnings = FALSE, recursive = TRUE)
dir.create(dp_out,   showWarnings = FALSE, recursive = TRUE)

# Candidates for the main raw Excel file (first existing wins)
raw_candidates <- c(
  # 1) Optional override via environment variable (set in Renv or shell)
  Sys.getenv("RAW_PATH", unset = ""),
  # 2) Repo-relative default (portable across machines)
  file.path(dp_raw, "copy_CDE_suspensions_1718-2324_sc_race.xlsx"),
  # 3) Your original absolute path (kept as a fallback for your current box)
  "/Users/michaelcorral/Library/CloudStorage/GoogleDrive-mdcorral@g.ucla.edu/.shortcut-targets-by-id/1qNAOKIg0UjuT3XWFlk4dkDLN6UPWJVGx/Center for the Transformation of Schools/Research/CA Race Education And Community Healing (REACH)/2. REACH Network (INTERNAL)/15. REACH Baseline Report_Summer 2025/6. R Data Analysis Project Folders/1. 250828_R Folder/copy_CDE_suspensions_1718-2324_sc_race.xlsx"
)

# Choose the first candidate that exists
raw_candidates <- raw_candidates[nzchar(raw_candidates)]
raw_path_idx <- which(file.exists(raw_candidates))[1]

if (length(raw_path_idx) == 0) {
  message("!! Could not find the raw Excel file in any of these locations:")
  for (p in raw_candidates) message("   - ", p)
  stop("Raw file not found. Set RAW_PATH env var or place the file in data-raw/.")
}

raw_path <- normalizePath(raw_candidates[raw_path_idx], winslash = "/", mustWork = TRUE)
message(">>> Using raw_path: ", raw_path)

# (Optional) quick sanity: fail early if directory assumptions drift
stopifnot(dir.exists(dp_raw) || file.exists(raw_path))
