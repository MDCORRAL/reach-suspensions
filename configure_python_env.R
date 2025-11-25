# configure_python_env.R
#
# This script configures reticulate to use the project's .venv Python environment
# instead of its cached Python. Run this BEFORE loading any Python code.
#
# Usage:
#   source("configure_python_env.R")

# Detect project root (works both from RStudio and command line)
get_project_root <- function() {
  if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
    # If in RStudio, use project directory
    proj_dir <- rstudioapi::getActiveProject()
    if (!is.null(proj_dir)) {
      return(proj_dir)
    }
  }

  # Otherwise, use the directory containing this script
  if (sys.nframe() > 0) {
    script_path <- sys.frame(1)$ofile
    if (!is.null(script_path)) {
      return(dirname(script_path))
    }
  }

  # Fallback to current working directory
  getwd()
}

project_root <- get_project_root()
venv_python <- file.path(project_root, ".venv", "bin", "python")

# Check if .venv exists
if (!file.exists(venv_python)) {
  stop(
    "Python virtual environment not found at: ", venv_python, "\n",
    "Please run: bash scripts/utilities/setup_python_env.sh\n",
    "to create the virtual environment first."
  )
}

# Set RETICULATE_PYTHON environment variable
Sys.setenv(RETICULATE_PYTHON = venv_python)

# Load reticulate
if (!requireNamespace("reticulate", quietly = TRUE)) {
  stop("Please install reticulate: install.packages('reticulate')")
}

library(reticulate)

# Show Python configuration
cat("\n=== Python Configuration ===\n")
cat("Project root:", project_root, "\n")
cat("Python path:", venv_python, "\n\n")

# Display detailed Python config
py_config()

# Verify key packages are available
cat("\n=== Verifying Required Packages ===\n")

check_package <- function(pkg_name) {
  tryCatch({
    py_module_available(pkg_name)
    cat("✓", pkg_name, "is available\n")
    TRUE
  }, error = function(e) {
    cat("✗", pkg_name, "NOT found\n")
    FALSE
  })
}

required_packages <- c("matplotlib", "pandas", "pyarrow", "numpy", "adjustText")
all_available <- all(sapply(required_packages, check_package))

if (all_available) {
  cat("\n✅ All required packages are available!\n")
  cat("You can now run Python scripts from R.\n\n")
  cat("Example:\n")
  cat('  reticulate::py_run_file("graph_scripts/20_suspension_reason_trends_by_level_and_locale.py")\n')
} else {
  cat("\n❌ Some packages are missing.\n")
  cat("Please reinstall the virtual environment:\n")
  cat("  bash scripts/utilities/setup_python_env.sh\n")
}
