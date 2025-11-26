# Configure Python environment BEFORE renv activates
# This ensures reticulate uses the project's .venv instead of its cached version
if (file.exists(".venv/bin/python")) {
  Sys.setenv(RETICULATE_PYTHON = file.path(getwd(), ".venv/bin/python"))
  if (interactive()) {
    message("Using Python from project .venv/")
  }
}

source("renv/activate.R")
