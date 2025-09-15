# run_helper.R — shared helper for running pipeline scripts
run <- function(f) {
  message("\n=== Running: ", f, " ===")
  tryCatch(
    {
      source(f, chdir = FALSE, local = new.env(parent = globalenv()))
      message("✓ Finished: ", f)
    },
    error = function(e) {
      message("✗ Error in ", f, "\n", conditionMessage(e))
      stop(e)
    }
  )
}
