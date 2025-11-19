# Convert Summary Markdown Files to Word Documents
# This script uses rmarkdown's built-in pandoc to convert summaries

library(rmarkdown)

# Get summaries directory
summaries_dir <- "outputs/summaries"

# Find all markdown summary files (exclude README and TEMPLATE)
md_files <- list.files(
  summaries_dir,
  pattern = "^\\d+_.*_SUMMARY\\.md$",
  full.names = TRUE
)

message("Found ", length(md_files), " summary files to convert:")
print(basename(md_files))

# Convert each file
for (md_file in md_files) {
  message("\n=== Converting: ", basename(md_file), " ===")

  # Output filename
  docx_file <- sub("\\.md$", ".docx", md_file)

  tryCatch({
    # Convert to Word using rmarkdown's bundled pandoc
    rmarkdown::render(
      input = md_file,
      output_format = "word_document",
      output_file = basename(docx_file),
      output_dir = summaries_dir,
      quiet = FALSE
    )
    message("✓ Created: ", basename(docx_file))
  }, error = function(e) {
    message("✗ Error converting ", basename(md_file), ": ", e$message)
  })
}

message("\n=== Conversion Complete ===")
message("Check outputs/summaries/ for .docx files")
