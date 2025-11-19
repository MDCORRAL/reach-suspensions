#!/usr/bin/env bash
# scripts/utilities/convert_summary_to_word.sh
# Convert markdown summary files to Word (.docx) format with proper formatting
# Preserves significance markers (*, **, ***) and ensures correct formatting

set -e  # Exit on error

# ============================================================================
# CONFIGURATION
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SUMMARIES_DIR="$REPO_ROOT/outputs/summaries"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

print_header() {
  echo -e "${BLUE}========================================${NC}"
  echo -e "${BLUE}$1${NC}"
  echo -e "${BLUE}========================================${NC}"
}

print_success() {
  echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
  echo -e "${RED}✗ $1${NC}"
}

print_warning() {
  echo -e "${YELLOW}⚠ $1${NC}"
}

print_info() {
  echo -e "${BLUE}ℹ $1${NC}"
}

# ============================================================================
# CHECK DEPENDENCIES
# ============================================================================

check_dependencies() {
  print_header "Checking Dependencies"

  local missing_deps=0

  # Check for pandoc
  if ! command -v pandoc &> /dev/null; then
    print_error "pandoc not found. Please install pandoc:"
    echo "  macOS: brew install pandoc"
    echo "  Ubuntu/Debian: sudo apt-get install pandoc"
    echo "  Or visit: https://pandoc.org/installing.html"
    missing_deps=1
  else
    local pandoc_version=$(pandoc --version | head -n1)
    print_success "pandoc found: $pandoc_version"
  fi

  if [ $missing_deps -eq 1 ]; then
    exit 1
  fi

  echo ""
}

# ============================================================================
# PREPROCESSING FUNCTIONS
# ============================================================================

preprocess_markdown() {
  # Preprocess markdown to preserve significance markers and improve conversion
  # Input: markdown file path
  # Output: writes to temporary preprocessed file

  local input_file="$1"
  local temp_file="$2"

  print_info "Preprocessing markdown to preserve formatting..."

  # Use sed to:
  # 1. Ensure significance markers are properly escaped
  # 2. Add explicit labels for significance markers in tables
  # 3. Preserve backslashes before asterisks

  # Copy input to temp, performing transformations
  cat "$input_file" | \
    # No changes needed - pandoc should handle \*\*\* properly
    cat > "$temp_file"

  print_success "Preprocessing complete"
}

# ============================================================================
# CONVERSION FUNCTION
# ============================================================================

convert_to_word() {
  local md_file="$1"
  local docx_file="${md_file%.md}.docx"
  local temp_file="${md_file%.md}_temp.md"

  print_header "Converting: $(basename "$md_file")"

  # Check if source file exists
  if [ ! -f "$md_file" ]; then
    print_error "Source file not found: $md_file"
    return 1
  fi

  # Preprocess markdown
  preprocess_markdown "$md_file" "$temp_file"

  # Convert with pandoc
  print_info "Converting to Word format..."

  # Pandoc command with options to preserve formatting
  pandoc "$temp_file" \
    -o "$docx_file" \
    --from markdown+escaped_line_breaks \
    --to docx \
    --toc \
    --toc-depth=3 \
    --highlight-style=tango \
    --metadata title="$(basename "${md_file%.md}")" \
    --metadata date="$(date +%Y-%m-%d)" \
    2>&1 | while read line; do
      # Filter out pandoc warnings if desired
      if [[ ! "$line" =~ "does not contain" ]]; then
        echo "$line"
      fi
    done

  # Clean up temp file
  rm -f "$temp_file"

  if [ -f "$docx_file" ]; then
    local size=$(du -h "$docx_file" | cut -f1)
    print_success "Created: $(basename "$docx_file") ($size)"

    # Verify significance markers
    print_info "Verifying significance markers in source..."
    local sig_count=$(grep -o '\\\*\\\*\\\*' "$md_file" | wc -l)
    if [ "$sig_count" -gt 0 ]; then
      print_warning "Found $sig_count significance markers (***) in source"
      print_warning "Please manually verify these appear correctly in Word"
      print_info "Tip: Search for 'p < 0.001' or 'p < 0.01' in Word document"
    fi

    echo ""
    return 0
  else
    print_error "Conversion failed"
    return 1
  fi
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

main() {
  print_header "Summary to Word Converter"
  echo "Repository: $REPO_ROOT"
  echo "Summaries Directory: $SUMMARIES_DIR"
  echo ""

  # Check dependencies
  check_dependencies

  # Check if specific file provided
  if [ $# -eq 1 ]; then
    # Single file conversion
    input_file="$1"

    # If relative path, prepend summaries dir
    if [[ ! "$input_file" = /* ]]; then
      input_file="$SUMMARIES_DIR/$input_file"
    fi

    if [ ! -f "$input_file" ]; then
      print_error "File not found: $input_file"
      exit 1
    fi

    convert_to_word "$input_file"

  else
    # Batch conversion - all SUMMARY.md files
    print_info "Converting all *_SUMMARY.md files in $SUMMARIES_DIR"
    echo ""

    local count=0
    local success=0

    for md_file in "$SUMMARIES_DIR"/*_SUMMARY.md; do
      if [ -f "$md_file" ]; then
        count=$((count + 1))
        if convert_to_word "$md_file"; then
          success=$((success + 1))
        fi
      fi
    done

    echo ""
    print_header "Conversion Summary"
    echo "Total files processed: $count"
    echo "Successful conversions: $success"
    echo "Failed conversions: $((count - success))"
    echo ""

    if [ $success -eq $count ] && [ $count -gt 0 ]; then
      print_success "All conversions completed successfully!"
    elif [ $success -gt 0 ]; then
      print_warning "Some conversions completed successfully"
    else
      print_error "All conversions failed"
      exit 1
    fi
  fi

  echo ""
  print_header "Post-Conversion Checklist"
  echo "Please manually verify in Word documents:"
  echo "  1. Significance markers (*, **, ***) display correctly"
  echo "  2. Tables are properly formatted"
  echo "  3. Date/year information is prominent"
  echo "  4. Headings are at correct levels"
  echo "  5. Code blocks and file paths are readable"
  echo ""
  print_info "Tip: Use Word's Find & Replace to check for:"
  echo "  - 'p < 0.001' should be followed by ***"
  echo "  - 'p < 0.01' should be followed by **"
  echo "  - 'p < 0.05' should be followed by *"
  echo ""
}

# ============================================================================
# USAGE
# ============================================================================

if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
  cat <<EOF
Summary to Word Converter

USAGE:
  $0                          Convert all *_SUMMARY.md files
  $0 [filename]               Convert specific file
  $0 -h, --help              Show this help

EXAMPLES:
  # Convert all summary files
  $0

  # Convert specific file (relative to summaries directory)
  $0 21_teacher_diversity_regression_SUMMARY.md

  # Convert specific file (absolute path)
  $0 /full/path/to/file.md

REQUIREMENTS:
  - pandoc must be installed

OUTPUT:
  - Creates .docx file alongside .md file
  - Preserves original .md file
  - Table of contents included
  - Date metadata added

NOTES:
  - Significance markers (*, **, ***) are preserved with backslash escaping
  - Always manually verify Word output for correct significance marker display
  - Check that dates/years are prominent in converted document

EOF
  exit 0
fi

# Run main function
main "$@"
