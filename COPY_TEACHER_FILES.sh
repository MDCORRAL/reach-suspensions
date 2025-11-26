#!/bin/bash
# Script to copy teacher TXT files to data-raw directory
# Run this script from your terminal on your Mac

SOURCE_DIR="/Users/michaelcorral/Library/CloudStorage/GoogleDrive-mdcorral@g.ucla.edu/.shortcut-targets-by-id/1qNAOKIg0UjuT3XWFlk4dkDLN6UPWJVGx/Center for the Transformation of Schools/Research/CA Race Education And Community Healing (REACH)/2. REACH Network (INTERNAL)/15. REACH Baseline Report_Summer 2025/6. R Data Analysis Project Folders/reach-suspensions"
DEST_DIR="$SOURCE_DIR/data-raw"

echo "=== Copying Teacher TXT Files ==="
echo ""
echo "Source: $SOURCE_DIR"
echo "Destination: $DEST_DIR"
echo ""

# Create data-raw directory if it doesn't exist
mkdir -p "$DEST_DIR"

# Copy files (trying both with and without .txt extension)
cd "$SOURCE_DIR"
COPIED=0

for file in stre1920 stre2021 stre2122 stre2223 stre2324 stre2425; do
  if [ -f "$file" ]; then
    echo "Copying: $file"
    cp "$file" "$DEST_DIR/${file}.txt"
    COPIED=$((COPIED + 1))
  elif [ -f "${file}.txt" ]; then
    echo "Copying: ${file}.txt"
    cp "${file}.txt" "$DEST_DIR/"
    COPIED=$((COPIED + 1))
  else
    echo "WARNING: File not found: $file (or ${file}.txt)"
  fi
done

echo ""
echo "=== Copy Complete ==="
echo "Copied $COPIED files"
echo ""
echo "Files in data-raw/:"
ls -lh "$DEST_DIR"/stre*.txt 2>/dev/null || echo "No .txt files found"

echo ""
echo "Next steps:"
echo "1. Verify files are in data-raw/ (see above)"
echo "2. Run: cd '$SOURCE_DIR' && Rscript run_all.R"
