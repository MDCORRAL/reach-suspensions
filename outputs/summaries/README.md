# Analysis Summaries Directory

This directory contains executive summaries of major analyses conducted for the REACH Suspensions project. Summaries are designed to be shareable, publication-ready documents that distill key findings from detailed technical analyses.

---

## Quick Reference: Creating & Converting Summaries

### ⚡ Quick Start

```bash
# 1. Copy template
cp outputs/summaries/TEMPLATE_SUMMARY.md outputs/summaries/[NN]_[name]_SUMMARY.md

# 2. Edit file, filling in:
#    - ALL dates (Analysis Date, Data Period, Academic Years)
#    - Significance markers (use \*\*\* not ***)
#    - Sample sizes and scope

# 3. Convert to Word
./scripts/utilities/convert_summary_to_word.sh [NN]_[name]_SUMMARY.md

# 4. Verify Word output (see checklist below)
```

### ⚠️ Critical Requirements

**MUST HAVE in Every Summary:**
- ✅ **Analysis Date** at top (YYYY-MM-DD)
- ✅ **Data Period** explicitly stated (e.g., "2017-18 through 2023-24")
- ✅ **Academic Years Included** listed
- ✅ **Significance Legend** (\*\*\* = p < 0.001, etc.)
- ✅ **Escaped significance markers** in markdown (`\*\*\*`)
- ✅ **Document Created** and **Last Updated** dates at bottom

**AVOID These Common Errors:**
- ❌ Unescaped asterisks (*** instead of \*\*\*)
- ❌ Missing or vague date information
- ❌ No significance legend in statistical results
- ❌ Converting to Word without verification checklist

---

## Purpose

Analysis summaries serve as:
- **Shareable briefings** for stakeholders and policymakers
- **Quick reference** for key findings without needing to review full analysis scripts
- **Documentation** of major research conclusions
- **Publication drafts** for reports and presentations

---

## Directory Structure

```
outputs/summaries/
├── README.md (this file)
├── [NN]_[analysis_name]_SUMMARY.md
└── archive/ (optional - for superseded summaries)
```

### Naming Convention

Summaries follow the pattern: `[NN]_[analysis_name]_SUMMARY.md`

Where:
- `[NN]` = Analysis number (matches source script in `Analysis/`)
- `[analysis_name]` = Descriptive name (underscores, lowercase)
- `_SUMMARY` = Suffix identifying this as a summary document
- `.md` = Markdown format for readability and version control

**Examples**:
- `02_black_rates_by_quartiles_SUMMARY.md`
- `16_tail_concentration_analysis_SUMMARY.md`
- `21_teacher_diversity_regression_SUMMARY.md`
- `22_black_suspension_teacher_demographics_SUMMARY.md`

---

## Current Summaries

| File | Analysis | Key Question | Date |
|------|----------|--------------|------|
| `22_black_suspension_teacher_demographics_SUMMARY.md` | Black suspension rates with teacher demographics | How do suspension rates vary by school composition and what are teacher demographics in high-suspension schools? | 2025-11-19 |

*(Add new summaries to this table as created)*

---

## Summary Document Template

Each summary should include:

### Required Sections

1. **Title and Metadata** ⭐ **CRITICAL: MUST BE PROMINENT**
   - Analysis name and number
   - **Analysis Date** (YYYY-MM-DD format)
   - **Data Period** (e.g., "2017-18 through 2023-24")
   - **Academic Years Included** (explicit list: 2017-18, 2018-19, etc.)
   - Sample size / scope
   - Total schools analyzed

2. **Key Question**
   - Clear statement of research question(s)

3. **Major Findings** ⭐ **INCLUDE SIGNIFICANCE LEGEND**
   - 3-5 headline findings with supporting data
   - Presented clearly with tables/numbers
   - **Significance column** in tables with proper markers (\*\*\*, \*\*, \*, NS)
   - **Significance legend** explaining markers
   - Actionable insights highlighted

4. **Detailed Breakdowns**
   - More granular results
   - **Data Scope and Time Period** subsection (repeat dates/years for clarity)
   - Methodology notes
   - Sample size breakdown (observations, unique schools, years)

5. **Implications**
   - Policy implications
   - Practice recommendations
   - Areas for intervention

6. **Limitations and Caveats**
   - **CRITICAL: Correlational, Not Causal** section (use template wording)
   - Data quality considerations
   - Scope limitations

7. **Recommendations for Further Analysis**
   - Follow-up questions
   - Complementary analyses needed

8. **Data Outputs Available**
   - List of tables, visualizations, and files
   - Location paths

9. **Methodological Notes** ⭐ **INCLUDE SIGNIFICANCE EXPLANATION**
   - **Statistical Significance** subsection explaining \*\*\*, \*\*, \*, NS
   - Method descriptions
   - Assumptions and limitations

10. **Citation and Contact**
    - How to cite the analysis
    - Data sources
    - Where to find technical documentation

11. **Document Information** ⭐ **CRITICAL: VERSION TRACKING**
    - Document Version
    - **Document Created** date
    - **Last Updated** date
    - Analysis Script path
    - Output Location

### Optional Sections

- Executive Summary (1-2 paragraphs)
- Visual Highlights (key charts/graphs)
- Appendices (technical details, extended tables)
- Glossary (for specialized terms)

---

## Usage Guidelines

### Creating a New Summary

1. **Run the analysis** and verify all outputs are complete
2. **Review key findings** in the analysis output messages
3. **Create summary file** following naming convention
4. **Use TEMPLATE_SUMMARY.md** as starting point
5. **Fill in ALL metadata fields** (dates, years, sample sizes)
6. **Write for non-technical audience** but include technical details in appropriate sections
7. **Add significance legend** for any statistical results
8. **Escape significance markers** in markdown: use `\*\*\*` not `***`
9. **Include data caveats** prominently
10. **Update this README** with new entry in Current Summaries table
11. **Test conversion to Word** using conversion script
12. **Verify Word output** using post-conversion checklist

### Updating an Existing Summary

- If making **minor updates** (typos, clarifications): Edit in place
- If making **major updates** (new data, revised findings):
  - Move old version to `archive/` with date suffix
  - Create new version with current date
  - Update README table

### Sharing Summaries

Summaries are designed to be:
- **Standalone documents** (no need to reference other files to understand)
- **Version controlled** (committed to git)
- **Exportable** (markdown can convert to PDF, Word, HTML)
- **Citable** (include citation format in each summary)

---

## Related Documentation

### For Technical Details
- **Analysis Scripts**: `Analysis/[NN]_*.R` - Full R code
- **Analysis Guides**: `Analysis/[NN]_ANALYSIS_GUIDE.md` - Technical documentation
- **CLAUDE.md**: Repository guide and conventions

### For Data Outputs
- **Tables**: `outputs/tables/` - CSV and Excel files
- **Graphs**: `outputs/graphs/` - PNG visualizations
- **Data Audit**: `outputs/data_audit/` - Quality reports

### For General Documentation
- **Project README**: Root `README.md`
- **Documentation Index**: `docs/README.md`
- **Audit Reports**: `docs/audits/`
- **Protocols**: `docs/protocols/`

---

## Converting to Other Formats

### RECOMMENDED: Using the Conversion Script

**Use the provided conversion script for best results, especially for preserving significance markers:**

```bash
# Convert all summary files
./scripts/utilities/convert_summary_to_word.sh

# Convert a specific file
./scripts/utilities/convert_summary_to_word.sh 21_teacher_diversity_regression_SUMMARY.md

# Show help
./scripts/utilities/convert_summary_to_word.sh --help
```

**Why use the conversion script?**
- ✅ Preserves significance markers (*, **, ***)
- ✅ Adds table of contents automatically
- ✅ Includes date metadata
- ✅ Provides post-conversion checklist
- ✅ Batch processing support

### Manual Conversion Methods

#### Markdown to Word (Manual)

```bash
# Basic conversion
pandoc 22_black_suspension_teacher_demographics_SUMMARY.md \
  -o 22_black_suspension_teacher_demographics_SUMMARY.docx \
  --from markdown+escaped_line_breaks \
  --to docx \
  --toc \
  --toc-depth=3

# ⚠️ WARNING: Manual conversion requires careful verification of:
#   - Significance markers (*, **, ***)
#   - Table formatting
#   - Date/year prominence
```

#### Markdown to PDF

```bash
# Using pandoc (requires LaTeX)
pandoc 22_black_suspension_teacher_demographics_SUMMARY.md \
  -o 22_black_suspension_teacher_demographics_SUMMARY.pdf \
  --toc \
  --pdf-engine=xelatex
```

#### Markdown to HTML

```bash
pandoc 22_black_suspension_teacher_demographics_SUMMARY.md \
  -o 22_black_suspension_teacher_demographics_SUMMARY.html \
  --standalone \
  --toc
```

### Post-Conversion Verification Checklist

**CRITICAL: Always verify after converting to Word**

When reviewing the Word document, check:

1. **Significance Markers**
   - [ ] Search for "p < 0.001" - should be followed by \*\*\*
   - [ ] Search for "p < 0.01" - should be followed by \*\*
   - [ ] Search for "p < 0.05" - should be followed by \*
   - [ ] Verify significance legend is present and readable

2. **Date/Year Information**
   - [ ] Analysis date is prominent at top of document
   - [ ] Data period (e.g., "2017-18 through 2023-24") is clearly stated
   - [ ] Academic years are explicitly listed where applicable
   - [ ] "Document Created" and "Last Updated" dates are present

3. **Tables**
   - [ ] All tables render correctly
   - [ ] Column headers are clear
   - [ ] Significance columns display properly
   - [ ] Numbers are aligned and readable

4. **Formatting**
   - [ ] Headings are at correct levels (H1, H2, H3)
   - [ ] Bold and italic formatting preserved
   - [ ] Code blocks/file paths are monospace
   - [ ] Lists and bullets render correctly

5. **Content Integrity**
   - [ ] No missing sections
   - [ ] Citations present
   - [ ] File paths and references accurate

### Known Conversion Issues

#### Issue: Significance Markers Lost
**Symptom**: Asterisks (*, **, ***) disappear or render incorrectly in Word

**Solution**:
- Use the conversion script (`convert_summary_to_word.sh`) instead of manual pandoc
- In markdown, escape asterisks: `\*\*\*` instead of `***`
- Always include significance legend section

**Manual Fix in Word**:
1. Use Find & Replace in Word
2. Search for "p < 0.001" and manually add *** after each
3. Search for "p < 0.01" and manually add ** after each
4. Search for "p < 0.05" and manually add * after each

#### Issue: Dates Not Prominent
**Symptom**: Date information buried or hard to find

**Solution**:
- Use template's metadata section at top of document
- Include both "Analysis Date" and "Data Period" prominently
- List specific academic years in metadata

**Manual Fix in Word**:
1. Add dates to header or prominently at top
2. Use bold formatting for date fields
3. Consider adding to filename: `[NN]_[name]_SUMMARY_2025-11-19.docx`

#### Issue: Tables Not Aligned
**Symptom**: Table columns misaligned or poorly formatted

**Solution**:
- In Word, select table → Table Design → AutoFit to Contents
- Adjust column widths manually if needed
- Consider converting complex tables to Excel and embedding

### Tips for High-Quality Word Documents

1. **Before Conversion**:
   - Verify markdown renders correctly in preview
   - Check all significance markers are escaped: `\*\*\*`
   - Ensure date/year fields are complete
   - Run spell check on markdown

2. **After Conversion**:
   - Save original .docx as backup before editing
   - Apply institution's style guide if available
   - Add page numbers and headers/footers
   - Generate final PDF from Word for distribution

3. **Version Control**:
   - Keep .md file as source of truth
   - Regenerate .docx when markdown is updated
   - Don't edit Word directly for content changes (edit markdown instead)

---

## Best Practices

### Writing for Stakeholders

✅ **DO**:
- Use clear, jargon-free language in key findings
- Include specific numbers and percentages
- Highlight actionable implications
- Provide appropriate context for interpreting results
- Include limitations and caveats prominently
- Use tables and structured formatting for scanability

❌ **DON'T**:
- Assume technical knowledge
- Make causal claims from correlational data
- Bury key findings in technical details
- Omit data limitations
- Use unexplained acronyms or specialized terms
- Present results without context

### Data Transparency

Always include:
- Sample sizes
- Time periods covered
- Exclusion criteria
- Methodology notes
- Data sources
- Uncertainty/confidence where appropriate

### Reproducibility

Each summary should:
- Reference the source analysis script
- Link to data outputs
- Provide citation format
- Include version/date information
- Be findable via README index

---

## Maintenance

### Regular Reviews

- **Quarterly**: Review summaries for accuracy as new data arrives
- **Annually**: Update with latest academic year data
- **As needed**: Revise when methodology changes or new findings emerge

### Archival Policy

- Keep current summaries in main directory
- Move superseded versions to `archive/` subdirectory
- Maintain README index with latest versions only
- Archive includes date suffix: `[NN]_[name]_SUMMARY_[YYYY-MM-DD].md`

---

## Questions or Issues

For questions about:
- **Creating summaries**: See template above or review existing summaries
- **Analysis methodology**: Refer to analysis script and guide in `Analysis/` folder
- **Repository conventions**: See `CLAUDE.md`
- **Documentation standards**: See `docs/protocols/CITATION_STANDARD.md`

---

**Last Updated**: 2025-11-19
**Maintained By**: REACH Suspensions Analysis Team
