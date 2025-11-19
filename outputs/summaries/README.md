# Analysis Summaries Directory

This directory contains executive summaries of major analyses conducted for the REACH Suspensions project. Summaries are designed to be shareable, publication-ready documents that distill key findings from detailed technical analyses.

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

1. **Title and Metadata**
   - Analysis name and number
   - Date of analysis
   - Data period covered
   - Sample size / scope

2. **Key Question**
   - Clear statement of research question(s)

3. **Major Findings**
   - 3-5 headline findings with supporting data
   - Presented clearly with tables/numbers
   - Actionable insights highlighted

4. **Detailed Breakdowns**
   - More granular results
   - Methodology notes
   - Data scope and coverage

5. **Implications**
   - Policy implications
   - Practice recommendations
   - Areas for intervention

6. **Limitations and Caveats**
   - Correlational vs causal language
   - Data quality considerations
   - Scope limitations

7. **Recommendations for Further Analysis**
   - Follow-up questions
   - Complementary analyses needed

8. **Data Outputs Available**
   - List of tables, visualizations, and files
   - Location paths

9. **Citation and Contact**
   - How to cite the analysis
   - Data sources
   - Where to find technical documentation

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
4. **Use template structure** (see above)
5. **Write for non-technical audience** but include technical details in appropriate sections
6. **Include data caveats** prominently
7. **Update this README** with new entry in Current Summaries table

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

### Markdown to PDF

```bash
# Using pandoc (if installed)
pandoc 22_black_suspension_teacher_demographics_SUMMARY.md \
  -o 22_black_suspension_teacher_demographics_SUMMARY.pdf \
  --toc \
  --pdf-engine=xelatex
```

### Markdown to Word

```bash
pandoc 22_black_suspension_teacher_demographics_SUMMARY.md \
  -o 22_black_suspension_teacher_demographics_SUMMARY.docx \
  --toc
```

### Markdown to HTML

```bash
pandoc 22_black_suspension_teacher_demographics_SUMMARY.md \
  -o 22_black_suspension_teacher_demographics_SUMMARY.html \
  --standalone \
  --toc
```

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
