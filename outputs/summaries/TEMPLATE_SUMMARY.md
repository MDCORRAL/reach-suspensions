# Analysis [NN]: [Analysis Title] - Executive Summary

**Analysis Date**: YYYY-MM-DD
**Data Period**: [Start Year] through [End Year] (e.g., 2017-18 through 2023-24)
**Academic Years Included**: [List specific years, e.g., 2017-18, 2018-19, 2019-20, 2021-22, 2022-23, 2023-24]
**[Key Scope Metric]**: [Number and description, e.g., "130,236 school-year observations"]
**Total Schools Analyzed**: [Number of unique schools if applicable]

---

## Executive Summary (1-2 Minute Read)

**Purpose**: [One sentence describing what this analysis examines]

**Key Findings**:
- **Finding 1**: [Brief, non-technical summary of most important finding]
- **Finding 2**: [Brief summary of second most important finding]
- **Finding 3**: [Brief summary of third finding]
- **Finding 4**: [Additional finding if needed]

**Bottom Line**: [One-sentence overall takeaway for decision-makers]

**Important Note**: This analysis examines **total suspension incidents** (not unique students suspended), so rates can exceed 100% when students experience multiple suspensions.

---

## Key Question

[State the primary research question in 1-2 clear sentences]

---

## CRITICAL: Suspension Rate Definition

**IMPORTANT METHODOLOGICAL NOTE**: This analysis uses **TOTAL SUSPENSION INCIDENTS**, not **UNDUPLICATED STUDENT COUNT**.

### What This Means

**Numerator**: `total_suspensions`
- Total count of all suspension incidents/events
- If a student is suspended multiple times, **each incident is counted**
- Example: Student A suspended 3 times = **3 suspensions**

**Denominator**: `cumulative_enrollment`
- Total student enrollment for the school-year

**Rate Calculation**:
```
Suspension Rate = total_suspensions / cumulative_enrollment
```

**Interpretation**:
- Represents the **average number of suspension incidents per enrolled student**
- **Can exceed 1.0** (or 100%) if students experience multiple suspensions
- Example: A rate of 0.15 (15%) means 0.15 suspension incidents per student on average
- Example: A rate of 1.5 (150%) means 1.5 suspension incidents per student (indicating repeat suspensions)

### Why This Measure?

**Advantages**:
- Captures **severity**: Multiple suspensions per student increase the rate
- Reflects **total disciplinary burden** on schools
- Consistent across all schools (comparable measure)

**Important Note**:
- Rates **CAN exceed 100%** if many students receive multiple suspensions
- This is NOT an error - it indicates high rates of repeat suspensions

### Alternative Measure (NOT Used Here)

**Unduplicated Suspension Rate**:
- Numerator: Count of unique students suspended at least once
- Example: Student A suspended 3 times = **1 student**
- Interpretation: Percentage of students who experienced at least one suspension
- **Always between 0-100%** (cannot exceed 100%)

**Why not use unduplicated count?**
- [Explain rationale for choosing total incidents over unduplicated - e.g., captures severity, matches research questions, etc.]
- [OR note that both measures are reported if applicable]

**This distinction appears on all graphs and tables in this analysis.**

---

## Major Findings

### 1. **[First Major Finding - Descriptive Title]**

[Paragraph describing the finding with supporting data]

| Column 1 | Column 2 | Column 3 | Significance | Interpretation |
|----------|----------|----------|--------------|----------------|
| Data     | Data     | Data     | p < 0.001 \*\*\* | What it means  |
| Data     | Data     | Data     | p < 0.01 \*\* | What it means  |
| Data     | Data     | Data     | p < 0.05 \* | What it means  |
| Data     | Data     | Data     | NS (not significant) | What it means  |

**Significance Legend**:
\*\*\* = p < 0.001 (highly significant)
\*\* = p < 0.01 (very significant)
\* = p < 0.05 (significant)
NS = not statistically significant

**Key Insight**: [One-sentence takeaway]

### 2. **[Second Major Finding]**

[Similar structure as above]

### 3. **[Third Major Finding]**

[Continue as needed - aim for 3-5 major findings]

---

## Detailed Breakdowns

### [Metric/Dimension Name] Explained

[Provide detailed explanation of key metrics, calculations, or dimensions]

### Data Scope and Time Period

**Analysis Date**: YYYY-MM-DD
**Data Collection Period**: [Start date] through [End date]
**Academic Years Covered**: [Specific years with any gaps noted]
**Sample Size**: [Detailed breakdown]
  - Total observations: [Number]
  - Unique schools: [Number]
  - School-year combinations: [Number]
  - Years per school: [Average or range]

**Geographic Coverage**: [Details, e.g., "All California public schools"]
**Inclusion Criteria**: [What was included]
**Exclusion Criteria**: [What was excluded and why]

### [Additional Breakdowns as Needed]

[Tables, lists, or detailed numbers]

---

## Implications for Practice and Policy

### 1. **[Implication Category 1]**

**Finding**: [Restate finding]

**Implication**:
- Point 1
- Point 2
- Point 3

**Recommended Actions**:
- Action 1
- Action 2

### 2. **[Implication Category 2]**

[Continue as needed]

---

## Limitations and Caveats

### **CRITICAL: Correlational, Not Causal**

This analysis uses [observational data / regression / descriptive statistics] which can detect **associations** but cannot prove **causation**.

**What we CAN say**: [Describe what the data shows]
**What we CANNOT say**: [Describe causal claims that are not supported]

### **[Other Limitation Category 1]**

[Describe limitation and its impact on interpretation]

### **[Other Limitation Category 2]**

[Continue as needed]

---

## Recommendations for Further Analysis

### **[Category 1]**

1. [Specific recommendation with rationale]
2. [Specific recommendation with rationale]

### **[Category 2]**

[Continue as needed]

---

## Data Outputs Available

### **Tables** (CSV format)
1. `[filename].csv` - [Description]
2. `[filename].csv` - [Description]

### **Excel Workbook**
`[filename].xlsx` - [Description of contents and sheets]

### **Visualizations** (PNG, 300 DPI)
1. `[filename].png` - [Description]
2. `[filename].png` - [Description]

**Output Location**: All files located in `outputs/[subdirectory]/`

---

## Methodological Notes

### **[Method 1 - e.g., Statistical Approach]**

**Approach**: [Describe method]
**Why this method**: [Rationale]
**Assumptions**: [List key assumptions]
**Limitations**: [Method-specific limitations]

### **[Method 2 - e.g., Sample Construction]**

[Continue as needed]

### **Statistical Significance**

Throughout this summary:
- **\*\*\*** indicates p < 0.001 (highly statistically significant)
- **\*\*** indicates p < 0.01 (very statistically significant)
- **\*** indicates p < 0.05 (statistically significant)
- **NS** indicates not statistically significant (p ≥ 0.05)

**Important**: Statistical significance does not imply practical importance. Always consider effect sizes and real-world magnitude.

---

## Citation

**Suggested Citation**:
> [Author/Organization] ([Year]). "[Full Title]: Executive Summary." UCLA Center for the Transformation of Schools, REACH Suspensions Analysis Project.

**Data Source**:
> California Department of Education. "[Dataset name]." [Years]. Retrieved from https://www.cde.ca.gov/

**Analysis Documentation**:
> Full methodology and code available at: `Analysis/[NN]_[script_name].R`

---

## Contact and Questions

For questions about:
- **Methodology**: See `Analysis/[NN]_ANALYSIS_GUIDE.md` (if available)
- **Data pipeline**: See `CLAUDE.md`
- **Code review**: Script at `Analysis/[NN]_[script_name].R`
- **Related analyses**: See `outputs/summaries/README.md`

---

## Document Information

**Document Version**: 1.0
**Document Created**: YYYY-MM-DD
**Last Updated**: YYYY-MM-DD
**Analysis Script**: `Analysis/[NN]_[script_name].R`
**Output Location**: `outputs/summaries/[NN]_[analysis_name]_SUMMARY.md`
**Word Version**: `outputs/summaries/[NN]_[analysis_name]_SUMMARY.docx` (generate using conversion script)

**Conversion to Word**:
```bash
./scripts/utilities/convert_summary_to_word.sh [NN]_[analysis_name]_SUMMARY.md
```

---

## Appendix: Technical Details (Optional)

### A. [Extended Tables]

[Include detailed tables that are too long for main body]

### B. [Supplementary Analyses]

[Include secondary findings]

### C. [Glossary]

**[Term 1]**: [Definition]
**[Term 2]**: [Definition]

---

**END OF SUMMARY**
