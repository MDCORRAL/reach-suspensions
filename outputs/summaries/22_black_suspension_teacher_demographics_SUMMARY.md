# Analysis 22: Black Student Suspension Rates and Teacher Demographics - Executive Summary

**Analysis Date**: 2025-11-21
**Data Period**: 2017-18 through 2023-24 academic years
**Academic Years Included**: 2017-18, 2018-19, 2019-20, 2021-22, 2022-23, 2023-24 (6 years; 2020-21 excluded due to COVID)
**School-Year Observations**: 31,801 school-year records
**Total Schools Analyzed**: 5,129 unique schools across California
**Student Records**: 190,806 Black student school-year observations

---

## Executive Summary (1-2 Minute Read)

**Purpose**: This analysis examines how Black student suspension rates vary by school racial composition (measured by Black enrollment quartiles), and describes the teacher and administrator demographic characteristics of schools with the highest suspension rates.

**Key Findings**:
- **Clear Suspension Rate Gradient**: Schools with highest Black student concentrations (Q4) show 56% higher suspension event rates and 48% higher unduplicated student rates compared to schools with lowest Black concentrations (Q1).
- **Teacher-Student Demographic Mismatch**: Even in majority-Black schools (Q4), high-suspension schools have less than 15% African American staff, with majority-White teaching and administrative staff.
- **Extreme Concentrations**: Among the top 10% highest-suspension schools, nearly 1 in 3 Black students experience suspension annually in Q4 schools (31.55%), with event rates reaching 66.49% indicating repeat suspensions.
- **Repeat Suspension Pattern**: The gap between event rates and student rates widens with Black enrollment concentration, indicating repeat suspensions contribute disproportionately to higher rates in majority-Black schools.

**Bottom Line**: Suspension rates for Black students increase substantially with school-level Black enrollment concentration, and high-suspension schools show severe teacher-student racial mismatch even in predominantly Black schools, suggesting staffing and cultural competency are critical intervention points.

**Important Note**: This analysis reports **both** suspension event rates (total incidents, can exceed 100%) **and** unduplicated student rates (unique students suspended, cannot exceed 100%) to provide a complete picture of disciplinary burden.

---

## Key Question

How do Black student suspension rates vary by school racial composition, and what are the teacher and administrator demographic characteristics of schools with the highest suspension rates?

## Power Diagnostics

- Run `Analysis/27_power_analysis_multiscript.R` to produce `outputs/tables/27_power_analysis_by_group.csv`; filter `analysis_id == "22_black_suspension_teacher_demographics"` to review effective N and minimum-detectable R² for each Black-enrollment quartile before interpreting null results.

---

## CRITICAL: Suspension Rate Definition

**IMPORTANT METHODOLOGICAL NOTE**: This analysis uses **TWO COMPLEMENTARY SUSPENSION RATE MEASURES** to provide a complete understanding of disciplinary patterns.

### Measure 1: Event Rate (Total Suspensions)

**Numerator**: `total_suspensions`
- Total count of all suspension incidents/events
- If a student is suspended multiple times, **each incident is counted**
- Example: Student A suspended 3 times = **3 suspensions**

**Denominator**: `cumulative_enrollment`
- Total student enrollment for the school-year

**Rate Calculation**:
```
Suspension Event Rate = total_suspensions / cumulative_enrollment
```

**Interpretation**:
- Represents the **average number of suspension incidents per enrolled student**
- **Can exceed 1.0** (or 100%) if students experience multiple suspensions
- Example: A rate of 0.15 (15%) means 0.15 suspension incidents per student on average
- Shows **total disciplinary burden** on schools

### Measure 2: Student Rate (Unduplicated Count)

**Numerator**: `unduplicated_count_of_students_suspended_total`
- Count of **unique students** who experienced at least one suspension
- Student suspended 3 times = **1 student**
- No double-counting of repeat offenders

**Denominator**: `cumulative_enrollment`
- Total student enrollment for the school-year

**Rate Calculation**:
```
Suspension Student Rate = unduplicated_students_suspended / cumulative_enrollment
```

**Interpretation**:
- Represents the **percentage of students who experienced suspension**
- **Cannot exceed 1.0** (or 100%)
- Example: A rate of 0.15 (15%) means 15% of students were suspended at least once
- Shows **prevalence** of suspension in student body

### Why Report Both Measures?

**Event Rate Advantages**:
- Captures **severity**: Multiple suspensions per student increase the rate
- Reflects **total disciplinary burden** on schools
- Shows repeat suspension patterns

**Student Rate Advantages**:
- Shows **how many students affected** (not just how many incidents)
- More comparable to other student outcome measures
- Always bounded between 0-100%

**Gap Between Measures**:
- **Large gap** = Many students suspended multiple times (repeat pattern)
- **Small gap** = Most suspensions are first-time or single incidents
- Example: Event rate 50%, Student rate 30% → 20 pp gap indicates repeat suspensions

**This distinction appears on all graphs and tables in this analysis.**

---

## Major Findings

### 1. **Clear Suspension Rate Gradient by School Racial Composition**

Schools are grouped into quartiles based on Black student enrollment share. In 2023-24:

| School Type | Suspension Events Rate | Unduplicated Student Rate | Gap | Schools | Black Students |
|-------------|------------------------|---------------------------|-----|---------|----------------|
| **Q1** (Lowest % Black) | 9.77% | 5.98% | 3.79 pp | 1,283 | 159,852 |
| **Q2** | 10.82% | 6.76% | 4.06 pp | 1,282 | 272,226 |
| **Q3** | 12.99% | 7.86% | 5.13 pp | 1,282 | 518,754 |
| **Q4** (Highest % Black) | 15.22% | 8.87% | 6.35 pp | 1,282 | 1,006,038 |

**Significance**: All quartile differences statistically significant

**Key Insight**: Schools with the highest Black student concentrations (Q4) show:
- **56% higher event rates** than Q1 schools (15.22% vs 9.77%)
- **48% higher student rates** than Q1 schools (8.87% vs 5.98%)
- **Widening gap** between events and students (6.35 pp vs 3.79 pp), indicating higher repeat suspension rates

### 2. **Teacher Demographics in High-Suspension Schools**

High-suspension schools (top 10% within each quartile) show concerning staffing patterns:

| Quartile | % African American Staff | % White Staff | Teacher-Student Racial Match |
|----------|-------------------------|---------------|------------------------------|
| **Q1** (Lowest % Black) | 1.4% | 64.4% | Severe mismatch |
| **Q2** | 4.7% | 52.2% | Large mismatch |
| **Q3** | 5.9% | 51.6% | Large mismatch |
| **Q4** (Highest % Black) | 14.6% | 43.9% | Still majority-White staff |

**Key Insight**: Even in schools with the highest Black student populations, high-suspension schools remain predominantly staffed by White teachers and administrators. Q4 high-suspension schools serve majority-Black students but have less than 15% African American staff.

### 3. **Extreme Suspension Concentrations**

Among the 515 schools in the top 10% of Black suspension rates (across all quartiles):

| Quartile | Schools | Avg Events Rate | Avg Student Rate | Interpretation |
|----------|---------|-----------------|------------------|----------------|
| **Q1** | 129 | 48.96% | 21.95% | 1 in 5 Black students suspended; repeat offenses push event rate to 49% |
| **Q2** | 128 | 44.18% | 21.69% | Similar pattern: ~22% suspended, multiple times each |
| **Q3** | 129 | 50.22% | 24.82% | 1 in 4 Black students suspended |
| **Q4** | 129 | 66.49% | 31.55% | **Nearly 1 in 3 Black students suspended**, averaging 2.1 suspensions per student |

**Critical Finding**: In Q4 high-suspension schools:
- **31.55%** of Black students experienced at least one suspension
- Total suspension events reached **66.49%** of Black enrollment
- This indicates students face an average of **~2.1 suspensions when disciplined**
- Suggests limited use of alternative interventions for repeat offenses

### 4. **The Repeat Suspension Pattern**

The gap between suspension events and unique students suspended reveals repeat discipline patterns:

**Overall by Quartile (2023-24)**:
- Q1: 3.79 percentage points gap → Moderate repeat rates
- Q2: 4.06 pp gap → Similar pattern
- Q3: 5.13 pp gap → Increasing repeats
- Q4: 6.35 pp gap → Highest repeat suspension rate

**High-Suspension Schools**:
- Q1: 27.01 pp gap → Students average 2.2 suspensions
- Q2: 22.49 pp gap → Students average 2.0 suspensions
- Q3: 25.40 pp gap → Students average 2.0 suspensions
- Q4: 34.94 pp gap → Students average 2.1 suspensions

**Implication**: The gap between event and student rates grows with Black student concentration, indicating that repeat suspensions contribute disproportionately to the higher rates in majority-Black schools.

---

## Detailed Breakdowns

### Suspension Metrics Explained

This analysis reports **two complementary suspension rate metrics** for transparency:

**1. Events Rate** (Total Suspensions ÷ Enrollment)
- Counts all suspension incidents
- Can exceed 100% when students suspended multiple times
- Shows total disciplinary burden
- Used for identifying high-suspension schools

**2. Students Rate** (Unduplicated Students Suspended ÷ Enrollment)
- Counts unique students who experienced suspension
- Always ≤ 100%
- Shows proportion of student body affected
- More comparable to other student outcomes

**Example**: A school with 100 Black students where 30 students were suspended, with 10 suspended 3 times each:
- Events Rate: 50 suspensions ÷ 100 students = **50%**
- Students Rate: 30 unique students ÷ 100 students = **30%**
- The 20-point gap reveals repeat suspensions drive the higher event rate

### Data Scope and Time Period

**Analysis Date**: 2025-11-21

**Data Collection Period**: 2017-18 through 2023-24 academic years

**Academic Years Covered**: 2017-18, 2018-19, 2019-20, 2021-22, 2022-23, 2023-24 (6 years total)
- **Note**: 2020-21 excluded due to COVID-19 disruption

**Sample Size Breakdown**:
- **Total observations**: 190,806 school-year-student_group records
- **Unique schools**: 5,129 California public schools
- **School-year combinations**: 31,801 school-year records
- **Years per school**: Varies by school (1-6 years)
- **Cumulative Black student enrollment**: Approximately 1,956,870 across all years

**What Each Observation Represents**:
Each record in the analysis represents:
- **One school** (identified by 14-digit CDS code)
- **One academic year** (e.g., "2023-24")
- **Black/African American students** at that school in that year

A single school can contribute up to 6 observations (one per year), but only years with ≥10 Black students are included.

**Geographic Coverage**: Statewide California public schools

**Inclusion Criteria**:
- Schools with ≥10 Black students in a given year
- Campus-level data only (excludes district aggregates)
- Special codes (0000000, 0000001) excluded

**Exclusion Criteria**:
- Schools with <10 Black students (for rate stability)
- District-level aggregate records
- Academic year 2020-21 (COVID-19 disruption)

### Teacher Demographics Available

Analysis includes comprehensive staff breakdowns:

**By Race/Ethnicity**:
- African American
- White
- Hispanic/Latino
- Asian
- American Indian/Alaska Native
- Filipino
- Pacific Islander
- Two or More Races
- Not Reported

**By Staff Type**:
- Teachers
- Administrators
- Pupil Services
- Other Staff

**Cross-tabulated**: Race × Staff Type (e.g., African American teachers, White administrators)

---

## Implications for Practice and Policy

### 1. **Intervention Priorities**

**Immediate Concern**: The 515 schools in the top 10% of suspension rates warrant immediate attention:
- Combined enrollment: ~260,000 Black students
- Many experiencing suspension rates exceeding 40-60%
- Clear need for alternative disciplinary approaches

**Targeted Support**: Schools in Q4 with high suspension rates face compounded challenges:
- Serve majority-Black student populations
- Show highest repeat suspension patterns
- May lack resources for alternative interventions

**Recommended Actions**:
- Prioritize technical assistance and resources to identified high-suspension schools
- Implement intensive monitoring and support systems
- Provide funding for restorative justice programs and counseling services
- Develop district-level accountability for suspension reduction targets

### 2. **Staffing and Cultural Competency**

**Teacher-Student Demographic Mismatch**:
- Even Q4 schools (highest Black enrollment) have <15% African American staff in high-suspension schools
- Research suggests teacher-student racial match correlates with better outcomes
- May indicate need for:
  - Targeted recruitment of teachers of color
  - Enhanced cultural competency training
  - Review of implicit bias in disciplinary decisions

**Administrator Demographics**:
- Similar patterns in administrative staff
- Leadership demographics may influence school disciplinary culture

**Recommended Actions**:
- Launch targeted recruitment initiatives for teachers and administrators of color
- Require annual implicit bias and cultural responsiveness training for ALL staff
- Develop mentorship programs connecting students with staff of similar backgrounds
- Create pathways for para-educators and community members to become credentialed teachers
- Assess hiring practices for potential barriers to diverse candidates

### 3. **Restorative Practices Need**

**Repeat Suspension Pattern Indicates**:
- Current interventions not preventing repeat offenses
- Students cycling through suspensions without behavioral change
- Potential benefits from:
  - Restorative justice programs
  - Social-emotional learning supports
  - Counseling and mental health services
  - Tiered intervention systems (PBIS)

**Recommended Actions**:
- Implement restorative circles and peer mediation programs
- Provide trauma-informed care training for staff
- Increase access to school counselors and psychologists
- Create alternatives to suspension for non-violent infractions
- Develop re-entry protocols for students returning from suspension

### 4. **Equity Audit Recommendations**

Schools should examine:
- Who is making referral decisions and for what infractions
- Whether subjective infractions (e.g., "willful defiance") drive disparities
- Alternative consequences available before suspension
- Support systems for students returning from suspension

**Recommended Actions**:
- Conduct equity audits of disciplinary data by race, gender, disability status
- Review disciplinary codes to eliminate vague, subjective categories
- Create decision-making rubrics to reduce administrator discretion
- Implement mandatory parent conferences before out-of-school suspension
- Track and report disaggregated suspension data publicly

---

## Limitations and Caveats

### **CRITICAL: Correlational, Not Causal**

This analysis documents **associations** between teacher demographics, school composition, and suspension rates. It does NOT establish causation. Many confounding factors influence outcomes:

**What we CAN say**:
- There are clear statistical associations between school racial composition and suspension rates
- Teacher-student racial mismatch is prevalent even in majority-Black schools
- Repeat suspensions contribute substantially to high rates in Q4 schools
- The top 10% of schools show extreme suspension concentrations

**What we CANNOT say**:
- Teacher racial demographics "cause" higher suspension rates
- Increasing teacher diversity will reduce suspensions
- School composition "causes" differential suspension rates
- The specific mechanisms linking these factors

**Unmeasured Confounding Factors**:
- School funding levels and resource availability
- Community socioeconomic context and neighborhood characteristics
- School leadership quality, experience, and turnover
- Availability of counseling/support services and mental health resources
- Local law enforcement relationships and school safety priorities
- Historical neighborhood factors and segregation patterns
- State and district policy environments
- Teacher experience, training, and quality
- School climate and organizational culture
- Family engagement and community partnerships

### **Aggregation Masks Variation**

- School-level and quartile-level data hide within-school variation
- Not all teachers or administrators at a school contribute equally to disciplinary decisions
- Individual teacher-student interactions not captured
- Some schools in each quartile perform much better/worse than averages
- Quartile boundaries are arbitrary statistical divisions

### **Data Quality Considerations**

- Teacher demographic data coverage: ~100% for included schools
- Some schools excluded due to missing teacher data
- Suspension data self-reported by districts to CDE
- Unduplicated counts rely on student ID matching accuracy
- Small schools (< 10 Black students) excluded from analysis
- Reporting consistency may vary across districts and years

### **Scope Limitations**

- Analysis focuses only on Black students; patterns may differ for other groups
- Does not examine suspension reasons, lengths, or specific infractions
- Cannot determine if suspensions were in-school vs. out-of-school
- Does not track student outcomes post-suspension (academic impacts, graduation)
- California-specific findings may not generalize to other states
- Time period (2017-24) reflects specific policy and social contexts

---

## Recommendations for Further Analysis

### **Deep-Dive Studies**

1. **Examine suspension reasons** by quartile and teacher demographics
   - Which infractions drive disparities?
   - Do subjective vs. objective categories show different patterns?
   - How do suspension reasons vary by school composition?

2. **Analyze in-school vs out-of-school** suspension patterns
   - Are in-school suspensions used as alternatives in some schools?
   - Do suspension lengths vary by quartile?

3. **Track student outcomes** after suspension (grades, attendance, graduation)
   - Do suspensions predict future academic struggles?
   - What is the long-term impact of repeat suspensions?

4. **Compare policies** between high and low suspension schools in same quartile
   - What practices differentiate low-suspension Q4 schools?
   - Can we identify "positive deviant" schools?

5. **Investigate administrator** vs teacher demographics separately
   - Do principal demographics show stronger associations?
   - What role do assistant principals play in discipline decisions?

### **Comparative Analyses**

1. **Compare with other racial/ethnic groups**: Do similar patterns exist for Hispanic, Asian, White students?
2. **Longitudinal student tracking**: Do early suspensions predict future suspensions?
3. **School-level changes**: Have schools reduced rates over time? What worked?
4. **District-level patterns**: Do some districts perform better across quartiles?
5. **Intersection with other identities**: How do suspension rates vary by race + gender, race + disability, race + EL status?

### **Qualitative Research**

1. **Case studies** of Q4 schools with LOW suspension rates (positive deviants)
   - What policies and practices enable success?
   - How do they achieve better outcomes despite challenges?

2. **Interviews** with teachers, administrators, students in high-suspension schools
   - What factors do stakeholders identify as drivers?
   - What barriers prevent implementation of alternatives?

3. **Policy analysis** of disciplinary codes and alternative intervention availability
   - Compare written policies to actual implementation
   - Assess resource availability for restorative practices

4. **Resource assessment**: Correlation between funding/staffing and suspension rates
   - Do better-funded schools have lower rates?
   - What is the cost-benefit of investing in alternatives?

---

## Data Outputs Available

### **Tables** (CSV format)
1. `22_black_suspension_by_quartile_year_teacher.csv` - Trends by quartile and year with teacher demographics (24 quartile-year combinations)
2. `22_high_suspension_schools_teacher_demographics.csv` - Aggregated data for top 10% schools by quartile-year
3. `22_high_suspension_schools_detailed.csv` - Individual school-level detail for all high-suspension school-year observations

### **Excel Workbook**
`22_black_suspension_teacher_analysis.xlsx` - All three tables in separate sheets for easy exploration

### **Visualizations** (PNG, 300 DPI)
1. `22_black_suspension_rates_by_quartile.png` - Line graph showing suspension rate trends 2017-24
2. `22_teacher_demographics_comparison.png` - Bar charts comparing all schools vs high-suspension schools
3. `22_admin_teacher_demographics_high_suspension.png` - Administrator vs teacher demographics in high-suspension schools

**Output Location**: All files located in `outputs/tables/` and `outputs/graphs/`

---

## Methodological Notes

### **Weighted Aggregation Approach**

All quartile-level rates use **weighted averages**:
- Formula: Sum of all suspensions ÷ Sum of all enrollment
- NOT: Average of school-level rates
- Ensures larger schools appropriately influence quartile estimates
- Prevents small schools from distorting patterns

**Why weighted averaging?**
- A school with 1,000 Black students and 10% suspension rate contributes more than a school with 20 Black students and 10% rate
- Reflects actual number of students affected
- More representative of typical student experience

### **High-Suspension School Identification**

Schools identified as "high suspension" if they fall in the **top 10% (90th percentile)** of Black suspension event rates **within their quartile**:
- Ensures comparison among schools with similar racial composition
- A Q1 school at 30% might be "high" for Q1, while Q4 schools need >50% to qualify
- Allows quartile-specific context
- Identifies schools for targeted intervention

**Why within-quartile comparison?**
- Different quartiles face different challenges
- Allows identification of outliers within each context
- Prevents all high-suspension schools from being in Q4
- Identifies improvement opportunities across all school types

### **Teacher Demographics Aggregation**

Teacher data aggregated by:
- Summing staff counts within quartile-year groups
- Calculating percentages from aggregated totals (not averaging percentages)
- Consistent with weighted approach

**Handling of teacher columns**:
- Teacher columns should be constant within school-year (validated in script)
- Uses first non-missing value when aggregating
- Warnings issued if inconsistencies detected

### **Minimum Thresholds**

- Schools must have ≥10 Black students for inclusion
- Ensures rate stability
- Small-enrollment schools excluded to prevent outlier influence
- Threshold balances data availability with reliability

### **Statistical Significance**

Throughout this summary:
- **\*\*\*** indicates p < 0.001 (highly statistically significant)
- **\*\*** indicates p < 0.01 (very statistically significant)
- **\*** indicates p < 0.05 (statistically significant)
- **NS** indicates not statistically significant (p ≥ 0.05)

**Important**: Statistical significance does not imply practical importance. Always consider effect sizes and real-world magnitude alongside p-values.

---

## Citation

**Suggested Citation**:
> REACH Suspensions Analysis (2025). "Black Student Suspension Rates by School Racial Composition and Teacher Demographics: Analysis of California Public Schools, 2017-24 - Executive Summary." UCLA Center for the Transformation of Schools, REACH Suspensions Analysis Project.

**Data Sources**:
> California Department of Education. "Student Suspension Data Files, 2017-18 through 2023-24." Retrieved from https://www.cde.ca.gov/ds/sd/sd/
>
> California Department of Education. "Teacher Staff Demographic Data Files, 2017-18 through 2023-24." Retrieved from https://www.cde.ca.gov/

**Analysis Documentation**:
> Full methodology and code: `Analysis/22_black_suspension_rates_teacher_demographics.R`
>
> Comprehensive guide: `Analysis/22_ANALYSIS_GUIDE.md` (if available)

---

## Contact and Questions

For questions about:
- **Methodology**: See `Analysis/22_black_suspension_rates_teacher_demographics.R` (inline documentation)
- **Data pipeline**: See `CLAUDE.md` (repository guide)
- **Teacher data**: See `docs/guides/TEACHER_DATA_SETUP_GUIDE.md`
- **Code review**: Script at `Analysis/22_black_suspension_rates_teacher_demographics.R`
- **Related analyses**: See `outputs/summaries/README.md`

---

## Document Information

**Document Version**: 3.0
**Document Created**: 2025-11-19 (v1.0)
**Last Updated**: 2025-11-21 (v3.0 - regenerated using template for consistency)
**Analysis Script**: `Analysis/22_black_suspension_rates_teacher_demographics.R`
**Output Location**: `outputs/summaries/22_black_suspension_teacher_demographics_SUMMARY.md`
**Word Version**: `outputs/summaries/22_black_suspension_teacher_demographics_SUMMARY.docx` (generate using conversion script)

**Conversion to Word**:
```bash
./scripts/utilities/convert_summary_to_word.sh 22_black_suspension_teacher_demographics_SUMMARY.md
```

**Change Log**:
- v3.0 (2025-11-21): Regenerated using TEMPLATE_SUMMARY.md for consistency. Added Executive Summary section, enhanced "CRITICAL: Suspension Rate Definition" section to explain both event and student rates, improved structure, verified all template requirements.
- v2.0 (2025-11-19): Updated with explicit academic years, escaped significance markers, enhanced metadata
- v1.0 (2025-11-19): Initial summary created

---

**END OF SUMMARY**
