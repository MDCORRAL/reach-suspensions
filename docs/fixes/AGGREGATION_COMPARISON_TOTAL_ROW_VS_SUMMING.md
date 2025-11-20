# Aggregation Approaches: Using "Total" Row vs. Summing Across Categories

**Date**: 2025-11-20
**Issue**: Should we use CDE's pre-calculated "All Students" (Total) row, or sum across individual race categories?
**User Insight**: "Wouldn't it be more precise to use a row labeled 'Total'?"
**Answer**: **YES! The user is absolutely correct.**

---

## TL;DR

**User's Suggestion (BETTER)**:
```r
# Filter to CDE's "All Students" total, then aggregate reasons
df %>%
  filter(race == "All Students") %>%
  group_by(cds_school, academic_year) %>%
  summarise(total_suspensions = sum(suspensions))  # Just summing reasons now
```

**Current Approach (LESS ACCURATE)**:
```r
# Sum across all races, then aggregate reasons
df %>%
  group_by(cds_school, academic_year) %>%
  summarise(total_suspensions = sum(suspensions))  # Summing races AND reasons
```

**Why User's Approach is Better**:
- ✅ Uses CDE's official totals (authoritative source)
- ✅ No risk of double-counting or missing races
- ✅ Simpler and more efficient (8x data reduction immediately)
- ✅ Enrollment is truly constant (not race-specific values)

---

## Background: CDE Data Structure

### What CDE Provides

California Department of Education suspension data includes **pre-calculated totals** in a special "TA" (Total All) category that appears as `race == "All Students"` after canonicalization.

**Example Data Structure**:
```
cds_school | academic_year | race                    | reason    | suspensions | enrollment
-----------|---------------|-------------------------|-----------|-------------|------------
12345      | 2023-24       | All Students            | Defiance  | 50          | 500
12345      | 2023-24       | All Students            | Violent   | 30          | 500
12345      | 2023-24       | Black/African American  | Defiance  | 15          | 100
12345      | 2023-24       | Black/African American  | Violent   | 10          | 100
12345      | 2023-24       | Hispanic/Latino         | Defiance  | 20          | 250
12345      | 2023-24       | Hispanic/Latino         | Violent   | 12          | 250
...
```

**Key Observation**:
- "All Students" row has `suspensions = 50` for Defiance (already the sum of 15 + 20 + ... across all races)
- "All Students" row has `enrollment = 500` (total school enrollment)
- Individual race rows have race-specific counts

### Why "All Students" Exists

CDE pre-calculates these totals to:
1. Provide official aggregated statistics
2. Avoid users making errors when summing
3. Handle suppressed values correctly (CDE knows which are suppressed)
4. Ensure consistency across all users

---

## Approach Comparison

### Approach 1: Use "All Students" Row (RECOMMENDED)

**Code**:
```r
df %>%
  filter(race == "All Students") %>%  # CDE's pre-calculated total
  group_by(cds_school, academic_year) %>%
  summarise(
    total_suspensions = sum(total_suspensions, na.rm = TRUE),  # Sum reasons
    cumulative_enrollment = first(cumulative_enrollment)        # Constant
  )
```

**Pros**:
- ✅ **Most accurate**: Uses CDE's official totals
- ✅ **Handles suppression correctly**: CDE already handled asterisks
- ✅ **Simpler logic**: One filter, then aggregate reasons only
- ✅ **More efficient**: Reduces data 8x immediately (8 races → 1)
- ✅ **Enrollment is constant**: Truly school-level enrollment
- ✅ **No double-counting risk**: CDE did the summing correctly

**Cons**:
- ❌ Requires "All Students" category to exist
- ❌ Slightly less flexible (can't exclude specific races)

**Use Cases**:
- School-level analyses (suspension rates for whole school)
- Analyses that don't need race breakdowns
- **Scripts 24 & 25** (teacher diversity effects at school level)

---

### Approach 2: Sum Across Races (FALLBACK)

**Code**:
```r
df %>%
  group_by(cds_school, academic_year) %>%
  summarise(
    total_suspensions = sum(total_suspensions, na.rm = TRUE),  # Sum races AND reasons
    cumulative_enrollment = max(cumulative_enrollment, na.rm = TRUE)  # Workaround
  )
```

**Pros**:
- ✅ Works even if "All Students" doesn't exist
- ✅ More flexible (can exclude specific races)
- ✅ Can create custom subtotals (e.g., "Students of Color")

**Cons**:
- ❌ **Less accurate**: We're re-creating what CDE already provided
- ❌ **Risk of double-counting**: If "All Students" row also exists, we sum it too!
- ❌ **Enrollment ambiguity**: Need `max()` workaround instead of constant value
- ❌ **Suppression handling**: May incorrectly sum suppressed values (asterisks)
- ❌ **More complex**: Extra aggregation step

**Use Cases**:
- Fallback when "All Students" doesn't exist
- Custom aggregations (e.g., "Non-White students" only)
- When you need to exclude specific races

---

## Detailed Comparison: Enrollment Handling

### With "All Students" (Correct)

```r
# After filtering to "All Students"
cds_school | academic_year | race         | reason    | enrollment
-----------|---------------|--------------|-----------|------------
12345      | 2023-24       | All Students | Defiance  | 500  (total school)
12345      | 2023-24       | All Students | Violent   | 500  (total school)
12345      | 2023-24       | All Students | Other     | 500  (total school)

# Aggregate across reasons
group_by(cds_school, academic_year) %>%
  summarise(
    cumulative_enrollment = first(enrollment)  # 500 - truly constant!
  )
```

**Result**: Enrollment = 500 (correct total school enrollment)

---

### Without "All Students" (Problematic)

```r
# All races included
cds_school | academic_year | race          | reason    | enrollment
-----------|---------------|---------------|-----------|------------
12345      | 2023-24       | Black         | Defiance  | 100  (Black students)
12345      | 2023-24       | Black         | Violent   | 100  (Black students)
12345      | 2023-24       | Hispanic      | Defiance  | 250  (Hispanic students)
12345      | 2023-24       | Hispanic      | Violent   | 250  (Hispanic students)
12345      | 2023-24       | White         | Defiance  | 150  (White students)
12345      | 2023-24       | White         | Violent   | 150  (White students)

# Aggregate across races and reasons
group_by(cds_school, academic_year) %>%
  summarise(
    cumulative_enrollment = first(enrollment)  # 100 (WRONG! Race-specific)
    # OR
    cumulative_enrollment = max(enrollment)    # 250 (WRONG! Largest race group)
  )
```

**Result**: Enrollment = 100 or 250 (incorrect - we wanted 500!)

**Why max() doesn't help here**: If enrollment values are race-specific (100, 250, 150), `max()` returns 250 (the largest race group), not 500 (total school).

**Why first() doesn't help**: If enrollment values vary by race, `first()` returns 100 (first race encountered), not 500 (total school).

---

## Real-World Example

### Scenario: Calculate suspension rate for a school

**School X in 2023-24**:
- Total enrollment: 500 students
- Black students: 100 (20% of school)
- Hispanic students: 250 (50% of school)
- White students: 150 (30% of school)
- Total suspensions: 50 (across all races)

**Correct calculation**:
```r
Suspension rate = 50 / 500 = 10%
```

**Using "All Students" row**:
```r
df %>%
  filter(race == "All Students") %>%
  summarise(
    suspensions = sum(suspensions),   # 50
    enrollment = first(enrollment)    # 500 (constant across All Students rows)
  ) %>%
  mutate(rate = suspensions / enrollment * 100)  # 50 / 500 = 10% ✓
```

**Using sum/max approach (WRONG)**:
```r
df %>%
  group_by(cds_school, academic_year) %>%
  summarise(
    suspensions = sum(suspensions),   # 50 (correct)
    enrollment = first(enrollment)    # 100 (WRONG! Black students)
    # OR
    enrollment = max(enrollment)      # 250 (WRONG! Hispanic students)
  ) %>%
  mutate(rate = suspensions / enrollment * 100)
# 50 / 100 = 50% ✗ (or 50 / 250 = 20% ✗)
```

---

## Why We Missed This Initially

### Assumptions That Led to Error

1. **Assumed enrollment was constant across races**:
   - We thought `cumulative_enrollment` would be 500 for ALL race rows
   - In reality, it's race-specific (100 for Black, 250 for Hispanic, etc.)

2. **Didn't check for "All Students" category**:
   - Didn't realize CDE provides pre-calculated totals
   - Went straight to aggregation approach

3. **Followed existing code patterns**:
   - Original scripts used summing, so we continued that pattern
   - Didn't question whether there was a better way

### How User Caught It

User asked a fundamental question:
> "Wouldn't it be more precise to use a row labeled 'Total'?"

This demonstrates good data intuition:
- **Always prefer authoritative source data** over re-calculations
- **Check for pre-calculated totals** before aggregating yourself
- **Question aggregation assumptions** (is enrollment truly constant?)

---

## Recommended Fix for Scripts 24 & 25

### Step 1: Check if "All Students" exists

```r
# Quick check
df_raw <- read_parquet("data-stage/susp_v6_teacher_features.parquet")

if ("race" %in% names(df_raw)) {
  cat("Race column exists!\n")
  cat("Unique races:\n")
  print(unique(df_raw$race))

  if ("All Students" %in% unique(df_raw$race)) {
    cat("\n✓ 'All Students' total exists! Use this instead of summing.\n")
  }
}
```

### Step 2: Update aggregation function

**Current** (scripts 24 & 25):
```r
aggregate_to_school_year <- function(df) {
  agg_df <- df %>%
    group_by(cds_school, academic_year) %>%
    summarise(
      across(any_of(susp_cols), ~sum(.x, na.rm = TRUE)),
      across(any_of(enrollment_cols), ~max(.x, na.rm = TRUE)),  # Workaround
      ...
    )
}
```

**Better**:
```r
aggregate_to_school_year <- function(df) {
  # STEP 1: Filter to "All Students" if it exists
  if ("race" %in% names(df) && "All Students" %in% unique(df$race)) {
    message(">>> Using 'All Students' total (CDE pre-calculated)")
    df <- df %>% filter(race == "All Students")
  } else {
    warning("'All Students' not found. Using sum aggregation (less accurate).")
  }

  # STEP 2: Aggregate across reasons
  agg_df <- df %>%
    group_by(cds_school, academic_year) %>%
    summarise(
      across(any_of(susp_cols), ~sum(.x, na.rm = TRUE)),      # Sum reasons
      across(any_of(enrollment_cols), ~first(.x)),            # Now truly constant
      ...
    )
}
```

### Step 3: Validate

After updating, check:
```r
# Enrollment should be realistic
summary(df_aggregated$cumulative_enrollment)
# Expected: Min ~ 100, Median ~ 500, Max ~ 3000

# Suspension rates should be realistic
df_aggregated %>%
  mutate(rate = total_suspensions / cumulative_enrollment * 100) %>%
  pull(rate) %>%
  summary()
# Expected: Min ~ 0%, Median ~ 5-10%, Max ~ 30%
```

---

## Implementation Roadmap

### Immediate Actions

1. **Verify data structure**:
   - Check if `susp_v6_teacher_features.parquet` has `race` column
   - Check if "All Students" category exists
   - Confirm enrollment values (constant or race-specific?)

2. **Update aggregation**:
   - Modify scripts 24 & 25 to filter `race == "All Students"` first
   - Update `R/aggregate_school_year.R` with this approach
   - Add validation to warn if "All Students" missing

3. **Re-run analyses**:
   - Run scripts 24 & 25 with corrected aggregation
   - Verify suspension rates are realistic (3-10%, not thousands)
   - Compare results with old approach

4. **Update documentation**:
   - Document this discovery in diagnostic report
   - Update `R/aggregate_school_year.R` with clear explanation
   - Add validation script to check for "All Students"

### Long-term Prevention

1. **Data structure validation**:
   - Document expected data structure in `CLAUDE.md`
   - Add checks for "All Students" category in ingestion scripts
   - Validate that enrollment is constant within race-category

2. **Standardized aggregation**:
   - Use `R/aggregate_school_year_v2.R` (filters to "All Students" first)
   - Add this to pipeline documentation
   - Update all scripts that aggregate to school-year level

3. **User education**:
   - Document when to use "All Students" vs. summing
   - Provide decision tree for aggregation approach
   - Add examples in `CLAUDE.md`

---

## Lessons Learned

### For Data Analysis

1. **Always check for pre-calculated totals** in source data
2. **Don't assume enrollment is constant** across categories
3. **Prefer authoritative source aggregations** over re-calculations
4. **Validate assumptions** (e.g., "enrollment is constant")
5. **Question inherited code** (just because it's there doesn't mean it's optimal)

### For Code Review

1. **User insights are valuable** - non-experts often ask fundamental questions
2. **"Why sum when we have a Total row?"** is a great sanity check
3. **Efficiency gains often indicate correctness** - simpler is usually better
4. **Test edge cases** - what if enrollment varies by race?

---

## Conclusion

**User's Question**: "Wouldn't it be more precise to use a row labeled 'Total'?"

**Answer**: **Absolutely yes!**

Using CDE's pre-calculated "All Students" (TA) category is:
- **More accurate** (CDE's official totals)
- **More efficient** (8x data reduction)
- **More robust** (handles suppression correctly)
- **Simpler** (one filter vs. complex aggregation)

This is a textbook example of why it's important to:
1. Understand your data structure thoroughly
2. Check for pre-existing aggregations
3. Question assumptions ("Is enrollment really constant?")
4. Listen to user insights (even from "novice coders")

**Next Steps**:
1. Verify `susp_v6_teacher_features.parquet` has "All Students" category
2. Update scripts 24 & 25 to filter to "All Students" first
3. Re-run analyses and verify realistic suspension rates
4. Update standardized aggregation function

---

**Document Version**: 1.0
**Created**: 2025-11-20
**Author**: REACH Suspensions Analysis Team
**Credit**: User insight about using "Total" row instead of summing
**Related Files**:
- `R/aggregate_school_year_v2.R` (improved aggregation function)
- `docs/fixes/FIX_SCRIPT_24_25_AGGREGATION.md` (original fix documentation)
- `Analysis/24_quartile_slope_comparison.R` (needs update)
- `Analysis/25_interaction_term_regression.R` (needs update)

**END OF DOCUMENT**
