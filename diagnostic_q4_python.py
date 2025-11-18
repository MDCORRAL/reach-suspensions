#!/usr/bin/env python3
"""
Diagnostic script to investigate missing suspension data in Q4 Black enrollment schools
"""

import pandas as pd
import sys

print("=== DIAGNOSTIC: Q4 Suspension Data Investigation ===\n")

# Paths
v6_long_path = "data-stage/susp_v6_long.parquet"
v6_features_path = "data-stage/susp_v6_features.parquet"
teacher_features_path = "data-stage/susp_v6_teacher_features.parquet"

# ============================================================================
# STEP 1: Check original v6_long data
# ============================================================================

print("STEP 1: Checking susp_v6_long.parquet...")
try:
    v6_long = pd.read_parquet(v6_long_path)
    print(f"  Total rows: {len(v6_long):,}")
    print(f"  Unique schools: {v6_long['cds_school'].nunique():,}")
    print(f"  Columns: {list(v6_long.columns[:20])}")

    # Check for Q4 schools
    q4_v6 = v6_long[v6_long['black_prop_q'].notna() & (v6_long['black_prop_q'] == 4)]

    print(f"\n  Q4 schools in v6_long:")
    print(f"    Total rows: {len(q4_v6):,}")
    print(f"    Unique schools: {q4_v6['cds_school'].nunique():,}")

    # Check suspension columns
    susp_cols = [col for col in v6_long.columns if 'suspension' in col.lower() or 'susp' in col.lower()]
    print(f"\n  Suspension columns: {', '.join(susp_cols)}")

    # Check data availability
    print("\n  Suspension data availability in Q4 schools:")
    for col in susp_cols:
        if col in q4_v6.columns:
            non_na = q4_v6[col].notna().sum()
            pct = 100 * non_na / len(q4_v6) if len(q4_v6) > 0 else 0
            print(f"    {col:40s}: {non_na:8,} / {len(q4_v6):8,} ({pct:5.1f}%)")

    # Sample Q4 schools with missing suspension data
    print("\n  Sample Q4 schools with missing suspension data:")
    missing_susp_v6 = q4_v6[q4_v6['total_suspensions'].isna()]
    if len(missing_susp_v6) > 0:
        cols_to_show = ['academic_year', 'cds_school', 'school_name', 'reporting_category',
                        'cumulative_enrollment', 'black_share', 'black_prop_q',
                        'total_suspensions', 'suspension_rate_percent_total']
        cols_to_show = [c for c in cols_to_show if c in missing_susp_v6.columns]
        print(missing_susp_v6[cols_to_show].head(10).to_string())
    else:
        print("    No missing suspension data in v6_long!")

except Exception as e:
    print(f"  ERROR: {e}")
    import traceback
    traceback.print_exc()

# ============================================================================
# STEP 2: Check v6_features data
# ============================================================================

print("\n\nSTEP 2: Checking susp_v6_features.parquet...")
try:
    v6_features = pd.read_parquet(v6_features_path)
    print(f"  Total rows: {len(v6_features):,}")
    print(f"  Unique schools: {v6_features['cds_school'].nunique():,}")

    # Check for is_traditional flag
    print(f"  is_traditional coverage: {v6_features['is_traditional'].notna().sum():,} / {len(v6_features):,}")

    q4_features = v6_features[v6_features['black_prop_q'].notna() & (v6_features['black_prop_q'] == 4)]

    print(f"\n  Q4 schools in v6_features:")
    print(f"    Total rows: {len(q4_features):,}")
    print(f"    Unique schools: {q4_features['cds_school'].nunique():,}")

    # Check suspension data in features
    susp_cols_feat = [col for col in v6_features.columns if 'suspension' in col.lower() or 'susp' in col.lower()]
    print(f"\n  Suspension columns in features: {', '.join(susp_cols_feat[:10])}")

    print("\n  Suspension data availability in Q4 features:")
    for col in susp_cols_feat[:10]:  # First 10 to avoid clutter
        if col in q4_features.columns:
            non_na = q4_features[col].notna().sum()
            pct = 100 * non_na / len(q4_features) if len(q4_features) > 0 else 0
            print(f"    {col:40s}: {non_na:8,} / {len(q4_features):8,} ({pct:5.1f}%)")

except Exception as e:
    print(f"  ERROR: {e}")
    import traceback
    traceback.print_exc()

# ============================================================================
# STEP 3: Check merged teacher-features data
# ============================================================================

print("\n\nSTEP 3: Checking susp_v6_teacher_features.parquet...")
try:
    teacher_features = pd.read_parquet(teacher_features_path)
    print(f"  Total rows: {len(teacher_features):,}")
    print(f"  Unique schools: {teacher_features['cds_school'].nunique():,}")

    q4_teacher = teacher_features[teacher_features['black_prop_q'].notna() & (teacher_features['black_prop_q'] == 4)]

    print(f"\n  Q4 schools in teacher_features:")
    print(f"    Total rows: {len(q4_teacher):,}")
    print(f"    Unique schools: {q4_teacher['cds_school'].nunique():,}")

    # Check suspension data
    susp_cols_teacher = [col for col in teacher_features.columns if 'suspension' in col.lower() or 'susp' in col.lower()]
    print(f"\n  Suspension columns in teacher_features: {', '.join(susp_cols_teacher[:10])}")

    print("\n  Suspension data availability in Q4 teacher_features:")
    for col in susp_cols_teacher[:10]:
        if col in q4_teacher.columns:
            non_na = q4_teacher[col].notna().sum()
            pct = 100 * non_na / len(q4_teacher) if len(q4_teacher) > 0 else 0
            print(f"    {col:40s}: {non_na:8,} / {len(q4_teacher):8,} ({pct:5.1f}%)")

    # Sample Q4 schools with missing suspension data
    print("\n  Sample Q4 schools with missing suspension data in teacher_features:")
    missing_susp_teacher = q4_teacher[q4_teacher['total_suspensions'].isna()]
    if len(missing_susp_teacher) > 0:
        cols_to_show = ['academic_year', 'cds_school', 'school_name', 'reporting_category',
                        'cumulative_enrollment', 'black_share', 'black_prop_q',
                        'total_suspensions', 'suspension_rate_percent_total']
        cols_to_show = [c for c in cols_to_show if c in missing_susp_teacher.columns]
        print(missing_susp_teacher[cols_to_show].head(10).to_string())
    else:
        print("    No missing suspension data!")

except Exception as e:
    print(f"  ERROR: {e}")
    import traceback
    traceback.print_exc()

# ============================================================================
# STEP 4: Check what happens after is_traditional filter
# ============================================================================

print("\n\nSTEP 4: Checking traditional schools filter impact...")
try:
    # Load features to get is_traditional
    features_for_join = pd.read_parquet(v6_features_path)[['cds_school', 'academic_year', 'is_traditional', 'black_share', 'white_share', 'hispanic_share']]

    # Join to teacher_features (mimicking script 23)
    teacher_with_trad = teacher_features.merge(features_for_join, on=['cds_school', 'academic_year'], how='left', suffixes=('', '_feat'))

    # Filter like script 23
    q4_traditional = teacher_with_trad[
        (teacher_with_trad['is_traditional'] == True) &
        (teacher_with_trad['black_prop_q'].notna()) &
        (teacher_with_trad['black_prop_q'] == 4)
    ].drop_duplicates(subset=['academic_year', 'cds_school'])

    print(f"  Q4 traditional schools (one row per school-year):")
    print(f"    Total rows: {len(q4_traditional):,}")
    print(f"    Unique schools: {q4_traditional['cds_school'].nunique():,}")

    # Check suspension data after all filters
    print("\n  Suspension data after traditional filter:")
    print(f"    With total_suspensions: {q4_traditional['total_suspensions'].notna().sum():,}")
    print(f"    With suspension_rate_percent_total: {q4_traditional['suspension_rate_percent_total'].notna().sum():,}")
    print(f"    Missing both: {(q4_traditional['total_suspensions'].isna() & q4_traditional['suspension_rate_percent_total'].isna()).sum():,}")

except Exception as e:
    print(f"  ERROR: {e}")
    import traceback
    traceback.print_exc()

# ============================================================================
# STEP 5: Detailed analysis of missing data patterns
# ============================================================================

print("\n\nSTEP 5: Analyzing patterns in missing suspension data...")
try:
    # By year
    by_year = q4_traditional.groupby('academic_year').agg(
        total_schools=('cds_school', 'count'),
        with_susp_data=('total_suspensions', lambda x: x.notna().sum())
    )
    by_year['pct_with_data'] = (by_year['with_susp_data'] / by_year['total_schools'] * 100).round(1)

    print("\n  By academic year:")
    print(by_year.to_string())

    # By school level
    if 'school_level' in q4_traditional.columns:
        by_level = q4_traditional[q4_traditional['school_level'].notna()].groupby('school_level').agg(
            total_schools=('cds_school', 'count'),
            with_susp_data=('total_suspensions', lambda x: x.notna().sum())
        )
        by_level['pct_with_data'] = (by_level['with_susp_data'] / by_level['total_schools'] * 100).round(1)

        print("\n  By school level:")
        print(by_level.to_string())

    # By enrollment size
    q4_traditional['enrollment_category'] = pd.cut(
        q4_traditional['cumulative_enrollment'],
        bins=[0, 100, 500, 1000, float('inf')],
        labels=['< 100', '100-499', '500-999', '1000+'],
        include_lowest=True
    )

    enrollment_analysis = q4_traditional.groupby('enrollment_category', observed=False).agg(
        total_schools=('cds_school', 'count'),
        with_susp_data=('total_suspensions', lambda x: x.notna().sum())
    )
    enrollment_analysis['pct_with_data'] = (enrollment_analysis['with_susp_data'] / enrollment_analysis['total_schools'] * 100).round(1)

    print("\n  By enrollment size:")
    print(enrollment_analysis.to_string())

except Exception as e:
    print(f"  ERROR: {e}")
    import traceback
    traceback.print_exc()

# ============================================================================
# STEP 6: Check if it's a race-specific issue
# ============================================================================

print("\n\nSTEP 6: Checking if it's a race/reporting_category issue...")
try:
    if 'reporting_category' in teacher_features.columns:
        by_race = q4_teacher[q4_teacher['reporting_category'].notna()].groupby('reporting_category').agg(
            total_rows=('cds_school', 'count'),
            with_susp_data=('total_suspensions', lambda x: x.notna().sum())
        )
        by_race['pct_with_data'] = (by_race['with_susp_data'] / by_race['total_rows'] * 100).round(1)

        print("\n  By reporting category:")
        print(by_race.to_string())
except Exception as e:
    print(f"  ERROR: {e}")
    import traceback
    traceback.print_exc()

# ============================================================================
# STEP 7: Write diagnostic outputs
# ============================================================================

print("\n\nSTEP 7: Writing diagnostic outputs...")
try:
    # Export schools with missing data for manual review
    missing_data_export = q4_traditional[q4_traditional['total_suspensions'].isna()]

    cols_to_export = ['academic_year', 'cds_school', 'county_name', 'district_name', 'school_name',
                      'school_level', 'locale_simple', 'cumulative_enrollment', 'black_share', 'black_prop_q',
                      'total_suspensions', 'suspension_rate_percent_total']
    cols_to_export = [c for c in cols_to_export if c in missing_data_export.columns]

    # Add teacher columns
    teacher_cols = [c for c in missing_data_export.columns if c.startswith('teacher_staff_count_total')]
    cols_to_export.extend([c for c in teacher_cols if c in missing_data_export.columns])

    missing_data_export = missing_data_export[cols_to_export].sort_values('cumulative_enrollment', ascending=False)

    out_path = "outputs/tables/DIAGNOSTIC_q4_missing_suspension_data.csv"
    missing_data_export.to_csv(out_path, index=False)
    print(f"  Wrote: {out_path}")
    print(f"  Schools with missing suspension data: {len(missing_data_export):,}")

except Exception as e:
    print(f"  ERROR: {e}")
    import traceback
    traceback.print_exc()

print("\n=== DIAGNOSTIC COMPLETE ===")
