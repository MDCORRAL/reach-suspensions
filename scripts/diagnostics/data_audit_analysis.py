#!/usr/bin/env python3
"""
Data Audit Analysis Script
Quantifies data loss through the processing pipeline and identifies recovery opportunities.
"""

import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import json

# Paths
PROJECT_ROOT = Path(__file__).parent
DATA_STAGE = PROJECT_ROOT / "data-stage"

# Output directory for audit results
AUDIT_OUTPUT = PROJECT_ROOT / "outputs" / "data_audit"
AUDIT_OUTPUT.mkdir(parents=True, exist_ok=True)

def safe_count(df, description):
    """Count records and print status."""
    count = len(df) if df is not None and not df.empty else 0
    print(f"{description}: {count:,} records")
    return count

def analyze_pipeline_stages():
    """Analyze data volume at each pipeline stage."""

    print("=" * 80)
    print("DATA PIPELINE AUDIT - QUANTIFYING RECORDS AT EACH STAGE")
    print("=" * 80)

    results = {}

    # Stage 0: Initial ingestion
    print("\n### STAGE 0: Initial Ingestion (susp_v0.parquet)")
    v0_path = DATA_STAGE / "susp_v0.parquet"
    if v0_path.exists():
        v0 = pq.read_table(v0_path).to_pandas()
        results['v0_total'] = safe_count(v0, "  Total records after ingestion")

        # Analyze charter_yn distribution
        print("\n  Charter_yn distribution:")
        charter_counts = v0['charter_yn'].value_counts(dropna=False)
        for val, count in charter_counts.items():
            print(f"    {val}: {count:,}")
        results['v0_charter_all'] = charter_counts.get('All', 0)

        # Aggregate level distribution
        if 'aggregate_level' in v0.columns:
            print("\n  Aggregate_level distribution:")
            agg_counts = v0['aggregate_level'].value_counts(dropna=False)
            for val, count in agg_counts.items():
                print(f"    {val}: {count:,}")
            results['v0_school_level'] = v0[v0['aggregate_level'].str.lower().isin(['s', 'school'])].shape[0]

        # Special school codes
        if 'school_code' in v0.columns:
            special_codes = v0['school_code'].astype(str).isin(['0000000', '0000001'])
            results['v0_special_codes'] = special_codes.sum()
            print(f"\n  Special school codes (0000000, 0000001): {results['v0_special_codes']:,}")

        del v0
    else:
        print("  FILE NOT FOUND")

    # Stage 1: After locale features
    print("\n### STAGE 1: After Locale Features (susp_v1.parquet)")
    v1_path = DATA_STAGE / "susp_v1.parquet"
    if v1_path.exists():
        v1 = pq.read_table(v1_path).to_pandas()
        results['v1_total'] = safe_count(v1, "  Total records")
        del v1

    # Stage 1-noall: After dropping charter "All"
    print("\n### STAGE 1-NOALL: After Dropping Charter 'All' (susp_v1_noall.parquet)")
    v1_noall_path = DATA_STAGE / "susp_v1_noall.parquet"
    if v1_noall_path.exists():
        v1_noall = pq.read_table(v1_noall_path).to_pandas()
        results['v1_noall_total'] = safe_count(v1_noall, "  Total records")
        if 'v1_total' in results:
            lost = results['v1_total'] - results['v1_noall_total']
            print(f"  Records LOST by dropping charter 'All': {lost:,}")
            results['lost_charter_all'] = lost
        del v1_noall

    # Stage 5: After reason shares
    print("\n### STAGE 5: After Reason Shares (susp_v5.parquet and susp_v5_long.parquet)")
    v5_path = DATA_STAGE / "susp_v5.parquet"
    v5_long_path = DATA_STAGE / "susp_v5_long.parquet"

    if v5_path.exists():
        v5 = pq.read_table(v5_path).to_pandas()
        results['v5_wide_total'] = safe_count(v5, "  v5 (wide) total records")
        del v5

    if v5_long_path.exists():
        v5_long = pq.read_table(v5_long_path).to_pandas()
        results['v5_long_total'] = safe_count(v5_long, "  v5_long total records")

        if 'subgroup' in v5_long.columns:
            print("\n  Subgroup distribution in v5_long:")
            subgroup_counts = v5_long['subgroup'].value_counts().head(15)
            for val, count in subgroup_counts.items():
                print(f"    {val}: {count:,}")
        del v5_long

    # Stage 6: Demographics (oth_long.parquet)
    print("\n### DEMOGRAPHICS: Other Demographic Data (oth_long.parquet)")
    oth_path = DATA_STAGE / "oth_long.parquet"
    if oth_path.exists():
        oth = pq.read_table(oth_path).to_pandas()
        results['oth_total'] = safe_count(oth, "  Total demographic records")

        if 'category_type' in oth.columns:
            print("\n  Category type distribution:")
            cat_counts = oth['category_type'].value_counts()
            for val, count in cat_counts.items():
                print(f"    {val}: {count:,}")

        # Check for impossible values (num > den)
        if all(col in oth.columns for col in ['unduplicated_suspensions', 'cumulative_enrollment']):
            impossible = oth[
                (oth['unduplicated_suspensions'].notna()) &
                (oth['cumulative_enrollment'].notna()) &
                ((oth['unduplicated_suspensions'] < 0) |
                 (oth['cumulative_enrollment'] <= 0) |
                 (oth['unduplicated_suspensions'] > oth['cumulative_enrollment']))
            ]
            results['oth_impossible'] = len(impossible)
            print(f"\n  Records with impossible num/den: {len(impossible):,}")

        del oth

    # Stage 6: Final v6 features
    print("\n### STAGE 6: Final v6 Features (susp_v6_features.parquet)")
    v6_feat_path = DATA_STAGE / "susp_v6_features.parquet"
    if v6_feat_path.exists():
        v6_feat = pq.read_table(v6_feat_path).to_pandas()
        results['v6_features_total'] = safe_count(v6_feat, "  Total campus-year records")

        if 'is_traditional' in v6_feat.columns:
            trad_counts = v6_feat['is_traditional'].value_counts(dropna=False)
            print("\n  Traditional status:")
            for val, count in trad_counts.items():
                print(f"    {val}: {count:,}")
            results['v6_traditional'] = trad_counts.get(True, 0)
            results['v6_nontraditional'] = trad_counts.get(False, 0)

        del v6_feat

    # Stage 6: Final v6 long
    print("\n### STAGE 6: Final v6 Long (susp_v6_long.parquet)")
    v6_long_path = DATA_STAGE / "susp_v6_long.parquet"
    if v6_long_path.exists():
        v6_long = pq.read_table(v6_long_path).to_pandas()
        results['v6_long_total'] = safe_count(v6_long, "  Total records")

        # Analyze subgroups
        if 'subgroup' in v6_long.columns:
            print("\n  Subgroup distribution:")
            subgroup_counts = v6_long['subgroup'].value_counts().head(15)
            for val, count in subgroup_counts.items():
                print(f"    {val}: {count:,}")

        # Analyze by school level
        if 'school_level' in v6_long.columns:
            print("\n  School level distribution:")
            level_counts = v6_long['school_level'].value_counts(dropna=False)
            for val, count in level_counts.items():
                print(f"    {val}: {count:,}")

        # Check aggregate_level if present
        if 'aggregate_level' in v6_long.columns:
            agg_counts = v6_long['aggregate_level'].value_counts(dropna=False)
            print("\n  Aggregate level distribution:")
            for val, count in agg_counts.items():
                print(f"    {val}: {count:,}")
            campus_only = v6_long[v6_long['aggregate_level'].str.lower().isin(['s', 'school'])]
            results['v6_long_campus_only'] = len(campus_only)
            print(f"  Campus-level only: {results['v6_long_campus_only']:,}")

        del v6_long

    return results

def analyze_filtering_impact():
    """Analyze the impact of common filtering operations."""

    print("\n" + "=" * 80)
    print("FILTERING IMPACT ANALYSIS")
    print("=" * 80)

    v6_long_path = DATA_STAGE / "susp_v6_long.parquet"
    if not v6_long_path.exists():
        print("susp_v6_long.parquet not found")
        return {}

    # Read with specific columns to save memory
    columns = [
        'school_code', 'academic_year', 'subgroup', 'aggregate_level',
        'cumulative_enrollment', 'total_suspensions', 'school_level',
        'locale_simple', 'black_prop_q_label', 'white_prop_q_label',
        'hispanic_prop_q_label'
    ]

    v6_long = pq.read_table(v6_long_path, columns=columns).to_pandas()

    results = {}
    total = len(v6_long)
    results['total'] = total
    print(f"\nStarting with v6_long: {total:,} records")

    # Impact of campus-only filter
    print("\n### Impact of Campus-Only Filter")
    if 'aggregate_level' in v6_long.columns:
        campus_mask = v6_long['aggregate_level'].str.lower().isin(['s', 'school'])
        campus_count = campus_mask.sum()
        non_campus = total - campus_count
        results['campus_only'] = campus_count
        results['lost_non_campus'] = non_campus
        print(f"  Campus-level records: {campus_count:,}")
        print(f"  Non-campus records (would be excluded): {non_campus:,} ({100*non_campus/total:.1f}%)")

    # Impact of special school codes
    print("\n### Impact of Special School Codes Filter")
    if 'school_code' in v6_long.columns:
        special_mask = v6_long['school_code'].astype(str).isin(['0000000', '0000001'])
        special_count = special_mask.sum()
        results['lost_special_codes'] = special_count
        print(f"  Special school code records (would be excluded): {special_count:,} ({100*special_count/total:.1f}%)")

    # Impact of "All Students"/"Total" filter
    print("\n### Impact of 'All Students'/'Total' Subgroup Filter")
    if 'subgroup' in v6_long.columns:
        all_students_mask = v6_long['subgroup'].str.lower().isin(['total', 'all students', 'ta'])
        all_students_count = all_students_mask.sum()
        other_subgroups = total - all_students_count
        results['all_students_only'] = all_students_count
        results['other_subgroups'] = other_subgroups
        print(f"  'All Students'/'Total' records: {all_students_count:,}")
        print(f"  Other subgroup records (excluded by dashboard/graphs): {other_subgroups:,} ({100*other_subgroups/total:.1f}%)")

    # Impact of missing enrollment/suspensions
    print("\n### Impact of Missing Data Filter")
    missing_enrollment = v6_long['cumulative_enrollment'].isna().sum()
    missing_suspensions = v6_long['total_suspensions'].isna().sum()
    zero_enrollment = (v6_long['cumulative_enrollment'] == 0).sum()
    negative_suspensions = (v6_long['total_suspensions'] < 0).sum()

    results['missing_enrollment'] = missing_enrollment
    results['missing_suspensions'] = missing_suspensions
    results['zero_enrollment'] = zero_enrollment
    results['negative_suspensions'] = negative_suspensions

    print(f"  Missing enrollment: {missing_enrollment:,} ({100*missing_enrollment/total:.1f}%)")
    print(f"  Missing suspensions: {missing_suspensions:,} ({100*missing_suspensions/total:.1f}%)")
    print(f"  Zero enrollment: {zero_enrollment:,} ({100*zero_enrollment/total:.1f}%)")
    print(f"  Negative suspensions: {negative_suspensions:,} ({100*negative_suspensions/total:.1f}%)")

    # Impact of quartile filters
    print("\n### Impact of Unknown Quartile Filters")
    for q_col, q_name in [
        ('black_prop_q_label', 'Black'),
        ('white_prop_q_label', 'White'),
        ('hispanic_prop_q_label', 'Hispanic/Latino')
    ]:
        if q_col in v6_long.columns:
            unknown_mask = v6_long[q_col].isna() | (v6_long[q_col] == 'Unknown')
            unknown_count = unknown_mask.sum()
            results[f'unknown_{q_name.lower().replace("/", "_")}_quartile'] = unknown_count
            print(f"  Unknown {q_name} quartile: {unknown_count:,} ({100*unknown_count/total:.1f}%)")

    # Combined typical analysis filter (Traditional + All Students + campus-only)
    print("\n### Combined 'Typical Analysis' Filter Impact")
    print("  (Campus-only + 'All Students' + valid enrollment/suspensions)")

    # Load is_traditional from features
    v6_feat_path = DATA_STAGE / "susp_v6_features.parquet"
    if v6_feat_path.exists():
        v6_feat = pq.read_table(
            v6_feat_path,
            columns=['school_code', 'academic_year', 'is_traditional']
        ).to_pandas()

        # Merge to get traditional status
        v6_with_trad = v6_long.merge(
            v6_feat,
            on=['school_code', 'academic_year'],
            how='left'
        )

        # Apply typical filters
        typical_mask = (
            (v6_with_trad['aggregate_level'].str.lower().isin(['s', 'school'])) &
            (~v6_with_trad['school_code'].astype(str).isin(['0000000', '0000001'])) &
            (v6_with_trad['subgroup'].str.lower().isin(['total', 'all students', 'ta'])) &
            (v6_with_trad['cumulative_enrollment'].notna()) &
            (v6_with_trad['total_suspensions'].notna()) &
            (v6_with_trad['cumulative_enrollment'] > 0) &
            (v6_with_trad['total_suspensions'] >= 0) &
            (v6_with_trad['is_traditional'] == True)
        )

        typical_count = typical_mask.sum()
        typical_excluded = total - typical_count
        results['typical_analysis_included'] = typical_count
        results['typical_analysis_excluded'] = typical_excluded

        print(f"  Records INCLUDED in typical analysis: {typical_count:,} ({100*typical_count/total:.1f}%)")
        print(f"  Records EXCLUDED from typical analysis: {typical_excluded:,} ({100*typical_excluded/total:.1f}%)")

    return results

def identify_recovery_opportunities():
    """Identify specific data that could be recovered."""

    print("\n" + "=" * 80)
    print("DATA RECOVERY OPPORTUNITIES")
    print("=" * 80)

    opportunities = []

    v6_long_path = DATA_STAGE / "susp_v6_long.parquet"
    if not v6_long_path.exists():
        return opportunities

    # Read full dataset
    v6_long = pq.read_table(v6_long_path).to_pandas()
    v6_feat_path = DATA_STAGE / "susp_v6_features.parquet"

    if v6_feat_path.exists():
        v6_feat = pq.read_table(
            v6_feat_path,
            columns=['school_code', 'academic_year', 'is_traditional']
        ).to_pandas()
        v6_long = v6_long.merge(v6_feat, on=['school_code', 'academic_year'], how='left')

    # 1. Non-traditional schools
    if 'is_traditional' in v6_long.columns:
        non_trad = v6_long[v6_long['is_traditional'] == False]
        if len(non_trad) > 0:
            non_trad_valid = non_trad[
                (non_trad['cumulative_enrollment'].notna()) &
                (non_trad['total_suspensions'].notna()) &
                (non_trad['cumulative_enrollment'] > 0)
            ]
            opp = {
                'category': 'Non-Traditional Schools',
                'total_records': len(non_trad),
                'valid_records': len(non_trad_valid),
                'description': 'Alternative, continuation, and other non-traditional schools excluded from most analyses',
                'recovery_action': 'Create separate analysis track for non-traditional schools',
                'impact': 'High - represents distinct student population'
            }
            opportunities.append(opp)
            print(f"\n1. NON-TRADITIONAL SCHOOLS:")
            print(f"   Total records: {len(non_trad):,}")
            print(f"   Valid records: {len(non_trad_valid):,}")
            print(f"   Action: {opp['recovery_action']}")

    # 2. Race/ethnicity subgroups (not "All Students")
    if 'subgroup' in v6_long.columns:
        race_subgroups = v6_long[
            ~v6_long['subgroup'].str.lower().isin(['total', 'all students', 'ta']) &
            v6_long['subgroup'].notna()
        ]
        if len(race_subgroups) > 0:
            opp = {
                'category': 'Race/Ethnicity Subgroups',
                'total_records': len(race_subgroups),
                'unique_subgroups': race_subgroups['subgroup'].nunique(),
                'description': 'Detailed race/ethnicity data currently used in some analyses but not all',
                'recovery_action': 'Ensure all visualizations offer race/ethnicity breakdown',
                'impact': 'High - critical for equity analysis'
            }
            opportunities.append(opp)
            print(f"\n2. RACE/ETHNICITY SUBGROUPS:")
            print(f"   Total records: {len(race_subgroups):,}")
            print(f"   Unique subgroups: {opp['unique_subgroups']}")
            print(f"   Top subgroups: {', '.join(race_subgroups['subgroup'].value_counts().head(5).index.tolist())}")

    # 3. Demographic subgroups (SPED, ELL, etc.) - from oth_long
    oth_path = DATA_STAGE / "oth_long.parquet"
    if oth_path.exists():
        oth = pq.read_table(oth_path).to_pandas()
        opp = {
            'category': 'Demographic Subgroups (SPED, ELL, etc.)',
            'total_records': len(oth),
            'unique_categories': oth['category_type'].nunique() if 'category_type' in oth.columns else 'N/A',
            'description': 'Students with Disabilities, English Learners, Foster Youth, etc.',
            'recovery_action': 'Expand dashboard to include intersectional analyses',
            'impact': 'High - critical for understanding disproportionality'
        }
        opportunities.append(opp)
        print(f"\n3. DEMOGRAPHIC SUBGROUPS:")
        print(f"   Total records: {len(oth):,}")
        if 'category_type' in oth.columns:
            print(f"   Categories: {', '.join(oth['category_type'].unique().tolist())}")

    # 4. Unknown quartiles
    unknown_quartile_count = 0
    for q_col in ['black_prop_q_label', 'white_prop_q_label', 'hispanic_prop_q_label']:
        if q_col in v6_long.columns:
            unknown_quartile_count += (v6_long[q_col].isna() | (v6_long[q_col] == 'Unknown')).sum()

    if unknown_quartile_count > 0:
        opp = {
            'category': 'Unknown Quartile Schools',
            'total_records': unknown_quartile_count,
            'description': 'Schools without assigned enrollment composition quartiles',
            'recovery_action': 'Investigate why quartiles are missing; recalculate if possible',
            'impact': 'Medium - affects quartile-based analyses'
        }
        opportunities.append(opp)
        print(f"\n4. UNKNOWN QUARTILE SCHOOLS:")
        print(f"   Total records with unknown quartiles: {unknown_quartile_count:,}")

    # 5. Charter "All" rows (if they still exist in v0)
    v0_path = DATA_STAGE / "susp_v0.parquet"
    if v0_path.exists():
        v0 = pq.read_table(v0_path).to_pandas()
        if 'charter_yn' in v0.columns:
            charter_all = v0[v0['charter_yn'] == 'All']
            if len(charter_all) > 0:
                opp = {
                    'category': 'Charter "All" Aggregate Records',
                    'total_records': len(charter_all),
                    'description': 'Aggregate rows that sum charter + non-charter (dropped to prevent double-counting)',
                    'recovery_action': 'No recovery needed - intentionally excluded',
                    'impact': 'N/A - would cause double-counting'
                }
                opportunities.append(opp)
                print(f"\n5. CHARTER 'ALL' AGGREGATE RECORDS:")
                print(f"   Total records: {len(charter_all):,}")
                print(f"   Action: {opp['recovery_action']}")

    return opportunities

def save_audit_report(pipeline_results, filtering_results, opportunities):
    """Save comprehensive audit report."""

    report = {
        'pipeline_stages': pipeline_results,
        'filtering_impact': filtering_results,
        'recovery_opportunities': opportunities,
        'summary': {
            'total_v0_records': pipeline_results.get('v0_total', 0),
            'final_v6_long_records': pipeline_results.get('v6_long_total', 0),
            'typical_analysis_records': filtering_results.get('typical_analysis_included', 0),
            'total_excluded_from_typical_analysis': filtering_results.get('typical_analysis_excluded', 0),
        }
    }

    # Save JSON report
    json_path = AUDIT_OUTPUT / "data_audit_report.json"
    with open(json_path, 'w') as f:
        json.dump(report, f, indent=2)
    print(f"\n\nSaved JSON report: {json_path}")

    # Save human-readable summary
    summary_path = AUDIT_OUTPUT / "data_audit_summary.txt"
    with open(summary_path, 'w') as f:
        f.write("DATA AUDIT SUMMARY\n")
        f.write("=" * 80 + "\n\n")

        f.write("PIPELINE OVERVIEW:\n")
        f.write(f"  Initial ingestion (v0): {pipeline_results.get('v0_total', 0):,} records\n")
        f.write(f"  Final v6_long: {pipeline_results.get('v6_long_total', 0):,} records\n")
        f.write(f"  Final v6_features (campus-years): {pipeline_results.get('v6_features_total', 0):,} records\n\n")

        f.write("KEY DATA EXCLUSIONS:\n")
        f.write(f"  Charter 'All' rows dropped: {pipeline_results.get('lost_charter_all', 0):,}\n")
        f.write(f"  Special school codes: {pipeline_results.get('v0_special_codes', 0):,}\n")
        f.write(f"  Non-traditional schools: {pipeline_results.get('v6_nontraditional', 0):,}\n")
        f.write(f"  Records excluded from typical analysis: {filtering_results.get('typical_analysis_excluded', 0):,}\n\n")

        f.write("RECOVERY OPPORTUNITIES:\n")
        for i, opp in enumerate(opportunities, 1):
            f.write(f"\n{i}. {opp['category']}\n")
            f.write(f"   Records: {opp.get('total_records', 'N/A'):,}\n")
            f.write(f"   Impact: {opp.get('impact', 'N/A')}\n")
            f.write(f"   Action: {opp.get('recovery_action', 'N/A')}\n")

    print(f"Saved summary report: {summary_path}")

    return report

def main():
    """Run complete data audit."""
    print("Starting Data Audit Analysis...")
    print("This will analyze all stages of the data processing pipeline.\n")

    # Run analyses
    pipeline_results = analyze_pipeline_stages()
    filtering_results = analyze_filtering_impact()
    opportunities = identify_recovery_opportunities()

    # Save report
    report = save_audit_report(pipeline_results, filtering_results, opportunities)

    print("\n" + "=" * 80)
    print("AUDIT COMPLETE")
    print("=" * 80)
    print(f"\nReports saved to: {AUDIT_OUTPUT}/")
    print("  - data_audit_report.json (detailed JSON)")
    print("  - data_audit_summary.txt (human-readable summary)")

if __name__ == '__main__':
    main()
