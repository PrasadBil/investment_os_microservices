
#!/usr/bin/env python3
"""
D7 V1.0 VS V2.0 COMPARISON SCRIPT
Compare scoring results between versions

FILE: compare_d7_versions.py
CREATED: 2026-01-07
AUTHOR: Investment OS

VERSION HISTORY:
    v1.0.0  2026-01-07  Initial creation — v1 vs v2 comparison with correlation analysis
    v1.0.1  2026-02-12  Migrated to services/scoring-7d (Phase 2 microservices)
    v1.0.2  2026-02-16  Added version history header (new project standard)
    v1.1.0  2026-02-21  Phase 1B-D support: auto-detect scorer version, surface
                         volume_trend, momentum_confluence, 5D/20D/60D breakdowns.
                         Added --save flag to write comparison CSV and report.

PURPOSE:
- Load v1.0 and v2.0 scores (auto-detects Phase 1A vs Phase 1B-D output)
- Calculate Pearson correlation
- Identify distributional differences
- Surface new Phase 1B-D signal dimensions (volume trend, momentum)
- Validate improvements before production promotion
"""

import pandas as pd
import numpy as np
import sys
import argparse
from datetime import datetime


# =============================================================================
# HELPERS
# =============================================================================

def pearson_correlation(x, y):
    """Calculate Pearson correlation without scipy."""
    x = np.array(x, dtype=float)
    y = np.array(y, dtype=float)

    # Remove NaN pairs
    mask = ~(np.isnan(x) | np.isnan(y))
    x = x[mask]
    y = y[mask]

    if len(x) < 2:
        return 0.0, 1.0

    mean_x = np.mean(x)
    mean_y = np.mean(y)

    numerator = np.sum((x - mean_x) * (y - mean_y))
    denominator = np.sqrt(np.sum((x - mean_x) ** 2) * np.sum((y - mean_y) ** 2))

    if denominator == 0:
        return 0.0, 1.0

    corr = numerator / denominator
    p_value = 0.001 if abs(corr) > 0.5 else 0.05

    return corr, p_value


def detect_scorer_version(v2: pd.DataFrame) -> str:
    """
    Auto-detect whether v2 scores are Phase 1A (3-component) or Phase 1B-D (5-component).

    Phase 1B-D marker columns: volume_trend, momentum_confluence.
    """
    if 'volume_trend' in v2.columns or 'momentum_confluence' in v2.columns:
        return 'phase1bcd'
    return 'phase1a'


# =============================================================================
# MAIN COMPARISON
# =============================================================================

def compare_versions(
    v1_file: str = 'dimension7_scores.csv',
    v2_file: str = 'dimension7_v2_scores.csv',
    save: bool = False,
    output_prefix: str = None
):
    """
    Compare D7 v1.0 and v2.0 scores.

    Args:
        v1_file:       v1.0 scores CSV
        v2_file:       v2.0 scores CSV (Phase 1A or Phase 1B-D, auto-detected)
        save:          If True, write merged CSV + text report to disk
        output_prefix: Prefix for saved files (default: 'd7_comparison_YYYYMMDD')
    """
    run_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    run_date = datetime.now().strftime('%Y%m%d')

    lines = []  # Collect all output for optional save

    def pr(msg=''):
        print(msg)
        lines.append(msg)

    pr("=" * 80)
    pr(f"D7 V1.0 VS V2.0 COMPARISON  |  Run: {run_ts}")
    pr("=" * 80)

    # ------------------------------------------------------------------
    # LOAD
    # ------------------------------------------------------------------
    try:
        pr(f"\n📊 Loading scores...")

        v1 = pd.read_csv(v1_file)
        pr(f"   v1.0  → {len(v1):,} stocks  |  file: {v1_file}")
        pr(f"   v1.0 columns: {list(v1.columns)}")

        v2 = pd.read_csv(v2_file)
        pr(f"   v2.0  → {len(v2):,} stocks  |  file: {v2_file}")
        pr(f"   v2.0 columns (first 8): {list(v2.columns[:8])}...")

    except FileNotFoundError as e:
        pr(f"\n❌ ERROR: File not found: {e}")
        pr("   Ensure both v1.0 and v2.0 scores are generated before comparing.")
        return None

    # ------------------------------------------------------------------
    # DETECT V2 PHASE
    # ------------------------------------------------------------------
    phase = detect_scorer_version(v2)
    pr(f"\n🔍 Detected v2.0 scorer version: {phase.upper()}")
    if phase == 'phase1bcd':
        pr("   ✅ Full 5-component scorer (Trading 30% | Volatility 10% | Recognition 25% | Volume 10% | Momentum 25%)")
    else:
        pr("   ⚠️  Phase 1A scorer (3 components). Upgrade to phase1bcd for full signal coverage.")

    # ------------------------------------------------------------------
    # FIND V1 SCORE COLUMN
    # ------------------------------------------------------------------
    v1_score_col = None
    for candidate in ['dimension7_score', 'd7_score', 'score', 'sentiment_score', 'dimension_7_score']:
        if candidate in v1.columns:
            v1_score_col = candidate
            break

    if v1_score_col is None:
        numeric_cols = v1.select_dtypes(include=[np.number]).columns.tolist()
        if numeric_cols:
            v1_score_col = numeric_cols[0]
            pr(f"   ⚠️  Using column '{v1_score_col}' as v1.0 score (auto-detected)")
        else:
            pr(f"\n❌ ERROR: Cannot find score column in v1.0 file")
            pr(f"   Available columns: {list(v1.columns)}")
            return None
    else:
        pr(f"   ✅ v1.0 score column: '{v1_score_col}'")

    v1 = v1.rename(columns={v1_score_col: 'dimension7_score'})

    # ------------------------------------------------------------------
    # MERGE — include all Phase 1B-D diagnostic columns if present
    # ------------------------------------------------------------------
    v2_cols = ['symbol', 'dimension7_v2_score', 'trade_density_class', 'volatility_level']
    phase_bcd_extras = ['volume_trend', 'momentum_confluence',
                        'momentum_5d_pct', 'momentum_20d_pct', 'momentum_60d_pct',
                        'momentum_5d_score', 'momentum_20d_score', 'momentum_60d_score']

    if phase == 'phase1bcd':
        v2_cols += [c for c in phase_bcd_extras if c in v2.columns]

    merged = v1[['symbol', 'dimension7_score']].merge(
        v2[v2_cols],
        on='symbol',
        how='inner'
    )

    pr(f"\n   Common stocks for comparison: {len(merged):,}")

    if len(merged) == 0:
        pr("\n❌ No common stocks found — check symbol formats match between files.")
        return None

    merged['delta'] = merged['dimension7_v2_score'] - merged['dimension7_score']

    # ------------------------------------------------------------------
    # CORRELATION
    # ------------------------------------------------------------------
    pr("\n" + "─" * 60)
    pr("📈  CORRELATION ANALYSIS")
    pr("─" * 60)
    corr, _ = pearson_correlation(merged['dimension7_score'], merged['dimension7_v2_score'])
    pr(f"   Pearson Correlation:  {corr:.3f}")

    if corr > 0.85:
        pr(f"   Assessment:          ✅ High agreement (>0.85) — ready for parallel deployment")
    elif corr > 0.70:
        pr(f"   Assessment:          ⚠️  Moderate agreement (0.70–0.85) — review differences")
    else:
        pr(f"   Assessment:          ❌ Low agreement (<0.70) — investigate methodology")

    # ------------------------------------------------------------------
    # DISTRIBUTION
    # ------------------------------------------------------------------
    pr("\n" + "─" * 60)
    pr("📊  DISTRIBUTION COMPARISON")
    pr("─" * 60)
    pr(f"   {'Metric':<12}  {'v1.0':>8}  {'v2.0':>8}  {'Delta':>8}")
    pr(f"   {'Mean':<12}  {merged['dimension7_score'].mean():>8.1f}  {merged['dimension7_v2_score'].mean():>8.1f}  {merged['delta'].mean():>+8.1f}")
    pr(f"   {'Median':<12}  {merged['dimension7_score'].median():>8.1f}  {merged['dimension7_v2_score'].median():>8.1f}  {(merged['dimension7_v2_score'].median() - merged['dimension7_score'].median()):>+8.1f}")
    pr(f"   {'Std Dev':<12}  {merged['dimension7_score'].std():>8.1f}  {merged['dimension7_v2_score'].std():>8.1f}  {(merged['dimension7_v2_score'].std() - merged['dimension7_score'].std()):>+8.1f}")
    pr(f"   {'Min':<12}  {merged['dimension7_score'].min():>8.1f}  {merged['dimension7_v2_score'].min():>8.1f}")
    pr(f"   {'Max':<12}  {merged['dimension7_score'].max():>8.1f}  {merged['dimension7_v2_score'].max():>8.1f}")

    improved = (merged['delta'] > 5).sum()
    similar = ((merged['delta'] >= -5) & (merged['delta'] <= 5)).sum()
    declined = (merged['delta'] < -5).sum()
    pr(f"\n   Score Movement:")
    pr(f"   Improved  (Δ > +5) : {improved:>4} stocks ({improved/len(merged)*100:.0f}%)")
    pr(f"   Similar   (±5)     : {similar:>4} stocks ({similar/len(merged)*100:.0f}%)")
    pr(f"   Declined  (Δ < -5) : {declined:>4} stocks ({declined/len(merged)*100:.0f}%)")

    # ------------------------------------------------------------------
    # TOP-20 STABILITY
    # ------------------------------------------------------------------
    pr("\n" + "─" * 60)
    pr("🏆  TOP-20 RANK STABILITY")
    pr("─" * 60)
    v1_top20 = set(v1.nlargest(20, 'dimension7_score')['symbol'])
    v2_top20 = set(v2.nlargest(20, 'dimension7_v2_score')['symbol'])
    overlap = len(v1_top20 & v2_top20)

    pr(f"   Common stocks in top-20:  {overlap}/20  ({overlap / 20 * 100:.0f}%)")
    pr(f"   Entered top-20 (v2 only): {sorted(v2_top20 - v1_top20)}")
    pr(f"   Exited top-20 (v1 only):  {sorted(v1_top20 - v2_top20)}")

    if overlap >= 14:
        pr(f"   Assessment:  ✅ Good stability (70%+)")
    elif overlap >= 10:
        pr(f"   Assessment:  ⚠️  Moderate churn (50–70%)")
    else:
        pr(f"   Assessment:  ❌ Major divergence (<50%) — investigate")

    # ------------------------------------------------------------------
    # TEST STOCKS
    # ------------------------------------------------------------------
    pr("\n" + "─" * 60)
    pr("🎯  TEST STOCKS (Key Holdings)")
    pr("─" * 60)
    test_stocks = ['CTC.N0000', 'LION.N0000', 'LOFC.N0000', 'LOLC.N0000', 'JKH.N0000']
    test_cols = ['symbol', 'dimension7_score', 'dimension7_v2_score', 'delta', 'trade_density_class']
    if phase == 'phase1bcd' and 'volume_trend' in merged.columns:
        test_cols += ['volume_trend', 'momentum_confluence']
    test_data = merged[merged['symbol'].isin(test_stocks)][test_cols].sort_values('dimension7_v2_score', ascending=False)

    if not test_data.empty:
        pr(f"\n   {test_data.to_string(index=False)}")
    else:
        pr("   (None of the test stocks found in merged dataset)")

    # ------------------------------------------------------------------
    # BIGGEST MOVERS
    # ------------------------------------------------------------------
    pr("\n" + "─" * 60)
    pr("📈  TOP 5 IMPROVERS  (v2.0 gains)")
    pr("─" * 60)
    mover_cols = ['symbol', 'dimension7_score', 'dimension7_v2_score', 'delta', 'trade_density_class']
    if phase == 'phase1bcd' and 'volume_trend' in merged.columns:
        mover_cols.append('volume_trend')
    pr(merged.nlargest(5, 'delta')[mover_cols].to_string(index=False))

    pr("\n" + "─" * 60)
    pr("📉  TOP 5 DECLINERS  (v2.0 drops)")
    pr("─" * 60)
    pr(merged.nsmallest(5, 'delta')[mover_cols].to_string(index=False))

    # ------------------------------------------------------------------
    # TRADE DENSITY BREAKDOWN
    # ------------------------------------------------------------------
    pr("\n" + "─" * 60)
    pr("💡  INSIGHTS BY TRADE DENSITY")
    pr("─" * 60)
    pr(f"   {'Class':<15}  {'Count':>6}  {'Avg v1.0':>9}  {'Avg v2.0':>9}  {'Avg Δ':>8}")
    for cls in ['institutional', 'mixed', 'retail']:
        subset = merged[merged['trade_density_class'] == cls]
        if len(subset) > 0:
            pr(f"   {cls.capitalize():<15}  {len(subset):>6}  "
               f"{subset['dimension7_score'].mean():>9.1f}  "
               f"{subset['dimension7_v2_score'].mean():>9.1f}  "
               f"{subset['delta'].mean():>+8.1f}")

    # ------------------------------------------------------------------
    # PHASE 1B-D EXCLUSIVE: VOLUME TREND & MOMENTUM
    # ------------------------------------------------------------------
    if phase == 'phase1bcd':
        if 'volume_trend' in merged.columns:
            pr("\n" + "─" * 60)
            pr("📦  VOLUME TREND DISTRIBUTION  (Phase 1B-D new — Component 4)")
            pr("─" * 60)
            vt_counts = merged['volume_trend'].value_counts()
            for trend, cnt in vt_counts.items():
                avg_score = merged[merged['volume_trend'] == trend]['dimension7_v2_score'].mean()
                pr(f"   {str(trend):<20}  {cnt:>4} stocks  |  avg D7 v2.0: {avg_score:.1f}")

        if 'momentum_confluence' in merged.columns:
            pr("\n" + "─" * 60)
            pr("🚀  MOMENTUM CONFLUENCE  (Phase 1B-D new — Component 5)")
            pr("─" * 60)
            mc_counts = merged['momentum_confluence'].value_counts()
            for mc, cnt in mc_counts.items():
                avg_score = merged[merged['momentum_confluence'] == mc]['dimension7_v2_score'].mean()
                pr(f"   {str(mc):<20}  {cnt:>4} stocks  |  avg D7 v2.0: {avg_score:.1f}")

        # Momentum return distributions
        for tf_col, label in [('momentum_5d_pct', '5D'), ('momentum_20d_pct', '20D'), ('momentum_60d_pct', '60D')]:
            if tf_col in merged.columns:
                valid = merged[tf_col].dropna()
                if len(valid) > 0:
                    pr(f"\n   {label} Return distribution:  "
                       f"mean={valid.mean():+.1f}%  "
                       f"median={valid.median():+.1f}%  "
                       f"std={valid.std():.1f}%  "
                       f"(n={len(valid)})")

    # ------------------------------------------------------------------
    # SUMMARY ASSESSMENT
    # ------------------------------------------------------------------
    pr("\n" + "=" * 80)
    pr("📊  FINAL ASSESSMENT")
    pr("=" * 80)

    if corr > 0.85 and overlap >= 14:
        assessment = "READY_FOR_PROMOTION"
        pr("   ✅ v2.0 shows HIGH consistency with v1.0")
        pr("   ✅ Good stability in top-20 recommendations")
        pr("   ✅ RECOMMENDATION: Promote v2.0 to production after 4-week parallel validation")
    elif corr > 0.70 and overlap >= 10:
        assessment = "REVIEW_DIFFERENCES"
        pr("   ⚠️  v2.0 shows MODERATE changes from v1.0")
        pr("   ⚠️  Review major movers before extending parallel run")
        pr("   ⚠️  RECOMMENDATION: Continue parallel run, monitor top-20 weekly")
    else:
        assessment = "INVESTIGATE_DIVERGENCE"
        pr("   ❌ v2.0 shows SIGNIFICANT divergence from v1.0")
        pr("   ❌ RECOMMENDATION: Investigate scoring methodology before deployment")

    pr(f"\n   Correlation:      {corr:.3f}")
    pr(f"   Top-20 Overlap:   {overlap}/20")
    pr(f"   Scorer Phase:     {phase}")
    pr(f"   Assessment:       {assessment}")

    pr("\n" + "=" * 80)
    pr(f"✅  COMPARISON COMPLETE  |  {run_ts}")
    pr("=" * 80)

    # ------------------------------------------------------------------
    # OPTIONAL SAVE
    # ------------------------------------------------------------------
    if save:
        prefix = output_prefix or f"d7_comparison_{run_date}"

        csv_path = f"{prefix}_merged.csv"
        report_path = f"{prefix}_report.txt"

        merged.to_csv(csv_path, index=False)
        print(f"\n💾 Merged CSV saved:   {csv_path}")

        with open(report_path, 'w') as f:
            f.write('\n'.join(lines))
        print(f"📄 Report saved:       {report_path}")

    return merged


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compare D7 v1.0 vs v2.0 scoring results (Phase 1A or Phase 1B-D auto-detected)"
    )
    parser.add_argument('v1_file', nargs='?', default='dimension7_scores.csv',
                        help='v1.0 scores CSV (default: dimension7_scores.csv)')
    parser.add_argument('v2_file', nargs='?', default='dimension7_v2_scores.csv',
                        help='v2.0 scores CSV (default: dimension7_v2_scores.csv)')
    parser.add_argument('--save', action='store_true',
                        help='Save merged CSV and text report to disk')
    parser.add_argument('--output-prefix', default=None,
                        help='Prefix for saved output files (default: d7_comparison_YYYYMMDD)')

    args = parser.parse_args()

    compare_versions(
        v1_file=args.v1_file,
        v2_file=args.v2_file,
        save=args.save,
        output_prefix=args.output_prefix
    )
