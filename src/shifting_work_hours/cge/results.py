"""Result analysis and export for CGE model runs.

Provides tools to:
1. Compare baseline vs shocked equilibrium.
2. Compute percentage changes in GDP, output, prices, wages.
3. Export results to CSV for plotting.
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class ResultsAnalyzer:
    """Collect and compare CGE results across scenarios / years.

    Usage::

        ra = ResultsAnalyzer()
        ra.add('baseline_2050', result_dict)
        ra.add('shocked_2050', result_dict)
        ra.compare('baseline_2050', 'shocked_2050')
        ra.to_csv('results.csv')
    """

    def __init__(self):
        self._results: Dict[str, dict] = {}

    def add(self, label: str, result: dict) -> None:
        """Store a named result set."""
        self._results[label] = result

    def compare(self, base_label: str, shocked_label: str) -> dict:
        """Compute difference between two result sets.

        Returns a dict with:
            - gdp_level_change: absolute difference in GDP index
            - gdp_pct_change: percentage change
            - price_changes: dict by sector
            - output_changes: dict by sector
            - factor_changes: wage, rental
        """
        base = self._results[base_label]
        shock = self._results[shocked_label]

        # Base GDP (all p=1, q=1, output=output0) → sum(output0)
        base_gdp = 1.0  # normalised
        shock_gdp = 1.0 + shock.get('gdp_change', 0.0)

        return {
            'base_gdp_index': base_gdp,
            'shock_gdp_index': shock_gdp,
            'gdp_pct_change': shock.get('gdp_change', 0.0) * 100,
            'price_changes': {
                s: v * 100 for s, v in shock.get('price_change', {}).items()
            },
            'output_changes': {
                s: v * 100 for s, v in shock.get('output_change', {}).items()
            },
            'wage_change_pct': shock.get('wage_change', 0.0) * 100,
            'rental_change_pct': shock.get('rental_change', 0.0) * 100,
        }

    def summary(self, base_label: str, shocked_label: str) -> str:
        """Human-readable summary of the comparison."""
        c = self.compare(base_label, shocked_label)
        lines = [
            "=" * 50,
            f"CGE Results: {base_label} → {shocked_label}",
            "=" * 50,
            f"  GDP change:       {c['gdp_pct_change']:+.4f}%",
            f"  Wage change:      {c['wage_change_pct']:+.4f}%",
            f"  Rental change:    {c['rental_change_pct']:+.4f}%",
            "",
            "  Output changes by sector:",
        ]
        for s, v in c['output_changes'].items():
            lines.append(f"    {s}: {v:+.4f}%")
        lines.append("")
        lines.append("  Price changes by sector:")
        for s, v in c['price_changes'].items():
            lines.append(f"    {s}: {v:+.4f}%")
        lines.append("=" * 50)
        return "\n".join(lines)

    def to_csv(self, path: Path) -> None:
        """Export all stored results to CSV."""
        if not self._results:
            logger.warning("No results to export.")
            return

        with open(path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'label', 'gdp_change', 'wage_change', 'rental_change',
                'price_AGR', 'price_IND', 'output_AGR', 'output_IND',
            ])
            for label, r in self._results.items():
                writer.writerow([
                    label,
                    f"{r.get('gdp_change', 0):.6f}",
                    f"{r.get('wage_change', 0):.6f}",
                    f"{r.get('rental_change', 0):.6f}",
                    f"{r.get('price_change', {}).get('AGR', 0):.6f}",
                    f"{r.get('price_change', {}).get('IND', 0):.6f}",
                    f"{r.get('output_change', {}).get('AGR', 0):.6f}",
                    f"{r.get('output_change', {}).get('IND', 0):.6f}",
                ])
        logger.info("Results exported to %s", path)

    def to_dataframe(self):
        """Convert to a pandas DataFrame (optional dependency)."""
        import pandas as pd
        rows = []
        for label, r in self._results.items():
            row = {
                'label': label,
                'gdp_change': r.get('gdp_change', 0),
                'wage_change': r.get('wage_change', 0),
                'rental_change': r.get('rental_change', 0),
            }
            for s, v in r.get('price_change', {}).items():
                row[f'price_{s}'] = v
            for s, v in r.get('output_change', {}).items():
                row[f'output_{s}'] = v
            rows.append(row)
        return pd.DataFrame(rows)
