"""CGE model configuration.

The ``CGEConfig`` dataclass holds ALL tuneable parameters for a CGE
model run.  A JSON file can be loaded/saved to switch between
different country or paper settings without touching code.

Default values are generic placeholders; each application should
provide its own config (see ``configs/`` directory).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class Elasticities:
    """CES / Armington / CET elasticities."""
    sigma_va: float = 0.8        # labour–capital substitution
    sigma_arm: float = 3.0       # Armington (domestic vs import)
    sigma_cet: float = 3.0       # CET (domestic vs export)
    income_elast: float = 0.9    # household income elasticity


@dataclass
class SectorMapping:
    """Maps fine-grained IO-table sectors to aggregate CGE sectors.

    Example::

        mapping = {'农业': 'AGR', '林业': 'AGR', '纺织业': 'MFG', ...}
    """
    mapping: Dict[str, str] = field(default_factory=dict)

    @property
    def aggregate_sectors(self) -> List[str]:
        """Unique aggregate sector names, preserving insertion order."""
        return list(dict.fromkeys(self.mapping.values()))


@dataclass
class ShockMapping:
    """Maps aggregate productivity loss to per-sector efficiency factors.

    ``loss_multiplier`` scales the national-level loss for each sector.
    E.g. agriculture typically suffers more from heat than services.

    ``default_multiplier`` is used for sectors not listed explicitly.
    """
    loss_multiplier: Dict[str, float] = field(default_factory=dict)
    default_multiplier: float = 1.0

    def to_efficiency(self, national_loss: float) -> Dict[str, float]:
        """Convert national loss fraction → per-sector efficiency factors."""
        result = {}
        for sector, mult in self.loss_multiplier.items():
            loss = national_loss * mult
            result[sector] = max(1.0 - loss, 0.01)
        return result


@dataclass
class CGEConfig:
    """Complete CGE configuration.

    Load from JSON::

        config = CGEConfig.from_json('configs/my_study.json')

    Or construct programmatically::

        config = CGEConfig(
            name='My Study',
            elasticities=Elasticities(sigma_va=0.6),
            ...
        )
    """
    # Metadata
    name: str = 'unnamed_cge'
    description: str = ''

    # Economy
    total_gdp: float = 100.0          # normalised to 100
    num_sectors: int = 2              # target aggregation level

    # SAM
    sam_file: Optional[str] = None    # path to SAM CSV (None = use demo)
    sector_mapping_file: Optional[str] = None  # path to sector map JSON

    # Elasticities
    elasticities: Elasticities = field(default_factory=Elasticities)

    # Sector mapping (fine → aggregate)
    sector_mapping: SectorMapping = field(default_factory=SectorMapping)

    # Shock mapping (national loss → per-sector)
    shock_mapping: ShockMapping = field(default_factory=ShockMapping)

    # Factor income shares (override if SAM not provided)
    # These are used only with the demo SAM.
    lab_share_by_sector: Dict[str, float] = field(default_factory=lambda: {
        'AGR': 0.85,
        'IND': 0.45,
    })

    # Final demand shares (demo SAM only)
    hou_consumption_share: float = 0.60
    gov_consumption_share: float = 0.15
    investment_share: float = 0.42
    gov_tax_rate: float = 0.10        # tax on capital income

    # ── I/O ──────────────────────────────────────────

    def to_json(self, path: Path) -> None:
        """Save config to JSON."""
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(asdict(self), f, indent=2, ensure_ascii=False)

    @classmethod
    def from_json(cls, path: Path) -> "CGEConfig":
        """Load config from JSON."""
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # Reconstruct nested dataclasses
        if 'elasticities' in data:
            data['elasticities'] = Elasticities(**data['elasticities'])
        if 'sector_mapping' in data:
            data['sector_mapping'] = SectorMapping(**data['sector_mapping'])
        if 'shock_mapping' in data:
            data['shock_mapping'] = ShockMapping(**data['shock_mapping'])
        return cls(**data)

    @classmethod
    def default(cls) -> "CGEConfig":
        """Return a minimal default config (2-sector, generic)."""
        return cls(
            name='default_2sector',
            description='Generic 2-sector CGE (AGR + IND)',
            sector_mapping=SectorMapping(mapping={'sector_1': 'AGR', 'sector_2': 'IND'}),
            lab_share_by_sector={'AGR': 0.85, 'IND': 0.45},
        )
