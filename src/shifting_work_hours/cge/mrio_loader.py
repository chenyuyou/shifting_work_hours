"""MRIO (Multi-Regional Input-Output) table loader.

Parses the CEADs-format MRIO Excel file using pandas for fast reading.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class MRIOData:
    """Parsed MRIO table data."""
    provinces: List[str] = field(default_factory=list)
    sector_names: List[str] = field(default_factory=list)
    n_provinces: int = 0
    n_sectors: int = 0
    intermediate: Optional[np.ndarray] = None
    value_added: Dict[str, np.ndarray] = field(default_factory=dict)
    final_demand: Dict[str, np.ndarray] = field(default_factory=dict)
    gross_output: Optional[np.ndarray] = None


# ── 5-sector aggregation ──
AGG_MAP_5 = {
    0: 'AGR',
    **{i: 'IND' for i in range(1, 26)},
    **{i: 'SER' for i in range(26, 42)},
}
AGG_NAMES_5 = ['AGR', 'IND', 'SER']

# ── 8-sector aggregation ──
AGG_MAP_8 = {
    0: 'AGR',
    **{i: 'MIN' for i in range(1, 5)},
    **{i: 'MFG' for i in range(5, 22)},
    **{i: 'ENE' for i in range(22, 25)},
    25: 'CON',
    **{i: 'TRA' for i in range(26, 29)},
    **{i: 'SER' for i in range(29, 42)},
}

# ── 10-sector aggregation (CHEER model, Lancet 2025) ──
# Paper: "China provincial dynamic CGE model with 10 economic sectors"
# Mapping from CEADs 42 sectors → CHEER 10 sectors
AGG_MAP_10 = {
    0: 'AGR',    # Agriculture, Forestry, Animal Husbandry and Fishery
    1: 'COA',    # Mining and washing of coal
    2: 'OIG',    # Extraction of petroleum and natural gas
    3: 'MIN',    # Mining and processing of metal ores
    4: 'MIN',    # Mining and processing of nonmetal ores
    10: 'PET',   # Processing of petroleum, coking and nuclear fuel
    **{i: 'OMF' for i in [5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21]},  # Other manufacturing
    22: 'ELC',   # Production and supply of electric power and heat power
    23: 'GAS',   # Production and distribution of gas
    24: 'SER',   # Production and distribution of water → service
    25: 'CON',   # Construction
    **{i: 'SER' for i in range(26, 42)},  # All services
}
AGG_NAMES_10 = ['AGR', 'COA', 'OIG', 'MIN', 'PET', 'OMF', 'ELC', 'GAS', 'CON', 'SER']
AGG_NAMES_8 = ['AGR', 'MIN', 'MFG', 'ENE', 'CON', 'TRA', 'SER']


def load_mrio(file_path: Path,
              sheet: str = 'Table_2020_English Version') -> MRIOData:
    """Load CEADs MRIO using pandas (fast).

    Structure:
    - Row 1: title
    - Row 2: province header
    - Row 3: sector names + unit
    - Row 4: sector codes
    - Rows 5+: data (42 sectors per province, then VA rows)
    - Cols 1-3: province, sector_name, sector_code
    - Cols 4+: intermediate use (31×42 cols) + final demand cols
    """
    logger.info("Loading MRIO from %s ...", file_path)

    # Read entire sheet at once
    df = pd.read_excel(file_path, sheet_name=sheet, header=None)
    logger.info("  Raw shape: %s", df.shape)

    # ── Identify province boundaries ──
    # Province names are in column A (index 0), starting at row 5 (index 4)
    col_a = df.iloc[:, 0].astype(str)
    province_rows = []
    current_prov = None
    for i in range(4, len(df)):
        val = str(df.iloc[i, 0]).strip()
        if val and val != 'nan' and val != '':
            current_prov = val
            province_rows.append((current_prov, i))

    provinces = [p for p, _ in province_rows]
    n_provinces = len(provinces)
    n_sectors = 42  # CEADs standard

    # Sector names from row 3 (index 2), columns 3+
    sector_names = []
    for j in range(3, 3 + n_sectors):
        val = str(df.iloc[2, j]).strip()
        if val and val != 'nan':
            sector_names.append(val)
    n_sectors = len(sector_names)
    logger.info("  %d provinces, %d sectors", n_provinces, n_sectors)

    n_total = n_provinces * n_sectors

    # ── Read intermediate use matrix ──
    # Each province block: 42 rows × (31×42) data columns
    intermediate = np.zeros((n_total, n_total))

    row_idx = 0
    for prov, start_row in province_rows:
        for s in range(n_sectors):
            data_row = df.iloc[start_row + s, 3:3 + n_total]
            vals = pd.to_numeric(data_row, errors='coerce').fillna(0.0).values.astype(np.float64)
            intermediate[row_idx, :len(vals)] = vals
            row_idx += 1

    # ── Read value added rows ──
    # VA rows are at the END of the table (after all province data)
    # They contain aggregate values across all provinces

    va_labels = {
        'VA001': 'compensation',
        'VA002': 'net_taxes',
        'VA003': 'depreciation',
        'VA004': 'surplus',
        'TI': 'total_input',
    }

    last_prov_row = province_rows[-1][1]
    va = {}
    gross_output = np.zeros(n_total)

    # Search for VA rows after the last province's data
    for i in range(last_prov_row + n_sectors, min(len(df), last_prov_row + n_sectors + 30)):
        code = str(df.iloc[i, 2]).strip()
        if code in va_labels:
            key = va_labels[code]
            vals = pd.to_numeric(df.iloc[i, 3:3 + n_total], errors='coerce').fillna(0.0).values
            if key == 'total_input':
                gross_output = vals
            else:
                va[key] = vals

    logger.info("  VA components: %s", list(va.keys()))
    logger.info("  Gross output sum: %.0f", gross_output.sum())

    # ── Read final demand columns ──
    # FD columns start after intermediate columns (col 3 + n_total)
    fd_start = 3 + n_total
    fd_labels_raw = df.iloc[2, fd_start:].values  # row 3 has labels

    # Map FD column labels
    fd_map = {}
    fd_components = ['rural_cons', 'urban_cons', 'gov_cons', 'fixed_capital', 'inventory', 'export']
    fd_idx = 0
    for j, label in enumerate(fd_labels_raw):
        label_str = str(label).strip().lower()
        if fd_idx < len(fd_components):
            if any(kw in label_str for kw in ['rural', 'household consumption']):
                if 'rural' not in fd_map:
                    fd_map['rural_cons'] = j
                    fd_idx += 1
            elif 'urban' in label_str:
                fd_map['urban_cons'] = j
                fd_idx += 1
            elif 'government' in label_str:
                fd_map['gov_cons'] = j
                fd_idx += 1
            elif 'fixed capital' in label_str:
                fd_map['fixed_capital'] = j
                fd_idx += 1
            elif 'inventory' in label_str or 'changes in' in label_str:
                fd_map['inventory'] = j
                fd_idx += 1
            elif 'export' in label_str:
                fd_map['export'] = j
                fd_idx += 1

    logger.info("  FD columns mapped: %s", fd_map)

    # Read FD data for first province (Beijing) as national proxy
    # Actually, FD is per-province. For national, we need to aggregate.
    # But the columns are structured per-province too.
    # Let me read the first province's FD as a starting point.
    fd = {}
    first_prov_row = province_rows[0][1]
    for key, col_offset in fd_map.items():
        col_idx = fd_start + col_offset
        vals = pd.to_numeric(df.iloc[first_prov_row:first_prov_row + n_sectors, col_idx],
                             errors='coerce').fillna(0.0).values
        fd[key] = vals

    # For a proper national SAM, we need to sum FD across all provinces
    # The FD columns repeat for each province
    n_fd_cols = len(fd_map)
    for key in fd_map:
        total = np.zeros(n_sectors)
        for p_idx, (prov, start_row) in enumerate(province_rows):
            col_idx = fd_start + p_idx * n_fd_cols + list(fd_map.keys()).index(key)
            if col_idx < df.shape[1]:
                vals = pd.to_numeric(df.iloc[start_row:start_row + n_sectors, col_idx],
                                     errors='coerce').fillna(0.0).values
                total += vals
        fd[key] = total

    data = MRIOData(
        provinces=provinces,
        sector_names=sector_names,
        n_provinces=n_provinces,
        n_sectors=n_sectors,
        intermediate=intermediate,
        value_added=va,
        final_demand=fd,
        gross_output=gross_output,
    )

    logger.info("MRIO loaded: %d × %d = %d units, output = %.0f",
                n_provinces, n_sectors, n_total, gross_output.sum())
    return data


def build_national_io(data: MRIOData) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Aggregate MRIO to national single-region IO table.

    VA is computed as: total_output - intermediate_inputs
    (Because the VA rows in the MRIO are per-province, not aggregate.)
    """
    n = data.n_sectors
    np_ = data.n_provinces

    Z_4d = data.intermediate.reshape(np_, n, np_, n)
    Z = Z_4d.sum(axis=(0, 2))

    # National total output per sector
    gross_4d = data.gross_output.reshape(np_, n)
    total_output = gross_4d.sum(axis=0)

    # VA = total_output - intermediate_sum
    intermediate_sum = Z.sum(axis=1)
    total_va = total_output - intermediate_sum

    # Split VA into components using Beijing's shares
    comp = data.value_added.get('compensation', np.zeros(n))[:n]
    taxes = data.value_added.get('net_taxes', np.zeros(n))[:n]
    depr = data.value_added.get('depreciation', np.zeros(n))[:n]
    surplus = data.value_added.get('surplus', np.zeros(n))[:n]
    beijing_va = comp + taxes + depr + surplus

    # Scale to national total
    scale = np.where(beijing_va > 0, total_va / beijing_va, 0.0)
    va = np.column_stack([comp * scale, taxes * scale, depr * scale, surplus * scale])

    fd_keys = ['rural_cons', 'urban_cons', 'gov_cons', 'fixed_capital', 'inventory', 'export']
    fd = np.column_stack([data.final_demand.get(k, np.zeros(n))[:n] for k in fd_keys])

    return Z, va, fd


def aggregate_sectors(data: MRIOData, mapping: Dict[int, str]) -> MRIOData:
    """Aggregate MRIO sectors according to a mapping.

    Works with the full provincial MRIO or national IO.
    """
    agg_sectors = list(dict.fromkeys(mapping.values()))
    n_agg = len(agg_sectors)
    agg_idx = {s: i for i, s in enumerate(agg_sectors)}

    n_total = data.n_sectors * data.n_provinces
    n = data.n_sectors

    # Build aggregation matrix: (n_agg × n_total)
    # For each aggregate sector, sum over original sectors and provinces
    agg_full = np.zeros((n_agg, n_total))
    for orig_idx, agg_name in mapping.items():
        if orig_idx < n:
            agg_full_idx = agg_idx[agg_name]
            # Sum across all provinces for this sector
            for p in range(data.n_provinces):
                full_idx = p * n + orig_idx
                agg_full[agg_full_idx, full_idx] = 1.0

    Z_agg = agg_full @ data.intermediate @ agg_full.T

    # Value added = output - intermediate
    gross_agg = agg_full @ data.gross_output
    va_total = gross_agg - Z_agg.sum(axis=1)

    fd_agg = {}
    for key, vals in data.final_demand.items():
        sector_agg = np.zeros((n_agg,))
        for orig_idx, agg_name in mapping.items():
            if orig_idx < len(vals):
                sector_agg[agg_idx[agg_name]] += vals[orig_idx]
        fd_agg[key] = sector_agg

    return MRIOData(
        provinces=['CHN'],
        sector_names=agg_sectors,
        n_provinces=1,
        n_sectors=n_agg,
        intermediate=Z_agg,
        value_added={'total_va': va_total},
        final_demand=fd_agg,
        gross_output=gross_agg,
    )
