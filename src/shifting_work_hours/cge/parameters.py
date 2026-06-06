"""Elasticity parameters and sector definitions for the CGE model.

Default values are drawn from GTAP database and Chinese CGE literature.
Users can override these via a YAML/JSON config file or direct arguments.
"""

from dataclasses import dataclass, field
from typing import Dict

# ──────────────────────────────────────────────────────
#  Sector classification (2-sector aggregation for Phase 1)
# ──────────────────────────────────────────────────────

# Map 42 IO-table sectors → 2 aggregate sectors
SECTOR_MAP_2: Dict[str, str] = {
    # Agriculture
    '农业': 'AGR',
    '林业': 'AGR',
    '畜牧业': 'AGR',
    '渔业': 'AGR',
    '农林牧渔服务业': 'AGR',
    # Everything else → Industry & Services
    '煤炭开采和洗选业': 'IND',
    '石油和天然气开采业': 'IND',
    '金属矿采选业': 'IND',
    '非金属矿采选业': 'IND',
    '食品制造及烟草加工业': 'IND',
    '纺织业': 'IND',
    '纺织服装鞋帽皮革羽绒及其制品业': 'IND',
    '木材加工及家具制造业': 'IND',
    '造纸印刷及文教体育用品制造业': 'IND',
    '石油加工、炼焦及核燃料加工业': 'IND',
    '化学工业': 'IND',
    '非金属矿物制品业': 'IND',
    '金属冶炼及压延加工业': 'IND',
    '金属制品业': 'IND',
    '通用、专用设备制造业': 'IND',
    '交通运输设备制造业': 'IND',
    '电气机械及器材制造业': 'IND',
    '通信设备、计算机及其他电子设备制造业': 'IND',
    '仪器仪表及文化、办公用机械制造业': 'IND',
    '其他制造业': 'IND',
    '废品废料': 'IND',
    '电力、热力的生产和供应业': 'IND',
    '燃气生产和供应业': 'IND',
    '水的生产和供应业': 'IND',
    '建筑业': 'IND',
    '交通运输及仓储业': 'IND',
    '邮政业': 'IND',
    '信息传输、计算机服务和软件业': 'IND',
    '批发和零售业': 'IND',
    '住宿和餐饮业': 'IND',
    '金融业': 'IND',
    '房地产业': 'IND',
    '租赁和商务服务业': 'IND',
    '旅游业': 'IND',
    '科学研究事业': 'IND',
    '综合技术服务业': 'IND',
    '教育事业': 'IND',
    '卫生、社会保障和社会福利业': 'IND',
    '文化、体育和娱乐业': 'IND',
    '公共管理和社会组织': 'IND',
}

AGGREGATE_SECTORS = ['AGR', 'IND']

# ──────────────────────────────────────────────────────
#  Region classification
# ──────────────────────────────────────────────────────

REGIONS_SINGLE = ['CHN']  # Phase 1: national aggregate

REGIONS_3 = ['EAST', 'CENTRAL', 'WEST']  # Phase 2: 3-region

REGIONS_31 = [  # Phase 3: full province-level
    '北京', '天津', '河北', '山西', '内蒙古',
    '辽宁', '吉林', '黑龙江',
    '上海', '江苏', '浙江', '安徽', '福建', '江西', '山东',
    '河南', '湖北', '湖南',
    '广东', '广西', '海南',
    '重庆', '四川', '贵州', '云南', '西藏',
    '陕西', '甘肃', '青海', '宁夏', '新疆',
]

# ──────────────────────────────────────────────────────
#  Elasticities
# ──────────────────────────────────────────────────────

@dataclass
class Elasticities:
    """Elasticity parameters for the CGE model.

    Attributes:
        sigma_prod: CES elasticity of substitution in production
            (between value-added and intermediate inputs).
        sigma_va: CES elasticity of substitution between labour and capital.
        sigma_arm: Armington elasticity (domestic vs import).
        sigma_cet: CET elasticity (domestic sales vs export).
        income_elast: Household income elasticity of demand.
        sub_between_sectors: Elasticity of substitution between sectors
            in household consumption.
    """
    sigma_prod: float = 0.5      # production nesting
    sigma_va: float = 0.8        # labour-capital nesting
    sigma_arm: float = 3.0       # Armington (GTAP default ~3-8)
    sigma_cet: float = 3.0       # CET
    income_elast: float = 0.9    # CDE income elasticity
    sub_between_sectors: float = 0.5  # consumption substitution


# ──────────────────────────────────────────────────────
#  Sector-level labour shares (fraction of value added)
#  Used for mapping aggregate labour productivity loss
#  to sector-specific shocks.
# ──────────────────────────────────────────────────────

LABOUR_SHARE_BY_SECTOR: Dict[str, float] = {
    'AGR': 0.85,   # agriculture is labour-intensive
    'IND': 0.45,   # industry & services
}

# ──────────────────────────────────────────────────────
#  SAM account labels
# ──────────────────────────────────────────────────────

ACCOUNTS_2SECTOR = [
    # Production sectors
    'AGR', 'IND',
    # Factors
    'LAB', 'CAP',
    # Institutions
    'HOU',    # Household
    'GOV',    # Government
    'INV',    # Investment / capital account
    'ROW',    # Rest of world
]

NUM_ACCOUNTS = len(ACCOUNTS_2SECTOR)
