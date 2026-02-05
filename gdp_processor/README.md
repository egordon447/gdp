# GDP Data Processing Pipeline

A modular, stage-based pipeline for processing GDP data from multiple sources (1800-2025) across 200 countries.

---

## 🎯 Project Overview

This project processes 20+ GDP CSV files from different databases with:
- **11 different reference years** (incomparable directly)
- **Missing countries** in some databases
- **Missing year data** for various countries

**Goal**: Create a unified, complete GDP dataset with consistent reference year and filled gaps.

---

## 📊 Processing Pipeline

The pipeline consists of 5 sequential stages, each producing intermediate outputs:

```
data/raw/*.csv
    │
    ├─ pwt_gdp/*.csv       (Penn World Table)
    ├─ mpd_gdp/*.csv       (Maddison Project)
    ├─ wdi_gdp/*.csv       (World Bank WDI)
    ├─ un_gdp/*.csv        (UN Data)
    └─ barroursua_gdp/*.csv
    │
    ▼
[Stage 01] Normalization
    │  → Standardize column names, data types, country codes
    │  → Output: outputs/01_normalized/*.csv
    │
    ▼
[Stage 02] Rebasing
    │  → Convert all GDP values to target reference year (2017)
    │  → Formula: GDP_new = GDP_old × (Deflator_2017 / Deflator_source)
    │  → Output: outputs/02_rebased/*.csv
    │
    ▼
[Stage 03] Panel Structure
    │  → Complete country-year combinations (no value filling yet)
    │  → Output: outputs/03_panel/*.csv
    │
    ▼
[Stage 04] Imputation
    │  → Fill missing values using growth rate interpolation
    │  → Output: outputs/04_imputed/*.csv
    │
    ▼
[Stage 05] Summary Statistics
    │  → Generate missing data & growth rate reports
    │  → Output: outputs/05_summary/*.csv
```

---

## 📁 Directory Structure

```
gdp_processor/
│
├── README.md                    # This file
├── config.py                    # Configuration & parameters
├── requirements.txt             # Python dependencies
├── .gitignore                   # Git ignore rules
│
├── scripts/                     # Processing scripts (one per stage)
│   ├── stage_01_normalize.py   # Standardize fields & types
│   ├── stage_02_rebase.py      # Reference year conversion
│   ├── stage_03_panel.py       # Complete country-year structure
│   ├── stage_04_impute.py      # Fill missing values
│   ├── stage_05_summarize.py   # Generate statistics
│   └── run_pipeline.py         # Full pipeline orchestrator
│
├── data/
│   ├── raw/                     # Original CSV files (symlinked)
│   └── metadata/                # Deflator data, country mappings
│
└── outputs/                     # Stage outputs
    ├── 01_normalized/           # Stage 1 output
    ├── 02_rebased/              # Stage 2 output
    ├── 03_panel/                # Stage 3 output
    ├── 04_imputed/              # Stage 4 output
    └── 05_summary/              # Stage 5 output (reports)
```

---

## 🔧 Stage Details

### Stage 01: Normalization
**Script**: `scripts/stage_01_normalize.py`

**Purpose**: Standardize heterogeneous CSV files

**Operations**:
- Map varying column names to standard schema: `[iso3, country, year, gdp, source]`
- Convert data types (year → int, gdp → float)
- Standardize country codes to ISO 3166-1 alpha-3
- Remove duplicates

**Input**: `data/raw/*.csv`  
**Output**: `outputs/01_normalized/<source>.csv`

**Example**:
```
Before: countryname, yr, rgdpna
After:  country, year, gdp
```

---

### Stage 02: Rebasing
**Script**: `scripts/stage_02_rebase.py`

**Purpose**: Convert all GDP values to a single reference year (2017)

**Operations**:
- Load GDP deflator data for each country and year
- Calculate conversion factor: `factor = Deflator_2017 / Deflator_source`
- Apply conversion: `GDP_2017 = GDP_source × factor`
- Preserve original values as metadata

**Input**: `outputs/01_normalized/*.csv`  
**Output**: `outputs/02_rebased/*.csv`

**Formula**:
```
GDP_target = GDP_source × (Price_Index_target / Price_Index_source)
```

---

### Stage 03: Panel Structure
**Script**: `scripts/stage_03_panel.py`

**Purpose**: Create complete country-year grids (no value filling)

**Operations**:
- Extract unique countries from each source
- Generate full year range (1800-2025) for each country
- Mark existing vs. missing data points
- **Do NOT** fill missing values (just structure)

**Input**: `outputs/02_rebased/*.csv`  
**Output**: `outputs/03_panel/*.csv`

**Schema**:
```csv
iso3,country,year,gdp,source,is_missing
USA,United States,2000,12345.67,pwt_11,FALSE
USA,United States,2001,,pwt_11,TRUE
```

---

### Stage 04: Imputation
**Script**: `scripts/stage_04_impute.py`

**Purpose**: Fill missing GDP values using growth rate interpolation

**Operations**:
- Calculate average annual growth rate for each country
- Forward fill: `GDP_t = GDP_{t-1} × (1 + r)`
- Backward fill: `GDP_t = GDP_{t+1} / (1 + r)`
- Limit fill distance (max 10 years forward, 5 backward)

**Input**: `outputs/03_panel/*.csv`  
**Output**: `outputs/04_imputed/*.csv`

**Strategy**:
```python
# Example: Missing 2005-2007
growth_rate = mean(year-over-year growth from available data)
GDP_2005 = GDP_2004 × (1 + r)
GDP_2006 = GDP_2005 × (1 + r)
GDP_2007 = GDP_2006 × (1 + r)
```

---

### Stage 05: Summary Statistics
**Script**: `scripts/stage_05_summarize.py`

**Purpose**: Generate data quality and imputation reports

**Operations**:
- Count missing data points per source
- Calculate average growth rates per country
- Compare original vs. imputed coverage
- Generate summary tables

**Input**: `outputs/04_imputed/*.csv`  
**Output**: `outputs/05_summary/*.csv`

**Reports**:
- `missing_data_summary.csv` - Missing data statistics
- `growth_rate_summary.csv` - Calculated growth rates
- `coverage_comparison.csv` - Before/after coverage

---

## 🚀 Quick Start

### 1. Install Dependencies
```bash
cd gdp_processor
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure Settings
Edit `config.py`:
- Set `TARGET_REFERENCE_YEAR` (default: 2017)
- Adjust year range if needed
- Configure imputation parameters

### 3. Run Pipeline

**Option A: Full pipeline (all stages)**
```bash
python scripts/run_pipeline.py
```

**Option B: Individual stages**
```bash
python scripts/stage_01_normalize.py
python scripts/stage_02_rebase.py
python scripts/stage_03_panel.py
python scripts/stage_04_impute.py
python scripts/stage_05_summarize.py
```

### 4. View Results
- Normalized data: `outputs/01_normalized/`
- Rebased data: `outputs/02_rebased/`
- Panel structure: `outputs/03_panel/`
- Imputed data: `outputs/04_imputed/`
- Summary reports: `outputs/05_summary/`

---

## 📦 Dependencies

```txt
pandas>=2.0.0      # Data manipulation
numpy>=1.24.0      # Numerical operations
tqdm>=4.65.0       # Progress bars
```

Optional:
```txt
matplotlib>=3.7.0  # Visualization
seaborn>=0.12.0    # Statistical plots
```

---

## 🔍 Data Sources

| Source | Reference Year | Count | Description |
|--------|---------------|-------|-------------|
| PWT (Penn World Table) | 1996-2017 | 13 versions | Comparable international data |
| MPD (Maddison Project) | 1990-2011 | 5 versions | Long-run historical GDP |
| WDI (World Bank) | 2011-2017 | 3 datasets | PPP-adjusted GDP |
| UN Data | 2015 | 1 dataset | UN statistics |
| Barro-Ursua | 2009 | 1 dataset | Macroeconomic data |

**Total**: 23 data sources covering 200 countries (1800-2025)

---

## ⚙️ Configuration

Key parameters in `config.py`:

```python
# Target reference year
TARGET_REFERENCE_YEAR = 2017

# Year range
MIN_YEAR = 1800
MAX_YEAR = 2025

# Imputation settings
MIN_DATA_POINTS = 5         # Min points for growth calculation
FORWARD_FILL_LIMIT = 10     # Max years to forward fill
BACKWARD_FILL_LIMIT = 5     # Max years to backward fill
```

---

## 🧪 Quality Checks

Each stage includes validation:

1. **Normalization**: Check column completeness
2. **Rebasing**: Validate conversion factors (no extreme values)
3. **Panel**: Ensure complete country-year grid
4. **Imputation**: Flag excessive interpolation (>10 years)
5. **Summary**: Report data coverage statistics

---

## 📈 Example Output

**After Stage 04 (Imputed Data)**:
```csv
iso3,country,year,gdp,source,is_imputed,imputation_method
USA,United States,2000,12345.67,pwt_11,FALSE,
USA,United States,2001,12678.90,pwt_11,TRUE,forward_fill
USA,United States,2002,13012.34,pwt_11,TRUE,forward_fill
USA,United States,2003,13500.00,pwt_11,FALSE,
```

**Stage 05 Summary Report**:
```csv
source,total_countries,total_years,missing_before,missing_after,imputed_count
pwt_11,180,226,4567,123,4444
mpd_2023,190,226,8901,234,8667
```

---

## 🛠️ Troubleshooting

**Issue: Missing deflator data**
- Use proxy countries (see `data/metadata/proxy_scaling.csv`)
- Fallback to CPI data

**Issue: Extreme growth rates (>50%)**
- Flag for manual review
- Consider outlier detection

**Issue: Large gaps (>10 years)**
- Do not impute (mark as unfillable)
- Report in summary statistics

---

## 📝 Notes

- **Modular Design**: Each stage is independent and reusable
- **Intermediate Outputs**: All stage outputs are saved for inspection
- **No Side Effects**: Original data is never modified
- **Reproducibility**: Pipeline can be re-run from any stage

---

## 🔗 Related Files

- `config.py` - Configuration parameters
- `requirements.txt` - Python dependencies
- `.gitignore` - Excluded files (large CSVs)

---

**Version**: 1.0  
**Last Updated**: 2026-02-05  
**Pipeline Status**: Structure defined, scripts to be implemented
