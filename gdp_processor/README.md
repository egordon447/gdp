# GDP Data Processing System

## Project Overview

This project processes 20 GDP CSV files from different data sources to solve issues with inconsistent reference years and missing data, ultimately generating a unified and complete GDP dataset with visualizations.

---

## Problem Statement

### Data Source Information
- **Number of Sources**: 20 CSV files
- **Coverage**: 200 countries, years 1800-2025
- **Reference Years**: 11 different reference years (making data incomparable directly)

### Main Issues
1. **Missing Countries**: Some countries are absent in certain databases
2. **Inconsistent Reference Years**: 20 data sources correspond to 11 different reference years
3. **Missing Year Data**: Some countries have missing data for certain years

---

## Solution Approach

### Task 1: Reference Year Alignment
Convert all CSV data to a selected reference year, generating 20 new aligned CSV files.

**Key Technical Points**:
- GDP year conversion formula: `GDP_new = GDP_old × (Price_Index_new / Price_Index_old)`
- Use GDP deflator or CPI for price adjustment

### Task 2: Gap Filling
For missing year data, use the average GDP annual growth rate of each data source for interpolation.

**Key Technical Points**:
- Calculate average annual growth rate for each country in each data source
- Forward/backward fill missing values
- Use exponential growth model: `GDP_t = GDP_{t-1} × (1 + growth_rate)`

### Task 3: Data Visualization
Generate simple visualizations to display processing results.

**Visualization Content**:
- Data completeness heatmap (Country × Year)
- GDP trend line comparison (before/after processing)
- Missing data distribution statistics

---

## Program Architecture

### Directory Structure

```
gdp_processor/
├── README.md                  # This document
├── requirements.txt           # Python dependencies
├── config.py                  # Configuration file
├── main.py                    # Main program entry
│
├── modules/                   # Core modules
│   ├── __init__.py
│   ├── data_loader.py         # Data loading module
│   ├── year_converter.py      # Reference year conversion module
│   ├── gap_filler.py          # Gap filling module
│   ├── visualizer.py          # Visualization module
│   └── utils.py               # Utility functions
│
├── data/                      # Data directory
│   ├── raw/                   # Original CSV files
│   │   ├── pwt_gdp/          # Penn World Table data
│   │   ├── mpd_gdp/          # Maddison Project data
│   │   ├── wdi_gdp/          # World Bank WDI data
│   │   ├── un_gdp/           # UN data
│   │   └── barroursua_gdp/   # Barro-Ursua data
│   │
│   ├── aligned/               # Task 1: Aligned CSV files
│   ├── filled/                # Task 2: Gap-filled CSV files
│   └── metadata/              # Metadata (reference year mappings, missing statistics)
│
├── output/                    # Output results
│   ├── logs/                  # Log files
│   ├── visualizations/        # Visualization charts
│   └── reports/               # Processing reports
│
└── tests/                     # Tests (optional)
    ├── test_converter.py
    └── test_filler.py
```

---

## Core Module Design

### 1. `data_loader.py` - Data Loading Module

**Functionality**:
- Scan and load all GDP CSV files
- Extract data source metadata (name, reference year, year range)
- Standardize data format (country codes, year columns)

**Key Functions**:
```python
def load_all_datasets(data_dir: str) -> Dict[str, pd.DataFrame]:
    """Load all CSV files into a dictionary"""
    
def extract_metadata(df: pd.DataFrame, source_name: str) -> Dict:
    """Extract data source metadata"""
    
def standardize_format(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize data format (country codes, column names, etc.)"""
```

---

### 2. `year_converter.py` - Reference Year Conversion Module

**Functionality**:
- Convert GDP values of all data sources based on target reference year
- Apply GDP deflator or price index for adjustment
- Save aligned CSV files

**Key Functions**:
```python
def get_conversion_factor(
    source_ref_year: int, 
    target_ref_year: int, 
    country: str
) -> float:
    """Calculate conversion factor"""
    
def convert_gdp_to_reference_year(
    df: pd.DataFrame, 
    target_ref_year: int
) -> pd.DataFrame:
    """Convert GDP data to target reference year"""
    
def batch_convert_all_sources(
    datasets: Dict[str, pd.DataFrame], 
    target_ref_year: int, 
    output_dir: str
) -> None:
    """Batch convert all data sources"""
```

**Conversion Formula**:
```
GDP_target = GDP_source × (Deflator_target / Deflator_source)
```

---

### 3. `gap_filler.py` - Gap Filling Module

**Functionality**:
- Detect missing years in each data source
- Calculate average annual growth rate for each country
- Fill missing data using growth rate

**Key Functions**:
```python
def detect_missing_years(
    df: pd.DataFrame, 
    country: str, 
    year_range: Tuple[int, int]
) -> List[int]:
    """Detect missing years"""
    
def calculate_avg_growth_rate(
    df: pd.DataFrame, 
    country: str
) -> float:
    """Calculate average GDP annual growth rate"""
    
def fill_missing_gdp(
    df: pd.DataFrame, 
    country: str, 
    missing_years: List[int], 
    avg_growth_rate: float
) -> pd.DataFrame:
    """Fill missing GDP data"""
    
def batch_fill_all_sources(
    aligned_dir: str, 
    output_dir: str
) -> None:
    """Batch fill all data sources"""
```

**Filling Strategy**:
- Forward fill: `GDP_t = GDP_{t-1} × (1 + r)`
- Backward fill: `GDP_t = GDP_{t+1} / (1 + r)`
- Prioritize forward fill, backward fill as supplement

---

### 4. `visualizer.py` - Visualization Module

**Functionality**:
- Generate data completeness heatmaps
- Plot GDP trend comparison charts
- Visualize missing data distribution statistics

**Key Functions**:
```python
def plot_data_completeness_heatmap(
    df: pd.DataFrame, 
    output_path: str
) -> None:
    """Plot data completeness heatmap"""
    
def plot_gdp_trends(
    countries: List[str], 
    datasets: Dict[str, pd.DataFrame], 
    output_path: str
) -> None:
    """Plot GDP trends for multiple countries"""
    
def plot_missing_data_statistics(
    datasets: Dict[str, pd.DataFrame], 
    output_path: str
) -> None:
    """Plot missing data statistics"""
    
def generate_summary_report(
    datasets: Dict[str, pd.DataFrame], 
    output_path: str
) -> None:
    """Generate processing summary report"""
```

---

### 5. `utils.py` - Utility Functions

**Functionality**:
- Logging setup
- File I/O helper functions
- Data validation

**Key Functions**:
```python
def setup_logger(log_path: str) -> logging.Logger:
    """Set up logger"""
    
def validate_dataframe(df: pd.DataFrame) -> bool:
    """Validate dataframe format"""
    
def save_csv_with_metadata(
    df: pd.DataFrame, 
    output_path: str, 
    metadata: Dict
) -> None:
    """Save CSV with metadata"""
```

---

## Configuration File (`config.py`)

```python
"""
Configuration Parameters
"""

# Target reference year (e.g., 2017 PPP)
TARGET_REFERENCE_YEAR = 2017

# Data paths
DATA_DIR = "./data/raw"
ALIGNED_DIR = "./data/aligned"
FILLED_DIR = "./data/filled"
OUTPUT_DIR = "./output"

# GDP deflator data source (for year conversion)
DEFLATOR_SOURCE = "./data/metadata/gdp_deflator.csv"

# Processing parameters
MIN_YEAR = 1800
MAX_YEAR = 2025
MIN_DATA_POINTS = 5  # Minimum data points for growth rate calculation

# Visualization parameters
FIGURE_SIZE = (14, 8)
DPI = 300
COLORMAP = "YlGnBu"

# Data source metadata
DATA_SOURCE_METADATA = {
    "pwt_11": {"ref_year": 2017, "name": "Penn World Table 11"},
    "mpd_2023": {"ref_year": 2011, "name": "Maddison Project 2023"},
    "wdi_ppp1": {"ref_year": 2017, "name": "World Bank WDI PPP"},
    # ... other data sources
}
```

---

## Main Program (`main.py`)

```python
"""
Main Program Entry Point
"""
import logging
from pathlib import Path
from modules.data_loader import load_all_datasets
from modules.year_converter import batch_convert_all_sources
from modules.gap_filler import batch_fill_all_sources
from modules.visualizer import (
    plot_data_completeness_heatmap,
    plot_gdp_trends,
    generate_summary_report
)
from modules.utils import setup_logger
from config import (
    DATA_DIR, ALIGNED_DIR, FILLED_DIR, OUTPUT_DIR,
    TARGET_REFERENCE_YEAR
)

def main():
    """Main function"""
    # 1. Setup logging
    logger = setup_logger(f"{OUTPUT_DIR}/logs/processing.log")
    logger.info("Starting GDP data processing pipeline")
    
    # 2. Task 1: Load raw data
    logger.info("Step 1/3: Loading raw data")
    datasets = load_all_datasets(DATA_DIR)
    logger.info(f"Successfully loaded {len(datasets)} data sources")
    
    # 3. Task 2: Reference year alignment
    logger.info(f"Step 2/3: Converting to reference year {TARGET_REFERENCE_YEAR}")
    batch_convert_all_sources(
        datasets, 
        TARGET_REFERENCE_YEAR, 
        ALIGNED_DIR
    )
    logger.info(f"Aligned data saved to {ALIGNED_DIR}")
    
    # 4. Task 3: Gap filling
    logger.info("Step 3/3: Filling missing data")
    batch_fill_all_sources(ALIGNED_DIR, FILLED_DIR)
    logger.info(f"Filled data saved to {FILLED_DIR}")
    
    # 5. Visualization
    logger.info("Generating visualization reports")
    filled_datasets = load_all_datasets(FILLED_DIR)
    
    # Plot heatmaps
    for name, df in filled_datasets.items():
        plot_data_completeness_heatmap(
            df, 
            f"{OUTPUT_DIR}/visualizations/{name}_heatmap.png"
        )
    
    # Plot trend comparison
    plot_gdp_trends(
        countries=["USA", "CHN", "IND", "DEU", "JPN"],
        datasets=filled_datasets,
        output_path=f"{OUTPUT_DIR}/visualizations/gdp_trends.png"
    )
    
    # Generate summary report
    generate_summary_report(
        filled_datasets,
        f"{OUTPUT_DIR}/reports/summary.txt"
    )
    
    logger.info("Processing complete!")

if __name__ == "__main__":
    main()
```

---

## Dependencies (`requirements.txt`)

```txt
pandas>=2.0.0
numpy>=1.24.0
matplotlib>=3.7.0
seaborn>=0.12.0
openpyxl>=3.1.0
tqdm>=4.65.0
```

---

## Execution Workflow

### Step 1: Environment Setup
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Step 2: Configure Parameters
Edit `config.py` to set:
- Target reference year
- Data paths
- Data source metadata (reference year mappings)

### Step 3: Run Program
```bash
python main.py
```

### Step 4: View Results
- Aligned data: `data/aligned/`
- Filled data: `data/filled/`
- Visualizations: `output/visualizations/`
- Reports: `output/reports/`

---

## Technical Challenges & Solutions

### Challenge 1: Obtaining Conversion Factors for Reference Years
**Problem**: GDP deflator data between different reference years may be missing

**Solution**:
1. Prioritize using GDP deflator data from World Bank or OECD
2. If missing, use CPI (Consumer Price Index) as substitute
3. For extreme cases, use proxy data from neighboring countries (refer to `proxy_scaling.csv`)

### Challenge 2: Diverse Missing Data Patterns
**Problem**: Missing data can be continuous, discrete, or entire time periods

**Solution**:
1. Continuous gaps: Use linear or exponential interpolation
2. Discrete gaps: Fill using average growth rate
3. Entire period gaps: Mark as unfillable, generate warning logs

### Challenge 3: Non-uniform Data Source Formats
**Problem**: 20 CSV files have different column names and country code formats

**Solution**:
1. Implement intelligent column name detection in `data_loader.py`
2. Standardize using ISO 3166 country codes
3. Create mapping tables to handle different naming conventions

---

## Data Quality Checks

The program automatically generates the following quality reports:

1. **Data Completeness Statistics**
   - Number of countries covered by each data source
   - Time span of each data source
   - Missing data percentage

2. **Conversion Quality Checks**
   - Value reasonableness check before/after conversion
   - Outlier detection (GDP change > 50%)

3. **Filling Quality Checks**
   - Number of filled data points
   - Deviation of filled values from adjacent real values

---

## Extensibility Considerations

While designed for this specific batch of data, the code maintains extensibility:

- Modular design for easy debugging and modification
- Separated configuration for easy parameter adjustment
- Comprehensive logging for problem tracking
- Visualization output for result verification

---

## Expected Timeline

- **Development Time**: 1-2 days
- **Execution Time**: ~5-15 minutes (depending on data volume)

---

## Important Notes

1. **Data Backup**: Back up original CSV files before running
2. **Reference Year Selection**: Recommend choosing the year with most data sources (e.g., 2017)
3. **Memory Usage**: Loading 20 datasets simultaneously may use 500MB-2GB of memory
4. **Result Verification**: Recommend spot-checking conversion and filling results for a few countries

---

## Contact & Support

For issues, please check:
1. `output/logs/processing.log` - Detailed logs
2. `output/reports/summary.txt` - Processing summary
3. Console output - Real-time progress

---

**Document Version**: v1.0  
**Last Updated**: 2026-02-05
