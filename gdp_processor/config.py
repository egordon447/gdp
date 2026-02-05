"""
GDP Data Processing Pipeline - Configuration
Modular processing workflow for GDP data normalization and imputation
"""

# ============================================================================
# TARGET REFERENCE YEAR
# ============================================================================
TARGET_REFERENCE_YEAR = 2017  # Base year for GDP deflator conversion

# ============================================================================
# DIRECTORY PATHS
# ============================================================================
# Input
DATA_DIR = "./data/raw"

# Output stages (pipeline)
OUTPUT_01_NORMALIZED = "./outputs/01_normalized"  # Standardized fields & types
OUTPUT_02_REBASED = "./outputs/02_rebased"       # Reference year conversion
OUTPUT_03_PANEL = "./outputs/03_panel"           # Country-year structure completion
OUTPUT_04_IMPUTED = "./outputs/04_imputed"       # Missing value imputation
OUTPUT_05_SUMMARY = "./outputs/05_summary"       # Statistics & reports

# Metadata
METADATA_DIR = "./data/metadata"

# ============================================================================
# PROCESSING PARAMETERS
# ============================================================================
# Year range
MIN_YEAR = 1800
MAX_YEAR = 2025

# Imputation settings
MIN_DATA_POINTS = 5      # Minimum data points for growth rate calculation
FORWARD_FILL_LIMIT = 10  # Maximum years to forward fill
BACKWARD_FILL_LIMIT = 5  # Maximum years to backward fill

# ============================================================================
# DATA SOURCE METADATA
# ============================================================================
# Format: source_name -> (reference_year, full_name)
DATA_SOURCES = {
    # Penn World Table
    "pwt_11": {"ref_year": 2017, "name": "Penn World Table 11.0"},
    "pwt_10_1": {"ref_year": 2011, "name": "Penn World Table 10.1"},
    "pwt_10": {"ref_year": 2011, "name": "Penn World Table 10.0"},
    "pwt_9_1": {"ref_year": 2011, "name": "Penn World Table 9.1"},
    "pwt_9": {"ref_year": 2011, "name": "Penn World Table 9.0"},
    "pwt_8_1": {"ref_year": 2011, "name": "Penn World Table 8.1"},
    "pwt_8": {"ref_year": 2005, "name": "Penn World Table 8.0"},
    "pwt_7_1": {"ref_year": 2005, "name": "Penn World Table 7.1"},
    "pwt_7": {"ref_year": 2005, "name": "Penn World Table 7.0"},
    "pwt_6_3": {"ref_year": 2005, "name": "Penn World Table 6.3"},
    "pwt_6_2": {"ref_year": 2000, "name": "Penn World Table 6.2"},
    "pwt_6_1": {"ref_year": 2000, "name": "Penn World Table 6.1"},
    "pwt_5_6": {"ref_year": 1996, "name": "Penn World Table 5.6"},
    
    # Maddison Project Database
    "mpd_2023": {"ref_year": 2011, "name": "Maddison Project Database 2023"},
    "mpd_2020": {"ref_year": 2011, "name": "Maddison Project Database 2020"},
    "mpd_2018": {"ref_year": 2011, "name": "Maddison Project Database 2018"},
    "mpd_2013": {"ref_year": 1990, "name": "Maddison Project Database 2013"},
    "mpd_2010": {"ref_year": 1990, "name": "Maddison Project Database 2010"},
    
    # World Bank World Development Indicators
    "wdi_ppp1": {"ref_year": 2017, "name": "WDI GDP PPP 2017"},
    "wdi_archive3": {"ref_year": 2011, "name": "WDI Archive 2011"},
    "wdi_current_lcu_ppp1": {"ref_year": 2017, "name": "WDI Current LCU PPP 2017"},
    
    # Other sources
    "un_data_ppp": {"ref_year": 2015, "name": "UN GDP PPP Data"},
    "barro_ursua": {"ref_year": 2009, "name": "Barro-Ursua Macroeconomic Dataset"},
}

# ============================================================================
# FIELD MAPPING (for normalization)
# ============================================================================
# Map various source column names to standard names
COLUMN_MAPPING = {
    # Country identifiers
    "country": ["country", "countryname", "country_name", "nation"],
    "iso3": ["iso3", "iso3c", "countrycode", "country_code", "isocode"],
    "iso_num": ["iso_num", "isonum", "country_num"],
    
    # Year
    "year": ["year", "yr", "time"],
    
    # GDP values
    "gdp": ["gdp", "rgdp", "rgdpna", "gdp_pc", "gdppc", "real_gdp"],
    
    # Population (optional)
    "pop": ["pop", "population", "pop_total"],
}

# Standard output columns (after normalization)
STANDARD_COLUMNS = ["iso3", "country", "year", "gdp", "source"]

# ============================================================================
# LOGGING
# ============================================================================
LOG_LEVEL = "INFO"  # DEBUG, INFO, WARNING, ERROR
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
