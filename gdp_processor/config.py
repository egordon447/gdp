"""
GDP Data Processing System - Configuration
"""

# Target reference year (e.g., 2017 PPP)
TARGET_REFERENCE_YEAR = 2017

# Data paths
DATA_DIR = "./data/raw"
ALIGNED_DIR = "./data/aligned"
FILLED_DIR = "./data/filled"
OUTPUT_DIR = "./output"

# GDP deflator data source (for year conversion)
# Can be CSV file path or external API
DEFLATOR_SOURCE = "./data/metadata/gdp_deflator.csv"

# Processing parameters
MIN_YEAR = 1800
MAX_YEAR = 2025
MIN_DATA_POINTS = 5  # Minimum data points needed for growth rate calculation

# Visualization parameters
FIGURE_SIZE = (14, 8)
DPI = 300
COLORMAP = "YlGnBu"

# Data source metadata (manually specified or auto-detected)
DATA_SOURCE_METADATA = {
    "pwt_11": {"ref_year": 2017, "name": "Penn World Table 11"},
    "pwt_10_1": {"ref_year": 2011, "name": "Penn World Table 10.1"},
    "pwt_10": {"ref_year": 2011, "name": "Penn World Table 10"},
    "pwt_9_1": {"ref_year": 2011, "name": "Penn World Table 9.1"},
    "pwt_9": {"ref_year": 2011, "name": "Penn World Table 9"},
    "pwt_8_1": {"ref_year": 2011, "name": "Penn World Table 8.1"},
    "pwt_8": {"ref_year": 2005, "name": "Penn World Table 8"},
    "pwt_7_1": {"ref_year": 2005, "name": "Penn World Table 7.1"},
    "pwt_7": {"ref_year": 2005, "name": "Penn World Table 7"},
    "pwt_6_3": {"ref_year": 2005, "name": "Penn World Table 6.3"},
    "pwt_6_2": {"ref_year": 2000, "name": "Penn World Table 6.2"},
    "pwt_6_1": {"ref_year": 2000, "name": "Penn World Table 6.1"},
    "pwt_5_6": {"ref_year": 1996, "name": "Penn World Table 5.6"},
    "mpd_2023": {"ref_year": 2011, "name": "Maddison Project 2023"},
    "mpd_2020": {"ref_year": 2011, "name": "Maddison Project 2020"},
    "mpd_2018": {"ref_year": 2011, "name": "Maddison Project 2018"},
    "mpd_2013": {"ref_year": 1990, "name": "Maddison Project 2013"},
    "mpd_2010": {"ref_year": 1990, "name": "Maddison Project 2010"},
    "wdi_ppp1": {"ref_year": 2017, "name": "World Bank WDI PPP 2017"},
    "wdi_archive3": {"ref_year": 2011, "name": "World Bank WDI Archive 2011"},
    "wdi_current_lcu_ppp1": {"ref_year": 2017, "name": "World Bank WDI Current LCU PPP"},
    "un_data_ppp": {"ref_year": 2015, "name": "UN GDP PPP Data"},
    "barro_ursua": {"ref_year": 2009, "name": "Barro-Ursua Dataset"},
}
