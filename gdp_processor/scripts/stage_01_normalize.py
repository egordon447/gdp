"""
Stage 01: Normalization
Standardize column names, data types, and country codes across all sources
"""

import pandas as pd
from pathlib import Path
import logging
from typing import Dict, List
import sys

# Import config
sys.path.append(str(Path(__file__).parent.parent))
from config import (
    DATA_DIR, OUTPUT_01_NORMALIZED, COLUMN_MAPPING, 
    STANDARD_COLUMNS, DATA_SOURCES, LOG_FORMAT, LOG_LEVEL
)

# Setup logging
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


def detect_column(df: pd.DataFrame, standard_name: str) -> str:
    """
    Detect which column in the dataframe corresponds to a standard name
    
    Args:
        df: Input dataframe
        standard_name: Standard column name (e.g., 'country', 'year', 'gdp')
    
    Returns:
        Detected column name from the dataframe
    """
    possible_names = COLUMN_MAPPING.get(standard_name, [])
    df_columns_lower = {col.lower(): col for col in df.columns}
    
    for candidate in possible_names:
        if candidate.lower() in df_columns_lower:
            return df_columns_lower[candidate.lower()]
    
    logger.warning(f"Could not detect column for '{standard_name}' in {df.columns.tolist()}")
    return None


def normalize_source(source_path: Path, source_name: str) -> pd.DataFrame:
    """
    Normalize a single data source
    
    Args:
        source_path: Path to the raw CSV file
        source_name: Name of the data source (for metadata)
    
    Returns:
        Normalized dataframe with standard columns
    """
    logger.info(f"Processing: {source_path.name}")
    
    # Load data
    df = pd.read_csv(source_path)
    logger.info(f"  Loaded {len(df)} rows, {len(df.columns)} columns")
    
    # Detect and map columns
    column_map = {}
    for std_col in ['iso3', 'country', 'year', 'gdp']:
        detected = detect_column(df, std_col)
        if detected:
            column_map[detected] = std_col
    
    if not column_map:
        logger.error(f"  Could not detect any standard columns in {source_path.name}")
        return None
    
    # Rename columns
    df_normalized = df[list(column_map.keys())].rename(columns=column_map)
    
    # Add source column
    df_normalized['source'] = source_name
    
    # Convert data types
    if 'year' in df_normalized.columns:
        df_normalized['year'] = pd.to_numeric(df_normalized['year'], errors='coerce').astype('Int64')
    
    if 'gdp' in df_normalized.columns:
        df_normalized['gdp'] = pd.to_numeric(df_normalized['gdp'], errors='coerce')
    
    # Remove rows with missing critical values
    initial_count = len(df_normalized)
    df_normalized = df_normalized.dropna(subset=['year', 'gdp'])
    dropped = initial_count - len(df_normalized)
    if dropped > 0:
        logger.warning(f"  Dropped {dropped} rows with missing year/gdp")
    
    # Standardize country codes if possible
    # TODO: Implement ISO3 standardization if needed
    
    # Remove duplicates
    df_normalized = df_normalized.drop_duplicates(subset=['country', 'year'], keep='first')
    
    logger.info(f"  Normalized to {len(df_normalized)} rows")
    return df_normalized


def main():
    """Main normalization pipeline"""
    logger.info("="*60)
    logger.info("Stage 01: Normalization")
    logger.info("="*60)
    
    # Create output directory
    output_dir = Path(OUTPUT_01_NORMALIZED)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Find all raw CSV files
    data_path = Path(DATA_DIR).parent  # Go up to access original structure
    raw_files = []
    for subfolder in ['pwt_gdp', 'mpd_gdp', 'wdi_gdp', 'un_gdp', 'barroursua_gdp']:
        folder_path = data_path / subfolder
        if folder_path.exists():
            raw_files.extend(folder_path.glob('*.csv'))
    
    if not raw_files:
        logger.error(f"No CSV files found in {data_path}")
        return
    
    logger.info(f"Found {len(raw_files)} raw CSV files")
    
    # Process each file
    processed_count = 0
    for csv_file in raw_files:
        source_name = csv_file.stem  # filename without extension
        
        try:
            df_norm = normalize_source(csv_file, source_name)
            
            if df_norm is not None and len(df_norm) > 0:
                # Save normalized CSV
                output_path = output_dir / f"{source_name}.csv"
                df_norm.to_csv(output_path, index=False)
                logger.info(f"  Saved: {output_path.name}")
                processed_count += 1
            
        except Exception as e:
            logger.error(f"  Error processing {csv_file.name}: {e}")
    
    logger.info("="*60)
    logger.info(f"Stage 01 Complete: {processed_count}/{len(raw_files)} files normalized")
    logger.info(f"Output directory: {output_dir}")
    logger.info("="*60)


if __name__ == "__main__":
    main()
