#!/usr/bin/env python3
"""
CleanClinic Bronze to Silver Processing Trigger

This script processes Parquet files from the bronze directory through:
1. UMLS Mapping (enriches clinical codes and outputs Delta format)
2. Geo Enrichment (adds location context and metadata)
3. PII Scrubbing (removes/cleanses personal identifiers)

Outputs processed files to the silver directory in Delta format.
"""

import os
import sys
import logging
import argparse
from pathlib import Path
from typing import List, Dict, Any
import pandas as pd
import yaml
from datetime import datetime

# Add the project root to the path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from transforms.umls_mapper import UMLSMapper
from transforms.geo_enricher import GeoEnricher
from transforms.pii_scrubber import PIIScrubber
from transforms.date_shifter import shift_dates

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BronzeToSilverProcessor:
    """Processes bronze Parquet files through UMLS mapping, geo enrichment, and PII scrubbing."""
    
    def __init__(self, config_path: str = None):
        """Initialize the processor with configuration."""
        self.config = self._load_config(config_path)
        self.bronze_dir = Path(self.config.get('bronze_dir', 'workspace/bronze'))
        self.silver_dir = Path(self.config.get('silver_dir', 'workspace/silver'))
        self.temp_dir = Path(self.config.get('temp_dir', 'workspace/temp'))
        
        # Create output directories
        self.silver_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize transforms
        self.transforms = self._initialize_transforms()
        
    def _load_config(self, config_path: str = None) -> Dict[str, Any]:
        """Load configuration from YAML file or use defaults."""
        if config_path and Path(config_path).exists():
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        
        # Default configuration
        return {
            'bronze_dir': 'workspace/bronze',
            'silver_dir': 'workspace/silver',
            'temp_dir': 'workspace/temp',
            'umls_api_key': os.getenv('UMLS_API_KEY'),
            'umls_data_path': os.getenv('UMLS_DATA_PATH'),
            'umls_method': os.getenv('UMLS_METHOD', 'EXACT'),
            'geo_api_key': os.getenv('GEO_API_KEY'),  # For reverse geocoding
            'pii_scrubbing_mode': 'remove',  # 'remove', 'mask', or 'hash'
            'date_shift_days': 100,
            'delta_format': True,  # Output in Delta format
            'delta_options': {
                'mode': 'overwrite',
                'partitionBy': ['source_file', 'processed_date']
            }
        }
    
    def _initialize_transforms(self) -> Dict[str, Any]:
        """Initialize all transformation components."""
        transforms = {}
        
        try:
            # UMLS mapper (always include, skip gracefully if not configured)
            transforms['umls_mapper'] = UMLSMapper(
                api_key=self.config.get('umls_api_key'),
                umls_data_path=self.config.get('umls_data_path'),
                method=self.config.get('umls_method', 'EXACT')
            )
            logger.info("✓ UMLS mapper initialized")
            # Geo enricher
            transforms['geo_enricher'] = GeoEnricher(
                api_key=self.config.get('geo_api_key')
            )
            logger.info("✓ Geo enricher initialized")
            # PII scrubber
            transforms['pii_scrubber'] = PIIScrubber(
                mode=self.config['pii_scrubbing_mode']
            )
            logger.info("✓ PII scrubber initialized")
        except Exception as e:
            logger.error(f"Failed to initialize transforms: {e}")
            raise
        
        return transforms
    
    def find_parquet_files(self) -> List[Path]:
        """Find all Parquet files in the bronze directory."""
        parquet_files = list(self.bronze_dir.rglob("*.parquet"))
        logger.info(f"Found {len(parquet_files)} Parquet files in bronze directory")
        return parquet_files
    
    def process_file(self, input_path: Path) -> Path:
        """Process a single Parquet file through all transforms."""
        logger.info(f"Processing: {input_path.name}")
        
        # Read the Parquet file
        try:
            df = pd.read_parquet(input_path)
            logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
        except Exception as e:
            logger.error(f"Failed to read {input_path}: {e}")
            raise
        
        # Apply transforms in sequence
        original_df = df.copy()
        
        # 1. PII Scrubbing (first to remove sensitive data)
        logger.info("Applying PII scrubbing...")
        try:
            df = self.transforms['pii_scrubber'].transform(df)
            logger.info("✓ PII scrubbing completed")
        except Exception as e:
            logger.error(f"PII scrubbing failed: {e}")
            # Continue with other transforms
        
        # 2. Date Shifting
        date_shift_days = self.config.get('date_shift_days', 100)
        logger.info(f"Applying date shifting by {date_shift_days} days...")
        try:
            df = shift_dates(df, date_shift_days)
            logger.info("✓ Date shifting completed")
        except Exception as e:
            logger.error(f"Date shifting failed: {e}")
        
        # 3. Geo Enrichment
        logger.info("Applying geo enrichment...")
        try:
            df = self.transforms['geo_enricher'].transform(df)
            logger.info("✓ Geo enrichment completed")
        except Exception as e:
            logger.error(f"Geo enrichment failed: {e}")
            # Continue with other transforms
        
        # 4. UMLS Mapping (if available)
        if 'umls_mapper' in self.transforms:
            logger.info("Applying UMLS mapping...")
            try:
                df = self.transforms['umls_mapper'].transform(df)
                logger.info("✓ UMLS mapping completed")
            except Exception as e:
                logger.error(f"UMLS mapping failed: {e}")
                # Continue with other transforms
        
        # Add metadata columns
        df['source_file'] = input_path.name
        df['processed_date'] = datetime.now().isoformat()
        df['processing_pipeline'] = 'bronze_to_silver'
        
        # Save processed file to silver directory
        if self.config.get('delta_format', True):
            output_path = self._save_as_delta(df, input_path)
        else:
            output_path = self._save_as_parquet(df, input_path)
        
        # Log transformation summary
        self._log_transformation_summary(input_path, output_path, original_df, df)
        
        return output_path
    
    def _save_as_delta(self, df: pd.DataFrame, input_path: Path) -> Path:
        """Save DataFrame as Delta table."""
        try:
            # Import Delta Lake and PyArrow
            from deltalake import write_deltalake
            import pyarrow as pa
            
            # Ensure DataFrame is properly formatted for Arrow
            # Reset index and ensure all columns are Arrow-compatible
            df_clean = df.reset_index(drop=True)
            
            # Convert any problematic data types
            for col in df_clean.columns:
                if df_clean[col].dtype == 'object':
                    # Convert object columns to string to avoid Arrow issues
                    df_clean[col] = df_clean[col].astype(str)
            
            # Convert to PyArrow table first
            table = pa.Table.from_pandas(df_clean)
            
            # Create Delta table path
            table_name = f"silver_{input_path.stem}"
            delta_path = self.silver_dir / table_name
            
            # Write as Delta table using PyArrow table
            write_deltalake(
                str(delta_path),
                table,
                mode=self.config['delta_options']['mode'],
                partition_by=self.config['delta_options'].get('partitionBy', [])
            )
            
            logger.info(f"✓ Saved Delta table: {delta_path}")
            return delta_path
            
        except ImportError:
            logger.warning("⚠ Delta Lake not available - falling back to Parquet")
            return self._save_as_parquet(df, input_path)
        except Exception as e:
            logger.error(f"Failed to save Delta table: {e}")
            logger.info("Falling back to Parquet format...")
            return self._save_as_parquet(df, input_path)
    
    def _save_as_parquet(self, df: pd.DataFrame, input_path: Path) -> Path:
        """Save DataFrame as Parquet file."""
        output_path = self.silver_dir / f"processed_{input_path.name}"
        
        try:
            df.to_parquet(output_path, index=False)
            logger.info(f"✓ Saved Parquet file: {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"Failed to save Parquet file: {e}")
            raise
    
    def _log_transformation_summary(self, input_path: Path, output_path: Path, 
                                  original_df: pd.DataFrame, processed_df: pd.DataFrame):
        """Log a summary of the transformations applied."""
        summary = {
            'input_file': input_path.name,
            'output_file': str(output_path),
            'output_format': 'delta' if self.config.get('delta_format', True) else 'parquet',
            'original_rows': len(original_df),
            'processed_rows': len(processed_df),
            'original_columns': len(original_df.columns),
            'processed_columns': len(processed_df.columns),
            'transforms_applied': list(self.transforms.keys()),
            'timestamp': datetime.now().isoformat()
        }
        
        # Save summary to YAML
        summary_path = output_path.with_suffix('.summary.yaml')
        if hasattr(output_path, 'with_suffix'):
            summary_path = output_path.with_suffix('.summary.yaml')
        else:
            summary_path = Path(str(output_path) + '.summary.yaml')
            
        with open(summary_path, 'w') as f:
            yaml.dump(summary, f, default_flow_style=False)
        
        logger.info(f"Transformation summary saved: {summary_path}")
    
    def process_all(self) -> List[Path]:
        """Process all Parquet files in the bronze directory."""
        parquet_files = self.find_parquet_files()
        
        if not parquet_files:
            logger.warning("No Parquet files found in bronze directory")
            return []
        
        processed_files = []
        
        for parquet_file in parquet_files:
            try:
                processed_file = self.process_file(parquet_file)
                processed_files.append(processed_file)
            except Exception as e:
                logger.error(f"Failed to process {parquet_file}: {e}")
                continue
        
        logger.info(f"Successfully processed {len(processed_files)} out of {len(parquet_files)} files")
        return processed_files

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Process bronze Parquet files to silver")
    parser.add_argument('--config', '-c', help='Path to configuration YAML file')
    parser.add_argument('--bronze-dir', help='Bronze directory path')
    parser.add_argument('--silver-dir', help='Silver directory path')
    parser.add_argument('--pii-mode', choices=['remove', 'mask', 'hash'], default='remove',
                       help='PII scrubbing mode (default: remove)')
    parser.add_argument('--delta-format', action='store_true', default=True,
                       help='Output in Delta format (default: True)')
    parser.add_argument('--parquet-format', action='store_true',
                       help='Output in Parquet format instead of Delta')
    
    args = parser.parse_args()
    
    # Initialize processor
    processor = BronzeToSilverProcessor(args.config)
    
    # Override config with command line arguments
    if args.bronze_dir:
        processor.bronze_dir = Path(args.bronze_dir)
    if args.silver_dir:
        processor.silver_dir = Path(args.silver_dir)
    if args.pii_mode:
        processor.config['pii_scrubbing_mode'] = args.pii_mode
    if args.parquet_format:
        processor.config['delta_format'] = False
    
    # Process all files
    try:
        processed_files = processor.process_all()
        
        if processed_files:
            logger.info("🎉 Bronze to Silver processing completed successfully!")
            logger.info(f"Processed files saved to: {processor.silver_dir}")
        else:
            logger.warning("No files were processed successfully")
            
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 