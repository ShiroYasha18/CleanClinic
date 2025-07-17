#!/usr/bin/env python3
"""
Process Silver to Gold Layer

This script processes all Parquet files in workspace/silver/ using GoldProcessor
and saves the results to workspace/gold/ as partitioned Parquet datasets.
"""
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
import pandas as pd
import logging
from transforms.gold_processor import GoldProcessor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SILVER_DIR = Path('workspace/silver')
GOLD_DIR = Path('workspace/gold')
GOLD_DIR.mkdir(parents=True, exist_ok=True)


def process_all_silver_files():
    processor = GoldProcessor()
    parquet_files = list(SILVER_DIR.glob('*.parquet'))
    if not parquet_files:
        logger.warning('No Parquet files found in workspace/silver/')
        return
    for file in parquet_files:
        logger.info(f'Processing {file.name}...')
        df = pd.read_parquet(file)
        gold_df = processor.transform(df)
        # Save as partitioned Parquet dataset
        gold_dataset_dir = GOLD_DIR / file.stem.replace('processed_', 'gold_')
        processor.save_partitioned(gold_df, str(gold_dataset_dir))
        # Save quality report
        report_file = gold_dataset_dir / 'quality_report.yaml'
        gold_dataset_dir.mkdir(parents=True, exist_ok=True)  # Ensure directory exists
        processor.save_quality_report(gold_df, str(report_file))
        logger.info(f'Saved Gold dataset: {gold_dataset_dir}')
    logger.info('Silver to Gold processing complete.')

if __name__ == '__main__':
    process_all_silver_files() 