#!/usr/bin/env python3
"""
GoldProcessor: Silver → Gold business-ready transform

- Keeps only business/analytics columns
- Flattens nested/list columns
- Precomputes metrics (e.g. word_count)
- Prepares for partitioned Parquet output
"""
import pandas as pd
import numpy as np
import logging
from datetime import datetime
import hashlib
import yaml

logger = logging.getLogger(__name__)

class GoldProcessor:
    def __init__(self, config=None):
        self.config = config or {}
        self.metrics = {}

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Starting Silver → Gold business transform...")
        # 1. Choose columns
        gold_cols = [
            'doc_id', 'lat', 'lon', 'clean_text', 'embedding', 'pii_flag',
            'doc_type', 'capture_date', 'source_file', 'processed_date'
        ]
        available_cols = [c for c in gold_cols if c in df.columns]
        gold_df = df[available_cols].copy()
        # 2. Flatten lists
        for col in gold_df.columns:
            if gold_df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                gold_df[col] = gold_df[col].apply(lambda x: str(x) if isinstance(x, (list, dict)) else x)
        # 3. Precompute metrics
        if 'clean_text' in gold_df.columns:
            gold_df['word_count'] = gold_df['clean_text'].fillna('').apply(lambda x: len(str(x).split()))
        # Example: add dummy pharmacy_density/risk_score if not present
        if 'pharmacy_density' not in gold_df.columns:
            gold_df['pharmacy_density'] = np.nan
        if 'risk_score' not in gold_df.columns:
            gold_df['risk_score'] = np.nan
        # 4. Partition/sort columns (for writing, not in-memory)
        # (Sorting by lat/lon for geospatial speed)
        if 'lat' in gold_df.columns and 'lon' in gold_df.columns:
            gold_df = gold_df.sort_values(['lat', 'lon'])
        # 5. Add dashboard metadata
        gold_df['gold_processed_date'] = datetime.now().isoformat()
        gold_df['dashboard_ready'] = True
        return gold_df

    def save_partitioned(self, df: pd.DataFrame, out_dir: str):
        # Partition by doc_type and capture_date if present
        partition_cols = [c for c in ['doc_type', 'capture_date'] if c in df.columns]
        import pyarrow as pa
        import pyarrow.parquet as pq
        table = pa.Table.from_pandas(df)
        pq.write_to_dataset(table, root_path=out_dir, partition_cols=partition_cols)
        logger.info(f"Saved Gold Parquet partitioned by {partition_cols} to {out_dir}")

    def save_quality_report(self, df: pd.DataFrame, out_path: str):
        report = {
            'row_count': len(df),
            'col_count': len(df.columns),
            'columns': list(df.columns),
            'timestamp': datetime.now().isoformat(),
        }
        with open(out_path, 'w') as f:
            yaml.dump(report, f)
        logger.info(f"Saved Gold quality report: {out_path}") 