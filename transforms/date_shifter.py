import argparse
import pandas as pd
from pathlib import Path
import pyarrow.parquet as pq
from datetime import timedelta
import os

def shift_dates(df, days):
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col] + timedelta(days=days)
    return df

def main():
    parser = argparse.ArgumentParser(description="Shift all date columns in Parquet files by a fixed number of days.")
    parser.add_argument("--input", required=True, help="Input directory or Parquet file.")
    parser.add_argument("--output", required=True, help="Output directory.")
    parser.add_argument("--days", type=int, default=100, help="Days to shift (default 100)")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    if input_path.is_file() and input_path.suffix == ".parquet":
        files = [input_path]
    else:
        files = list(input_path.glob("*.parquet"))

    for f in files:
        df = pd.read_parquet(f)
        df = shift_dates(df, args.days)
        out_file = output_dir / f.name
        df.to_parquet(out_file, index=False)
        print(f"Shifted dates in {f} -> {out_file}")

if __name__ == "__main__":
    main()