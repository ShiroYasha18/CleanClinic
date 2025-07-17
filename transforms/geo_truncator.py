import argparse
import pandas as pd
from pathlib import Path
import pyarrow.parquet as pq
import os

def truncate_geo(df):
    geo_cols = [c for c in df.columns if c.lower() in ["latitude", "longitude", "lat", "lon", "lng"]]
    for col in geo_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').round(2)
    return df

def main():
    parser = argparse.ArgumentParser(description="Truncate latitude/longitude columns in Parquet files to 2 decimal places.")
    parser.add_argument("--input", required=True, help="Input directory or Parquet file.")
    parser.add_argument("--output", required=True, help="Output directory.")
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
        df = truncate_geo(df)
        out_file = output_dir / f.name
        df.to_parquet(out_file, index=False)
        print(f"Truncated geo columns in {f} -> {out_file}")

if __name__ == "__main__":
    main()