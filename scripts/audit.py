#!/usr/bin/env python3
import argparse
import pandas as pd
from pathlib import Path
import hashlib
import yaml
from datetime import datetime

def hash_file(path):
    BUF_SIZE = 65536
    sha256 = hashlib.sha256()
    with open(path, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            sha256.update(data)
    return sha256.hexdigest()

def audit_parquet(parquet_path):
    df = pd.read_parquet(parquet_path)
    return {
        'filename': str(parquet_path),
        'hash': hash_file(parquet_path),
        'row_count': len(df),
        'column_count': len(df.columns),
        'timestamp': datetime.now().isoformat()
    }

def write_yaml_report(reports, out_path):
    with open(out_path, 'w') as f:
        yaml.dump(reports, f)
    print(f"Wrote YAML audit to {out_path}")

def generate_safe_harbor_pdf(report, out_path):
    # Placeholder for PDF generation
    with open(out_path, 'w') as f:
        f.write("SAFE HARBOR CHECKLIST\n\n")
        for k, v in report.items():
            f.write(f"{k}: {v}\n")
    print(f"Wrote Safe-Harbor checklist (stub) to {out_path}")

def main():
    parser = argparse.ArgumentParser(description="Audit Parquet files: hash, YAML snapshot, Safe-Harbor checklist.")
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

    reports = [audit_parquet(f) for f in files]
    yaml_path = output_dir / "audit_snapshot.yaml"
    write_yaml_report(reports, yaml_path)
    # Optionally, generate a PDF for the first file
    if reports:
        pdf_path = output_dir / "safe_harbor_checklist.pdf"
        generate_safe_harbor_pdf(reports[0], pdf_path)

if __name__ == "__main__":
    main() 