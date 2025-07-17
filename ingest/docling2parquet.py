"""
DPK transform:
  - reads *all* files under `input_path`
  - CSVs → pandas (structured data)
  - TXT/PDF/images/DOCX → Docling2Parquet (unstructured documents)
  - writes ONE parquet into `output_path`
"""

import sys
from pathlib import Path
import pandas as pd
from dpk.data_prep_kit.transforms.language.docling2parquet.dpk_docling2parquet.transform_python import Docling2Parquet

# Only process these specific document types through Docling2Parquet
SUPPORTED_DOC_EXTS = ['.txt', '.pdf', '.png', '.jpeg', '.jpg', '.docx']
CSV_EXT = '.csv'

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Ingest documents using DPK Docling2Parquet and pandas for CSVs.")
    parser.add_argument("--input", required=True, help="Input directory containing documents.")
    parser.add_argument("--output", required=True, help="Output directory for Parquet file.")
    parser.add_argument("--contents_type", default="text/markdown", choices=["text/markdown", "text/plain", "application/json"], help="Output format for document contents.")
    parser.add_argument("--batch_size", type=int, default=10, help="Number of documents per output Parquet file.")
    args = parser.parse_args()

    input_dir = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Find all files
    all_files = list(input_dir.rglob("*"))
    csv_files = [f for f in all_files if f.suffix.lower() == CSV_EXT and f.is_file()]
    doc_files = [f for f in all_files if f.suffix.lower() in SUPPORTED_DOC_EXTS and f.is_file()]

    print(f"Found {len(csv_files)} CSV files and {len(doc_files)} document files (TXT/PDF/images/DOCX)")

    dfs = []
    # Process CSVs with pandas ONLY
    if csv_files:
        print(f"Processing {len(csv_files)} CSV files with pandas...")
        for csv_file in csv_files:
            try:
                print(f"  Reading {csv_file.name}...")
                df = pd.read_csv(csv_file)
                df['source_file'] = str(csv_file.relative_to(input_dir))
                df['file_type'] = 'csv'
                dfs.append(df)
                print(f"  ✓ {csv_file.name}: {len(df)} rows")
            except Exception as e:
                print(f"  ❌ Failed to process {csv_file}: {e}")
        if dfs:
            csv_df = pd.concat(dfs, ignore_index=True)
            print(f"✓ Combined CSV data: {len(csv_df)} total rows")
        else:
            csv_df = None
    else:
        csv_df = None

    # Process specific docs (TXT/PDF/images/DOCX) with Docling2Parquet ONLY
    if doc_files:
        print(f"Processing {len(doc_files)} document files (TXT/PDF/images/DOCX) with Docling2Parquet...")
        for doc_file in doc_files:
            print(f"  Found document: {doc_file.name}")
        
        # Only process the specific document types
        result = Docling2Parquet(
            input_folder=str(input_dir),
            output_folder=str(output_dir),
            data_files_to_use=SUPPORTED_DOC_EXTS,  # Only TXT/PDF/images/DOCX
            docling2parquet_contents_type=args.contents_type,
            docling2parquet_batch_size=args.batch_size
        ).transform()
        
        if result == 0:
            # Find the output parquet(s) from Docling2Parquet
            docling_parquets = list(output_dir.glob("*.parquet"))
            if docling_parquets:
                print(f"Found {len(docling_parquets)} Docling2Parquet output files")
                docling_dfs = []
                for pq in docling_parquets:
                    try:
                        df = pd.read_parquet(pq)
                        df['file_type'] = 'document'
                        docling_dfs.append(df)
                    except Exception as e:
                        print(f"  ❌ Failed to read {pq}: {e}")
                
                if docling_dfs:
                    docling_df = pd.concat(docling_dfs, ignore_index=True)
                    print(f"✓ Combined document data: {len(docling_df)} total rows")
                else:
                    docling_df = None
            else:
                docling_df = None
        else:
            print(f"❌ Docling2Parquet failed with code {result}")
            docling_df = None
    else:
        docling_df = None

    # Merge results if both exist
    if csv_df is not None and docling_df is not None:
        print("Merging CSV and document data...")
        merged_df = pd.concat([csv_df, docling_df], ignore_index=True, sort=False)
    elif csv_df is not None:
        merged_df = csv_df
    elif docling_df is not None:
        merged_df = docling_df
    else:
        print("❌ No supported files found to process.")
        sys.exit(1)

    print(f"Final merged dataset: {len(merged_df)} rows, {len(merged_df.columns)} columns")

    # Force all columns to string to avoid pyarrow type issues
    merged_df = merged_df.astype(str)

    # Write single output Parquet
    out_file = output_dir / "bronze_unified.parquet"
    merged_df.to_parquet(out_file, index=False)
    print(f"✅ Ingestion completed successfully. Output in {out_file}") 