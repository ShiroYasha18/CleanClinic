# cleanclinic/transforms/ingest/dcm2parquet.py
"""
Lightweight DICOM → Parquet converter
- reads every *.dcm* file under *src_dir*
- extracts selected meta-data tags (no pixel data)
- writes a single Parquet file to *dst_parquet*
"""

from pathlib import Path
from typing import List, Dict, Any

import pydicom
import pandas as pd
import logging

LOGGER = logging.getLogger(__name__)

# Tags we want to keep – extend as needed
TAGS = {
    "PatientID": "patient_id",
    "PatientName": "patient_name",
    "StudyDate": "study_date",
    "StudyTime": "study_time",
    "Modality": "modality",
    "BodyPartExamined": "body_part",
    "StudyDescription": "study_desc",
    "SeriesDescription": "series_desc",
    "InstitutionName": "institution",
    "Manufacturer": "manufacturer",
    "ManufacturerModelName": "model",
    "InstitutionAddress": "institution_address",
    "AccessionNumber": "accession_number",
    "StudyInstanceUID": "study_uid",
    "SeriesInstanceUID": "series_uid",
    "SOPInstanceUID": "sop_uid",
}

def extract_tags(ds: pydicom.Dataset) -> Dict[str, Any]:
    """Return a flat dict with selected DICOM tags."""
    record = {}
    for tag_key, col_name in TAGS.items():
        value = ds.get(tag_key, None)
        if value is None:
            record[col_name] = None
        else:
            # Convert multi-value elements to string
            record[col_name] = str(value) if len(str(value).split("\\")) == 1 else str(value)
    return record

def dcm_to_parquet(src_dir: Path, dst_parquet: Path) -> None:
    """
    Convert all *.dcm files under `src_dir` into a single Parquet file.
    """
    src_dir = Path(src_dir)
    dst_parquet = Path(dst_parquet)
    dst_parquet.parent.mkdir(parents=True, exist_ok=True)

    dcm_files: List[Path] = list(src_dir.rglob("*.dcm"))
    if not dcm_files:
        LOGGER.warning("No .dcm files found in %s", src_dir)
        return

    records: List[Dict[str, Any]] = []
    for dcm_file in dcm_files:
        try:
            ds = pydicom.dcmread(dcm_file, stop_before_pixels=True)
            record = extract_tags(ds)
            record["file_path"] = str(dcm_file.relative_to(src_dir))
            records.append(record)
        except Exception as e:
            LOGGER.error("Skipping %s: %s", dcm_file, e)

    df = pd.DataFrame.from_records(records)
    df.to_parquet(dst_parquet, index=False)
    LOGGER.info("Wrote %d rows → %s", len(df), dst_parquet)

# CLI shim (optional)
if __name__ == "__main__":
    import argparse, sys
    parser = argparse.ArgumentParser(description="DICOM → Parquet")
    parser.add_argument("src_dir", type=Path, help="Folder with .dcm files")
    parser.add_argument("dst_parquet", type=Path, help="Output parquet file")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    dcm_to_parquet(args.src_dir, args.dst_parquet)