# CleanClinic

A clinical data pipeline for ingesting, transforming, and orchestrating clinical datasets with PHI redaction and enrichment.

## Structure

- `pipeline.yaml` - Orchestration DAG
- `ingest/` - Ingestion scripts (DICOM to Parquet, docling wrapper)
- `transforms/` - Data transformations (PHI redaction, enrichment, UMLS mapping, FHIR flattening)
- `configs/` - Configuration files
- `scripts/` - Run scripts for pipeline execution
- `docker/` - Docker-related files
- `examples/` - Example data or usage
- `workspace/` - Runtime outputs (raw, bronze, silver, gold, _metadata) 