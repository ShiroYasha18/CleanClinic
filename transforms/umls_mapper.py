import os
import csv
import json
import logging
from collections import defaultdict
from typing import Dict, List, Tuple, Optional
import pandas as pd
import re

logger = logging.getLogger(__name__)

class UMLSMapper:
    """Maps clinical codes to UMLS terminology for enrichment."""
    
    def __init__(self, api_key: Optional[str] = None, 
                 umls_data_path: Optional[str] = None,
                 method: str = "EXACT"):
        """
        Initialize the UMLS Mapper.
        
        Args:
            api_key: UMLS API key (optional for online mapping)
            umls_data_path: Path to UMLS data files (optional)
            method: Mapping method ("RO", "PAR_CHD", "EXACT")
        """
        self.api_key = api_key
        self.umls_data_path = umls_data_path
        self.method = method
        self.mappings_loaded = False
        self.cui_to_snomed = {}
        self.cui_to_icd10 = {}
        
        # Try to load mappings if UMLS data path is provided
        if self.umls_data_path and os.path.exists(self.umls_data_path):
            try:
                self._load_mappings()
            except Exception as e:
                logger.warning(f"Failed to load UMLS mappings: {e}")
        else:
            logger.info("UMLS data path not provided - will use online mapping if API key available")
    
    def _load_mappings(self):
        """Load UMLS mappings from local files."""
        if self.mappings_loaded:
            return
        
        # Check for existing JSON mappings first
        snomed_file = os.path.join(self.umls_data_path, f"cui_to_snomed_{self.method}.json")
        icd10_file = os.path.join(self.umls_data_path, f"cui_to_icd10_{self.method}.json")
        
        if os.path.exists(snomed_file) and os.path.exists(icd10_file):
            logger.info("Loading existing UMLS mappings from JSON files")
            with open(snomed_file, 'r') as f:
                self.cui_to_snomed = json.load(f)
            with open(icd10_file, 'r') as f:
                self.cui_to_icd10 = json.load(f)
            self.mappings_loaded = True
            return
        
        # Try to generate mappings from RRF files
        mrconso_file = os.path.join(self.umls_data_path, "MRCONSO.RRF")
        mrrel_file = os.path.join(self.umls_data_path, "MRREL.RRF")
        
        if not os.path.exists(mrconso_file):
            logger.warning(f"MRCONSO.RRF not found at {mrconso_file}")
            return
        
        logger.info("Generating UMLS mappings from RRF files")
        self.cui_to_snomed, self.cui_to_icd10 = self._generate_mappings(mrconso_file, mrrel_file)
        
        # Save mappings for future use
        try:
            with open(snomed_file, 'w') as f:
                json.dump(self.cui_to_snomed, f)
            with open(icd10_file, 'w') as f:
                json.dump(self.cui_to_icd10, f)
            logger.info("UMLS mappings saved to JSON files")
        except Exception as e:
            logger.warning(f"Failed to save UMLS mappings: {e}")
        
        self.mappings_loaded = True
    
    def _generate_mappings(self, mrconso_file: str, mrrel_file: str) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
        """Generate mappings between UMLS CUIs and SNOMED CT and ICD-10 codes."""
        cui_to_snomed = defaultdict(list)
        cui_to_icd10 = defaultdict(list)

        # Read MRCONSO file and populate the dictionaries
        with open(mrconso_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter="|", quoting=csv.QUOTE_NONE)
            for row in reader:
                if len(row) < 14:
                    continue
                    
                cui = row[0]
                sab = row[11]  # Abbreviation for the source of the code
                code = row[13]  # Code assigned by the source

                # Check if the code source is SNOMED CT or ICD-10
                if sab == "SNOMEDCT_US":
                    if code not in cui_to_snomed[cui]:
                        cui_to_snomed[cui].append(code)
                elif sab == "ICD10CM":
                    if code not in cui_to_icd10[cui]:
                        cui_to_icd10[cui].append(code)

        # Read MRREL file and update the dictionaries based on the selected method
        if self.method in ["RO", "PAR_CHD"] and os.path.exists(mrrel_file):
            with open(mrrel_file, "r", encoding="utf-8") as f:
                reader = csv.reader(f, delimiter="|", quoting=csv.QUOTE_NONE)
                for row in reader:
                    if len(row) < 8:
                        continue
                        
                    cui1 = row[0]
                    cui2 = row[4]
                    rel = row[7]  # Abbreviation for the relationship between the concepts

                    # Check if the relationship is selected and both CUIs are in the respective dictionaries
                    if ((self.method == "RO" and rel == "RO") or
                        (self.method == "PAR_CHD" and (rel == "PAR" or rel == "CHD"))) and cui1 in cui_to_snomed and cui2 in cui_to_icd10:
                        for code in cui_to_snomed[cui2]: 
                            if code not in cui_to_snomed[cui1]:
                                cui_to_snomed[cui1].append(code)
                        for code in cui_to_icd10[cui2]:  
                            if code not in cui_to_icd10[cui1]:
                                cui_to_icd10[cui1].append(code)

        return dict(cui_to_snomed), dict(cui_to_icd10)
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform DataFrame by enriching clinical codes with UMLS terminology.
        
        Args:
            df: Input DataFrame with clinical codes
            
        Returns:
            DataFrame with enriched UMLS mappings
        """
        logger.info("Starting UMLS mapping enrichment...")
        
        # Create a copy to avoid modifying original
        enriched_df = df.copy()
        
        # Find clinical code columns
        code_columns = self._find_clinical_code_columns(enriched_df)
        
        if not code_columns:
            logger.info("No clinical code columns found - skipping UMLS mapping")
            return enriched_df
        
        logger.info(f"Found clinical code columns: {code_columns}")
        
        # Apply UMLS mapping to each code column
        for column in code_columns:
            enriched_df = self._enrich_code_column(enriched_df, column)
        
        logger.info("UMLS mapping enrichment completed")
        return enriched_df
    
    def _find_clinical_code_columns(self, df: pd.DataFrame) -> List[str]:
        """Find columns that likely contain clinical codes."""
        code_columns = []
        
        # Common clinical code column patterns
        code_patterns = [
            'icd', 'snomed', 'cpt', 'hcpcs', 'loinc', 'rxnorm',
            'diagnosis', 'procedure', 'code', 'cui', 'concept'
        ]
        
        for column in df.columns:
            column_lower = column.lower()
            
            # Check if column name matches code patterns
            if any(pattern in column_lower for pattern in code_patterns):
                code_columns.append(column)
                continue
            
            # Check if column content looks like codes
            if self._column_contains_codes(df[column]):
                code_columns.append(column)
        
        return code_columns
    
    def _column_contains_codes(self, series: pd.Series) -> bool:
        """Check if a column contains clinical codes."""
        # Sample the column to avoid processing all data
        sample_size = min(1000, len(series))
        sample = series.dropna().sample(n=sample_size, random_state=42)
        
        if len(sample) == 0:
            return False
        
        # Look for common code patterns
        sample_str = sample.astype(str).str.cat(sep=' ')
        
        # ICD-10 pattern: A00.0-Z99.9
        icd_pattern = r'\b[A-Z]\d{2}\.\d{1,2}\b'
        if re.search(icd_pattern, sample_str):
            return True
        
        # SNOMED pattern: 6-18 digit numbers
        snomed_pattern = r'\b\d{6,18}\b'
        if re.search(snomed_pattern, sample_str):
            return True
        
        # CPT pattern: 5 digit numbers
        cpt_pattern = r'\b\d{5}\b'
        if re.search(cpt_pattern, sample_str):
            return True
        
        return False
    
    def _enrich_code_column(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        """Enrich a specific code column with UMLS mappings."""
        # Add UMLS mapping columns
        df[f'{column}_umls_cui'] = ''
        df[f'{column}_umls_snomed'] = ''
        df[f'{column}_umls_icd10'] = ''
        df[f'{column}_umls_concept'] = ''
        
        # Apply mappings if available
        if self.mappings_loaded:
            # Use local mappings
            for idx, code in df[column].items():
                if pd.notna(code):
                    code_str = str(code).strip()
                    
                    # Find matching CUI and get mappings
                    cui = self._find_cui_for_code(code_str)
                    if cui:
                        df.loc[idx, f'{column}_umls_cui'] = cui
                        df.loc[idx, f'{column}_umls_snomed'] = ','.join(self.cui_to_snomed.get(cui, []))
                        df.loc[idx, f'{column}_umls_icd10'] = ','.join(self.cui_to_icd10.get(cui, []))
        
        elif self.api_key:
            # Use online UMLS API (simplified - would need actual API implementation)
            logger.info("Online UMLS mapping not implemented - skipping")
        
        return df
    
    def _find_cui_for_code(self, code: str) -> Optional[str]:
        """Find CUI for a given code (simplified implementation)."""
        # This is a simplified lookup - in practice you'd have a proper code-to-CUI mapping
        # For now, we'll return None to indicate no mapping found
        return None

# Legacy functions for backward compatibility
def generate_mappings(method: str) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    """Legacy function - use UMLSMapper class instead."""
    logger.warning("generate_mappings is deprecated - use UMLSMapper class instead")
    return {}, {}

def save_mappings(method: str):
    """Legacy function - use UMLSMapper class instead."""
    logger.warning("save_mappings is deprecated - use UMLSMapper class instead")
    pass