#!/usr/bin/env python3
"""
PII Scrubber Transform

Removes or cleanses personal identifiable information (PII) from data:
- Names, addresses, phone numbers, emails
- Social security numbers, medical record numbers
- Credit card numbers, driver's license numbers
- IP addresses, device identifiers
- Medical license numbers, provider IDs
"""

import pandas as pd
import numpy as np
import re
import hashlib
import logging
from typing import Dict, List, Optional, Union, Callable
from datetime import datetime
import json

logger = logging.getLogger(__name__)

class PIIScrubber:
    """Scrubs personal identifiable information from data."""
    
    def __init__(self, mode: str = 'remove', 
                 hash_salt: Optional[str] = None,
                 preserve_format: bool = True):
        """
        Initialize the PII Scrubber.
        
        Args:
            mode: Scrubbing mode ('remove', 'mask', 'hash', 'anonymize')
            hash_salt: Salt for hashing (if mode is 'hash')
            preserve_format: Whether to preserve original format when masking
        """
        self.mode = mode
        self.hash_salt = hash_salt or "cleanclinic_salt_2024"
        self.preserve_format = preserve_format
        
        # PII patterns and detection rules
        self.pii_patterns = self._initialize_patterns()
        
        # Column name patterns that likely contain PII
        self.pii_column_patterns = [
            'name', 'first', 'last', 'middle', 'full',
            'address', 'street', 'city', 'state', 'zip',
            'phone', 'tel', 'mobile', 'fax',
            'email', 'e-mail', 'mail',
            'ssn', 'social', 'security',
            'mrn', 'medical_record', 'patient_id',
            'credit', 'card', 'cc_',
            'license', 'drivers', 'dl_',
            'ip', 'device', 'mac',
            'provider', 'physician', 'doctor',
            'npi', 'national_provider'
        ]
        
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform DataFrame by scrubbing PII.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with PII scrubbed
        """
        logger.info(f"Starting PII scrubbing in {self.mode} mode...")
        
        # Create a copy to avoid modifying original
        scrubbed_df = df.copy()
        
        # Identify columns that likely contain PII
        pii_columns = self._identify_pii_columns(scrubbed_df)
        
        if not pii_columns:
            logger.info("No PII columns identified - skipping PII scrubbing")
            return scrubbed_df
        
        logger.info(f"Identified {len(pii_columns)} potential PII columns: {pii_columns}")
        
        # Scrub each PII column
        for column in pii_columns:
            scrubbed_df[column] = self._scrub_column(scrubbed_df[column], column)
        
        # Add scrubbing metadata
        scrubbed_df = self._add_scrubbing_metadata(scrubbed_df, pii_columns)
        
        logger.info(f"PII scrubbing completed in {self.mode} mode")
        
        return scrubbed_df
    
    def _initialize_patterns(self) -> Dict[str, Dict]:
        """Initialize PII detection patterns."""
        return {
            'ssn': {
                'pattern': r'\b\d{3}-?\d{2}-?\d{4}\b',
                'description': 'Social Security Number'
            },
            'phone': {
                'pattern': r'\b(\+\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b',
                'description': 'Phone Number'
            },
            'email': {
                'pattern': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
                'description': 'Email Address'
            },
            'credit_card': {
                'pattern': r'\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b',
                'description': 'Credit Card Number'
            },
            'medical_license': {
                'pattern': r'\b[A-Z]{2}\d{6,10}\b',
                'description': 'Medical License Number'
            },
            'npi': {
                'pattern': r'\b\d{10}\b',
                'description': 'National Provider Identifier'
            },
            'ip_address': {
                'pattern': r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b',
                'description': 'IP Address'
            },
            'mac_address': {
                'pattern': r'\b([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})\b',
                'description': 'MAC Address'
            },
            'date_of_birth': {
                'pattern': r'\b(0[1-9]|1[0-2])[/-](0[1-9]|[12]\d|3[01])[/-]\d{4}\b',
                'description': 'Date of Birth'
            }
        }
    
    def _identify_pii_columns(self, df: pd.DataFrame) -> List[str]:
        """Identify columns that likely contain PII."""
        pii_columns = []
        
        for column in df.columns:
            column_lower = column.lower()
            
            # Check if column name matches PII patterns
            if any(pattern in column_lower for pattern in self.pii_column_patterns):
                pii_columns.append(column)
                continue
            
            # Check if column content contains PII patterns
            if self._column_contains_pii(df[column]):
                pii_columns.append(column)
        
        return pii_columns
    
    def _column_contains_pii(self, series: pd.Series) -> bool:
        """Check if a column contains PII patterns."""
        # Sample the column to avoid processing all data
        sample_size = min(1000, len(series))
        sample = series.dropna().sample(n=sample_size, random_state=42)
        
        if len(sample) == 0:
            return False
        
        # Convert to string and check for patterns
        sample_str = sample.astype(str).str.cat(sep=' ')
        
        for pattern_name, pattern_info in self.pii_patterns.items():
            if re.search(pattern_info['pattern'], sample_str, re.IGNORECASE):
                logger.debug(f"Found {pattern_name} pattern in column")
                return True
        
        return False
    
    def _scrub_column(self, series: pd.Series, column_name: str) -> pd.Series:
        """Scrub PII from a specific column."""
        if self.mode == 'remove':
            return self._remove_pii(series, column_name)
        elif self.mode == 'mask':
            return self._mask_pii(series, column_name)
        elif self.mode == 'hash':
            return self._hash_pii(series, column_name)
        elif self.mode == 'anonymize':
            return self._anonymize_pii(series, column_name)
        else:
            logger.warning(f"Unknown scrubbing mode: {self.mode}")
            return series
    
    def _remove_pii(self, series: pd.Series, column_name: str) -> pd.Series:
        """Remove PII by replacing with empty string or null."""
        scrubbed_series = series.copy()
        
        for pattern_name, pattern_info in self.pii_patterns.items():
            pattern = pattern_info['pattern']
            scrubbed_series = scrubbed_series.astype(str).str.replace(
                pattern, '', regex=True, flags=re.IGNORECASE
            )
        
        # Replace empty strings with null
        scrubbed_series = scrubbed_series.replace('', np.nan)
        
        return scrubbed_series
    
    def _mask_pii(self, series: pd.Series, column_name: str) -> pd.Series:
        """Mask PII by replacing with asterisks or similar characters."""
        scrubbed_series = series.copy()
        
        def mask_text(text):
            if pd.isna(text) or text == '':
                return text
            
            text_str = str(text)
            
            # Mask different types of PII
            for pattern_name, pattern_info in self.pii_patterns.items():
                if pattern_name == 'ssn':
                    # Mask SSN: 123-45-6789 -> ***-**-6789
                    text_str = re.sub(
                        r'(\d{3})-?(\d{2})-?(\d{4})',
                        r'***-**-\3',
                        text_str
                    )
                elif pattern_name == 'phone':
                    # Mask phone: (555) 123-4567 -> (***) ***-4567
                    text_str = re.sub(
                        r'(\+\d{1,3}[-.\s]?)?\(?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})',
                        r'(***) ***-\4',
                        text_str
                    )
                elif pattern_name == 'email':
                    # Mask email: john.doe@example.com -> j***.d***@example.com
                    text_str = re.sub(
                        r'([A-Za-z0-9._%+-])@([A-Za-z0-9.-]+\.[A-Z|a-z]{2,})',
                        r'\1***@\2',
                        text_str
                    )
                elif pattern_name == 'credit_card':
                    # Mask credit card: 1234-5678-9012-3456 -> ****-****-****-3456
                    text_str = re.sub(
                        r'(\d{4})[-\s]?(\d{4})[-\s]?(\d{4})[-\s]?(\d{4})',
                        r'****-****-****-\4',
                        text_str
                    )
                else:
                    # Generic masking for other patterns
                    text_str = re.sub(
                        pattern_info['pattern'],
                        '*' * len(pattern_info['pattern']),
                        text_str,
                        flags=re.IGNORECASE
                    )
            
            return text_str
        
        scrubbed_series = scrubbed_series.apply(mask_text)
        return scrubbed_series
    
    def _hash_pii(self, series: pd.Series, column_name: str) -> pd.Series:
        """Hash PII using SHA-256."""
        def hash_text(text):
            if pd.isna(text) or text == '':
                return text
            
            text_str = str(text)
            salted_text = text_str + self.hash_salt
            return hashlib.sha256(salted_text.encode()).hexdigest()[:16]
        
        scrubbed_series = series.apply(hash_text)
        return scrubbed_series
    
    def _anonymize_pii(self, series: pd.Series, column_name: str) -> pd.Series:
        """Anonymize PII by replacing with consistent pseudonyms."""
        # Create a mapping for consistent anonymization
        if not hasattr(self, '_anonymization_map'):
            self._anonymization_map = {}
        
        def anonymize_text(text):
            if pd.isna(text) or text == '':
                return text
            
            text_str = str(text)
            
            # Check if we've seen this value before
            if text_str in self._anonymization_map:
                return self._anonymization_map[text_str]
            
            # Generate new anonymized value
            anonymized = f"ANON_{len(self._anonymization_map):06d}"
            self._anonymization_map[text_str] = anonymized
            
            return anonymized
        
        scrubbed_series = series.apply(anonymize_text)
        return scrubbed_series
    
    def _add_scrubbing_metadata(self, df: pd.DataFrame, pii_columns: List[str]) -> pd.DataFrame:
        """Add metadata about the scrubbing process."""
        # Add scrubbing info columns
        df['pii_scrubbing_mode'] = self.mode
        df['pii_scrubbing_timestamp'] = datetime.now().isoformat()
        df['pii_columns_scrubbed'] = ','.join(pii_columns)
        
        # Add scrubbing statistics
        total_rows = len(df)
        scrubbed_rows = 0
        
        for column in pii_columns:
            # Count rows that were modified
            original_na = df[column].isna().sum()
            # This is a simplified approach - in practice you'd track actual changes
            scrubbed_rows = max(scrubbed_rows, total_rows - original_na)
        
        df['pii_rows_affected'] = scrubbed_rows
        df['pii_scrubbing_percentage'] = (scrubbed_rows / total_rows * 100) if total_rows > 0 else 0
        
        return df
    
    def get_scrubbing_report(self, df: pd.DataFrame) -> Dict:
        """Generate a report of PII scrubbing activities."""
        report = {
            'scrubbing_mode': self.mode,
            'timestamp': datetime.now().isoformat(),
            'total_rows': len(df),
            'pii_columns_identified': [],
            'scrubbing_statistics': {}
        }
        
        # Identify PII columns
        pii_columns = self._identify_pii_columns(df)
        report['pii_columns_identified'] = pii_columns
        
        # Generate statistics for each PII column
        for column in pii_columns:
            column_stats = {
                'total_values': len(df[column]),
                'null_values': df[column].isna().sum(),
                'unique_values': df[column].nunique(),
                'pii_patterns_found': []
            }
            
            # Check for specific PII patterns
            sample = df[column].dropna().sample(n=min(1000, len(df[column])), random_state=42)
            sample_str = sample.astype(str).str.cat(sep=' ')
            
            for pattern_name, pattern_info in self.pii_patterns.items():
                if re.search(pattern_info['pattern'], sample_str, re.IGNORECASE):
                    column_stats['pii_patterns_found'].append(pattern_name)
            
            report['scrubbing_statistics'][column] = column_stats
        
        return report 