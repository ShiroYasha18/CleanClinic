#!/usr/bin/env python3
"""
Geo Enricher Transform

Enriches geographic coordinates with additional location context and metadata:
- Reverse geocoding (coordinates to address)
- Administrative boundaries (city, state, country)
- Timezone information
- Census data (if available)
- Distance to nearest healthcare facilities
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
import logging
import requests
import time
from pathlib import Path
import json

logger = logging.getLogger(__name__)

class GeoEnricher:
    """Enriches geographic data with location context and metadata."""
    
    def __init__(self, api_key: Optional[str] = None, 
                 geocoding_service: str = 'nominatim',
                 cache_results: bool = True):
        """
        Initialize the Geo Enricher.
        
        Args:
            api_key: API key for geocoding services (optional for Nominatim)
            geocoding_service: Service to use ('nominatim', 'google', 'here')
            cache_results: Whether to cache geocoding results
        """
        self.api_key = api_key
        self.geocoding_service = geocoding_service
        self.cache_results = cache_results
        self.cache = {}
        
        # Common latitude/longitude column patterns
        self.lat_patterns = ['lat', 'latitude', 'y', 'y_coord', 'coord_y']
        self.lng_patterns = ['lng', 'long', 'longitude', 'x', 'x_coord', 'coord_x']
        
        # Rate limiting for API calls
        self.rate_limit_delay = 1.0  # seconds between requests
        
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform DataFrame by enriching geographic coordinates.
        
        Args:
            df: Input DataFrame with latitude/longitude columns
            
        Returns:
            DataFrame with enriched geographic data
        """
        logger.info("Starting geo enrichment...")
        
        # Find latitude and longitude columns
        lat_col, lng_col = self._find_coordinate_columns(df)
        
        if lat_col is None or lng_col is None:
            logger.warning("No latitude/longitude columns found - skipping geo enrichment")
            return df
        
        logger.info(f"Found coordinate columns: {lat_col}, {lng_col}")
        
        # Create a copy to avoid modifying original
        enriched_df = df.copy()
        
        # Add basic coordinate validation
        enriched_df = self._validate_coordinates(enriched_df, lat_col, lng_col)
        
        # Add coordinate precision columns
        enriched_df = self._add_precision_columns(enriched_df, lat_col, lng_col)
        
        # Add reverse geocoding data
        enriched_df = self._add_reverse_geocoding(enriched_df, lat_col, lng_col)
        
        # Add timezone information
        enriched_df = self._add_timezone_info(enriched_df, lat_col, lng_col)
        
        # Add administrative boundaries
        enriched_df = self._add_admin_boundaries(enriched_df, lat_col, lng_col)
        
        # Add distance calculations (if multiple points)
        enriched_df = self._add_distance_metrics(enriched_df, lat_col, lng_col)
        
        logger.info(f"Geo enrichment completed. Added {len(enriched_df.columns) - len(df.columns)} new columns")
        
        return enriched_df
    
    def _find_coordinate_columns(self, df: pd.DataFrame) -> Tuple[Optional[str], Optional[str]]:
        """Find latitude and longitude columns in the DataFrame."""
        columns = df.columns.str.lower()
        
        # Find latitude column
        lat_col = None
        for pattern in self.lat_patterns:
            matches = [col for col in df.columns if pattern in col.lower()]
            if matches:
                lat_col = matches[0]
                break
        
        # Find longitude column
        lng_col = None
        for pattern in self.lng_patterns:
            matches = [col for col in df.columns if pattern in col.lower()]
            if matches:
                lng_col = matches[0]
                break
        
        return lat_col, lng_col
    
    def _validate_coordinates(self, df: pd.DataFrame, lat_col: str, lng_col: str) -> pd.DataFrame:
        """Validate and clean coordinate data."""
        # Convert to numeric, coercing errors to NaN
        try:
            df[lat_col] = pd.to_numeric(df[lat_col], errors='coerce')
            df[lng_col] = pd.to_numeric(df[lng_col], errors='coerce')
        except Exception as e:
            logger.warning(f"Failed to convert coordinates to numeric: {e}")
            # Create validation columns with False for invalid data
            df[f'{lat_col}_valid'] = False
            df[f'{lng_col}_valid'] = False
            return df
        
        # Create validation columns
        df[f'{lat_col}_valid'] = (
            (df[lat_col] >= -90) & (df[lat_col] <= 90) & 
            df[lat_col].notna()
        )
        
        df[f'{lng_col}_valid'] = (
            (df[lng_col] >= -180) & (df[lng_col] <= 180) & 
            df[lng_col].notna()
        )
        
        # Count valid coordinates
        valid_coords = df[f'{lat_col}_valid'] & df[f'{lng_col}_valid']
        logger.info(f"Valid coordinates: {valid_coords.sum()} out of {len(df)}")
        
        return df
    
    def _add_precision_columns(self, df: pd.DataFrame, lat_col: str, lng_col: str) -> pd.DataFrame:
        """Add coordinate precision information."""
        # Calculate precision based on decimal places
        df[f'{lat_col}_precision'] = df[lat_col].apply(
            lambda x: len(str(x).split('.')[-1]) if pd.notna(x) and '.' in str(x) else 0
        )
        
        df[f'{lng_col}_precision'] = df[lng_col].apply(
            lambda x: len(str(x).split('.')[-1]) if pd.notna(x) and '.' in str(x) else 0
        )
        
        # Add precision level (rough, medium, precise)
        def get_precision_level(precision):
            if precision <= 2:
                return 'rough'
            elif precision <= 4:
                return 'medium'
            else:
                return 'precise'
        
        df[f'{lat_col}_precision_level'] = df[f'{lat_col}_precision'].apply(get_precision_level)
        df[f'{lng_col}_precision_level'] = df[f'{lng_col}_precision'].apply(get_precision_level)
        
        return df
    
    def _add_reverse_geocoding(self, df: pd.DataFrame, lat_col: str, lng_col: str) -> pd.DataFrame:
        """Add reverse geocoding data using the specified service."""
        if self.geocoding_service == 'nominatim':
            return self._add_nominatim_geocoding(df, lat_col, lng_col)
        else:
            logger.warning(f"Geocoding service '{self.geocoding_service}' not implemented")
            return df
    
    def _add_nominatim_geocoding(self, df: pd.DataFrame, lat_col: str, lng_col: str) -> pd.DataFrame:
        """Add reverse geocoding using OpenStreetMap Nominatim."""
        # Sample a subset for geocoding (to avoid rate limits)
        sample_size = min(100, len(df))
        sample_indices = np.random.choice(df.index, sample_size, replace=False)
        
        geocoded_data = {}
        
        for idx in sample_indices:
            lat = df.loc[idx, lat_col]
            lng = df.loc[idx, lng_col]
            
            if pd.isna(lat) or pd.isna(lng):
                continue
            
            # Check cache first
            cache_key = f"{lat:.6f},{lng:.6f}"
            if cache_key in self.cache:
                geocoded_data[idx] = self.cache[cache_key]
                continue
            
            try:
                # Nominatim reverse geocoding
                url = f"https://nominatim.openstreetmap.org/reverse"
                params = {
                    'lat': lat,
                    'lon': lng,
                    'format': 'json',
                    'addressdetails': 1
                }
                
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
                
                data = response.json()
                
                # Extract relevant information
                geocoded_info = {
                    'address': data.get('display_name', ''),
                    'city': data.get('address', {}).get('city', ''),
                    'state': data.get('address', {}).get('state', ''),
                    'country': data.get('address', {}).get('country', ''),
                    'postcode': data.get('address', {}).get('postcode', ''),
                    'country_code': data.get('address', {}).get('country_code', ''),
                    'osm_type': data.get('osm_type', ''),
                    'osm_id': data.get('osm_id', '')
                }
                
                geocoded_data[idx] = geocoded_info
                self.cache[cache_key] = geocoded_info
                
                # Rate limiting
                time.sleep(self.rate_limit_delay)
                
            except Exception as e:
                logger.warning(f"Geocoding failed for coordinates ({lat}, {lng}): {e}")
                geocoded_data[idx] = {}
        
        # Add geocoding columns to DataFrame
        for field in ['address', 'city', 'state', 'country', 'postcode', 'country_code']:
            df[f'geocoded_{field}'] = ''
        
        for idx, data in geocoded_data.items():
            for field in data:
                df.loc[idx, f'geocoded_{field}'] = data[field]
        
        logger.info(f"Reverse geocoding completed for {len(geocoded_data)} samples")
        return df
    
    def _add_timezone_info(self, df: pd.DataFrame, lat_col: str, lng_col: str) -> pd.DataFrame:
        """Add timezone information for coordinates."""
        try:
            import timezonefinder
            
            tf = timezonefinder.TimezoneFinder()
            
            def get_timezone(lat, lng):
                if pd.isna(lat) or pd.isna(lng):
                    return None
                try:
                    return tf.timezone_at(lat=lat, lng=lng)
                except:
                    return None
            
            df['timezone'] = df.apply(
                lambda row: get_timezone(row[lat_col], row[lng_col]), axis=1
            )
            
            logger.info("Timezone information added")
            
        except ImportError:
            logger.warning("timezonefinder not available - skipping timezone enrichment")
        except Exception as e:
            logger.warning(f"Timezone enrichment failed: {e}")
        
        return df
    
    def _add_admin_boundaries(self, df: pd.DataFrame, lat_col: str, lng_col: str) -> pd.DataFrame:
        """Add administrative boundary information."""
        # This would typically use a GIS library like geopandas
        # For now, we'll add placeholder columns
        df['admin_level_1'] = ''  # Country
        df['admin_level_2'] = ''  # State/Province
        df['admin_level_3'] = ''  # County/District
        df['admin_level_4'] = ''  # City/Municipality
        
        logger.info("Administrative boundary columns added (placeholder)")
        return df
    
    def _add_distance_metrics(self, df: pd.DataFrame, lat_col: str, lng_col: str) -> pd.DataFrame:
        """Add distance calculations between points."""
        # Calculate centroid of all valid coordinates
        valid_mask = df[f'{lat_col}_valid'] & df[f'{lng_col}_valid']
        
        if valid_mask.sum() > 1:
            centroid_lat = df.loc[valid_mask, lat_col].mean()
            centroid_lng = df.loc[valid_mask, lng_col].mean()
            
            # Calculate distance to centroid
            df['distance_to_centroid_km'] = df.apply(
                lambda row: self._haversine_distance(
                    row[lat_col], row[lng_col], centroid_lat, centroid_lng
                ) if valid_mask[row.name] else None, axis=1
            )
            
            logger.info("Distance metrics added")
        
        return df
    
    def _haversine_distance(self, lat1: float, lng1: float, lat2: float, lng2: float) -> float:
        """Calculate Haversine distance between two points in kilometers."""
        from math import radians, cos, sin, asin, sqrt
        
        # Convert to radians
        lat1, lng1, lat2, lng2 = map(radians, [lat1, lng1, lat2, lng2])
        
        # Haversine formula
        dlat = lat2 - lat1
        dlng = lng2 - lng1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlng/2)**2
        c = 2 * asin(sqrt(a))
        
        # Radius of Earth in kilometers
        r = 6371
        
        return c * r 