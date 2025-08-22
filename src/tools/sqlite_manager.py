"""
SQLite database manager for GEOmetadb.

This module provides functionality to download and query the GEOmetadb SQLite database
locally, replacing the need for ENTREZ API calls.
"""

import os
import sqlite3
import pandas as pd
import requests
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from tqdm import tqdm
import hashlib
import json
import time


class GEOmetadbManager:
    """
    Manager for the GEOmetadb SQLite database.
    
    This class handles downloading, updating, and querying the local SQLite database
    that contains GEO metadata, eliminating the need for network calls to ENTREZ API.
    """
    
    def __init__(self, db_path: str = "GEOmetadb.sqlite", cache_dir: str = "geo_cache"):
        """
        Initialize the GEOmetadb manager.
        
        Parameters
        ----------
        db_path : str
            Path to the SQLite database file
        cache_dir : str
            Directory to store downloaded files and cache
        """
        self.db_path = Path(db_path)
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        
        # Database connection
        self.connection = None
        
        # GEOmetadb download URLs and metadata
        self.geometadb_info = {
            "url": "https://gbnci.abcc.ncifcrf.gov/geo/GEOmetadb.sqlite.gz",
            "md5_url": "https://gbnci.abcc.ncifcrf.gov/geo/GEOmetadb.sqlite.gz.md5",
            "last_updated": None,
            "file_size": None
        }
        
        # Initialize database if it exists
        if self.db_path.exists():
            self._connect()
    
    def _connect(self):
        """Establish connection to the SQLite database."""
        try:
            self.connection = sqlite3.connect(self.db_path)
            # Enable foreign keys and optimize for read-heavy workloads
            self.connection.execute("PRAGMA foreign_keys = ON")
            self.connection.execute("PRAGMA journal_mode = WAL")
            self.connection.execute("PRAGMA synchronous = NORMAL")
            self.connection.execute("PRAGMA cache_size = 10000")
            self.connection.execute("PRAGMA temp_store = MEMORY")
        except Exception as e:
            print(f"❌ Error connecting to database: {e}")
            self.connection = None
    
    def download_database(self, force_download: bool = False) -> bool:
        """
        Download the GEOmetadb SQLite database.
        
        Parameters
        ----------
        force_download : bool
            Force download even if database already exists
            
        Returns
        -------
        bool
            True if download successful, False otherwise
        """
        if self.db_path.exists() and not force_download:
            print(f"✅ Database already exists at {self.db_path}")
            return True
        
        print("📥 Downloading GEOmetadb SQLite database...")
        
        try:
            # Download the compressed database
            compressed_path = self.cache_dir / "GEOmetadb.sqlite.gz"
            
            # Download with progress bar
            response = requests.get(self.geometadb_info["url"], stream=True)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            
            with open(compressed_path, 'wb') as f:
                with tqdm(total=total_size, unit='B', unit_scale=True, desc="Downloading") as pbar:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
            
            # Extract the gzipped file
            print("🔓 Extracting database...")
            import gzip
            import shutil
            
            with gzip.open(compressed_path, 'rb') as f_in:
                with open(self.db_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            # Clean up compressed file
            compressed_path.unlink()
            
            # Connect to the new database
            self._connect()
            
            print(f"✅ Database downloaded and extracted to {self.db_path}")
            return True
            
        except Exception as e:
            print(f"❌ Error downloading database: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def get_database_info(self) -> Dict[str, Any]:
        """
        Get information about the current database.
        
        Returns
        -------
        Dict[str, Any]
            Database information including tables, row counts, and last update
        """
        if not self.connection:
            return {"error": "Database not connected"}
        
        try:
            # Get table information
            tables_query = """
                SELECT name, sql FROM sqlite_master 
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
            """
            tables_df = pd.read_sql_query(tables_query, self.connection)
            
            # Get row counts for main tables
            row_counts = {}
            for table in ['gse', 'gsm', 'gpl', 'gds', 'gse_gsm', 'gse_gpl']:
                try:
                    count = pd.read_sql_query(f"SELECT COUNT(*) as count FROM {table}", self.connection)
                    row_counts[table] = count.iloc[0]['count']
                except:
                    row_counts[table] = 0
            
            # Get database file info
            db_stats = self.db_path.stat()
            
            return {
                "tables": tables_df.to_dict('records'),
                "row_counts": row_counts,
                "file_size_mb": round(db_stats.st_size / (1024 * 1024), 2),
                "last_modified": time.ctime(db_stats.st_mtime),
                "database_path": str(self.db_path)
            }
            
        except Exception as e:
            return {"error": f"Error getting database info: {e}"}
    
    def get_gse_metadata(self, gse_id: str) -> Dict[str, Any]:
        """
        Get GSE metadata from the local database.
        
        Parameters
        ----------
        gse_id : str
            The GSE ID to retrieve metadata for
            
        Returns
        -------
        Dict[str, Any]
            GSE metadata
        """
        if not self.connection:
            return {"error": "Database not connected"}
        
        try:
            query = """
                SELECT 
                    gse,
                    title,
                    summary,
                    overall_design,
                    pubmed_id,
                    submission_date,
                    last_update_date,
                    status,
                    type,
                    contributor,
                    web_link,
                    repeats,
                    repeats_sample_list,
                    variable,
                    variable_description,
                    contact,
                    supplementary_file
                FROM gse 
                WHERE gse = ?
            """
            
            df = pd.read_sql_query(query, self.connection, params=[gse_id])
            
            if df.empty:
                return {"error": f"GSE {gse_id} not found in database"}
            
            # Convert to dictionary and handle NaN values
            metadata = df.iloc[0].to_dict()
            metadata = {k: v if pd.notna(v) else "" for k, v in metadata.items()}
            
            # Get associated samples
            samples_query = """
                SELECT gsm FROM gse_gsm WHERE gse = ?
            """
            samples_df = pd.read_sql_query(samples_query, self.connection, params=[gse_id])
            metadata['samples'] = samples_df['gsm'].tolist() if not samples_df.empty else []
            
            # Get associated platforms
            platforms_query = """
                SELECT gpl FROM gse_gpl WHERE gse = ?
            """
            platforms_df = pd.read_sql_query(platforms_query, self.connection, params=[gse_id])
            metadata['platforms'] = platforms_df['gpl'].tolist() if not platforms_df.empty else []
            
            return metadata
            
        except Exception as e:
            return {"error": f"Error retrieving GSE metadata: {e}"}
    
    def get_gsm_metadata(self, gsm_id: str) -> Dict[str, Any]:
        """
        Get GSM metadata from the local database.
        
        Parameters
        ----------
        gsm_id : str
            The GSM ID to retrieve metadata for
            
        Returns
        -------
        Dict[str, Any]
            GSM metadata
        """
        if not self.connection:
            return {"error": "Database not connected"}
        
        try:
            query = """
                SELECT 
                    gsm,
                    title,
                    source_name_ch1,
                    organism_ch1,
                    characteristics_ch1,
                    molecule_ch1,
                    label_ch1,
                    treatment_protocol_ch1,
                    extract_protocol_ch1,
                    label_protocol_ch1,
                    source_name_ch2,
                    organism_ch2,
                    characteristics_ch2,
                    molecule_ch2,
                    label_ch2,
                    treatment_protocol_ch2,
                    extract_protocol_ch2,
                    label_protocol_ch2,
                    hyb_protocol,
                    description,
                    data_processing,
                    contact,
                    supplementary_file,
                    data_row_count,
                    submission_date,
                    last_update_date,
                    status,
                    type,
                    channel_count
                FROM gsm 
                WHERE gsm = ?
            """
            
            df = pd.read_sql_query(query, self.connection, params=[gsm_id])
            
            if df.empty:
                return {"error": f"GSM {gsm_id} not found in database"}
            
            # Convert to dictionary and handle NaN values
            metadata = df.iloc[0].to_dict()
            metadata = {k: v if pd.notna(v) else "" for k, v in metadata.items()}
            
            # Get associated series
            series_query = """
                SELECT gse FROM gse_gsm WHERE gsm = ?
            """
            series_df = pd.read_sql_query(series_query, self.connection, params=[gsm_id])
            metadata['series'] = series_df['gse'].tolist() if not series_df.empty else []
            
            return metadata
            
        except Exception as e:
            return {"error": f"Error retrieving GSM metadata: {e}"}
    
    def get_pubmed_metadata(self, pmid: str) -> Dict[str, Any]:
        """
        Get PubMed metadata from the local database.
        
        Note: The GEOmetadb may not contain full PubMed abstracts.
        This method provides basic paper information if available.
        
        Parameters
        ----------
        pmid : str
            The PubMed ID to retrieve metadata for
            
        Returns
        -------
        Dict[str, Any]
            PubMed metadata
        """
        if not self.connection:
            return {"error": "Database not connected"}
        
        try:
            # First try to find papers in GSE metadata
            query = """
                SELECT 
                    gse,
                    title,
                    summary,
                    pubmed_id,
                    contributor
                FROM gse 
                WHERE pubmed_id = ?
            """
            
            df = pd.read_sql_query(query, self.connection, params=[pmid])
            
            if not df.empty:
                # Found papers with this PMID
                papers = []
                for _, row in df.iterrows():
                    paper_info = {
                        "pmid": pmid,
                        "title": row['title'] if pd.notna(row['title']) else "",
                        "abstract": row['summary'] if pd.notna(row['summary']) else "",
                        "gse_ids": [row['gse']],
                        "contributor": row['contributor'] if pd.notna(row['contributor']) else ""
                    }
                    papers.append(paper_info)
                
                return {
                    "pmid": pmid,
                    "papers": papers,
                    "total_papers": len(papers)
                }
            else:
                return {"error": f"PubMed ID {pmid} not found in database"}
                
        except Exception as e:
            return {"error": f"Error retrieving PubMed metadata: {e}"}
    
    def search_geo(self, query: str, search_type: str = "all", limit: int = 100) -> Dict[str, Any]:
        """
        Search GEO database using SQL LIKE queries.
        
        Parameters
        ----------
        query : str
            Search query string
        search_type : str
            Type of search: 'all', 'gse', 'gsm', 'gpl'
        limit : int
            Maximum number of results to return
            
        Returns
        -------
        Dict[str, Any]
            Search results
        """
        if not self.connection:
            return {"error": "Database not connected"}
        
        try:
            if search_type == "gse":
                sql = """
                    SELECT gse, title, summary, pubmed_id, submission_date
                    FROM gse 
                    WHERE title LIKE ? OR summary LIKE ?
                    LIMIT ?
                """
                params = [f"%{query}%", f"%{query}%", limit]
                
            elif search_type == "gsm":
                sql = """
                    SELECT gsm, title, source_name_ch1, characteristics_ch1
                    FROM gsm 
                    WHERE title LIKE ? OR source_name_ch1 LIKE ? OR characteristics_ch1 LIKE ?
                    LIMIT ?
                """
                params = [f"%{query}%", f"%{query}%", f"%{query}%", limit]
                
            else:  # all
                sql = """
                    SELECT 'GSE' as type, gse as id, title, summary as description, pubmed_id
                    FROM gse 
                    WHERE title LIKE ? OR summary LIKE ?
                    UNION ALL
                    SELECT 'GSM' as type, gsm as id, title, source_name_ch1 as description, '' as pubmed_id
                    FROM gsm 
                    WHERE title LIKE ? OR source_name_ch1 LIKE ?
                    LIMIT ?
                """
                params = [f"%{query}%", f"%{query}%", f"%{query}%", f"%{query}%", limit]
            
            df = pd.read_sql_query(sql, self.connection, params=params)
            
            return {
                "query": query,
                "search_type": search_type,
                "results": df.to_dict('records'),
                "total_results": len(df)
            }
            
        except Exception as e:
            return {"error": f"Error searching database: {e}"}
    
    def get_series_sample_mapping(self, gse_ids: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Get mapping between series and samples.
        
        Parameters
        ----------
        gse_ids : Optional[List[str]]
            List of GSE IDs to get mapping for. If None, returns all mappings.
            
        Returns
        -------
        Dict[str, Any]
            Series-sample mapping
        """
        if not self.connection:
            return {"error": "Database not connected"}
        
        try:
            if gse_ids:
                placeholders = ','.join(['?' for _ in gse_ids])
                sql = f"""
                    SELECT gse, gsm 
                    FROM gse_gsm 
                    WHERE gse IN ({placeholders})
                    ORDER BY gse, gsm
                """
                df = pd.read_sql_query(sql, self.connection, params=gse_ids)
            else:
                sql = """
                    SELECT gse, gsm 
                    FROM gse_gsm 
                    ORDER BY gse, gsm
                """
                df = pd.read_sql_query(sql, self.connection)
            
            # Group by GSE
            mapping = {}
            for _, row in df.iterrows():
                gse = row['gse']
                gsm = row['gsm']
                if gse not in mapping:
                    mapping[gse] = []
                mapping[gse].append(gsm)
            
            return {
                "mapping": mapping,
                "total_series": len(mapping),
                "total_samples": len(df)
            }
            
        except Exception as e:
            return {"error": f"Error getting series-sample mapping: {e}"}
    
    def close(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# Convenience functions for backward compatibility
def get_geometadb_manager(db_path: str = "GEOmetadb.sqlite") -> GEOmetadbManager:
    """Get a GEOmetadb manager instance."""
    return GEOmetadbManager(db_path)


def download_geometadb(db_path: str = "GEOmetadb.sqlite", force: bool = False) -> bool:
    """Download the GEOmetadb database."""
    with GEOmetadbManager(db_path) as manager:
        return manager.download_database(force_download=force)
