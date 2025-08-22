"""
PubMed SQLite Database Manager

This module provides a manager class for querying the local PubMed SQLite database
created by pubmed_ingest.py. It allows for fast, local lookups of PubMed metadata
without API rate limits.
"""

import sqlite3
import os
from pathlib import Path
from typing import Dict, List, Optional, Any
from contextlib import contextmanager


class PubMedSQLiteManager:
    """
    Manager for the local PubMed SQLite database created by pubmed_ingest.py.
    
    This class provides methods to query PubMed metadata (title, abstract, authors, journal)
    from a local SQLite database, eliminating the need for HTTP API calls and rate limits.
    """
    
    def __init__(self, db_path: str = None):
        """
        Initialize the PubMed SQLite manager.
        
        Parameters
        ----------
        db_path : str, optional
            Path to the PubMed SQLite database. If not provided, looks for
            PUBMED_SQLITE_PATH environment variable or uses default path.
        """
        if db_path is None:
            db_path = os.getenv("PUBMED_SQLITE_PATH", "~/data/pubmed/pubmed.sqlite")
        
        self.db_path = Path(os.path.expanduser(db_path)).resolve()
        self._connection = None
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections.
        
        Yields
        ------
        sqlite3.Connection
            SQLite connection object
        """
        if not self.db_path.exists():
            raise FileNotFoundError(
                f"PubMed SQLite database not found: {self.db_path}\n"
                f"Please run pubmed_ingest.py to create the database first."
            )
        
        conn = sqlite3.connect(str(self.db_path))
        try:
            yield conn
        finally:
            conn.close()
    
    def get_pubmed_metadata(self, pmid: str) -> Dict[str, Any]:
        """
        Get complete metadata for a PubMed ID.
        
        Parameters
        ----------
        pmid : str
            PubMed ID to look up
            
        Returns
        -------
        Dict[str, Any]
            Dictionary containing metadata fields:
            - pmid: str
            - title: str
            - abstract: str
            - journal: str
            - iso_abbrev: str (journal ISO abbreviation)
            - pub_year: int
            - publication_date: str (raw publication date)
            - authors: List[str] (formatted author names)
            - doi: str (empty, for compatibility)
            - keywords: List (empty, for compatibility)
            - mesh_terms: List (empty, for compatibility)
        """
        pmid_str = str(pmid).strip()
        
        try:
            with self.get_connection() as conn:
                # Get article metadata
                cursor = conn.execute("""
                    SELECT pmid, title, abstract, journal, iso_abbrev, pub_year, pub_date_raw
                    FROM articles 
                    WHERE pmid = ?
                """, (pmid_str,))
                
                row = cursor.fetchone()
                if not row:
                    return {"error": f"PMID {pmid_str} not found in local database"}
                
                pmid, title, abstract, journal, iso_abbrev, pub_year, pub_date_raw = row
                
                # Get authors
                authors = []
                author_cursor = conn.execute("""
                    SELECT position, last_name, fore_name, initials, collective_name 
                    FROM authors 
                    WHERE pmid = ? 
                    ORDER BY position
                """, (pmid_str,))
                
                for author_row in author_cursor.fetchall():
                    pos, ln, fn, ini, coll = author_row
                    if coll:
                        authors.append(coll)
                    else:
                        # Format individual author name
                        name_parts = []
                        if fn:
                            name_parts.append(fn)
                        if ln:
                            name_parts.append(ln)
                        elif ini:
                            name_parts.append(ini)
                        
                        if name_parts:
                            authors.append(" ".join(name_parts))
                
                # Return metadata in format compatible with original workflow
                return {
                    "pmid": pmid,
                    "title": title or "",
                    "abstract": abstract or "",
                    "journal": journal or "",
                    "iso_abbrev": iso_abbrev or "",
                    "pub_year": pub_year,
                    "publication_date": pub_date_raw or "",
                    "authors": authors,
                    # Fields for compatibility with HTTP API version
                    "doi": "",  # Not available in PubMed baseline
                    "keywords": [],  # Not available in PubMed baseline
                    "mesh_terms": [],  # Not available in PubMed baseline
                }
                
        except Exception as e:
            return {"error": f"Database error for PMID {pmid_str}: {str(e)}"}
    
    def lookup_pmid(self, pmid: str) -> Optional[Dict[str, Any]]:
        """
        Simple lookup method that returns metadata or None if not found.
        
        Parameters
        ----------
        pmid : str
            PubMed ID to look up
            
        Returns
        -------
        Optional[Dict[str, Any]]
            Metadata dictionary or None if not found
        """
        result = self.get_pubmed_metadata(pmid)
        return result if "error" not in result else None
    
    def is_available(self) -> bool:
        """
        Check if the PubMed database is available and accessible.
        
        Returns
        -------
        bool
            True if database exists and is accessible, False otherwise
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM articles LIMIT 1")
                cursor.fetchone()
                return True
        except Exception:
            return False
    
    def get_database_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the PubMed database.
        
        Returns
        -------
        Dict[str, Any]
            Dictionary with database statistics
        """
        try:
            with self.get_connection() as conn:
                # Get article count
                cursor = conn.execute("SELECT COUNT(*) FROM articles")
                article_count = cursor.fetchone()[0]
                
                # Get author count
                cursor = conn.execute("SELECT COUNT(*) FROM authors")
                author_count = cursor.fetchone()[0]
                
                # Get year range
                cursor = conn.execute("SELECT MIN(pub_year), MAX(pub_year) FROM articles WHERE pub_year IS NOT NULL")
                year_row = cursor.fetchone()
                min_year, max_year = year_row if year_row[0] is not None else (None, None)
                
                return {
                    "article_count": article_count,
                    "author_count": author_count,
                    "year_range": f"{min_year}-{max_year}" if min_year and max_year else "Unknown",
                    "database_path": str(self.db_path),
                    "database_size_mb": round(self.db_path.stat().st_size / (1024 * 1024), 2) if self.db_path.exists() else 0
                }
        except Exception as e:
            return {"error": f"Could not get database stats: {str(e)}"}


@contextmanager
def get_pubmed_manager(db_path: str = None):
    """
    Context manager for PubMed SQLite manager.
    
    Parameters
    ----------
    db_path : str, optional
        Path to the PubMed SQLite database
        
    Yields
    ------
    PubMedSQLiteManager
        PubMed SQLite manager instance
    """
    manager = PubMedSQLiteManager(db_path)
    try:
        yield manager
    finally:
        # Cleanup if needed
        pass
