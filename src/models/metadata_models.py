"""
Pydantic models for metadata structures.

These models represent the core data structures for GEO and PubMed metadata,
replacing the previous JSON-based approach with validated, typed objects.
"""

from datetime import datetime
from typing import Dict, List, Optional, Any, Union
from pathlib import Path
from pydantic import BaseModel, Field, validator, ConfigDict


class GSMAttributes(BaseModel):
    """Attributes section of GSM metadata."""
    
    # Removed strict model_config for DendroForge pattern compatibility
    
    # Core required fields
    title: str = Field(..., description="Sample title")
    geo_accession: str = Field(..., pattern=r"^GSM\d+$", description="GEO sample accession")
    
    # Status and dates (often removed during cleaning)
    status: Optional[str] = Field(None, description="Public status")
    submission_date: Optional[str] = Field(None, description="Date submitted")
    last_update_date: Optional[str] = Field(None, description="Last update date")
    
    # Experimental details
    type: Optional[str] = Field(None, description="Sample type (e.g., SRA)")
    channel_count: Optional[str] = Field(None, description="Number of channels")
    source_name_ch1: Optional[str] = Field(None, description="Source name for channel 1")
    organism_ch1: Optional[str] = Field(None, description="Organism for channel 1")
    taxid_ch1: Optional[str] = Field(None, description="Taxonomy ID for channel 1")
    characteristics_ch1: Optional[str] = Field(None, description="Sample characteristics")
    
    # Protocols
    treatment_protocol_ch1: Optional[str] = Field(None, description="Treatment protocol")
    growth_protocol_ch1: Optional[str] = Field(None, description="Growth protocol")
    extract_protocol_ch1: Optional[str] = Field(None, description="Extraction protocol")
    
    # Technical details
    molecule_ch1: Optional[str] = Field(None, description="Molecule type")
    description: Optional[str] = Field(None, description="Sample description")
    data_processing: Optional[str] = Field(None, description="Data processing steps")
    platform_id: Optional[str] = Field(None, description="Platform ID")
    instrument_model: Optional[str] = Field(None, description="Instrument model")
    library_selection: Optional[str] = Field(None, description="Library selection method")
    library_source: Optional[str] = Field(None, description="Library source")
    library_strategy: Optional[str] = Field(None, description="Library strategy")
    
    # Contact information (often removed during cleaning)
    contact_name: Optional[str] = Field(None, description="Contact name")
    contact_email: Optional[str] = Field(None, description="Contact email")
    contact_laboratory: Optional[str] = Field(None, description="Contact laboratory")
    contact_department: Optional[str] = Field(None, description="Contact department")
    contact_institute: Optional[str] = Field(None, description="Contact institute")
    contact_address: Optional[str] = Field(None, description="Contact address")
    contact_city: Optional[str] = Field(None, description="Contact city")
    contact_state: Optional[str] = Field(None, description="Contact state")
    contact_country: Optional[str] = Field(None, description="Contact country")
    contact_phone: Optional[str] = Field(None, description="Contact phone")
    contact_fax: Optional[str] = Field(None, description="Contact fax")
    
    # Additional fields for multi-series samples
    all_series_ids: Optional[str] = Field(None, description="Comma-separated list of all series IDs")


class GSMMetadata(BaseModel):
    """Complete GSM (Gene Expression Omnibus Sample) metadata."""
    
    # Removed strict model_config for DendroForge pattern compatibility
    
    gsm_id: str = Field(..., pattern=r"^GSM\d+$", description="GSM identifier")
    status: str = Field(default="retrieved", description="Retrieval status")
    attributes: GSMAttributes = Field(..., description="Sample attributes")
    
    @validator('gsm_id')
    def validate_gsm_id(cls, v):
        if not v.upper().startswith('GSM'):
            raise ValueError('GSM ID must start with GSM')
        return v.upper()


class GSEAttributes(BaseModel):
    """Attributes section of GSE metadata."""
    
    # Removed strict model_config for DendroForge pattern compatibility
    
    # Core required fields
    title: str = Field(..., description="Series title")
    geo_accession: str = Field(..., pattern=r"^GSE\d+$", description="GEO series accession")
    
    # Status and dates (often removed during cleaning)
    status: Optional[str] = Field(None, description="Public status")
    submission_date: Optional[str] = Field(None, description="Date submitted")
    last_update_date: Optional[str] = Field(None, description="Last update date")
    
    # Publication info
    pubmed_id: Optional[str] = Field(None, description="Comma-separated PubMed IDs")
    summary: Optional[str] = Field(None, description="Study summary")
    overall_design: Optional[str] = Field(None, description="Overall experimental design")
    type: Optional[str] = Field(None, description="Study type")
    
    # Contributors (often removed during cleaning)
    contributor: Optional[str] = Field(None, description="Contributors")
    
    # Sample information
    sample_id: Optional[str] = Field(None, description="Comma-separated sample IDs")
    
    # Contact information (often removed during cleaning)
    contact_name: Optional[str] = Field(None, description="Contact name")
    contact_email: Optional[str] = Field(None, description="Contact email")
    contact_laboratory: Optional[str] = Field(None, description="Contact laboratory")
    contact_department: Optional[str] = Field(None, description="Contact department")
    contact_institute: Optional[str] = Field(None, description="Contact institute")
    contact_address: Optional[str] = Field(None, description="Contact address")
    contact_city: Optional[str] = Field(None, description="Contact city")
    contact_state: Optional[str] = Field(None, description="Contact state")
    contact_country: Optional[str] = Field(None, description="Contact country")
    contact_phone: Optional[str] = Field(None, description="Contact phone")
    contact_fax: Optional[str] = Field(None, description="Contact fax")


class GSEMetadata(BaseModel):
    """Complete GSE (Gene Expression Omnibus Series) metadata."""
    
    # Removed strict model_config for DendroForge pattern compatibility
    
    gse_id: str = Field(..., pattern=r"^GSE\d+$", description="GSE identifier")
    status: str = Field(default="retrieved", description="Retrieval status") 
    attributes: GSEAttributes = Field(..., description="Series attributes")
    
    @validator('gse_id')
    def validate_gse_id(cls, v):
        if not v.upper().startswith('GSE'):
            raise ValueError('GSE ID must start with GSE')
        return v.upper()


class PMIDMetadata(BaseModel):
    """PubMed article metadata."""
    
    # Removed strict model_config for DendroForge pattern compatibility
    
    pmid: int = Field(..., gt=0, description="PubMed ID")
    title: str = Field(..., description="Article title")
    abstract: Optional[str] = Field(None, description="Article abstract")
    
    # Author information (often removed during cleaning)
    authors: Optional[List[str]] = Field(None, description="List of authors")
    journal: Optional[str] = Field(None, description="Journal name")
    publication_date: Optional[str] = Field(None, description="Publication date")
    keywords: Optional[List[str]] = Field(None, description="Keywords")
    mesh_terms: Optional[List[str]] = Field(None, description="MeSH terms")


class SeriesSampleMapping(BaseModel):
    """Series to sample mapping structure."""
    
    # Removed strict model_config for DendroForge pattern compatibility
    
    mapping: Dict[str, List[str]] = Field(..., description="Series ID to sample IDs mapping")
    reverse_mapping: Dict[str, str] = Field(..., description="Sample ID to series ID mapping")
    total_series: int = Field(..., ge=0, description="Total number of series")
    total_samples: int = Field(..., ge=0, description="Total number of samples")
    generated_at: str = Field(..., description="Path where mapping was generated")
    session_directory: str = Field(..., description="Session directory path")


class LinkedData(BaseModel):
    """Linked and processed data for a sample."""
    
    # Removed strict model_config for DendroForge pattern compatibility
    
    sample_id: str = Field(..., pattern=r"^GSM\d+$", description="Sample ID")
    series_id: str = Field(..., pattern=r"^GSE\d+$", description="Series ID")
    directory: str = Field(..., description="Directory path")
    cleaned_files: List[str] = Field(..., description="Paths to cleaned files")
    sample_metadata: Optional[dict] = Field(default=None, description="Sample metadata")
    processing_summary: Optional[dict] = Field(default=None, description="Processing summary") 