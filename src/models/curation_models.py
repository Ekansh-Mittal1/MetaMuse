"""
Pydantic models for metadata curation workflow.

These models package cleaned metadata from different sources (series, sample, abstract)
for efficient handoff to the CuratorAgent without requiring file I/O operations.
"""

from typing import Dict, List, Any, Optional
from pydantic import BaseModel, Field, ConfigDict


class CleanedSeriesMetadata(BaseModel):
    """Cleaned series metadata from GSE files."""
    
    model_config = ConfigDict(extra="forbid")
    
    series_id: str = Field(..., description="Series ID (e.g., GSE29282)")
    content: Dict[str, Any] = Field(..., description="Cleaned series metadata content")
    source_type: str = Field(default="series", description="Source type identifier")
    original_file_path: Optional[str] = Field(None, description="Original file path for reference")


class CleanedSampleMetadata(BaseModel):
    """Cleaned sample metadata from GSM files."""
    
    model_config = ConfigDict(extra="forbid")
    
    sample_id: str = Field(..., description="Sample ID (e.g., GSM1000981)")
    content: Dict[str, Any] = Field(..., description="Cleaned sample metadata content")
    source_type: str = Field(default="sample", description="Source type identifier")
    original_file_path: Optional[str] = Field(None, description="Original file path for reference")


class CleanedAbstractMetadata(BaseModel):
    """Cleaned abstract metadata from PubMed papers."""
    
    model_config = ConfigDict(extra="forbid")
    
    pmid: str = Field(..., description="PubMed ID (e.g., 23911289)")
    content: Dict[str, Any] = Field(..., description="Cleaned abstract metadata content")
    source_type: str = Field(default="abstract", description="Source type identifier")
    original_file_path: Optional[str] = Field(None, description="Original file path for reference")


class CurationDataPackage(BaseModel):
    """Complete package of cleaned metadata for one sample from all sources."""
    
    model_config = ConfigDict(extra="forbid")
    
    sample_id: str = Field(..., description="Primary sample ID being curated")
    series_metadata: Optional[CleanedSeriesMetadata] = Field(None, description="Cleaned series metadata")
    sample_metadata: Optional[CleanedSampleMetadata] = Field(None, description="Cleaned sample metadata")
    abstract_metadata: Optional[CleanedAbstractMetadata] = Field(None, description="Cleaned abstract metadata")
    
    def get_available_sources(self) -> List[str]:
        """Get list of available data sources."""
        sources = []
        if self.series_metadata:
            sources.append("series")
        if self.sample_metadata:
            sources.append("sample")
        if self.abstract_metadata:
            sources.append("abstract")
        return sources
    
    def get_all_content_text(self) -> str:
        """Flatten all available metadata content to text for LLM processing."""
        text_parts = []
        
        if self.series_metadata:
            text_parts.append(f"=== SERIES METADATA ({self.series_metadata.series_id}) ===")
            text_parts.append(str(self.series_metadata.content))
            
        if self.sample_metadata:
            text_parts.append(f"=== SAMPLE METADATA ({self.sample_metadata.sample_id}) ===")
            text_parts.append(str(self.sample_metadata.content))
            
        if self.abstract_metadata:
            text_parts.append(f"=== ABSTRACT METADATA (PMID {self.abstract_metadata.pmid}) ===")
            text_parts.append(str(self.abstract_metadata.content))
            
        return "\n\n".join(text_parts)


class ExtractedCandidate(BaseModel):
    """A single extracted candidate from metadata."""
    
    model_config = ConfigDict(extra="forbid")
    
    value: str = Field(..., description="Extracted candidate value")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Confidence score (0-1)")
    source: str = Field(..., description="Source type (series/sample/abstract)")
    context: str = Field(..., description="Context where candidate was found")
    rationale: str = Field(..., description="Explicit reasoning for why this candidate was extracted")


class CurationResult(BaseModel):
    """Result of curation for a single sample and target field."""
    
    model_config = ConfigDict(extra="forbid")
    
    sample_id: str = Field(..., description="Sample ID that was curated")
    target_field: str = Field(..., description="Target metadata field (e.g., Disease)")
    
    # Extracted candidates by source
    series_candidates: List[ExtractedCandidate] = Field(default_factory=list, description="Candidates from series metadata")
    sample_candidates: List[ExtractedCandidate] = Field(default_factory=list, description="Candidates from sample metadata")
    abstract_candidates: List[ExtractedCandidate] = Field(default_factory=list, description="Candidates from abstract metadata")
    
    # Final result
    final_candidate: Optional[str] = Field(None, description="Final reconciled candidate value")
    final_confidence: Optional[float] = Field(None, ge=0.0, le=1.0, description="Confidence in final result")
    reconciliation_needed: bool = Field(False, description="Whether manual reconciliation is needed")
    reconciliation_reason: Optional[str] = Field(None, description="Reason for manual reconciliation")
    
    # Processing metadata
    sources_processed: List[str] = Field(default_factory=list, description="Sources that were processed")
    processing_notes: List[str] = Field(default_factory=list, description="Processing notes and warnings")
    
    def get_all_candidates(self) -> List[ExtractedCandidate]:
        """Get all candidates from all sources."""
        return self.series_candidates + self.sample_candidates + self.abstract_candidates
    
    def has_conflicting_candidates(self) -> bool:
        """Check if there are conflicting candidates across sources."""
        all_values = [c.value.lower().strip() for c in self.get_all_candidates()]
        unique_values = set(all_values)
        return len(unique_values) > 1 