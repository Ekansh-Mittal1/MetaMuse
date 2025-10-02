from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class FieldEvaluation(BaseModel):
    """Evaluation for a single target field."""

    field_name: str = Field(..., description="Name of the target field, e.g., disease, tissue, organ, etc.")
    curated_value: Optional[str] = Field(
        None, description="Value produced by curation for this field, if available."
    )
    suggested_curation: Optional[str] = Field(
        None, description="If curated value is incorrect, a suggested corrected curation proposed by the arbitrator."
    )
    normalized_term: Optional[str] = Field(
        None, description="Ontology-normalized term for this field, if applicable."
    )
    normalized_id: Optional[str] = Field(
        None, description="Ontology or controlled vocabulary ID for the normalized term, if applicable."
    )
    is_curated_correct: Optional[bool] = Field(
        None, description="Whether the curated value is correct based on the provided raw data."
    )
    curated_reason: Optional[str] = Field(
        None, description="Reasoning for why the curated value is correct or incorrect."
    )
    is_normalized_correct: Optional[bool] = Field(
        None, description="Whether the normalized term/id are correct given the curated value and raw data."
    )
    normalized_reason: Optional[str] = Field(
        None, description="Reasoning for why the normalized term/id are correct or incorrect."
    )


class SampleEvaluation(BaseModel):
    """Structured evaluation output for a single sample across all fields."""

    sample_id: str
    series_id: Optional[str] = None
    sample_type: Optional[str] = None
    pubmed_id: Optional[str] = None

    # Raw context included for traceability
    abstract_text: Optional[str] = Field(
        None, description="Abstract text from PubMed or related source, if available."
    )
    # Use explicit key/value pairs to satisfy strict schemas (avoid additionalProperties)
    class KeyValue(BaseModel):
        key: str = Field(..., description="Metadata key")
        value: str = Field(..., description="Metadata value")

    series_metadata: Optional[List[KeyValue]] = Field(
        default=None, description="Series-level metadata expressed as key/value pairs."
    )
    sample_metadata: Optional[List[KeyValue]] = Field(
        default=None, description="Sample-level metadata expressed as key/value pairs."
    )

    # Per-field evaluations
    fields: List[FieldEvaluation]

    # Optional overall judgment summaries
    overall_curated_accuracy: Optional[float] = Field(
        None, description="Proportion of fields with is_curated_correct == True."
    )
    overall_normalized_accuracy: Optional[float] = Field(
        None, description="Proportion of fields with is_normalized_correct == True (consider only fields with normalization)."
    )


class BatchEvaluationSummary(BaseModel):
    """Aggregate metrics for a batch."""

    batch_dir: str
    num_samples: int
    per_field_curated_accuracy: Dict[str, float]
    per_field_normalized_accuracy: Dict[str, float]


