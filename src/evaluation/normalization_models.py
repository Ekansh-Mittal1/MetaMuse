from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class NormalizationFieldEvaluation(BaseModel):
    """Evaluation for normalization accuracy of a single target field."""

    field_name: str = Field(..., description="Name of the target field, e.g., disease, tissue, organ, etc.")
    curated_value: Optional[str] = Field(
        None, description="Value produced by curation for this field."
    )
    normalized_term: Optional[str] = Field(
        None, description="Ontology-normalized term for this field."
    )
    normalized_id: Optional[str] = Field(
        None, description="Ontology or controlled vocabulary ID for the normalized term."
    )
    is_normalization_correct: Optional[bool] = Field(
        None, description="Whether the normalized term/id correctly represent the curated value. Only mark as False if a better term exists."
    )
    normalization_reason: Optional[str] = Field(
        None, description="Reasoning for why the normalization is correct or incorrect."
    )
    suggested_term: Optional[str] = Field(
        None, description="If normalization is incorrect, suggest a better ontology term that would be more accurate. Leave None if normalization is correct or if no better term exists."
    )
    suggested_id: Optional[str] = Field(
        None, description="If normalization is incorrect, suggest the ontology ID for the suggested_term. Leave None if normalization is correct or if no better term exists."
    )


class SampleNormalizationEvaluation(BaseModel):
    """Structured normalization evaluation output for a single sample across normalized fields."""

    sample_id: str
    series_id: Optional[str] = None
    sample_type: Optional[str] = None

    # Per-field normalization evaluations (only for fields that have normalization)
    normalized_fields: List[NormalizationFieldEvaluation]

    # Optional overall judgment summary
    overall_normalization_accuracy: Optional[float] = Field(
        None, description="Proportion of normalized fields that are correct."
    )


class BatchNormalizationEvaluationSummary(BaseModel):
    """Aggregate normalization metrics for a batch."""

    batch_dir: str
    num_samples: int
    per_field_normalization_accuracy: Dict[str, float]

