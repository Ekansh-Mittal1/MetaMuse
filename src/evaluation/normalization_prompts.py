from typing import List


NORMALIZATION_SYSTEM_PROMPT = """You are a scientific metadata normalization evaluator.

Your task is to evaluate whether normalized ontology terms and IDs correctly represent the curated values.

NORMALIZATION EVALUATION RULES:

1. **Semantic Accuracy:**
   - The normalized term should accurately represent the meaning of the curated value
   - Minor variations in wording are acceptable if the meaning is preserved
   - Example: "breast cancer" → "breast carcinoma" is CORRECT (semantically equivalent)

2. **Ontology ID Consistency:**
   - The normalized ID should match the normalized term
   - Check that the ID corresponds to the correct ontology entry
   - Example: "lung cancer" with MONDO:0005097 is CORRECT for lung squamous cell carcinoma

3. **Standard Ontology Usage:**
   - Disease: MONDO ontology (MONDO:XXXXXXX)
   - Tissue: UBERON ontology (UBERON:XXXXXXX) or Cell Ontology (CL:XXXXXXX)
   - Organ: UBERON ontology (UBERON:XXXXXXX)

4. **Special Cases:**
   - SKIP (set is_normalization_correct to None): "None reported" or blank curated values with "No Term Found" or empty normalized terms
   - INCORRECT (is_normalization_correct = False): Curated value exists (not "None reported" or blank) but normalized term is "No Term Found" or empty - this is a normalization failure
   - "control [healthy]" may normalize to specific healthy/normal terms
   - Empty curated values should have empty normalized terms (CORRECT only if curated is also empty)

5. **Accuracy Assessment - CRITICAL RULE:**
   - CORRECT: Normalized term semantically matches curated value → Set is_normalization_correct = True, leave suggested_term and suggested_id as None
   - INCORRECT: Only mark as incorrect (is_normalization_correct = False) if you can identify a BETTER ontology term that would be more accurate
   - If the normalized term is wrong BUT you cannot identify a better term, mark as CORRECT (is_normalization_correct = True) with a note explaining the limitation
   - When marking as INCORRECT, you MUST provide:
     * suggested_term: A better ontology term that more accurately represents the curated value
     * suggested_id: The ontology ID for the suggested_term
     * normalization_reason: Explanation of why the current term is wrong and why the suggested term is better
   - INCORRECT: Normalized ID does not match the normalized term (only if you can suggest the correct ID)

FOCUS: Only evaluate the accuracy of normalization mapping from curated value to normalized term/ID.
Do not evaluate whether the original curation was correct - assume curated values are the ground truth."""


def build_normalization_prompt(
    sample_id: str,
    series_id: str,
    sample_type: str,
    normalized_fields: List[str],
    curated_values: dict,
    normalized_values: dict,
) -> str:
    """Construct the user prompt for normalization evaluation."""
    
    fields_list = ", ".join(normalized_fields)
    
    # Build field-by-field comparison
    field_comparisons = []
    for field in normalized_fields:
        curated = curated_values.get(field, "")
        norm_term = normalized_values.get(field, {}).get("term", "")
        norm_id = normalized_values.get(field, {}).get("id", "")
        
        field_comparisons.append(f"""
{field}:
  - Curated value: "{curated}"
  - Normalized term: "{norm_term}"
  - Normalized ID: "{norm_id}"
""")
    
    prompt = f"""
Evaluate the normalization accuracy for the following sample.

Identifiers:
- Sample ID: {sample_id}
- Series ID: {series_id}
- Sample type: {sample_type}

Normalized fields to evaluate: {fields_list}

Field comparisons:
{"".join(field_comparisons)}

Instructions:
- For each field, determine if the normalized term/ID correctly represents the curated value
- Consider semantic equivalence (e.g., "cancer" vs "carcinoma")
- Verify ontology ID matches the normalized term
- CRITICAL: Only mark as INCORRECT (False) if you can identify a BETTER ontology term. If no better term exists, mark as CORRECT (True) even if the current term is imperfect.
- When marking as INCORRECT, you MUST provide suggested_term and suggested_id with a better alternative
- IMPORTANT SKIP RULE: For "None reported" or blank curated values with "No Term Found" or empty normalized terms, set is_normalization_correct to None (skip evaluation)
- IMPORTANT ERROR RULE: If curated value exists (not "None reported" or blank) but normalized term is "No Term Found" or empty, mark as INCORRECT (False) - this is a normalization failure
- Provide brief reasoning for each evaluation (or "Skipped - None reported with No Term Found" for skipped cases)
- Remember: curated values are ground truth - only evaluate normalization accuracy
"""
    return prompt

