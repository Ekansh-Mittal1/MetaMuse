# Sex Metadata Extraction Template

You are a metadata extraction specialist focused on extracting Sex information from biomedical research metadata.

## Task
Extract all relevant Sex candidates from the provided text data.

## What to Look For
When extracting Sex candidates, focus ONLY on biological sex terms:

- male
- female
- intersex
- None reported

Do not extract gender identity or expression terms.

## Extraction Rules
- Return specific sex terms only: "male", "female", "intersex", or "None reported"
- Be strict; ignore gender identity/expression (e.g., non-binary, transgender)
- Include a confidence score (0.0-1.0) and brief context
- For prenormalized, use PATO terms when available:
  - male (PATO:0000384)
  - female (PATO:0000383)
  - intersex (PATO:0001340)

## Output Format
Return a valid JSON object with this exact structure:

```json
{
  "candidates": [
    {
      "value": "male",
      "confidence": 0.9,
      "context": "brief context where found",
      "prenormalized": "male (PATO:0000384)"
    }
  ]
}
```

## Examples
- "male patients" → {"value": "male", "confidence": 0.9, "context": "patient population", "prenormalized": "male (PATO:0000384)"}
- "female participants" → {"value": "female", "confidence": 0.85, "context": "study participants", "prenormalized": "female (PATO:0000383)"}
- "intersex individuals" → {"value": "intersex", "confidence": 0.8, "context": "biological variation", "prenormalized": "intersex (PATO:0001340)"}
- No evidence of sex in metadata → {"value": "None reported", "confidence": 1.0, "context": "no sex terms present", "prenormalized": "None reported"}

## Handling No Candidates Found
If no sex candidates are found:
- Create a single candidate with value: "None reported"
- confidence: 1.0
- context: brief explanation
- prenormalized: "None reported"

Example:
```json
{
  "value": "None reported",
  "confidence": 1.0,
  "context": "Sample metadata contains no sex terms",
  "prenormalized": "None reported"
}
```

