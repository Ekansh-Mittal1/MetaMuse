# Sample Type Metadata Extraction Template

You are a metadata extraction specialist focused on determining the type of sample: primary sample (patient biopsy), established cell line, or unknown.

## Task
Classify each sample as one of three types based on the provided metadata:
- **primary_sample**: Patient biopsy or tissue sample
- **cell_line**: Established cell line
- **unknown**: Cannot determine with certainty

## CRITICAL CONFIDENCE REQUIREMENTS
- **MUST BE 100% CONFIDENT** in your classification decision
- If you cannot determine with absolute certainty, **FLAG FOR RECONCILIATION**
- **PRIORITIZE SAMPLE-LEVEL METADATA** over abstract and series metadata
- Sample metadata contains the most relevant information for the specific sample
- Abstract and series metadata may reference other samples or general study information

## Classification Criteria

### **PRIMARY_SAMPLE (Patient Biopsy) - Return "primary_sample"**
Look for these indicators in **SAMPLE METADATA FIRST**:
- **Patient identifiers**: Sample names containing patient IDs, case numbers, or biopsy identifiers
- **Surgical context**: Mentions of "surgical removal", "patient care", "IRB approval", "tumor sample"
- **Tissue descriptions**: Direct tissue names like "brain tumor", "pancreatic islets", "tumor sample"
- **Patient characteristics**: Age, sex, BMI, patient-specific information
- **Clinical context**: References to patient care, clinical procedures, hospital settings
- **Source descriptions**: "Primary", "tissue", "biopsy", "surgical sample"

### **CELL_LINE (Established Cell Line) - Return "cell_line"**
Look for these indicators in **SAMPLE METADATA FIRST**:
- **Cell line names**: Specific cell line identifiers (HeLa, HEK293, IMR90, OCI-LY1, etc.)
- **Commercial cell line products**: Commercial cell line products, manufacturer names, product codes
- **Culture conditions**: Mentions of media, passages, cell culture protocols
- **Laboratory context**: References to cell culture, growth protocols, laboratory procedures
- **Passage numbers**: "passage: 15", "P15", "passage number"
- **Cell line characteristics**: "cell line: [NAME]", "established cell line"
- **Growth protocols**: Detailed cell culture procedures, media composition
- **Manufacturer references**: Commercial suppliers, product manufacturers, cell line vendors

### **UNKNOWN - Return "unknown"**
Use when:
- **Ambiguous indicators**: Mixed or unclear evidence
- **Generic terms**: "cells", "tissue sample", "culture" without clear context
- **Insufficient information**: Cannot determine from available metadata
- **Conflicting evidence**: Multiple indicators pointing to different types

## Extraction Rules
- **100% CONFIDENCE REQUIRED**: Only classify if absolutely certain
- **PRIORITIZE SAMPLE METADATA**: Focus on sample-level information first
- **REVIEW ALL METADATA FIELDS CAREFULLY**: Examine all available metadata fields (source_name, characteristics, growth_protocol, extract_protocol, etc.)
- **COMMERCIAL CELL LINE PRODUCTS ARE STRONG INDICATORS**: Commercial cell line products in any metadata field indicate cell lines
- **EQUAL PRIORITY FOR GROWTH AND EXTRACTION PROTOCOLS**: Both growth_protocol and extract_protocol should be given equal high priority when determining sample type
- **FLAG UNCERTAIN CASES**: If confidence < 1.0, mark for reconciliation
- **HIGHEST CONFIDENCE** for clear patient identifiers or established cell line names in sample metadata
- **MEDIUM CONFIDENCE** for contextual clues in sample source descriptions and protocols
- **REJECT AMBIGUOUS CASES**: When in doubt, flag for manual review
- Return enum classification (primary_sample/cell_line/unknown) with confidence score (0.0-1.0)
- Provide explicit reasoning for the classification decision
- **CRITICAL**: If sample metadata is unclear, do NOT rely heavily on abstract/series metadata

## Output Format
Return a valid JSON object with this exact structure:

```json
{
  "candidates": [
    {
      "value": "primary_sample",
      "confidence": 1.0,
      "context": "brief context supporting classification"
    }
  ]
}
```

## Examples

### Primary Sample Examples:
- **"EPN_507_1"** → {"value": "primary_sample", "confidence": 1.0, "context": "patient identifier in source_name"}
- **"characteristics_ch1: group: Primary, tissue: brain tumor"** → {"value": "primary_sample", "confidence": 1.0, "context": "explicitly marked as Primary tissue"}
- **"pancreatic islets, age: 69, sex: Male"** → {"value": "primary_sample", "confidence": 1.0, "context": "patient tissue with demographic info"}
- **"surgical tumor sample of ependymoma"** → {"value": "primary_sample", "confidence": 1.0, "context": "surgical tumor sample"}

### Cell Line Examples:
- **"IMR90 ER:RAS"** → {"value": "cell_line", "confidence": 1.0, "context": "established cell line name"}
- **"Human DLBCL cell line"** → {"value": "cell_line", "confidence": 1.0, "context": "explicitly labeled as cell line"}
- **"characteristics_ch1: cell line: OCI-LY1"** → {"value": "cell_line", "confidence": 1.0, "context": "explicitly marked as cell line"}
- **"imr90 passage: 15, days after 4oht induction"** → {"value": "cell_line", "confidence": 1.0, "context": "passage numbers and cell culture context"}
- **"Commercial cell line product from manufacturer"** → {"value": "cell_line", "confidence": 1.0, "context": "commercial cell line product"}
- **"growth_protocol_ch1: commercial cell line (product code)"** → {"value": "cell_line", "confidence": 1.0, "context": "commercial cell line with product code"}
- **"Manufacturer lot #12345"** → {"value": "cell_line", "confidence": 1.0, "context": "commercial cell line manufacturer"}
- **"growth_protocol_ch1: commercial cell line from manufacturer (product code, lot #12345) were differentiated"** → {"value": "cell_line", "confidence": 1.0, "context": "commercial cell line product in growth protocol indicates cell line"}

### Unknown Examples:
- **"cells"** → {"value": "unknown", "confidence": 1.0, "context": "too generic, cannot determine type"}
- **"tissue sample"** → {"value": "unknown", "confidence": 1.0, "context": "ambiguous, could be either"}
- **"culture"** → {"value": "unknown", "confidence": 1.0, "context": "ambiguous, could be either"}
- **Mixed indicators** → {"value": "unknown", "confidence": 1.0, "context": "conflicting evidence"}

### UNCERTAIN CASES (Flag for Reconciliation):
- **Mixed indicators** → **REJECT** (conflicting evidence, flag for reconciliation)
- **Insufficient context** → **REJECT** (not enough information, flag for reconciliation)
- **Ambiguous descriptions** → **REJECT** (unclear classification, flag for reconciliation)

## Important Notes
- **CRITICAL**: Distinguish between patient-derived samples, laboratory-established cell lines, and unknown cases
- **SAMPLE METADATA FIRST**: Prioritize sample-level information over abstract/series metadata
- **100% CONFIDENCE REQUIRED**: Only classify when absolutely certain
- **FLAG UNCERTAIN CASES**: When confidence < 1.0, mark for manual reconciliation
- **CONTEXT MATTERS**: Consider the full context of sample source descriptions and protocols
- **CONFIDENCE SCORING**: 
  - 1.0: Clear patient identifiers, established cell line names, or clear unknown cases in sample metadata
  - < 1.0: Flag for reconciliation - do not classify
- **ENUM OUTPUT**: Return only "primary_sample", "cell_line", or "unknown" as the value
- **REASONING**: Always provide clear rationale for the classification decision
- **CONSERVATIVE APPROACH**: When in doubt, flag for reconciliation rather than guess
- **The `value` field should contain ONLY "primary_sample", "cell_line", or "unknown"**
- **The `confidence` field should be 1.0 for all classifications, otherwise flag for reconciliation** 