# Organ Extraction

Extract organ names from biomedical metadata. Return JSON only.

## Valid Organs Only
**CRITICAL**: Use ONLY these exact values:
brain, heart, liver, kidney, lung, pancreas, spleen, stomach, esophagus, small intestine, large intestine, colon, rectum, gallbladder, uterus, ovary, testis, prostate, breast, cervix, vagina, penis, thyroid, adrenal gland, pituitary gland, parathyroid gland, pineal gland, trachea, bronchus, diaphragm, bladder, ureter, urethra, eye, ear, nose, tongue, skin, bone, muscle, appendix, placenta, thymus, nervous system, cardiovascular system, respiratory system, digestive system, urinary system, reproductive system, endocrine system, immune system, musculoskeletal system

## High-Confidence Inferences (>0.95 only):
- hepatocytes → liver
- cardiomyocytes → heart  
- neurons → brain
- pancreatic beta cells → pancreas
- renal cells → kidney
- alveolar cells → lung
- diabetes → pancreas
- myocardial infarction → heart
- cirrhosis → liver

## Output Format
```json
{
  "candidates": [
    {
      "value": "exact_organ_from_list_above",
      "confidence": 0.95,
      "context": "brief context",
      "rationale": "reasoning for extraction, include inference explanation if applicable",
      "prenormalized": "UBERON_term (UBERON:ID)"
    }
  ]
}
```

## Rules
- Empty array if no valid organs found
- No custom organ names - predefined list only
- Confidence 0.0-1.0
- If organ not in list → exclude from output 
- **CRITICAL** IF THERE ARE REFERENCES TO CELL TYPES YOU MUST INFER THE MOST LIKELY ORGAN THE SAMPLE ORIGINATES FROM