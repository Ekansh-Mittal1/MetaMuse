# Normalization Parallelization Analysis

## Current State Analysis

### ✅ What IS Parallelized
1. **Field-level parallelization**: Multiple fields (disease, tissue, organ, etc.) run in parallel
   - Evidence: All 7 fields start at the same timestamp (2025-10-31 00:59:09)
   - Total execution time (720s) matches longest field, not sum - confirms parallelization works

### ❌ What is NOT Parallelized (Bottlenecks)

#### 1. **Value-level Sequential Processing**
- **Location**: `semantic_search_candidates_impl()` and `ols_search_candidates_impl()` functions
- **Issue**: Within each field, values are processed sequentially
- **Evidence**: 
  - Treatment field: 4 values processed one after another (lines 116-131)
  - Each OLS call takes 13-14 seconds sequentially
  - Treatment field total: 357-376 seconds vs ~108 seconds of actual OLS work

#### 2. **Ontology-level Sequential Processing**
- **Location**: Lines 702-713 in `normalizer_tools.py`
- **Issue**: Multiple ontologies for same candidate are searched sequentially
- **Evidence**: 
  - Treatment: CHEBI (13.9s) then DRON (13.6s) = 27.6s total (could be ~14s parallel)
  - Tissue: uberon (0.3s) then cl (0.2s) = 0.5s total (could be ~0.3s parallel)

#### 3. **OLS Call Synchronous Blocking**
- **Location**: `_ols_map_value_to_candidates()` function
- **Issue**: OLS API calls are synchronous and blocking
- **Evidence**: Each OLS call takes 10-14 seconds and blocks the event loop

#### 4. **LLM Agent Sequential Processing**
- **Location**: `run_normalizer_agent()` 
- **Issue**: LLM agent processes all samples in a field sequentially
- **Evidence**: Disease field takes 720 seconds (likely processing multiple samples sequentially within the agent)

## Performance Metrics from Logs

### First Batch (primary_sample, 4 samples):
- **Field completion times**: 376s, 394s, 426s, 438s, **720s** (disease/cell_type)
- **Total execution**: 720.34 seconds
- **OLS call times**: 10-27 seconds each (sequential within value)
- **Semantic search times**: 0.2-0.8 seconds each (sequential within candidate)

### Second Batch (cell_line, 5 samples):
- **Field completion times**: 277s, **357-522s** (varying by field)
- **Total execution**: 522.87 seconds
- **Similar patterns**: Sequential OLS calls, sequential value processing

## Recommended Optimizations

### Priority 1: Parallelize OLS Calls for Multiple Ontologies
**Impact**: High (2x speedup for fields using multiple ontologies like treatment)

**Implementation**:
```python
# In _ols_map_value_to_candidates()
import asyncio

async def _ols_map_value_to_candidates_async(value: str, ontologies: List[str], top_k: int = 10):
    if not (value and ontologies and map_query_to_term):
        return []
    
    async def query_ontology(ont: str):
        try:
            ols_call_start = time.time()
            hits = await asyncio.to_thread(
                map_query_to_term, query=value, ontology=ont, rows=25, top_k=top_k, debug=False
            ) or []
            ols_call_end = time.time()
            print(f"⏱️  DEBUG: OLS call for value '{value}' in ontology '{ont}' took {ols_call_end - ols_call_start:.3f} seconds (returned {len(hits)} hits)")
            return ont, hits
        except Exception:
            return ont, []
    
    # Query all ontologies in parallel
    results = await asyncio.gather(*[query_ontology(ont) for ont in ontologies])
    
    combined = []
    for ont, hits in results:
        for h in hits:
            # Process hits...
```

### Priority 2: Parallelize Semantic Search Across Ontologies
**Impact**: Medium-High (2x speedup for tissue/organ fields)

**Implementation**:
```python
# In semantic_search_candidates_impl()
async def search_candidate_across_ontologies(candidate, ontologies, top_k, min_score):
    async def search_ontology(ontology):
        ontology_search_start = time.time()
        searcher = OntologySemanticSearch(f"src/normalization/dictionaries/{ontology}_terms.json")
        searcher.load_index()
        matches = await asyncio.to_thread(searcher.search, candidate.value, k=top_k)
        ontology_search_end = time.time()
        print(f"⏱️  DEBUG: Semantic search for '{candidate.value}' in ontology '{ontology}' took {ontology_search_end - ontology_search_start:.3f} seconds (found {len(matches)} matches)")
        return matches
    
    # Search all ontologies in parallel
    all_matches_lists = await asyncio.gather(*[search_ontology(ont) for ont in ontologies])
    
    # Flatten and process results
    all_matches = []
    for matches in all_matches_lists:
        for term, term_id, score in matches:
            if score >= min_score:
                all_matches.append((term, term_id, ontology, score))
```

### Priority 3: Parallelize Value Processing Within Field
**Impact**: Very High (Nx speedup where N = number of values)

**Implementation**:
```python
# In ols_search_candidates_impl()
async def normalize_value_async(val, ont_list, top_k):
    return await _ols_map_value_to_candidates_async(val, ontologies=ont_list, top_k=top_k)

# Process all values in parallel
normalization_tasks = [normalize_value_async(val, ont_list, top_k) for val in values[:3]]
all_matches = await asyncio.gather(*normalization_tasks)
```

### Priority 4: Parallelize Candidate Processing in Semantic Search
**Impact**: High (speedup proportional to number of candidates)

**Implementation**:
```python
# In semantic_search_candidates_impl()
async def process_candidate_async(candidate, ontologies, top_k, min_score):
    # ... candidate processing logic ...
    return candidates_with_matches

# Process all candidates in parallel
candidate_tasks = [
    process_candidate_async(candidate, ontologies, top_k, min_score) 
    for candidate in curation_result.final_candidates
]
all_candidates_with_matches = await asyncio.gather(*candidate_tasks)
candidates_with_matches = [item for sublist in all_candidates_with_matches for item in sublist]
```

## Expected Performance Improvements

### Current Performance:
- Treatment field: ~360 seconds (4 values × 2 ontologies × 14s sequential)
- Disease field: ~720 seconds (multiple candidates processed sequentially)

### After Optimizations:
- Treatment field: ~90 seconds (4 values × 2 ontologies × 14s parallel ontologies, values in parallel)
- Disease field: ~240 seconds (candidates in parallel, ontologies in parallel)
- **Overall speedup: 3-4x for normalization phase**

## Implementation Notes

1. **Convert blocking calls to async**: Use `asyncio.to_thread()` for synchronous OLS calls
2. **Batch OLS calls**: Group by ontology to maximize parallelization
3. **Memory considerations**: Don't parallelize too aggressively - limit concurrent OLS calls
4. **Error handling**: Ensure one failure doesn't block all parallel tasks

## Risk Assessment

- **Low Risk**: Priority 2 (semantic search - already async-friendly)
- **Medium Risk**: Priority 1 (OLS calls - need thread pool management)
- **Higher Risk**: Priority 3 & 4 (requires refactoring tool functions to async)

