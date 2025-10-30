#!/usr/bin/env python3
"""
ols_map.py — Map a free-text query to ontology terms via OLS4, with top-k results.

Usage:
  python ols_map.py --ontology UBERON "human islets" --top-k 3
  python ols_map.py --ontology GO "apoptotic process" --json --top-k 5

Requirements: Python 3.8+, no external packages (stdlib only).

Endpoints used:
- Search (primary):  https://www.ebi.ac.uk/ols4/api/search
- Select (fallback): https://www.ebi.ac.uk/ols4/api/select
- Terms:             https://www.ebi.ac.uk/ols4/api/ontologies/{ontology}/terms
"""

from __future__ import annotations
import argparse
import json
import sys
import time
import re
import urllib.parse
import urllib.request
import urllib.error
from typing import List, Dict, Optional

BASE = "https://www.ebi.ac.uk/ols4"
SEARCH_URL = f"{BASE}/api/search"
SELECT_URL = f"{BASE}/api/select"  # fallback
TERMS_URL  = f"{BASE}/api/ontologies/{{ontology}}/terms"  # ?iri=... or ?obo_id=... or ?short_form=...

# Species adjectives we often want to ignore for ontology label matching
COMMON_SPECIES = re.compile(
    r"\b(human|mouse|rat|murine|zebrafish|drosophila|arabidopsis|yeast|canine|feline|porcine|bovine)\b",
    re.I,
)

def _normalize_query(q: str) -> str:
    """Strip common species adjectives and naive singularization (for one-word plurals)."""
    q = COMMON_SPECIES.sub("", q).strip()
    if " " not in q:
        # very simple plural heuristics (safe-ish for common biology nouns)
        if q.endswith("ies") and len(q) > 3:
            q = q[:-3] + "y"
        elif q.endswith("s") and not q.endswith("ss") and len(q) > 1:
            q = q[:-1]
    return q or q  # keep as-is if it empties out

def http_get(url: str, retries: int = 3, backoff: float = 0.6, debug: bool = False) -> dict:
    """GET JSON with basic retry/backoff for transient 5xx and network errors."""
    last_err: Optional[Exception] = None
    for i in range(retries):
        try:
            if debug:
                print(f"[http_get] GET {url}", file=sys.stderr)
            req = urllib.request.Request(
                url,
                headers={
                    "Accept": "application/json",
                    "User-Agent": "ols-map/1.1 (+https://github.com/)",
                },
            )
            with urllib.request.urlopen(req, timeout=20) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status} for {url}")
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            # Retry on 5xx, fail fast on 4xx
            if 500 <= e.code < 600 and i < retries - 1:
                time.sleep(backoff * (2 ** i))
                last_err = e
                continue
            raise
        except urllib.error.URLError as e:
            # Network hiccup -> retry
            if i < retries - 1:
                time.sleep(backoff * (2 ** i))
                last_err = e
                continue
            raise
    if last_err:
        raise last_err
    return {}  # unreachable, but keeps type-checkers happy

def _build_search_url(query: str, ontology: str, *, rows: int, exact: bool, with_fields: bool) -> str:
    params = {
        "q": query,
        "ontology": ontology,
        "type": "class",
        "rows": str(rows),
    }
    if exact:
        params["exact"] = "true"
    if with_fields:
        # Bias label/synonyms, but still allow description/IDs/IRI
        params["queryFields"] = "label^10 synonym^6 description short_form obo_id iri"
    return f"{SEARCH_URL}?{urllib.parse.urlencode(params)}"

def _build_select_url(query: str, ontology: str, *, rows: int) -> str:
    # Solr select fallback (robust; returns response.docs as well)
    params = {
        "q": query,
        "ontology": ontology,
        "rows": str(rows),
        "wt": "json",
        "defType": "edismax",
        "qf": "label^10 synonym^6 description short_form obo_id iri",
        "fq": "type:class",
    }
    return f"{SELECT_URL}?{urllib.parse.urlencode(params)}"

def search_ols(query: str, ontology: str, rows: int = 25, exact: bool = False, debug: bool = False) -> List[Dict]:
    """
    Try a cascade of increasingly permissive searches, returning normalized hits:
    1) /api/search with exact + boosted fields
    2) /api/search without exact + boosted fields
    3) /api/search with normalized query + boosted fields
    4) /api/search with normalized query + no boosted fields
    5) /api/select (Solr) with normalized query
    """
    q_raw = query.strip()
    q_norm = _normalize_query(q_raw)

    attempts = [
        _build_search_url(q_raw, ontology, rows=rows, exact=exact, with_fields=True),
        _build_search_url(q_raw, ontology, rows=rows, exact=False, with_fields=True),
        _build_search_url(q_norm, ontology, rows=rows, exact=False, with_fields=True),
        _build_search_url(q_norm, ontology, rows=rows, exact=False, with_fields=False),
        _build_select_url(q_norm, ontology, rows=rows),
    ]

    last_err: Optional[Exception] = None
    for url in attempts:
        try:
            data = http_get(url, debug=debug)
            docs = (data.get("response") or {}).get("docs") or []
            hits = []
            for d in docs[:rows]:
                desc = d.get("description")
                if isinstance(desc, list):
                    desc = desc[0] if desc else None
                hits.append({
                    "label": d.get("label"),
                    "ontology_name": d.get("ontology_name") or d.get("ontology_prefix") or d.get("ontologyId"),
                    "short_form": d.get("short_form"),
                    "obo_id": d.get("obo_id"),
                    "iri": d.get("iri") or d.get("iri_autosuggest"),
                    "description": desc,
                    "synonym": d.get("synonym") or [],
                    "is_obsolete": bool(d.get("is_obsolete", False)),
                    "score": d.get("score"),
                })
            if hits:
                if debug:
                    print(f"[search_ols] hits from: {url}", file=sys.stderr)
                return hits
        except Exception as e:
            last_err = e
            if debug:
                print(f"[search_ols] failed: {e}", file=sys.stderr)
            continue

    if last_err:
        # Re-raise last error so the caller can show a useful message
        raise last_err
    return []

def fetch_term_payload(
    ontology: str,
    iri: Optional[str] = None,
    obo_id: Optional[str] = None,
    short_form: Optional[str] = None,
    debug: bool = False,
) -> Optional[Dict]:
    """Retrieve canonical term JSON (tidy fields, synonyms, etc.)."""
    params = {}
    if iri:
        params["iri"] = iri
    elif obo_id:
        params["obo_id"] = obo_id
    elif short_form:
        params["short_form"] = short_form
    else:
        return None

    url = f"{TERMS_URL.format(ontology=ontology.lower())}?{urllib.parse.urlencode(params)}"
    data = http_get(url, debug=debug)
    term_list = ((data.get("_embedded") or {}).get("terms")) or []
    return term_list[0] if term_list else None

def map_query_to_term(
    query: str,
    ontology: str,
    rows: int = 25,
    top_k: int = 1,
    debug: bool = False
) -> List[Dict]:
    """
    Search OLS and return up to top_k matching terms in rank order.
    Each element contains label, IRI, OBO ID, etc.
    """
    if top_k < 1:
        top_k = 1

    hits = search_ols(query, ontology, rows=rows, exact=False, debug=debug)
    if not hits:
        return []

    # Sort: highest score first, non-obsolete preferred
    hits.sort(key=lambda h: (-(h.get("score") or 0), h.get("is_obsolete", False)))

    results: List[Dict] = []
    for h in hits[:top_k]:
        term = fetch_term_payload(
            ontology=ontology,
            iri=h.get("iri"),
            obo_id=h.get("obo_id"),
            short_form=h.get("short_form"),
            debug=debug,
        ) or {}

        results.append({
            "label": h.get("label") or term.get("label"),
            "ontology": ontology.upper(),
            "iri": h.get("iri") or term.get("iri"),
            "obo_id": h.get("obo_id") or term.get("obo_id") or term.get("oboId"),
            "short_form": h.get("short_form") or term.get("short_form") or term.get("shortForm"),
            "description": h.get("description") or term.get("description"),
            "synonyms": h.get("synonym") or term.get("synonyms") or term.get("synonym") or [],
            "is_obsolete": bool(h.get("is_obsolete") or term.get("is_obsolete") or term.get("isObsolete")),
            "search_score": h.get("score"),
        })
    return results

def main():
    ap = argparse.ArgumentParser(description="Map a query to OLS terms in a target ontology (top-k supported).")
    ap.add_argument("query", help="Free-text term to look up (e.g., 'apoptotic process').")
    ap.add_argument(
        "--ontology", "-o", required=True,
        help="Target ontology prefix/ID, e.g., GO, EFO, HP, UBERON (case-insensitive)."
    )
    ap.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    ap.add_argument("--rows", type=int, default=25, help="Max hits to fetch before ranking (default: 25).")
    ap.add_argument("--top-k", type=int, default=1, help="Number of top matches to return (default: 1).")
    ap.add_argument("--debug", action="store_true", help="Verbose debugging to stderr.")
    args = ap.parse_args()

    try:
        results = map_query_to_term(
            args.query,
            args.ontology,
            rows=args.rows,
            top_k=args.top_k,
            debug=args.debug
        )
    except Exception as e:
        print(f"Error querying OLS: {e}", file=sys.stderr)
        sys.exit(2)

    if not results:
        print("No matching terms found.", file=sys.stderr)
        sys.exit(1)

    if args.json:
        print(json.dumps(results, indent=2, ensure_ascii=False))
    else:
        for i, r in enumerate(results, 1):
            print(f"\n=== Match {i} ===")
            print(f"Label:       {r['label']}")
            print(f"Ontology:    {r['ontology']}")
            print(f"IRI:         {r['iri']}")
            print(f"OBO ID:      {r['obo_id']}")
            print(f"Short form:  {r['short_form']}")
            print(f"Obsolete:    {r['is_obsolete']}")
            if r.get("description"):
                print(f"Description: {r['description']}")
            syns = r.get("synonyms") or []
            if syns:
                print("Synonyms:    " + "; ".join(syns[:10]) + (" ..." if len(syns) > 10 else ""))
            if r.get("search_score") is not None:
                print(f"(search score={r['search_score']})")

if __name__ == "__main__":
    main()
