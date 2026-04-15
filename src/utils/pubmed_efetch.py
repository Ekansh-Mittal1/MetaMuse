"""
Build a local PubMed SQLite DB using NCBI efetch (no baseline FTP download).

Uses the same schema as ``pubmed_ingest.py`` (``articles`` / ``authors`` tables).
"""

from __future__ import annotations

import re
import sqlite3
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import List, Optional

from tqdm import tqdm

from src.utils.pubmed_ingest import ensure_db, upsert_article_from_medline_citation

EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"


def collect_pmids_from_geometadb(geometadb_path: Path, max_pmids: int) -> List[str]:
    """
    Distinct numeric PubMed IDs from GEOmetadb ``gse.pubmed_id`` (capped).

    ``pubmed_id`` may contain multiple IDs separated by ``;``, comma, or whitespace.
    """
    con = sqlite3.connect(str(geometadb_path))
    try:
        cur = con.execute(
            """
            SELECT pubmed_id FROM gse
            WHERE pubmed_id IS NOT NULL AND TRIM(CAST(pubmed_id AS TEXT)) != ''
            """
        )
        seen: set[str] = set()
        out: List[str] = []
        for (raw,) in cur:
            if len(out) >= max_pmids:
                break
            s = str(raw).strip()
            for part in re.split(r"[;,\s]+", s):
                p = part.strip()
                if p.isdigit() and p not in seen:
                    seen.add(p)
                    out.append(p)
                    if len(out) >= max_pmids:
                        break
        return out
    finally:
        con.close()


def ingest_efetch_xml_bytes(db: sqlite3.Connection, xml_bytes: bytes) -> tuple[int, int]:
    """Parse ``PubmedArticleSet`` XML from efetch; return (success_count, fail_count)."""
    oks = 0
    fails = 0
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        print(f"⚠️  efetch XML parse error: {e}")
        return 0, 0
    for article in root.findall("PubmedArticle"):
        cit = article.find("MedlineCitation")
        if cit is None:
            continue
        st = upsert_article_from_medline_citation(db, cit, pmid_filter=None, context=" [efetch]")
        if st == "ok":
            oks += 1
        elif st == "fail":
            fails += 1
    return oks, fails


def build_pubmed_sqlite_from_pmids(
    pmids: List[str],
    db_path: Path,
    email: str,
    api_key: Optional[str],
    *,
    batch_size: int = 200,
) -> int:
    """
    Fetch articles from NCBI efetch in batches and write ``pubmed.sqlite``.

    Returns
    -------
    int
        Approximate number of articles successfully upserted.
    """
    if not email or not str(email).strip():
        raise ValueError("NCBI_EMAIL is required for PubMed efetch (set in .env).")

    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = ensure_db(db_path)
    total_ok = 0
    delay = 0.11 if api_key else 0.34
    unique = list(dict.fromkeys(pmids))

    batches = [unique[i : i + batch_size] for i in range(0, len(unique), batch_size)]
    for i, batch in enumerate(tqdm(batches, desc="PubMed efetch", unit="batch")):
        if i:
            time.sleep(delay)
        params = {
            "db": "pubmed",
            "retmode": "xml",
            "id": ",".join(batch),
            "tool": "metamuse_setup_data",
            "email": email.strip(),
        }
        if api_key:
            params["api_key"] = api_key.strip()
        url = EFETCH_URL + "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(
            url,
            headers={"User-Agent": f"MetaMuse-setup-data/1.0 ({email.strip()})"},
        )
        try:
            with urllib.request.urlopen(req, timeout=180) as resp:
                body = resp.read()
        except Exception as e:
            print(f"⚠️  efetch batch failed ({len(batch)} ids): {e}")
            continue
        oks, _fails = ingest_efetch_xml_bytes(db, body)
        total_ok += oks
        try:
            db.commit()
        except sqlite3.Error as e:
            print(f"⚠️  commit after efetch batch: {e}")

    db.close()
    return total_ok
