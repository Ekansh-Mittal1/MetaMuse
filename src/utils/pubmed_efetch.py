"""
Build a local PubMed SQLite DB using NCBI efetch (no baseline FTP download).

Uses the same schema as ``pubmed_ingest.py`` (``articles`` / ``authors`` tables).
"""

from __future__ import annotations

import errno
import os
import re
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections import deque
from pathlib import Path
from typing import Dict, List, Optional

from tqdm import tqdm

from src.utils.pubmed_ingest import ensure_db, upsert_article_from_medline_citation

EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"


def _efetch_url() -> str:
    """Full efetch endpoint; override with ``NCBI_EUTILS_EFETCH_URL`` if needed (e.g. proxy)."""
    return (os.getenv("NCBI_EUTILS_EFETCH_URL") or "").strip() or EFETCH_URL


def _retryable_efetch_error(exc: BaseException) -> bool:
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code in (429, 500, 502, 503, 504)
    if isinstance(exc, urllib.error.URLError):
        r = exc.reason
        if isinstance(r, BaseException):
            return _retryable_efetch_error(r)
        if isinstance(r, str):
            low = r.lower()
            if any(
                x in low
                for x in (
                    "timed out",
                    "connection refused",
                    "connection reset",
                    "network is unreachable",
                    "name or service not known",
                    "nodename nor servname",
                    "ssl",
                    "eof",
                )
            ):
                return True
    if isinstance(exc, (TimeoutError, ConnectionResetError, ConnectionRefusedError, BrokenPipeError)):
        return True
    if isinstance(exc, OSError) and exc.errno in {
        errno.ECONNREFUSED,
        errno.ECONNRESET,
        errno.ETIMEDOUT,
        errno.EPIPE,
        errno.ENETUNREACH,
        errno.EHOSTUNREACH,
    }:
        return True
    return False


def _efetch_request_headers(email: str) -> Dict[str, str]:
    return {"User-Agent": f"MetaMuse-setup-data/1.0 ({email.strip()})"}


def _fetch_efetch_once(
    *,
    method: str,
    efetch_url: str,
    params: Dict[str, str],
    email: str,
    timeout: int,
) -> bytes:
    if method == "GET":
        url = efetch_url + "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers=_efetch_request_headers(email), method="GET")
    elif method == "POST":
        body = urllib.parse.urlencode(params).encode("utf-8")
        req = urllib.request.Request(
            efetch_url,
            data=body,
            headers={
                **_efetch_request_headers(email),
                "Content-Type": "application/x-www-form-urlencoded",
            },
            method="POST",
        )
    else:
        raise ValueError(method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def _fetch_efetch_with_retries(
    batch: List[str],
    *,
    email: str,
    api_key: Optional[str],
    efetch_url: str,
    timeout: int,
    max_retries: int,
    base_delay: float,
) -> Optional[bytes]:
    params: Dict[str, str] = {
        "db": "pubmed",
        "retmode": "xml",
        "id": ",".join(batch),
        "tool": "metamuse_setup_data",
        "email": email.strip(),
    }
    if api_key:
        params["api_key"] = api_key.strip()

    def try_method(method: str) -> Optional[bytes]:
        for attempt in range(max_retries):
            if attempt:
                time.sleep(base_delay * (2 ** (attempt - 1)))
            try:
                return _fetch_efetch_once(
                    method=method, efetch_url=efetch_url, params=params, email=email, timeout=timeout
                )
            except Exception as e:
                if not _retryable_efetch_error(e):
                    print(f"⚠️  efetch {method} non-retryable ({len(batch)} ids): {e}")
                    return None
                if attempt + 1 == max_retries:
                    print(
                        f"⚠️  efetch {method} exhausted retries ({len(batch)} ids, "
                        f"{max_retries} tries): {e}"
                    )
        return None

    body = try_method("GET")
    if body is None:
        time.sleep(base_delay)
        body = try_method("POST")
    return body


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
    request_timeout: int = 180,
    max_retries: int | None = None,
    retry_base_delay: float | None = None,
) -> int:
    """
    Fetch articles from NCBI efetch in batches and write ``pubmed.sqlite``.

    On transport failures (timeouts, connection refused, etc.), each batch is
    retried with exponential backoff, then the same IDs are retried via POST.
    If that still fails, the batch is split in half until single-PMID requests.

    Environment (optional): ``PUBMED_EFETCH_MAX_RETRIES`` (default 5),
    ``PUBMED_EFETCH_RETRY_BASE`` (seconds, default 1.0),
    ``NCBI_EUTILS_EFETCH_URL`` (full efetch URL override).

    Returns
    -------
    int
        Approximate number of articles successfully upserted.
    """
    if not email or not str(email).strip():
        raise ValueError("NCBI_EMAIL is required for PubMed efetch (set in .env).")

    mr = max_retries if max_retries is not None else int(os.getenv("PUBMED_EFETCH_MAX_RETRIES", "5"))
    if mr < 1:
        mr = 1
    rbd = retry_base_delay if retry_base_delay is not None else float(os.getenv("PUBMED_EFETCH_RETRY_BASE", "1.0"))
    if rbd < 0:
        rbd = 0.0

    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = ensure_db(db_path)
    total_ok = 0
    delay = 0.11 if api_key else 0.34
    unique = list(dict.fromkeys(pmids))
    efetch_url = _efetch_url()

    initial = [unique[i : i + batch_size] for i in range(0, len(unique), batch_size)]
    queue: deque[List[str]] = deque(initial)
    first_request = True
    pbar = tqdm(total=len(unique), desc="PubMed efetch", unit="pmid")

    while queue:
        batch = queue.popleft()
        if not first_request:
            time.sleep(delay)
        first_request = False

        body = _fetch_efetch_with_retries(
            batch,
            email=email,
            api_key=api_key,
            efetch_url=efetch_url,
            timeout=request_timeout,
            max_retries=mr,
            base_delay=rbd,
        )
        if body:
            oks, _fails = ingest_efetch_xml_bytes(db, body)
            total_ok += oks
            pbar.update(len(batch))
            try:
                db.commit()
            except sqlite3.Error as e:
                print(f"⚠️  commit after efetch batch: {e}")
            continue

        if len(batch) > 1:
            mid = len(batch) // 2
            queue.appendleft(batch[mid:])
            queue.appendleft(batch[:mid])
            print(f"⚠️  efetch split batch → {len(batch[:mid])} + {len(batch[mid:])} PMIDs (retry smaller chunks)")
            continue

        print(f"⚠️  efetch giving up on PMID {batch[0]} after retries and POST fallback")
        pbar.update(1)

    pbar.close()
    db.close()
    return total_ok
