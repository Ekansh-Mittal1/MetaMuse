#!/usr/bin/env python3
"""
pubmed_ingest.py

Download PubMed baseline/updatefiles, verify MD5s, and ingest into a local SQLite database
for instant lookups by PMID (title, abstract, authors, journal info) with zero API rate limits.

USAGE (examples)
----------------
# 1) Download baseline to ~/data/pubmed/baseline
python pubmed_ingest.py download \
  --which baseline \
  --out ~/data/pubmed/baseline

# 2) (Optional) Download daily update files to ~/data/pubmed/updatefiles
python pubmed_ingest.py download \
  --which updates \
  --out ~/data/pubmed/updatefiles

# 3) Ingest baseline (and optionally updates) into a SQLite DB
python pubmed_ingest.py ingest \
  --db ~/data/pubmed/pubmed.sqlite \
  --dirs ~/data/pubmed/baseline ~/data/pubmed/updatefiles

# 4) Create a new filtered database with only specific PubMed IDs
python pubmed_ingest.py ingest \
  --db ~/data/pubmed/pubmed_filtered.sqlite \
  --dirs ~/data/pubmed/baseline ~/data/pubmed/updatefiles \
  --filter-ids /path/to/pmid_list.txt

# 5) Query a PMID locally
python pubmed_ingest.py lookup \
  --db ~/data/pubmed/pubmed.sqlite \
  --pmid 12345678

NOTES
-----
- The script parses gzipped PubMed XML using streaming iterparse to keep RAM small.
- Abstracts can have multiple <AbstractText> sections with "Label" attributes; we join them with labels.
- Authors may be individual names or a <CollectiveName> (e.g., consortia).
- Run "download --which updates" periodically to stay in sync with PubMed daily changes.
- When using --filter-ids, a new database will be created containing only the specified PubMed IDs.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import os
import re
import sqlite3
import sys
import time
import urllib.request
import urllib.error
import urllib.parse
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable, List, Tuple, Optional, Dict, Set
import xml.etree.ElementTree as ET

from tqdm import tqdm

# -------------------------
# Constants
# -------------------------

BASELINE_URL = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"
UPDATES_URL = "https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/"
ACCEPT_EXTS = (".xml.gz", ".md5")

# -------------------------
# Helpers: minimal HTML href extractor
# -------------------------

class _HrefParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.hrefs: List[str] = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "a":
            return
        d = dict(attrs)
        href = d.get("href")
        if href:
            self.hrefs.append(href)

def list_remote_files(base_url: str, accept_exts: Tuple[str, ...]) -> List[str]:
    """
    Fetch a directory listing page and return absolute URLs of files with accepted extensions.
    """
    req = urllib.request.Request(base_url, headers={"User-Agent": "pubmed-ingest/1.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        html = resp.read().decode("utf-8", errors="ignore")
    parser = _HrefParser()
    parser.feed(html)
    urls = []
    for href in parser.hrefs:
        # skip directories (trailing '/'), parent links, query params
        if href.endswith("/"):
            continue
        if any(href.endswith(ext) for ext in accept_exts):
            urls.append(urllib.parse.urljoin(base_url, href))
    # Only keep actual pubmed file patterns or md5s
    return sorted(set(urls))

# -------------------------
# Download + MD5 verification
# -------------------------

def _download_file(url: str, out_path: Path, overwrite: bool=False, retry: int=3):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists() and not overwrite:
        return
    last_err = None
    for i in range(retry):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "pubmed-ingest/1.0"})
            with urllib.request.urlopen(req, timeout=120) as resp, open(out_path, "wb") as f:
                # stream without progress printing (tqdm handles overall progress)
                while True:
                    chunk = resp.read(1024 * 1024)
                    if not chunk:
                        break
                    f.write(chunk)
            return
        except Exception as e:
            last_err = e
            time.sleep(2 * (i + 1))
    raise RuntimeError(f"Failed to download {url}: {last_err}")

def _read_md5_file(md5_path: Path) -> Optional[str]:
    """
    md5 file usually formatted like: "<md5sum>  <filename>"
    """
    try:
        txt = md5_path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return None
    m = re.search(r"^([a-fA-F0-9]{32})\b", txt.strip())
    return m.group(1).lower() if m else None

def _md5_of_file(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest().lower()

def download_command(args):
    base_url = BASELINE_URL if args.which == "baseline" else UPDATES_URL
    out_dir = Path(os.path.expanduser(args.out)).resolve()
    print(f"Listing remote files from {base_url} ...")
    urls = list_remote_files(base_url, ACCEPT_EXTS)
    # Split into data and md5s
    data_urls = [u for u in urls if u.endswith(".xml.gz")]
    md5_urls  = [u for u in urls if u.endswith(".md5")]
    
    total_files = len(data_urls) + len(md5_urls)
    print(f"Found {len(data_urls)} data files and {len(md5_urls)} MD5 files to download")
    
    # Use tqdm progress bar
    with tqdm(total=total_files, desc="Downloading", unit="file") as pbar:
        # Download md5s first
        pbar.set_description("Downloading MD5 files")
        for u in md5_urls:
            p = out_dir / os.path.basename(u)
            pbar.set_postfix_str(f"MD5: {os.path.basename(u)}")
            _download_file(u, p, overwrite=False)
            pbar.update(1)
        
        # Download data files
        pbar.set_description("Downloading data files")
        for i, u in enumerate(data_urls):
            gz_path = out_dir / os.path.basename(u)
            pbar.set_postfix_str(f"Data: {os.path.basename(u)}")
            _download_file(u, gz_path, overwrite=False)
            
            # Verify if md5 present
            md5_path = out_dir / (gz_path.name + ".md5")
            if md5_path.exists():
                #pbar.set_postfix_str(f"Verifying: {gz_path.name}")
                want = _read_md5_file(md5_path)
                have = _md5_of_file(gz_path)
                if want and have and want != have:
                    pbar.set_postfix_str(f"Re-downloading: {gz_path.name}")
                    _download_file(u, gz_path, overwrite=True)
                    have2 = _md5_of_file(gz_path)
                    if want != have2:
                        raise RuntimeError(f"MD5 mismatch persists for {gz_path}")
            
            pbar.update(1)
    
    print(f"🎉 Download complete! {total_files} files downloaded to {out_dir}")

# -------------------------
# SQLite schema
# -------------------------

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS articles (
  pmid TEXT PRIMARY KEY,
  title TEXT,
  abstract TEXT,
  journal TEXT,
  iso_abbrev TEXT,
  pub_year INTEGER,
  pub_date_raw TEXT
);
CREATE TABLE IF NOT EXISTS authors (
  pmid TEXT,
  position INTEGER,
  last_name TEXT,
  fore_name TEXT,
  initials TEXT,
  collective_name TEXT,
  PRIMARY KEY (pmid, position),
  FOREIGN KEY (pmid) REFERENCES articles(pmid) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_authors_pmid ON authors(pmid);
"""

def ensure_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(db_path))
    con.executescript(SCHEMA_SQL)
    return con

def load_pmid_filter(filter_file: Optional[str]) -> Optional[Set[str]]:
    """
    Load PubMed IDs from a filter file. Each line should contain one PMID.
    Returns None if no filter file specified, or a set of PMIDs to include.
    """
    if not filter_file:
        return None
    
    filter_path = Path(filter_file)
    if not filter_path.exists():
        raise FileNotFoundError(f"Filter file not found: {filter_file}")
    
    pmids = set()
    with open(filter_path, 'r') as f:
        for line in f:
            pmid = line.strip()
            if pmid and pmid.isdigit():
                pmids.add(pmid)
    
    print(f"Loaded {len(pmids)} PubMed IDs from filter file: {filter_file}")
    return pmids

# -------------------------
# Parsing PubMed XML (gz) with streaming iterparse
# -------------------------

NS = ""  # PubMed uses no XML namespace in baseline/update files

def _text(x: Optional[ET.Element]) -> str:
    return (x.text or "").strip() if x is not None else ""

def extract_article_fields(cit: ET.Element) -> Tuple[str, str, str, str, str, Optional[int], str, List[Dict]]:
    """
    Extract fields from <MedlineCitation> element.
    Returns: (pmid, title, abstract, journal, iso_abbrev, pub_year, pub_date_raw, authors_list)
    authors_list: list of dicts with keys (last_name, fore_name, initials, collective_name)
    """
    try:
        # PMID
        pmid = _text(cit.find("PMID"))
        art = cit.find("Article")
        title = _text(art.find("ArticleTitle")) if art is not None else ""

        # Abstract (may have multiple AbstractText with Label)
        abstract = ""
        if art is not None:
            abs_el = art.find("Abstract")
            if abs_el is not None:
                parts = []
                for at in abs_el.findall("AbstractText"):
                    label = at.attrib.get("Label")
                    sec = (at.text or "").strip()
                    if label:
                        parts.append(f"{label}: {sec}")
                    else:
                        parts.append(sec)
                abstract = "\n\n".join([p for p in parts if p])

        # Journal info
        journal = ""
        iso_abbrev = ""
        pub_year = None
        pub_date_raw = ""
        if art is not None:
            j = art.find("Journal")
            if j is not None:
                jt = j.find("Title")
                journal = _text(jt)
                iso = j.find("ISOAbbreviation")
                iso_abbrev = _text(iso)
                # Try PubDate (could be Year/Month/Day or MedlineDate text like '1998 Jan-Feb')
                pub_date = j.find("./JournalIssue/PubDate")
                if pub_date is not None:
                    year_el = pub_date.find("Year")
                    medline_date = pub_date.find("MedlineDate")
                    if year_el is not None and (year_el.text or "").strip().isdigit():
                        pub_year = int(year_el.text.strip())
                    elif medline_date is not None:
                        m = re.search(r"\b(\d{4})\b", (medline_date.text or ""))
                        if m:
                            pub_year = int(m.group(1))
                    # Raw string
                    pieces = []
                    for tag in ("Year", "Month", "Day", "MedlineDate"):
                        el = pub_date.find(tag)
                        if el is not None and (el.text or "").strip():
                            pieces.append(el.text.strip())
                    pub_date_raw = " ".join(pieces).strip()

        # Authors
        authors_list: List[Dict] = []
        if art is not None:
            al = art.find("AuthorList")
            if al is not None:
                pos = 0
                for au in al.findall("Author"):
                    pos += 1
                    last_name = _text(au.find("LastName"))
                    fore_name = _text(au.find("ForeName"))
                    initials = _text(au.find("Initials"))
                    collective = _text(au.find("CollectiveName"))
                    authors_list.append({
                        "position": pos,
                        "last_name": last_name or None,
                        "fore_name": fore_name or None,
                        "initials": initials or None,
                        "collective_name": collective or None,
                    })

        return pmid, title, abstract, journal, iso_abbrev, pub_year, pub_date_raw, authors_list
        
    except Exception as e:
        # Return empty/default values if parsing fails
        print(f"⚠️  Error parsing article fields: {e}")
        return "", "", "", "", "", None, "", []


def upsert_article_from_medline_citation(
    db: sqlite3.Connection,
    cit: ET.Element,
    pmid_filter: Optional[Set[str]],
    *,
    context: str = "",
) -> str:
    """
    Insert or update one article from a ``MedlineCitation`` element.

    Returns
    -------
    str
        ``"ok"``, ``"fail"``, or ``"skip"`` (no PMID or not in ``pmid_filter``).
    """
    pmid, title, abstract, journal, iso_abbrev, pub_year, pub_date_raw, authors = extract_article_fields(cit)
    if not pmid:
        return "skip"
    if pmid_filter is not None and pmid not in pmid_filter:
        return "skip"
    try:
        db.execute(
            """
            INSERT INTO articles (pmid, title, abstract, journal, iso_abbrev, pub_year, pub_date_raw)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(pmid) DO UPDATE SET
              title=excluded.title,
              abstract=excluded.abstract,
              journal=excluded.journal,
              iso_abbrev=excluded.iso_abbrev,
              pub_year=excluded.pub_year,
              pub_date_raw=excluded.pub_date_raw
            """,
            (pmid, title, abstract, journal, iso_abbrev, pub_year, pub_date_raw),
        )
        db.execute("DELETE FROM authors WHERE pmid = ?", (pmid,))
        for a in authors:
            db.execute(
                """
                INSERT INTO authors (pmid, position, last_name, fore_name, initials, collective_name)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    pmid,
                    a["position"],
                    a["last_name"],
                    a["fore_name"],
                    a["initials"],
                    a["collective_name"],
                ),
            )
        return "ok"
    except sqlite3.Error as e:
        print(f"⚠️  Database error processing PMID {pmid}{context}: {e}")
        return "fail"


def ingest_gz_xml(db: sqlite3.Connection, gz_path: Path, pbar: tqdm = None, commit_every: int = 2000, pmid_filter: Optional[Set[str]] = None):
    """
    Stream-parse a PubMed gz XML file and insert/update rows.
    If pmid_filter is provided, only process articles with PMIDs in the filter set.
    """
    if pbar:
        pbar.set_postfix_str(f"Processing: {gz_path.name}")
    
    try:
        with gzip.open(gz_path, "rb") as f:
            # Use iterparse on the underlying bytes; we expect top-level elements like <PubmedArticle> or <DeleteCitation>
            context = ET.iterparse(f, events=("end",))
            count = 0
            filtered_count = 0
            error_count = 0
            
            for event, elem in context:
                try:
                    tag = elem.tag
                    if tag == "PubmedArticle":
                        cit = elem.find("MedlineCitation")
                        if cit is not None:
                            st = upsert_article_from_medline_citation(db, cit, pmid_filter)
                            if st == "ok":
                                count += 1
                                filtered_count += 1
                            elif st == "fail":
                                error_count += 1
                        elem.clear()
                    elif tag == "DeleteCitation":
                        # delete citations contain PMID children
                        try:
                            for pmid_el in elem.findall("PMID"):
                                pmid = (pmid_el.text or "").strip()
                                if pmid:
                                    # Only delete if not filtering or if PMID is in filter
                                    if pmid_filter is None or pmid in pmid_filter:
                                        db.execute("DELETE FROM articles WHERE pmid = ?", (pmid,))
                                        db.execute("DELETE FROM authors  WHERE pmid = ?", (pmid,))
                        except sqlite3.Error as e:
                            error_count += 1
                            print(f"⚠️  Database error processing delete citation in {gz_path.name}: {e}")
                            # Continue processing other records
                        elem.clear()

                    if count and (count % commit_every == 0):
                        try:
                            db.commit()
                            if pbar:
                                pbar.set_postfix_str(f"Processing: {gz_path.name} ({count:,} records, {filtered_count:,} filtered, {error_count:,} errors)")
                        except sqlite3.Error as e:
                            print(f"⚠️  Commit error in {gz_path.name}: {e}")
                            # Try to continue with the next batch
                            continue

                except Exception as e:
                    error_count += 1
                    print(f"⚠️  Error processing element in {gz_path.name}: {e}")
                    # Clear the element and continue with the next one
                    elem.clear()
                    continue

            # Final commit
            try:
                db.commit()
            except sqlite3.Error as e:
                print(f"⚠️  Final commit error in {gz_path.name}: {e}")
            
            if pbar:
                if pmid_filter:
                    pbar.set_postfix_str(f"Completed: {gz_path.name} ({count:,} records, {filtered_count:,} filtered, {error_count:,} errors from {len(pmid_filter)} total)")
                else:
                    pbar.set_postfix_str(f"Completed: {gz_path.name} ({count:,} records, {error_count:,} errors)")
            
            return count
            
    except Exception as e:
        print(f"❌ Critical error processing file {gz_path.name}: {e}")
        print("🔍 Full traceback:")
        import traceback
        traceback.print_exc()
        # Return 0 records processed for this file, but don't crash the script
        return 0

def ingest_command(args):
    db_path = Path(os.path.expanduser(args.db))
    
    # Load PMID filter if specified
    pmid_filter = load_pmid_filter(args.filter_ids)
    
    # If filtering, ensure we're creating a new database
    if pmid_filter and db_path.exists():
        print(f"⚠️  Warning: Database {db_path} already exists. Creating a new filtered database.")
        # Remove existing database to start fresh
        db_path.unlink()
    elif pmid_filter and not db_path.exists():
        print(f"🔍 Creating new filtered database: {db_path}")
    elif not pmid_filter and db_path.exists():
        print(f"🔍 Adding to existing database: {db_path}")
    else:
        print(f"🔍 Creating new database: {db_path}")
    
    db = ensure_db(db_path)
    dirs = [Path(os.path.expanduser(d)) for d in args.dirs]
    
    # Find all *.xml.gz files in provided dirs, sorted by name (baseline first, then updates by lexicographic order)
    all_files: List[Path] = []
    for d in dirs:
        if not d.exists():
            print(f"[WARN] Directory does not exist: {d}")
            continue
        all_files.extend(sorted(d.glob("*.xml.gz")))
    if not all_files:
        print("[ERROR] No .xml.gz files found to ingest.")
        sys.exit(2)

    print(f"Found {len(all_files)} XML files to ingest")
    if pmid_filter:
        print(f"Creating filtered database with {len(pmid_filter)} specific PubMed IDs")
    total_records = 0
    total_errors = 0
    failed_files = []
    
    # Use tqdm progress bar for ingestion
    with tqdm(total=len(all_files), desc="Ingesting", unit="file") as pbar:
        for p in all_files:
            try:
                records_processed = ingest_gz_xml(db, p, pbar, pmid_filter=pmid_filter)
                total_records += records_processed
            except Exception as e:
                total_errors += 1
                failed_files.append(p.name)
                print(f"❌ Error processing file {p.name}: {e}")
                print("🔍 Full traceback:")
                import traceback
                traceback.print_exc()
                print(f"⚠️  Skipping file {p.name} and continuing with next file...")
                # Continue with next file instead of crashing
                continue
            finally:
                pbar.update(1)
    
    # Print summary
    print(f"\n📊 Ingestion Summary:")
    print(f"   Total files processed: {len(all_files)}")
    print(f"   Successful files: {len(all_files) - len(failed_files)}")
    print(f"   Failed files: {len(failed_files)}")
    print(f"   Total records processed: {total_records:,}")
    
    if failed_files:
        print(f"\n❌ Failed files:")
        for failed_file in failed_files:
            print(f"   - {failed_file}")
    
    if pmid_filter:
        print(f"\n🎉 Filtered database creation complete! {total_records:,} filtered records processed from {len(all_files)} files")
        print(f"📊 Database contains {total_records:,} articles matching the {len(pmid_filter)} specified PubMed IDs")
    else:
        print(f"\n🎉 Ingestion complete! {total_records:,} total records processed from {len(all_files)} files")

def lookup_command(args):
    db = sqlite3.connect(str(Path(os.path.expanduser(args.db))))
    pmid = str(args.pmid).strip()
    cur = db.execute("SELECT pmid, title, abstract, journal, iso_abbrev, pub_year, pub_date_raw FROM articles WHERE pmid = ?", (pmid,))
    row = cur.fetchone()
    if not row:
        print(f"PMID {pmid} not found.", file=sys.stderr)
        sys.exit(1)
    pmid, title, abstract, journal, iso, year, raw = row
    print(f"PMID: {pmid}")
    print(f"Title: {title or ''}")
    print(f"Journal: {journal or ''} ({iso or ''})")
    print(f"PubDate: {raw or ''}  Year: {year or ''}")
    print("Authors:")
    for a in db.execute("SELECT position, last_name, fore_name, initials, collective_name FROM authors WHERE pmid = ? ORDER BY position", (pmid,)):
        pos, ln, fn, ini, coll = a
        if coll:
            print(f"  {pos}. {coll}")
        else:
            name = " ".join([x for x in [fn, ln] if x])
            if ini and not fn:
                name = f"{ln} {ini}".strip()
            print(f"  {pos}. {name}")
    print("\nAbstract:\n---------")
    print(abstract or "")

def main():
    ap = argparse.ArgumentParser(description="Download and locally index PubMed baseline/updatefiles.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    ap_dl = sub.add_parser("download", help="Download baseline or updates to a local folder (with MD5 verification).")
    ap_dl.add_argument("--which", choices=["baseline", "updates"], required=True, help="Which PubMed dataset to download.")
    ap_dl.add_argument("--out", required=True, help="Output directory.")
    ap_dl.set_defaults(func=download_command)

    ap_ing = sub.add_parser("ingest", help="Ingest one or more directories of .xml.gz into a SQLite DB.")
    ap_ing.add_argument("--db", required=True, help="SQLite path to create/use.")
    ap_ing.add_argument("--dirs", nargs="+", required=True, help="Directories containing .xml.gz (baseline first, then updates).")
    ap_ing.add_argument("--filter-ids", help="Optional file containing PubMed IDs to filter (one per line). Creates a new filtered database.")
    ap_ing.set_defaults(func=ingest_command)

    ap_lu = sub.add_parser("lookup", help="Lookup a PMID in the local SQLite DB.")
    ap_lu.add_argument("--db", required=True, help="SQLite path to use.")
    ap_lu.add_argument("--pmid", required=True, help="PMID to look up.")
    ap_lu.set_defaults(func=lookup_command)

    args = ap.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
