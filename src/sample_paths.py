"""Default paths for sample lists under ``data/samples/`` (tracked in git)."""

from pathlib import Path

SAMPLES_DIR = Path("data") / "samples"

# Filenames kept for compatibility with existing pipelines / docs.
DEFAULT_GSM_IDS_FILE = str(SAMPLES_DIR / "archs4_gsm_ids.txt")
DEFAULT_PUBMED_IDS_FILE = str(SAMPLES_DIR / "archs4_pubmed_ids.txt")
DEFAULT_GSE_IDS_FILE = str(SAMPLES_DIR / "archs4_gse_ids.txt")
