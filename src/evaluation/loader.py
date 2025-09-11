import json
import os
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


def _try_read_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def load_raw_context(
    batch_dir: str,
    series_id: str,
    sample_id: str,
) -> Tuple[Optional[str], Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Load abstract text, series metadata, and sample metadata for a sample.

    The batch structure appears under {batch_dir}/data_intake/{GSE...} with
    files named like GSE*_metadata.json, GSM*_metadata.json, and possibly
    PMID_*_metadata.json including abstract text.
    """

    base = Path(batch_dir) / "data_intake" / series_id

    series_meta = _try_read_json(base / f"{series_id}_metadata.json")
    sample_meta = _try_read_json(base / f"{sample_id}_metadata.json")

    abstract_text: Optional[str] = None
    # Try PubMed-derived abstract under the series directory following observed convention
    for candidate in base.glob("PMID_*_metadata.json"):
        data = _try_read_json(candidate)
        if data and isinstance(data, dict):
            abstract_text = (
                data.get("abstract")
                or data.get("Abstract")
                or data.get("abstract_text")
                or data.get("ABSTRACT")
            )
            if abstract_text:
                break

    return abstract_text, series_meta, sample_meta



