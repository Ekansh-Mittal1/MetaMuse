"""
Ensembl REST API client helper functions for DendroForge.

This module provides a minimal, dependency-free wrapper around the public
Ensembl REST API (https://rest.ensembl.org).  It intentionally avoids
third-party HTTP libraries so we do not add extra runtime requirements – the
standard library's ``urllib`` is sufficient here and keeps the tool portable.

Only a subset of endpoints that have proven broadly useful for downstream
bioinformatics agents are surfaced for now.  The design emphasises **clarity**
& **robustness** over absolute performance.  If in the future more endpoints
are required we can extend the :class:`EnsemblRestClient` or expose new helper
functions without breaking the current public API.

All publicly exposed callables are re-exported via ``src.tools.__init__`` so
external agents can simply ``from src.tools import get_variants`` (or import
this module directly).

----------
Public API
----------

get_variants
    Retrieve all variation features that overlap a gene *symbol* in a given
    *species*.  A convenience wrapper around two REST calls:

    1. ``GET /xrefs/symbol/:species/:symbol`` to resolve the symbol to a stable
       Ensembl gene identifier (ENSG…)
    2. ``GET /overlap/id/:stable_id?feature=variation`` to fetch overlapping
       variants.

symbol_lookup
    Return the Ensembl record for a gene symbol (wrapper around
    ``GET /xrefs/symbol/:species/:symbol``).

Notes
-----
1. **Rate limiting** – the Ensembl public server enforces ~15 req/s.  A very
   lightweight self-throttling mechanism is included so agents that perform
   tight loops do not get HTTP 429 errors.
2. **Error handling** – network/HTTP issues raise a descriptive
   :class:`RuntimeError`.  Callers can catch and decide how to proceed.
3. **Type hints** & docstrings are provided throughout (NumPy style), honouring
   the project's contribution guidelines.
"""

from __future__ import annotations

from dataclasses import dataclass
from json import loads as _json_loads
from time import sleep, time
from typing import Any, Dict, List, Optional
import urllib.error
import urllib.parse
import urllib.request

__all__ = [
    "EnsemblRestClient",
    "symbol_lookup",
    "get_variants",
]


@dataclass(slots=True)
class EnsemblRestClient:
    """Minimal client for the Ensembl REST API.

    Parameters
    ----------
    server : str, default 'https://rest.ensembl.org'
        Base URL of the REST service.
    reqs_per_sec : int, default 15
        Maximal number of requests per second to *self-throttle* at.  The
        public Ensembl instance limits anonymous traffic to ~15 req/s – we
        replicate that client-side so we stay a good citizen.
    """

    server: str = "https://rest.ensembl.org"
    reqs_per_sec: int = 15

    # Internal bookkeeping for naive self-throttling – *not* thread-safe.
    _req_counter: int = 0
    _last_reset: float = time()

    # ---------------------------------------------------------------------
    # Low-level helpers
    # ---------------------------------------------------------------------
    def _rate_limit(self) -> None:
        """Sleep if we have exceeded ``reqs_per_sec`` during the last second."""
        now = time()
        if now - self._last_reset >= 1.0:
            # Reset the window.
            self._req_counter = 0
            self._last_reset = now
            return

        if self._req_counter >= self.reqs_per_sec:
            sleep(1.0 - (now - self._last_reset))
            # Window reset happens after sleep.
            self._req_counter = 0
            self._last_reset = time()

    # ------------------------------------------------------------------
    # Public methods that correspond to useful high-level operations
    # ------------------------------------------------------------------
    def perform_request(
        self,
        endpoint: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        content_type: str = "application/json",
    ) -> Any:
        """Issue *one* HTTP GET request and return the decoded JSON.

        Parameters
        ----------
        endpoint : str
            The *path* part of the URL **including the leading ``/``**.
        params : dict of str to Any, optional
            Query parameters to append.
        content_type : str
            Desired ``Content-Type`` for the response.  The default of JSON is
            what almost every Ensembl endpoint supports.

        Returns
        -------
        Any
            Parsed JSON – either ``dict`` or ``list`` depending on endpoint.

        Raises
        ------
        RuntimeError
            If the request fails (network error or non-200 response other than
            a 429 that we handled internally).
        """
        self._rate_limit()

        if params:
            endpoint = f"{endpoint}?{urllib.parse.urlencode(params)}"

        url = f"{self.server}{endpoint}"
        headers = {"Content-Type": content_type}

        request = urllib.request.Request(url, headers=headers)
        try:
            response = urllib.request.urlopen(request)  # nosec B310 – public URL
            raw: bytes = response.read()
            self._req_counter += 1
            return _json_loads(raw.decode()) if raw else None
        except urllib.error.HTTPError as e:
            if e.code == 429:  # Too Many Requests – honour server back-off
                retry_after = float(e.headers.get("Retry-After", 1.0))
                sleep(retry_after)
                return self.perform_request(endpoint)  # recurse *once*
            elif e.code in (400, 404):  # Bad Request or Not Found - return None gracefully
                return None
            raise RuntimeError(
                f"Ensembl REST request failed – status {e.code}: {e.reason}"
            ) from e
        except urllib.error.URLError as e:  # network / DNS issues
            raise RuntimeError(f"Network error while contacting Ensembl REST: {e}") from e

    # ------------------------------------------------------------------
    # High-level convenience wrappers
    # ------------------------------------------------------------------
    def symbol_lookup(self, species: str, symbol: str) -> Optional[Dict[str, Any]]:
        """Resolve *symbol* to an Ensembl record.

        Parameters
        ----------
        species : str
            Organism, e.g. ``"human"`` or ``"homo_sapiens"``.
        symbol : str
            Gene symbol, e.g. ``"TP53"``.

        Returns
        -------
        dict or None
            Parsed JSON for the **first** hit or ``None`` when no record is
            found.
        """
        records: List[Dict[str, Any]] = self.perform_request(
            f"/xrefs/symbol/{species}/{symbol}", params={"object_type": "gene"}
        )
        return records[0] if records else None

    def get_variants(self, species: str, symbol: str) -> Optional[List[Dict[str, Any]]]:
        """Return all variants overlapping *symbol*.

        The helper first resolves the symbol to its stable ID and then queries
        the *overlap* endpoint.

        Parameters
        ----------
        species : str
            Organism name or alias accepted by Ensembl.
        symbol : str
            HGNC/Ensembl gene symbol.

        Returns
        -------
        list of dict or None
            Variants as returned by Ensembl, or ``None`` if the symbol could
            not be resolved.
        """
        gene = self.symbol_lookup(species, symbol)
        if not gene:
            return None

        stable_id = gene["id"]
        variants: List[Dict[str, Any]] = self.perform_request(
            f"/overlap/id/{stable_id}", params={"feature": "variation"}
        )
        return variants or []


# ---------------------------------------------------------------------------
# Module-level convenience functions (public API)
# ---------------------------------------------------------------------------

def symbol_lookup(species: str, symbol: str) -> Optional[Dict[str, Any]]:
    """Functional interface for :meth:`EnsemblRestClient.symbol_lookup`."""
    return EnsemblRestClient().symbol_lookup(species, symbol)


def get_variants(species: str, symbol: str) -> Optional[List[Dict[str, Any]]]:
    """Functional interface for :meth:`EnsemblRestClient.get_variants`."""
    return EnsemblRestClient().get_variants(species, symbol)