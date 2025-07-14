from __future__ import annotations

from functools import partial
from pathlib import Path

from agents import function_tool

from src.tools.file_ops import (
    delete_file,
    list_dir,
    read_file,
    update_file,
    write_file,
)
from src.tools.move_file import move_file as move_file_impl
from src.tools.shell import shell_command as shell_command_impl
from src.tools.pdb_query import (
    pdb_search,
    pdb_get_info,
    pdb_sequence_search,
    pdb_structure_search,
    PDBQueryError,
)


def get_session_tools(session_dir: str | Path) -> list:
    """
    Creates a suite of tools that are bound to a specific session directory.

    This approach avoids redefining tools within agent creation functions and
    provides a centralized, reusable way to create session-specific tools.

    Parameters
    ----------
    session_dir : str or Path
        The directory path for the session.

    Returns
    -------
    list
        A list of session-bound tools.
    """
    session_dir = str(session_dir)

    @function_tool
    def shell_command(command: str, max_length: int = 20_000) -> str:
        """Execute a shell command in a specific directory and return the output.

        Parameters
        ----------
        command : str
            The command to execute.
        max_length : int, optional
            The maximum length of the command output to return. Default is 20,000 characters.

        Returns
        -------
        str
            The stdout and stderr of the command, truncated if necessary.
        """
        return shell_command_impl(command, sandbox_dir=session_dir, max_length=max_length)

    @function_tool
    def move_file(source_path: str, destination_path: str) -> str:
        """Move or copy a file to a destination relative to the session directory.

        If the source is outside the session directory, it will be copied.
        If the source is inside the session directory, it will be moved.

        Parameters
        ----------
        source_path : str
            The path to the source file.
        destination_path : str
            The destination path, relative to the session directory.

        Returns
        -------
        str
            Success or error message.
        """
        return move_file_impl(source_path, destination_path, session_dir)

    # Partial application for file operations to bind them to the session dir
    # We assume file operations are relative to the session directory
    
    @function_tool
    def session_read_file(path: str, max_length: int = 20_000) -> str:
        """
        Read the content of a file from the session directory.

        Parameters
        ----------
        path : str
            The path to the file, relative to the session directory.
        max_length : int, optional
            The maximum length of the file content to read. Default is 20,000 characters.

        Returns
        -------
        str
            The content of the file or an error message.
        """
        return read_file(str(Path(session_dir) / path), max_length)

    @function_tool
    def session_write_file(path: str, content: str) -> str:
        """
        Write content to a file in the session directory. This will create the file if it doesn't exist,
        and overwrite it if it does.

        Parameters
        ----------
        path : str
            The path to the file, relative to the session directory.
        content : str
            The content to write to the file.

        Returns
        -------
        str
            A success or error message.
        """
        return write_file(str(Path(session_dir) / path), content)

    @function_tool
    def session_update_file(path: str, find: str, replace: str) -> str:
        """
        Find and replace content in a file in the session directory.

        Parameters
        ----------
        path : str
            The path to the file, relative to the session directory.
        find : str
            The content to find.
        replace : str
            The content to replace with.

        Returns
        -------
        str
            A success or error message.
        """
        return update_file(str(Path(session_dir) / path), find, replace)

    @function_tool
    def session_list_dir(path: str = ".", max_length: int = 20_000) -> str:
        """
        List the contents of a directory in the session recursively, ignoring hidden files/folders.

        Parameters
        ----------
        path : str
            The path to the directory or file, relative to the session directory.
        max_length : int, optional
            The maximum length of the directory listing to return. Default is 20,000 characters.

        Returns
        -------
        str
            A structured string of the directory contents or the file name, truncated if necessary.
        """
        return list_dir(str(Path(session_dir) / path), max_length)

    @function_tool
    def session_delete_file(path: str) -> str:
        """
        Delete a file in the session directory.

        Parameters
        ----------
        path : str
            The path to the file to delete, relative to the session directory.

        Returns
        -------
        str
            A success or error message.
        """
        return delete_file(str(Path(session_dir) / path))

    @function_tool
    def ensembl_get_variants(species: str, symbol: str) -> str:
        """Fetch variants overlapping *symbol* for *species* via Ensembl REST.

        Parameters
        ----------
        species : str
            Organism accepted by Ensembl, for example ``"human"`` or ``"mus_musculus"``.
        symbol : str
            Gene symbol (HGNC/common name).

        Returns
        -------
        str
            JSON-encoded list of variant feature objects or an error message.
        """
        import json
        from src.tools.ensembl_rest_client import get_variants

        try:
            variants = get_variants(species, symbol)
            return json.dumps(variants) if variants is not None else "[]"
        except Exception as exc:  # noqa: BLE001 – we want to surface any issue
            return f"Error querying Ensembl REST API: {exc}"

    @function_tool
    def ensembl_symbol_lookup(species: str, symbol: str) -> str:
        """Resolve a gene symbol to its Ensembl record via Ensembl REST.

        Parameters
        ----------
        species : str
            Organism accepted by Ensembl, for example ``"human"`` or ``"mus_musculus"``.
        symbol : str
            Gene symbol (HGNC/common name).

        Returns
        -------
        str
            JSON-encoded Ensembl record or an error message.
        """
        import json
        from src.tools.ensembl_rest_client import symbol_lookup

        try:
            record = symbol_lookup(species, symbol)
            return json.dumps(record) if record is not None else "null"
        except Exception as exc:  # noqa: BLE001 – we want to surface any issue
            return f"Error querying Ensembl REST API: {exc}"

    @function_tool
    def pubchem_search_compounds_by_name(name: str, max_results: int = 10) -> str:
        """Search for chemical compounds by name using PubChem.

        Parameters
        ----------
        name : str
            Chemical name to search for.
        max_results : int, optional
            Maximum number of results to return (default: 10).

        Returns
        -------
        str
            JSON-encoded list of compound information or an error message.
        """
        import json
        from src.tools.pubchem_client import search_compounds_by_name

        try:
            compounds = search_compounds_by_name(name, max_results)
            return json.dumps(compounds)
        except Exception as exc:  # noqa: BLE001 – we want to surface any issue
            return f"Error querying PubChem API: {exc}"

    @function_tool
    def pubchem_get_compound_details(cid: int) -> str:
        """Get detailed information about a compound using PubChem.

        Parameters
        ----------
        cid : int
            PubChem Compound ID.

        Returns
        -------
        str
            JSON-encoded compound information or an error message.
        """
        import json
        from src.tools.pubchem_client import get_compound_details

        try:
            details = get_compound_details(cid)
            return json.dumps(details) if details is not None else "null"
        except Exception as exc:  # noqa: BLE001 – we want to surface any issue
            return f"Error querying PubChem API: {exc}"

    @function_tool
    def pubchem_get_compound_literature(cid: int) -> str:
        """Get literature information for a compound using PubChem.

        Parameters
        ----------
        cid : int
            PubChem Compound ID.

        Returns
        -------
        str
            JSON-encoded literature information including PMIDs or an error message.
        """
        import json
        from src.tools.pubchem_client import get_compound_literature

        try:
            literature = get_compound_literature(cid)
            return json.dumps(literature)
        except Exception as exc:  # noqa: BLE001 – we want to surface any issue
            return f"Error querying PubChem API: {exc}"

    @function_tool
    def pubchem_search_compounds_by_topic(topic: str, max_compounds: int = 20) -> str:
        """Search for compounds related to a topic using PubChem.

        Parameters
        ----------
        topic : str
            Topic or keyword to search for (e.g., "diabetes", "cancer", "antimicrobial").
        max_compounds : int, optional
            Maximum number of compounds to return (default: 20).

        Returns
        -------
        str
            JSON-encoded list of compound information with literature associations or an error message.
        """
        import json
        from src.tools.pubchem_client import search_compounds_by_topic

        try:
            compounds = search_compounds_by_topic(topic, max_compounds)
            return json.dumps(compounds)
        except Exception as exc:  # noqa: BLE001 – we want to surface any issue
            return f"Error querying PubChem API: {exc}"

    @function_tool
    def pubchem_get_paper_content(pmid: int) -> str:
        """Get full paper content including abstract and full-text when available for a PMID.

        Parameters
        ----------
        pmid : int
            PubMed ID for the paper.

        Returns
        -------
        str
            JSON-encoded dictionary containing paper content including abstract, full-text,
            title, authors, journal, and other metadata.
        """
        import json
        from src.tools.pubchem_client import get_paper_content

        try:
            paper_content = get_paper_content(pmid)
            return json.dumps(paper_content) if paper_content else "null"
        except Exception as exc:  # noqa: BLE001 – we want to surface any issue
            return f"Error getting paper content for PMID {pmid}: {exc}"

    @function_tool
    def pubchem_get_papers_content(pmids: str) -> str:
        """Get full paper content for multiple PMIDs.

        Parameters
        ----------
        pmids : str
            Comma-separated list of PubMed IDs for the papers.

        Returns
        -------
        str
            JSON-encoded list of dictionaries containing paper content for each PMID.
        """
        import json
        from src.tools.pubchem_client import get_papers_content

        try:
            # Parse comma-separated PMIDs
            pmid_list = [int(pmid.strip()) for pmid in pmids.split(",") if pmid.strip().isdigit()]
            if not pmid_list:
                return "[]"
            
            papers_content = get_papers_content(pmid_list)
            return json.dumps(papers_content) if papers_content else "[]"
        except Exception as exc:  # noqa: BLE001 – we want to surface any issue
            return f"Error getting papers content for PMIDs '{pmids}': {exc}"

    @function_tool
    def pubmed_search_papers(query: str, max_results: int = 20, 
                           include_full_text: bool = False) -> str:
        """
        Search PubMed for papers related to a specific topic and return detailed results.
        
        This tool allows you to search the PubMed database for scientific papers using
        keywords, author names, MeSH terms, and other search criteria. It returns
        structured information including titles, authors, abstracts, publication details,
        and optionally full text from PMC when available.
        
        Parameters
        ----------
        query : str
            Search query. You can use simple keywords, phrases, or advanced PubMed
            search syntax. Examples:
            - "CRISPR gene editing" - simple keyword search
            - "Smith[Author]" - search by author
            - "cancer[MeSH]" - search using MeSH terms
            - "Nature[journal]" - search in specific journal
            - "2023[pdat]" - search by publication date
            - "clinical trial[pt]" - search by publication type
            You can combine terms with AND, OR, NOT operators.
        max_results : int, optional
            Maximum number of papers to return (default: 20, maximum: 100).
            Use smaller numbers for faster results.
        include_full_text : bool, optional
            Whether to attempt to retrieve enhanced full text from multiple sources
            including PMC (structured content), arXiv (preprints), and publisher
            links via Crossref (default: False). Note: This significantly increases
            processing time. Returns structured sections, figures/tables, and
            references when available.
        
        Returns
        -------
        str
            JSON string containing search results with the following structure:
            {
                "query": "original search query",
                "total_results": number of papers found,
                "papers": [
                    {
                        "pmid": "PubMed ID",
                        "title": "Paper title",
                        "authors": ["Author1", "Author2"],
                        "abstract": "Abstract text",
                        "journal": "Journal name",
                        "publication_date": "YYYY-MM-DD",
                        "doi": "DOI identifier",
                        "pmcid": "PMC identifier (if available)",
                        "keywords": ["MeSH term1", "MeSH term2"],
                        "affiliations": ["Institution1", "Institution2"],
                        "full_text": "Full text content (if requested and available)"
                    }
                ]
            }
        
        Examples
        --------
        Basic search:
        >>> pubmed_search_papers("machine learning bioinformatics", max_results=5)
        
        Author search:
        >>> pubmed_search_papers("Doudna[Author] CRISPR", max_results=10)
        
        MeSH term search:
        >>> pubmed_search_papers("Neoplasms[MeSH] immunotherapy", max_results=5)
        
        Recent papers:
        >>> pubmed_search_papers("COVID-19 vaccine 2023[pdat]", max_results=5)
        
        With full text:
        >>> pubmed_search_papers("open access genomics", max_results=3, include_full_text=True)
        """
        from src.tools.pubmed_search import search_pubmed_papers
        return search_pubmed_papers(query, max_results, include_full_text)
    
    @function_tool
    def pubmed_get_paper_details(pmid: str, include_full_text: bool = False) -> str:
        """
        Retrieve detailed information for a specific PubMed paper using its PMID.
        
        This tool retrieves comprehensive information about a scientific paper
        when you know its PubMed ID (PMID). It's useful for getting details
        about specific papers referenced in other work or found through searches.
        
        Parameters
        ----------
        pmid : str
            PubMed identifier (PMID) of the paper. This is a unique numerical
            identifier assigned to each paper in PubMed. Examples: "12345678",
            "32580960". You can find PMIDs in citations or PubMed URLs.
        include_full_text : bool, optional
            Whether to attempt to retrieve enhanced full text from multiple sources
            including PMC (structured content), arXiv (preprints), and publisher
            links via Crossref (default: False). Returns structured sections,
            figures/tables, and references when available.
        
        Returns
        -------
        str
            JSON string containing paper details with the following structure:
            {
                "pmid": "PubMed ID",
                "title": "Paper title",
                "authors": ["Author1", "Author2"],
                "abstract": "Abstract text",
                "journal": "Journal name",
                "publication_date": "YYYY-MM-DD",
                "doi": "DOI identifier",
                "pmcid": "PMC identifier (if available)",
                "keywords": ["MeSH term1", "MeSH term2"],
                "affiliations": ["Institution1", "Institution2"],
                "full_text": "Full text content (if requested and available)"
            }
        
        Examples
        --------
        Basic paper retrieval:
        >>> pubmed_get_paper_details("32580960")
        
        With full text:
        >>> pubmed_get_paper_details("32580960", include_full_text=True)
        """
        from src.tools.pubmed_search import get_pubmed_paper_details
        return get_pubmed_paper_details(pmid, include_full_text)

    @function_tool
    def pdb_search_tool(
        query_text: str = None,
        pdb_id: str = None,
        organism: str = None,
        method: str = None,
        resolution_min: float = None,
        resolution_max: float = None,
        date_from: str = None,
        date_to: str = None,
        limit: int = 100
    ) -> str:
        """Search the PDB database with various criteria.

        Parameters
        ----------
        query_text : str, optional
            Free text search query
        pdb_id : str, optional
            Specific PDB ID to search for
        organism : str, optional
            Source organism name (e.g., "Homo sapiens")
        method : str, optional
            Experimental method (e.g., "X-RAY DIFFRACTION", "NMR", "ELECTRON MICROSCOPY")
        resolution_min : float, optional
            Minimum resolution in Angstroms
        resolution_max : float, optional
            Maximum resolution in Angstroms
        date_from : str, optional
            Start date in YYYY-MM-DD format
        date_to : str, optional
            End date in YYYY-MM-DD format
        limit : int, default 100
            Maximum number of results to return

        Returns
        -------
        str
            JSON-encoded search results or an error message.
        """
        import json
        try:
            results = pdb_search(
                query_text=query_text,
                pdb_id=pdb_id,
                organism=organism,
                method=method,
                resolution_min=resolution_min,
                resolution_max=resolution_max,
                date_from=date_from,
                date_to=date_to,
                limit=limit
            )
            return json.dumps(results)
        except Exception as exc:  # noqa: BLE001
            return f"Error searching PDB: {exc}"

    @function_tool
    def pdb_get_info_tool(pdb_id: str) -> str:
        """Get detailed information for a specific PDB entry.

        Parameters
        ----------
        pdb_id : str
            PDB identifier (e.g., "1ABC")

        Returns
        -------
        str
            JSON-encoded entry information or an error message.
        """
        import json
        try:
            result = pdb_get_info(pdb_id)
            return json.dumps(result) if result is not None else "null"
        except Exception as exc:  # noqa: BLE001
            return f"Error getting PDB info: {exc}"

    @function_tool
    def pdb_sequence_search_tool(
        sequence: str,
        sequence_type: str = "protein",
        e_value_cutoff: float = 0.001,
        identity_cutoff: float = 0.9,
        limit: int = 100
    ) -> str:
        """Perform sequence similarity search against the PDB.

        Parameters
        ----------
        sequence : str
            Query sequence (amino acid or nucleotide)
        sequence_type : str, default "protein"
            Type of sequence ("protein", "dna", or "rna")
        e_value_cutoff : float, default 0.001
            E-value cutoff for matches
        identity_cutoff : float, default 0.9
            Identity cutoff for matches (0.0 to 1.0)
        limit : int, default 100
            Maximum number of results

        Returns
        -------
        str
            JSON-encoded search results or an error message.
        """
        import json
        try:
            results = pdb_sequence_search(
                sequence=sequence,
                sequence_type=sequence_type,
                e_value_cutoff=e_value_cutoff,
                identity_cutoff=identity_cutoff,
                limit=limit
            )
            return json.dumps(results)
        except Exception as exc:  # noqa: BLE001
            return f"Error performing sequence search: {exc}"

    @function_tool
    def pdb_structure_search_tool(
        pdb_id: str,
        assembly_id: str = "1",
        operator: str = "strict_shape_match",
        limit: int = 100
    ) -> str:
        """Perform structure similarity search against the PDB.

        Parameters
        ----------
        pdb_id : str
            Query PDB ID
        assembly_id : str, default "1"
            Assembly ID to use for comparison
        operator : str, default "strict_shape_match"
            Comparison operator ("strict_shape_match" or "relaxed_shape_match")
        limit : int, default 100
            Maximum number of results

        Returns
        -------
        str
            JSON-encoded search results or an error message.
        """
        import json
        try:
            results = pdb_structure_search(
                pdb_id=pdb_id,
                assembly_id=assembly_id,
                operator=operator,
                limit=limit
            )
            return json.dumps(results)
        except Exception as exc:  # noqa: BLE001
            return f"Error performing structure search: {exc}"

    return [
        shell_command,
        move_file,
        session_read_file,
        session_write_file,
        session_update_file,
        session_list_dir,
        session_delete_file,
        ensembl_get_variants,
        ensembl_symbol_lookup,
        pubchem_search_compounds_by_name,
        pubchem_get_compound_details,
        pubchem_get_compound_literature,
        pubchem_search_compounds_by_topic,
        pubchem_get_paper_content,
        pubchem_get_papers_content,
        pubmed_search_papers,
        pubmed_get_paper_details,
        pdb_search_tool,
        pdb_get_info_tool,
        pdb_sequence_search_tool,
        pdb_structure_search_tool,
    ] 