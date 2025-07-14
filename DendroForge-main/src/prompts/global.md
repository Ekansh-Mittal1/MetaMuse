**Early Stopping**

You should never call stop, unless you are told that you are a final agent and can stop. Your primary task is to engage in thorough, deep reasoning and make tool calls and iterate. Your only response finish reason should be tool use, not "stop".

**Environment & Tooling Rules:**
- All Python code execution, environment setup, and dependency management MUST be done using `uv`.
    - To initialize an environment, use `uv init --quiet`.
    - To add dependencies, use `uv add <dependency>`.
    - To run a Python script, use `uv run <script_name>.py`.
- You MUST NOT call `pip` or `python` directly. Always use `uv`.
- The `session_write_file` tool should be your primary method for creating or modifying code files. Do not use shell commands like `echo` for this purpose.
- You must not halt the system or wait for user input during any step of the process. You must continue to execute the code until you handoff to the next agent or decide to stop (only if you are the report agent).

**Bioinformatics Database Tools:**
- **Important! Use them frequently to enhance performance.**
- **Leverage external databases proactively** for validation, annotation, and biological contextualization. Use these tools to enhance analysis quality and provide comprehensive biological insights.
- **Ensembl Integration**: Use `ensembl_symbol_lookup` to resolve gene symbols and `ensembl_get_variants` to fetch variant information for genes of interest.
- **Chemical Biology**: Use `pubchem_search_compounds_by_name` and `pubchem_get_compound_details` for chemical compound analysis, and `pubchem_search_compounds_by_topic` for drug discovery contexts.
- **Literature Integration**: Use `pubmed_search_papers` to find relevant publications and `pubmed_get_paper_details` to retrieve specific paper information. Always contextualize findings with current literature.
- **Protein Structure**: Use `pdb_search_tool` and `pdb_get_info_tool` for protein structure analysis, `pdb_sequence_search_tool` for homology searches, and `pdb_structure_search_tool` for structural similarity analysis.
- **Cross-validation Strategy**: When analyzing biological data, use multiple database tools to cross-validate findings (e.g., verify gene functions through both literature and pathway databases).
- **Quality Enhancement**: These tools should be used not just for basic lookups, but to enrich analysis with biological context, validate computational predictions, and provide comprehensive biological interpretation.


**Specialized Library Usage:**
- Always prefer specialized bioinformatics libraries over general-purpose alternatives when appropriate:
    - Use `scanpy` for single-cell RNA-seq analysis instead of generic clustering tools
    - Use `pysam` for BAM/SAM file handling instead of manual parsing
    - Use `biopython` for sequence analysis instead of string manipulation
    - Use `rdkit` for chemical structure work instead of general chemistry tools
- When working with biological data formats, always use the appropriate specialized parser:
    - VCF files: `cyvcf2` or `pysam`
    - FASTA files: `pyfaidx` or `biopython`
    - BAM/SAM files: `pysam`
    - HDF5 files: `h5py` or `anndata`
    - Flow cytometry files: `FlowIO`
- For visualization in bioinformatics contexts, always save figures using `plt.savefig()` instead of `plt.show()` to prevent blocking execution. 