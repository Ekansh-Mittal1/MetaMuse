# DendroForge System Design Document

## Document Information
- **Version**: Alpha
- **Date**: July 2024
- **Author**: Orion Li
- **Status**: Draft

---

# Context

## Objective

DendroForge is an autonomous agentic system designed to perform end-to-end biological data analysis without human intervention. The system takes research plans from QuantaQuill (a paper-writing agent) and raw user inputs, then autonomously discovers data, plans analysis workflows, executes code, generates visualizations, and produces comprehensive reports for downstream paper writing.

## Background

Traditional biological data analysis requires significant manual intervention, domain expertise, and iterative refinement. Researchers must manually:
- Discover and curate relevant datasets
- Design appropriate analysis pipelines
- Write and debug analysis code
- Generate and refine visualizations
- Interpret results and write reports

DendroForge addresses these challenges by creating a fully autonomous system that leverages large language models (LLMs) with multiple agents and specialized tools to perform the entire analysis pipeline. The system is designed with the principle that "a single LLM call is limited, but multiple agents aren't," emphasizing multi-pass refinement and autonomous iteration.

The system integrates with QuantaQuill, a research paper writing agent, to form a complete research-to-publication pipeline for biological research.

---

# Design

## Overview

DendroForge operates as a multi-phase agentic system that autonomously processes biological data through discovery, planning, execution, inspection, and summarization phases. The system uses Google Gemini-2.5-pro as its core reasoning engine and employs a comprehensive toolkit for code generation, execution, and data visualization.

## Functional Requirements

### Primary Use Cases
1. **Autonomous Biological Data Analysis**: Process complex biological datasets (h5ad, CSV, FASTQ, etc.) without human intervention
2. **Multi-modal Output Generation**: Produce executable code, publication-ready visualizations, and comprehensive reports
3. **Integration with Research Workflow**: Seamlessly integrate with QuantaQuill for paper writing and publication
4. **Iterative Refinement**: Automatically detect and correct analysis issues through visual inspection
5. **Domain-specific Flexibility**: Handle various biological domains (genomics, proteomics, metabolomics, etc.) through numerous sub-agents and tools that cater to specific modalities.

### System Boundaries
- **Input**: Raw biological data files, research questions, QuantaQuill-generated analysis plans
- **Output**: Executable Python code, publication-ready figures, analysis reports, structured data for paper writing
- **External Dependencies**: GCP Gemini API, biological databases, computational environments

### Input APIs (tentative)

#### Primary Input Interface
```json
POST /api/v1/analysis/submit
{
  "user_query": "string",
  "quantaquill_plan": {
    "research_objectives": ["string"],
    "suggested_methods": ["string"],
    "data_requirements": ["string"]
  },
  "data_files": [
    {
      "filename": "string",
      "format": "h5ad|csv|fastq|bam|vcf",
      "url": "string",
      "metadata": {}
    }
  ],
  "constraints": {
    "max_execution_time": "integer (minutes)",
  }
}
```

#### Real-time Status Interface
```
GET /api/v1/analysis/{session_id}/status
WebSocket /ws/analysis/{session_id}
```

### Output APIs

#### Analysis Results Interface
```json
GET /api/v1/analysis/{session_id}/results
{
  "status": "completed|running|failed",
  "execution_phases": [
    {
      "phase": "discovery|planning|execution|inspection|summary",
      "status": "completed|running|failed",
      "outputs": {},
      "duration": "integer (seconds)"
    }
  ],
  "visualizations": [
    {
      "figure_id": "string",
      "title": "string",
      "file_path": "string",
      "description": "string",
      "quality_score": "float"
    }
  ],
  "report": {
    "executive_summary": "string",
    "methodology": "string",
    "results": "string",
    "conclusions": "string",
    "quantaquill_payload": {}
  },
  "code": {
    "file_path": "string,
  }
}
```

---

# Detailed Design

## System Architecture

### Multi-Phase Processing Pipeline

DendroForge implements a six-phase processing pipeline, each designed for autonomous operation with built-in error handling and quality assessment. Each phase ideally represents a separate agent with its own system prompts that can look at the chat history from the previous agents.

#### Phase 1: Data Discovery
- **Objective**: Autonomous identification and cataloging of relevant data sources
- **Tools**: `database_scanner_tool`, `data_loader_tool`
- **LLM Role**: Interpretation of user requirements, data relevance assessment
- **Outputs**: Curated dataset inventory, assess compatibility, and decide what additional files to request

#### Phase 2: Additional Files Collection  
- **Objective**: Intelligent gathering of supplementary data and reference materials
- **Tools**: `data_loader_tool`
- **LLM Role**: Gap analysis, contextual data requirements identification
- **Outputs**: Extended dataset collection, reference annotations, data provenance

#### Phase 3: Analysis Planning
- **Objective**: Autonomous design of comprehensive analysis workflows
- **LLM Role**: Scientific reasoning, methodology selection, workflow optimization
- **Outputs**: Detailed execution plan, resource allocation strategy, success criteria

#### Phase 4: Code Execution (multiple self-reflective runs based on planning)
- **Objective**: Autonomous implementation and execution of analysis code
- **Tools**: Core execution toolkit (detailed below)
- **LLM Role**: Code generation, debugging, optimization
- **Outputs**: Executable code, intermediate results, performance metrics

#### Phase 5: Visual Inspection (multiple runs until no detected bugs)
- **Objective**: Autonomous quality assessment and iterative improvement
- **Sub-Agents**: `plot_debugger_agent`
- **LLM Role**: Visual interpretation, quality judgment, improvement suggestions
- **Outputs**: Refined visualizations

#### Phase 6: Summary & Report Generation
- **Objective**: Comprehensive documentation and knowledge synthesis
- **Sub-Agents**: `report_generator_agent`
- **LLM Role**: Scientific writing, result interpretation, conclusion generation
- **Outputs**: Analysis reports, QuantaQuill integration payload

### Comprehensive Tool Ecosystem

#### Core Execution Tools
- **`create_file_tool`**: Autonomous file creation with template management
- **`create_code_tool`**: Intelligent code generation with best practices enforcement  
- **`edit_code_tool`**: Context-aware code modification via git-diff-style edits (instead of whole-file edits)
- **`terminal_tool`**: Secure command execution with sandbox isolation
- **`env_tools`**: Environment management and dependency resolution
- **`uv_tools`**: Modern Python package management integration
- more?

#### Data Tools
- **`database_scanner_tool`**: Identifies useful data from the data lake
- **`data_loader_tool`**: Multi-format data structure ingestion (h5ad, CSV, FASTQ, BAM, VCF, etc.)
- more?

#### Bioinformatics Tools (tentative)
- **`blast_tool`**: Sequence similarity search and alignment analysis
- **`ensembl_tool`**: Genome annotation and variant data retrieval
- **`pubmed_tool`**: Literature search and citation management
- **`uniprot_tool`**: Protein sequence and annotation database access
- **`kegg_tool`**: Pathway and metabolic network analysis
- **`sra_tool`**: Sequence Read Archive data retrieval and processing
- **`pdb_tool`**: Protein structure database access and analysis
- **`geo_tool`**: Gene Expression Omnibus dataset retrieval
- **`string_tool`**: Protein-protein interaction network analysis
- **`reactome_tool`**: Pathway database and biological process analysis
- **`mirbase_tool`**: MicroRNA sequence and annotation database
- **`pfam_tool`**: Protein family and domain analysis
- **`interpro_tool`**: Protein classification and functional analysis
- **`biomart_tool`**: Biological data warehouse query interface
- **`ena_tool`**: European Nucleotide Archive sequence data access

### LLM Integration Strategy

#### Google Gemini-2.5-pro Integration
- **Primary Reasoning Engine**: All high-level decision making and planning
- **Code Generation**: Context-aware Python code generation with biological domain knowledge
- **Natural Language Processing**: User query interpretation and requirement extraction
- **Scientific Reasoning**: Methodology selection and result interpretation
- **Quality Assessment**: Visual and analytical output evaluation

#### Multi-Pass Refinement Architecture
The system implements iterative refinement where each LLM call builds upon previous results, with automated quality checks triggering additional refinement passes when needed.

#### Prompt Engineering Framework
- **System Prompts**: Domain-specific biological knowledge and best practices
- **Context Management**: Efficient context window utilization for long analyses
- **Error Handling**: Intelligent error interpretation and recovery strategies
- **Domain Adaptation**: Specialized prompts for different biological domains

## Dependencies

### External Service Dependencies
- **Google Cloud Platform (GCP)**: Core infrastructure and Gemini API
- **Biological Databases**: NCBI, ENSEMBL, UniProt, KEGG for reference data
- **Container Orchestration**: Kubernetes for scalable compute resources
- **Object Storage**: Cloud storage for large dataset management
- **Message Queuing**: Redis/RabbitMQ for asynchronous task processing

### Fallback Strategies
- **LLM API Failover**: Multiple LLM providers (OpenAI, Anthropic) as backup
- **Compute Resource Elasticity**: Auto-scaling compute resources based on demand
- **Data Replication**: Multi-region data replication for availability
- **Graceful Degradation**: Reduced functionality during partial service outages

### Circular Dependency Prevention
- **Service Mesh Architecture**: Clear service boundaries and communication patterns
- **Dependency Injection**: Configurable service dependencies
- **Circuit Breaker Pattern**: Protection against cascading failures

## Technical Debt Considerations

### Known Technical Debt
- **Legacy Data Format Support**: Maintaining compatibility with older biological data formats
- **Manual Configuration**: Some specialized analysis pipelines require manual parameter tuning
- **Model Drift**: LLM performance degradation over time requiring updates to the system prompts

### Mitigation Strategies
- **Automated Testing**: Comprehensive test suites for all analysis pipelines
- **Documentation**: Detailed documentation of workarounds and limitations
- **Regular Updates**: Scheduled updates for dependencies and models
- **Performance Monitoring**: Continuous monitoring of system performance and accuracy

---

# Benchmark Specifications

## Primary Benchmark: Age Analysis Pipeline

### Input Specification
```
Input Files:
- age.h5ad: Bulk RNA sequencing data with age-related metadata from GEO
- Format: AnnData HDF5 format
- Size: ~1-10GB typical
- Sample Count: 10,000-100,000 samples
- Gene Count: 20,000-50,000 genes

User Prompt Examples:
- "Analyze age-related changes in gene expression"
- "Identify aging biomarkers in this dataset"
```

### Expected Outputs

#### 1. Generated Code
- **Data Loading**: Robust h5ad file loading with error handling
- **Quality Control**: Sample and gene filtering based on standard metrics  
- **Preprocessing**: Normalization, scaling, highly variable gene selection
- **Analysis**: Differential expression, pathway analysis, clustering
- **Visualization**: Publication-ready plots with proper annotations

#### 2. Generated Images
- **QC Plots**: Violin plots, scatter plots for quality metrics
- **Expression Plots**: Heatmaps, volcano plots, gene expression distributions
- **Clustering Visualizations**: UMAP/t-SNE plots with age annotations
- **Pathway Analysis**: Enrichment plots, network diagrams
- **Statistical Plots**: Box plots, correlation matrices

#### 3. Analysis Reports
- **Executive Summary**: Key findings and biological significance
- **Methods Section**: Detailed methodology with parameter specifications
- **Results Section**: Quantitative results with statistical significance
- **Discussion**: Biological interpretation and clinical relevance
- **Conclusions**: Summary of findings and future directions

#### 4. QuantaQuill Integration
- **Structured Data**: JSON payload with results for paper writing
- **Figure References**: Properly formatted figure citations
- **Statistical Results**: Tables of differential expression and pathway results
- **Methodology Text**: Ready-to-use methods descriptions

### Success Criteria
- **Accuracy**: >95% alignment with expert-generated analyses
- **Completeness**: All expected output categories generated
- **Quality**: Publication-ready figures and reports
- **Performance**: <10 minutes total execution time
- **Reproducibility**: Consistent results across multiple runs

### Extended Benchmark Suite

#### Genomics Benchmarks
- **Variant Analysis**: VCF file processing and annotation
- **RNA-seq Analysis**: Bulk RNA sequencing differential expression
- **ChIP-seq Analysis**: Peak calling and motif analysis
- **Metagenomics**: Microbiome composition and diversity analysis

#### Proteomics Benchmarks  
- **Mass Spectrometry**: Protein identification and quantification
- **Protein-Protein Interactions**: Network analysis and visualization
- **Structural Analysis**: Protein structure prediction and analysis

#### Multi-omics Benchmarks
- **Integration Analysis**: Multi-omics data integration and correlation
- **Systems Biology**: Pathway and network-based analysis
- **Biomarker Discovery**: Feature selection and validation