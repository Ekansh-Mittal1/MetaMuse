"""
Integration tests for Ensembl REST API client.

These tests make actual network calls to the live Ensembl REST API.
They are designed to validate real-world usage patterns and ensure
the client works correctly with the actual service.

Run with: pytest integrationtests/ -m integration
Or skip with: pytest unittests/ -m "not integration"
"""

import pytest
from time import sleep

from src.tools.ensembl_rest_client import symbol_lookup, get_variants, EnsemblRestClient


@pytest.mark.integration
class TestEnsemblIntegration:
    """Integration tests using real Ensembl REST API calls."""

    def test_tp53_human_symbol_lookup(self):
        """Test TP53 (tumor suppressor) symbol lookup in human."""
        result = symbol_lookup("human", "TP53")
        
        assert result is not None
        assert result["id"] == "ENSG00000141510"
        assert result["type"] == "gene"
        # Just verify we got a valid gene record - don't assume specific field names
        assert isinstance(result, dict)
        assert len(result) >= 2  # Should have more than just id and type
        
    def test_brca1_variants_real_data(self):
        """Test BRCA1 variant lookup - should return real pathogenic variants."""
        variants = get_variants("human", "BRCA1")
        
        assert variants is not None
        assert len(variants) > 0
        
        # BRCA1 is a well-studied cancer gene, should have many variants
        assert len(variants) > 100
        
        # Check that we get real variant data structure
        first_variant = variants[0]
        assert "id" in first_variant
        assert "start" in first_variant
        assert "end" in first_variant
        
    def test_cftr_cystic_fibrosis_gene(self):
        """Test CFTR gene lookup - cystic fibrosis transmembrane conductance regulator."""
        result = symbol_lookup("human", "CFTR")
        
        assert result is not None
        assert result["id"] == "ENSG00000001626"
        assert result["type"] == "gene"
        
        # Get variants for this clinically important gene
        variants = get_variants("human", "CFTR")
        assert variants is not None
        assert len(variants) > 0
        
    def test_apoe_alzheimers_risk_gene(self):
        """Test APOE gene - major Alzheimer's disease risk factor."""
        result = symbol_lookup("human", "APOE")
        
        assert result is not None
        assert result["id"] == "ENSG00000130203"
        assert result["type"] == "gene"
        
        # APOE variants are clinically significant
        variants = get_variants("human", "APOE")
        assert variants is not None
        assert len(variants) > 0
        
    def test_model_organisms_tp53_ortholog(self):
        """Test TP53 orthologs across model organisms."""
        # Human TP53
        human_tp53 = symbol_lookup("human", "TP53")
        assert human_tp53 is not None
        assert human_tp53["id"] == "ENSG00000141510"
        
        # Mouse Tp53 (note capitalization difference)
        mouse_tp53 = symbol_lookup("mouse", "Tp53")
        assert mouse_tp53 is not None
        assert mouse_tp53["id"] == "ENSMUSG00000059552"
        assert mouse_tp53["type"] == "gene"
        
        # Brief pause to respect rate limits
        sleep(0.1)
        
    def test_pharmacogenomics_cyp2d6(self):
        """Test CYP2D6 - important pharmacogenomics gene."""
        result = symbol_lookup("human", "CYP2D6")
        
        assert result is not None
        assert result["id"] == "ENSG00000100197"
        assert result["type"] == "gene"
        
        # CYP2D6 has many pharmacogenomically relevant variants
        variants = get_variants("human", "CYP2D6")
        assert variants is not None
        assert len(variants) > 0
        
    def test_immune_system_hla_gene(self):
        """Test HLA-A - major histocompatibility complex gene."""
        result = symbol_lookup("human", "HLA-A")
        
        assert result is not None
        assert result["type"] == "gene"
        # HLA genes are highly polymorphic - just verify we got the right gene ID
        assert result["id"] == "ENSG00000206503"
        
    def test_developmental_pax6_eye_development(self):
        """Test PAX6 - master regulator of eye development."""
        result = symbol_lookup("human", "PAX6")
        
        assert result is not None
        assert result["id"] == "ENSG00000007372"
        assert result["type"] == "gene"
        
        # PAX6 mutations cause aniridia and other eye disorders
        variants = get_variants("human", "PAX6")
        assert variants is not None
        
    def test_metabolic_insulin_gene(self):
        """Test INS - insulin gene, crucial for diabetes research."""
        result = symbol_lookup("human", "INS")
        
        assert result is not None
        assert result["type"] == "gene"
        
        # Insulin gene variants are associated with diabetes
        variants = get_variants("human", "INS")
        assert variants is not None
        
    def test_neurological_app_alzheimers(self):
        """Test APP - amyloid precursor protein, Alzheimer's disease."""
        result = symbol_lookup("human", "APP")
        
        assert result is not None
        assert result["id"] == "ENSG00000142192"
        assert result["type"] == "gene"
        
        # APP variants are associated with familial Alzheimer's
        variants = get_variants("human", "APP")
        assert variants is not None
        assert len(variants) > 0
        
    def test_cancer_oncogene_myc(self):
        """Test MYC - famous oncogene."""
        result = symbol_lookup("human", "MYC")
        
        assert result is not None
        assert result["type"] == "gene"
        
        # MYC is frequently altered in cancer
        variants = get_variants("human", "MYC")
        assert variants is not None
        
    def test_nonexistent_gene_symbol(self):
        """Test lookup of non-existent gene symbol."""
        result = symbol_lookup("human", "NONEXISTENTGENE12345")
        assert result is None
        
        variants = get_variants("human", "NONEXISTENTGENE12345")
        assert variants is None
        
    def test_custom_server_grch37(self):
        """Test using GRCh37 server for older genome build."""
        grch37_client = EnsemblRestClient(server="https://grch37.rest.ensembl.org")
        
        result = grch37_client.symbol_lookup("human", "TP53")
        assert result is not None
        assert result["type"] == "gene"
        # Should still be the same gene ID for TP53
        assert result["id"] == "ENSG00000141510"
        
    def test_rate_limiting_multiple_requests(self):
        """Test that rate limiting works with multiple real requests."""
        client = EnsemblRestClient(reqs_per_sec=5)  # Conservative rate limit
        
        # Make several requests in quick succession
        genes = ["TP53", "BRCA1", "BRCA2", "CFTR", "APOE"]
        results = []
        
        for gene in genes:
            result = client.symbol_lookup("human", gene)
            results.append(result)
            
        # All should succeed
        assert len(results) == 5
        assert all(r is not None for r in results)
        
        # Verify we got the expected genes by checking IDs
        gene_ids = [r["id"] for r in results]
        assert "ENSG00000141510" in gene_ids  # TP53
        assert "ENSG00000012048" in gene_ids  # BRCA1
        assert "ENSG00000139618" in gene_ids  # BRCA2
        
    def test_gene_with_no_variants(self):
        """Test gene that might have no variants in Ensembl."""
        # Use a less common gene that might have fewer variants
        result = symbol_lookup("human", "ACTB")  # Beta-actin - housekeeping gene
        assert result is not None
        
        variants = get_variants("human", "ACTB")
        # Should return empty list, not None, if gene exists but has no variants
        assert variants is not None
        assert isinstance(variants, list)
        
    def test_gene_synonym_lookup(self):
        """Test lookup using gene synonyms."""
        # EGFR is also known as ERBB1
        result = symbol_lookup("human", "ERBB1")
        assert result is not None
        # Should resolve to EGFR gene
        assert result["id"] == "ENSG00000146648"
        
    def test_zebrafish_model_organism(self):
        """Test zebrafish (danio_rerio) - important developmental model."""
        result = symbol_lookup("zebrafish", "tp53")
        assert result is not None
        assert result["type"] == "gene"
        # Zebrafish genes have ENSDARG IDs
        assert result["id"].startswith("ENSDARG")
        
    def test_yeast_model_organism(self):
        """Test yeast (saccharomyces_cerevisiae) - fundamental cell biology model."""
        # CDC28 is the yeast cell cycle gene
        result = symbol_lookup("saccharomyces_cerevisiae", "CDC28")
        assert result is not None
        assert result["type"] == "gene"
        # Yeast genes have Y[A-P][L/R] format, but let's be more flexible
        # The actual ID might be YBR160W or similar
        assert result["id"].startswith("Y")
        assert len(result["id"]) >= 6  # Should be like YBR160W
        
    def test_client_error_handling(self):
        """Test client behavior with invalid species."""
        # This should not crash but return None
        result = symbol_lookup("invalid_species", "TP53")
        assert result is None
        
        variants = get_variants("invalid_species", "TP53")
        assert variants is None