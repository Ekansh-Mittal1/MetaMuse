import json
from unittest.mock import patch, MagicMock

import pytest

from src.tools.ensembl_rest_client import get_variants, symbol_lookup, EnsemblRestClient


def _mock_response(payload: object, status: int = 200):
    """Utility to craft a mock HTTPResponse-like object for urlopen."""
    mock = MagicMock()
    mock.read.return_value = json.dumps(payload).encode()
    mock.getcode.return_value = status
    return mock


class TestEnsemblRestClient:
    """Tests for the high-level helper functions that wrap the Ensembl REST API."""

    def test_symbol_lookup_success(self):
        """symbol_lookup returns the first record when API succeeds."""
        payload = [{"id": "ENSG00000141510", "type": "gene"}]

        with patch("src.tools.ensembl_rest_client.urllib.request.urlopen", return_value=_mock_response(payload)):
            result = symbol_lookup("human", "TP53")
            assert result == payload[0]

    def test_get_variants_success(self):
        """get_variants resolves symbol and returns variant list."""
        # First call: symbol lookup
        lookup_payload = [{"id": "ENSG00000141510", "type": "gene"}]
        # Second call: overlap/variant endpoint
        variant_payload = [
            {"id": "rs123", "start": 100, "end": 100, "consequence_type": "missense_variant"}
        ]

        with patch(
            "src.tools.ensembl_rest_client.urllib.request.urlopen",
            side_effect=[_mock_response(lookup_payload), _mock_response(variant_payload)],
        ):
            result = get_variants("human", "TP53")
            assert result == variant_payload

    def test_get_variants_symbol_not_found(self):
        """get_variants returns None when symbol cannot be resolved."""
        with patch(
            "src.tools.ensembl_rest_client.urllib.request.urlopen",
            return_value=_mock_response([]),  # symbol_lookup returns empty list
        ):
            result = get_variants("human", "NON_EXISTENT")
            assert result is None

    # ===== Real-world versatility tests =====

    def test_cancer_genes_brca1_brca2(self):
        """Test lookup of common cancer genes BRCA1 and BRCA2."""
        brca1_payload = [{"id": "ENSG00000012048", "type": "gene"}]
        brca2_payload = [{"id": "ENSG00000139618", "type": "gene"}]

        with patch("src.tools.ensembl_rest_client.urllib.request.urlopen", side_effect=[
            _mock_response(brca1_payload),
            _mock_response(brca2_payload),
        ]):
            brca1_result = symbol_lookup("human", "BRCA1")
            brca2_result = symbol_lookup("human", "BRCA2")
            
            assert brca1_result["id"] == "ENSG00000012048"
            assert brca2_result["id"] == "ENSG00000139618"

    def test_model_organisms_multiple_species(self):
        """Test symbol lookup across common model organisms."""
        # Human TP53
        human_tp53 = [{"id": "ENSG00000141510", "type": "gene", "species": "homo_sapiens"}]
        # Mouse Tp53
        mouse_tp53 = [{"id": "ENSMUSG00000059552", "type": "gene", "species": "mus_musculus"}]
        # Zebrafish tp53
        zebrafish_tp53 = [{"id": "ENSDARG00000035559", "type": "gene", "species": "danio_rerio"}]

        with patch("src.tools.ensembl_rest_client.urllib.request.urlopen", side_effect=[
            _mock_response(human_tp53),
            _mock_response(mouse_tp53),
            _mock_response(zebrafish_tp53),
        ]):
            human_result = symbol_lookup("human", "TP53")
            mouse_result = symbol_lookup("mouse", "Tp53")
            fish_result = symbol_lookup("zebrafish", "tp53")
            
            assert human_result["id"] == "ENSG00000141510"
            assert mouse_result["id"] == "ENSMUSG00000059552"
            assert fish_result["id"] == "ENSDARG00000035559"

    def test_pharmacogenomics_genes(self):
        """Test lookup of pharmacogenomics-relevant genes."""
        cyp2d6_payload = [{"id": "ENSG00000100197", "type": "gene", "biotype": "protein_coding"}]
        cyp3a4_payload = [{"id": "ENSG00000160868", "type": "gene", "biotype": "protein_coding"}]

        with patch("src.tools.ensembl_rest_client.urllib.request.urlopen", side_effect=[
            _mock_response(cyp2d6_payload),
            _mock_response(cyp3a4_payload),
        ]):
            cyp2d6_result = symbol_lookup("human", "CYP2D6")
            cyp3a4_result = symbol_lookup("human", "CYP3A4")
            
            assert cyp2d6_result["id"] == "ENSG00000100197"
            assert cyp3a4_result["id"] == "ENSG00000160868"

    def test_variants_with_clinical_significance(self):
        """Test variant lookup with clinically significant variants."""
        # First call: symbol lookup for CFTR (cystic fibrosis gene)
        cftr_lookup = [{"id": "ENSG00000001626", "type": "gene"}]
        # Second call: variants including pathogenic ones
        cftr_variants = [
            {
                "id": "rs113993960",
                "start": 117199644,
                "end": 117199647,
                "allele_string": "CTT/-",
                "consequence_type": "frameshift_variant",
                "clinical_significance": "pathogenic"
            },
            {
                "id": "rs397508256",
                "start": 117199645,
                "end": 117199645,
                "allele_string": "T/G",
                "consequence_type": "missense_variant",
                "clinical_significance": "pathogenic"
            }
        ]

        with patch("src.tools.ensembl_rest_client.urllib.request.urlopen", side_effect=[
            _mock_response(cftr_lookup),
            _mock_response(cftr_variants),
        ]):
            result = get_variants("human", "CFTR")
            assert len(result) == 2
            assert result[0]["clinical_significance"] == "pathogenic"
            assert result[1]["consequence_type"] == "missense_variant"

    def test_immune_system_genes(self):
        """Test lookup of immune system genes commonly studied."""
        hla_a_payload = [{"id": "ENSG00000206503", "type": "gene", "chromosome": "6"}]
        il6_payload = [{"id": "ENSG00000136244", "type": "gene", "chromosome": "7"}]

        with patch("src.tools.ensembl_rest_client.urllib.request.urlopen", side_effect=[
            _mock_response(hla_a_payload),
            _mock_response(il6_payload),
        ]):
            hla_result = symbol_lookup("human", "HLA-A")
            il6_result = symbol_lookup("human", "IL6")
            
            assert hla_result["id"] == "ENSG00000206503"
            assert il6_result["id"] == "ENSG00000136244"

    def test_neurological_disorder_genes(self):
        """Test lookup of genes associated with neurological disorders."""
        app_payload = [{"id": "ENSG00000142192", "type": "gene", "description": "amyloid beta precursor protein"}]
        snca_payload = [{"id": "ENSG00000145335", "type": "gene", "description": "synuclein alpha"}]

        with patch("src.tools.ensembl_rest_client.urllib.request.urlopen", side_effect=[
            _mock_response(app_payload),
            _mock_response(snca_payload),
        ]):
            app_result = symbol_lookup("human", "APP")  # Alzheimer's
            snca_result = symbol_lookup("human", "SNCA")  # Parkinson's
            
            assert app_result["id"] == "ENSG00000142192"
            assert snca_result["id"] == "ENSG00000145335"

    def test_metabolic_pathway_genes(self):
        """Test lookup of genes in metabolic pathways."""
        insulin_payload = [{"id": "ENSG00000254647", "type": "gene", "biotype": "protein_coding"}]
        ldlr_payload = [{"id": "ENSG00000130164", "type": "gene", "biotype": "protein_coding"}]

        with patch("src.tools.ensembl_rest_client.urllib.request.urlopen", side_effect=[
            _mock_response(insulin_payload),
            _mock_response(ldlr_payload),
        ]):
            ins_result = symbol_lookup("human", "INS")
            ldlr_result = symbol_lookup("human", "LDLR")
            
            assert ins_result["id"] == "ENSG00000254647"
            assert ldlr_result["id"] == "ENSG00000130164"

    def test_developmental_biology_genes(self):
        """Test lookup of genes important in development."""
        hox_payload = [{"id": "ENSG00000106031", "type": "gene", "chromosome": "7"}]
        pax_payload = [{"id": "ENSG00000007372", "type": "gene", "chromosome": "11"}]

        with patch("src.tools.ensembl_rest_client.urllib.request.urlopen", side_effect=[
            _mock_response(hox_payload),
            _mock_response(pax_payload),
        ]):
            hox_result = symbol_lookup("human", "HOXA1")
            pax_result = symbol_lookup("human", "PAX6")
            
            assert hox_result["id"] == "ENSG00000106031"
            assert pax_result["id"] == "ENSG00000007372"

    def test_variants_with_population_frequencies(self):
        """Test variant lookup including population frequency data."""
        # First call: symbol lookup for APOE (Alzheimer's risk gene)
        apoe_lookup = [{"id": "ENSG00000130203", "type": "gene"}]
        # Second call: variants with population frequencies
        apoe_variants = [
            {
                "id": "rs429358",
                "start": 45411941,
                "end": 45411941,
                "allele_string": "T/C",
                "consequence_type": "missense_variant",
                "minor_allele": "C",
                "minor_allele_freq": 0.1378,
                "populations": [
                    {"population": "1000GENOMES:phase_3:EUR", "frequency": 0.1506},
                    {"population": "1000GENOMES:phase_3:AFR", "frequency": 0.0378}
                ]
            }
        ]

        with patch("src.tools.ensembl_rest_client.urllib.request.urlopen", side_effect=[
            _mock_response(apoe_lookup),
            _mock_response(apoe_variants),
        ]):
            result = get_variants("human", "APOE")
            assert len(result) == 1
            assert result[0]["minor_allele_freq"] == 0.1378
            assert len(result[0]["populations"]) == 2

    def test_rate_limiting_behavior(self):
        """Test that rate limiting works correctly."""
        client = EnsemblRestClient(reqs_per_sec=2)
        
        # Mock time to control rate limiting
        with patch("src.tools.ensembl_rest_client.time") as mock_time:
            mock_time.return_value = 1000.0  # Fixed time
            
            # First request should go through
            with patch("src.tools.ensembl_rest_client.urllib.request.urlopen", 
                      return_value=_mock_response([{"id": "ENSG00000141510", "type": "gene"}])):
                result1 = client.symbol_lookup("human", "TP53")
                assert result1["id"] == "ENSG00000141510"
                
            # Second request should go through
            with patch("src.tools.ensembl_rest_client.urllib.request.urlopen", 
                      return_value=_mock_response([{"id": "ENSG00000139618", "type": "gene"}])):
                result2 = client.symbol_lookup("human", "BRCA2")
                assert result2["id"] == "ENSG00000139618"

    def test_empty_variant_list_handling(self):
        """Test handling when gene has no variants."""
        # First call: symbol lookup succeeds
        gene_lookup = [{"id": "ENSG00000000000", "type": "gene"}]
        # Second call: no variants found
        no_variants = []

        with patch("src.tools.ensembl_rest_client.urllib.request.urlopen", side_effect=[
            _mock_response(gene_lookup),
            _mock_response(no_variants),
        ]):
            result = get_variants("human", "FAKEGENE")
            assert result == []

    def test_client_custom_server(self):
        """Test client with custom server URL."""
        custom_client = EnsemblRestClient(server="https://grch37.rest.ensembl.org")
        
        with patch("src.tools.ensembl_rest_client.urllib.request.urlopen", 
                  return_value=_mock_response([{"id": "ENSG00000141510", "type": "gene"}])) as mock_urlopen:
            result = custom_client.symbol_lookup("human", "TP53")
            
            # Verify the correct server was used
            called_url = mock_urlopen.call_args[0][0].full_url
            assert "grch37.rest.ensembl.org" in called_url
            assert result["id"] == "ENSG00000141510"

    def test_synonym_gene_symbols(self):
        """Test lookup of genes by their synonyms."""
        # Test EGFR (also known as ERBB1)
        egfr_payload = [{"id": "ENSG00000146648", "type": "gene", "synonyms": ["ERBB1"]}]
        
        with patch("src.tools.ensembl_rest_client.urllib.request.urlopen", 
                  return_value=_mock_response(egfr_payload)):
            result = symbol_lookup("human", "ERBB1")
            assert result["id"] == "ENSG00000146648"
            assert result["type"] == "gene"