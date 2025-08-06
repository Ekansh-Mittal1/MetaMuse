from pronto import Ontology
import json
import os
import requests
import warnings

# Suppress all warnings from pronto and related libraries
warnings.filterwarnings("ignore", category=SyntaxWarning)
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", message=".*unknown element.*")
warnings.filterwarnings("ignore", message=".*several names found.*")
warnings.filterwarnings("ignore", message=".*cannot process.*")
warnings.filterwarnings("ignore", message=".*no datatype.*")
warnings.filterwarnings("ignore", message=".*could not extract.*")
warnings.filterwarnings("ignore", message=".*unknown axiom property.*")

# Define the ontologies and their URLs
ONTOLOGIES = {
    #'MONDO': 'http://purl.obolibrary.org/obo/mondo.owl',
    #'UBERON': 'http://purl.obolibrary.org/obo/uberon.owl',
    #'ChEMBL': 'http://purl.obolibrary.org/obo/chembl.obo',
    #'EFO': 'https://raw.githubusercontent.com/EBISPOT/efo/main/efo.owl',  # GitHub release
    #'NCBITaxon': 'http://purl.obolibrary.org/obo/ncbitaxon.owl',
    #'HANCESTRO': 'http://purl.obolibrary.org/obo/hancestro.owl',
    #'HSAPDV': 'http://purl.obolibrary.org/obo/hsapdv.owl',
    #'PATO': 'http://purl.obolibrary.org/obo/pato.owl',
    #'CLO': 'http://purl.obolibrary.org/obo/clo.owl',
    "CL": "http://purl.obolibrary.org/obo/cl.owl"
}

# Define namespace prefixes for each ontology
ONTOLOGY_NAMESPACES = {
    "MONDO": "MONDO:",
    "UBERON": "UBERON:",
    "ChEMBL": "CHEMBL.",
    "EFO": "EFO_",  # Keep this as is for now
    "NCBITaxon": "NCBITaxon:",
    "HANCESTRO": "HANCESTRO:",
    "HSAPDV": "HsapDv:",
    "PATO": "PATO:",
    "CLO": "CLO:",
    "CL": "CL:",
}


def download_ontology(url, ontology_name):
    """
    Download ontology file and save it locally.

    Args:
        url (str): URL to the ontology file
        ontology_name (str): Name of the ontology

    Returns:
        str: Path to the downloaded file
    """
    try:
        print(f"  - Downloading {ontology_name} from {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        # Save to temporary file
        temp_file = f"temp_{ontology_name.lower()}.owl"
        with open(temp_file, "wb") as f:
            f.write(response.content)

        print(f"  - Downloaded {len(response.content)} bytes")
        return temp_file

    except Exception as e:
        print(f"  - Error downloading {ontology_name}: {str(e)}")
        return None


def create_dictionary_with_pronto(ontology_name, url):
    """
    Create a single dictionary for terms and synonyms from an ontology using pronto.
    Only include terms that belong to the specific ontology namespace.

    Args:
        ontology_name (str): Name of the ontology
        url (str): URL to the ontology file

    Returns:
        dict: Dictionary mapping terms/synonyms to ontology IDs
    """
    print(f"Processing {ontology_name} from {url}")

    try:
        # Try to download the ontology first
        temp_file = download_ontology(url, ontology_name)

        if temp_file and os.path.exists(temp_file):
            # Read the ontology from local file using pronto
            ontology = Ontology(temp_file)

            # Clean up temp file
            os.remove(temp_file)
        else:
            # Try direct URL reading
            print(f"  - Trying direct URL reading for {ontology_name}")
            ontology = Ontology(url)

        # Get the namespace prefix for this ontology
        namespace_prefix = ONTOLOGY_NAMESPACES.get(ontology_name, f"{ontology_name}:")

        # Single dictionary for both terms and synonyms
        term_to_id_dict = {}

        # Debug: Count total terms and namespace-specific terms
        total_terms = 0
        namespace_terms = 0

        for term in ontology.terms():
            if not term.id or not term.name:
                continue

            total_terms += 1

            # Only include terms that belong to this ontology's namespace
            if term.id.startswith(namespace_prefix):
                namespace_terms += 1
                # Add primary label
                term_to_id_dict[term.name.lower()] = term.id

                # Add synonyms
                for syn in term.synonyms:
                    term_to_id_dict[syn.description.lower()] = term.id

        print(f"  - Total terms in ontology: {total_terms}")
        print(f"  - Terms with {namespace_prefix} prefix: {namespace_terms}")
        print(
            f"  - Found {len(term_to_id_dict)} total terms (including synonyms) for {ontology_name}"
        )

        # Debug: Show first few term IDs for EFO
        if ontology_name == "EFO":
            print("  - First 10 term IDs:")
            count = 0
            for term in ontology.terms():
                if count >= 10:
                    break
                print(f"    {term.id}")
                count += 1

        return term_to_id_dict

    except Exception as e:
        print(f"  - Error processing {ontology_name}: {str(e)}")
        import traceback

        traceback.print_exc()
        # Clean up temp file if it exists
        temp_file = f"temp_{ontology_name.lower()}.owl"
        if os.path.exists(temp_file):
            os.remove(temp_file)
        return {}


def save_dictionary(ontology_name, term_to_id_dict, output_dir):
    """
    Save dictionary to JSON file.

    Args:
        ontology_name (str): Name of the ontology
        term_to_id_dict (dict): Dictionary mapping terms/synonyms to ontology IDs
        output_dir (str): Directory to save the file
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Save combined dictionary
    dict_file = os.path.join(output_dir, f"{ontology_name.lower()}_terms.json")
    with open(dict_file, "w") as f:
        json.dump(term_to_id_dict, f, indent=2)

    print(f"  - Saved dictionary to {dict_file}")


def main():
    """
    Main function to process all ontologies and create dictionaries.
    """
    output_dir = "src/normalization/dictionaries"

    print("Creating dictionaries for ontologies...")
    print("=" * 50)

    for ontology_name, url in ONTOLOGIES.items():
        print(f"\nProcessing {ontology_name}...")

        # Create dictionary
        term_to_id_dict = create_dictionary_with_pronto(ontology_name, url)

        # Save dictionary
        if term_to_id_dict:
            save_dictionary(ontology_name, term_to_id_dict, output_dir)
        else:
            print(f"  - No data found for {ontology_name}")

    print("\n" + "=" * 50)
    print("Dictionary creation completed!")


if __name__ == "__main__":
    main()
