#!/usr/bin/env python3

from src.tools.extract_geo_metadata import get_paper_abstract

def main():
    print("Testing Updated Paper Abstract Function")
    print("=" * 50)
    
    try:
        result = get_paper_abstract(23902433)
        
        print(f"PMID: {result.get('pmid')}")
        print(f"Title: {result.get('title', 'No title')}")
        print(f"Journal: {result.get('journal', 'No journal')}")
        print(f"Authors: {result.get('authors', [])}")
        print(f"Publication Date: {result.get('publication_date', 'No date')}")
        print(f"DOI: {result.get('doi', 'No DOI')}")
        
        abstract = result.get('abstract', 'No abstract')
        if abstract:
            print(f"\nAbstract ({len(abstract)} characters):")
            print("-" * 40)
            print(abstract[:500] + "..." if len(abstract) > 500 else abstract)
        else:
            print("\nNo abstract found")
        
        print(f"\nKeywords: {result.get('keywords', [])}")
        print(f"MeSH Terms: {result.get('mesh_terms', [])}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main() 