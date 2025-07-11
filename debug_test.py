#!/usr/bin/env python3

from src.tools.extract_geo_metadata import get_gse_metadata, get_paper_abstract, NCBIClient
import urllib.request
import urllib.parse
import json

def test_pubmed_search():
    print("Testing PubMed Search...")
    try:
        # Test direct PubMed search
        search_params = {
            'db': 'pubmed',
            'term': '23902433',
            'retmode': 'json'
        }
        
        search_url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?{urllib.parse.urlencode(search_params)}"
        print(f"Search URL: {search_url}")
        
        response = urllib.request.urlopen(search_url)
        content = response.read().decode('utf-8')
        print(f"Search Response: {content[:500]}...")
        
        if content:
            data = json.loads(content)
            id_list = data.get('esearchresult', {}).get('idlist', [])
            print(f"ID List: {id_list}")
            
            if id_list:
                # Test summary
                summary_params = {
                    'db': 'pubmed',
                    'id': id_list[0],
                    'retmode': 'json'
                }
                
                summary_url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?{urllib.parse.urlencode(summary_params)}"
                print(f"Summary URL: {summary_url}")
                
                summary_response = urllib.request.urlopen(summary_url)
                summary_content = summary_response.read().decode('utf-8')
                print(f"Summary Response: {summary_content[:500]}...")
        
    except Exception as e:
        print(f"PubMed Search Error: {e}")

def test_client_url():
    print("\nTesting Client URL Construction...")
    try:
        client = NCBIClient()
        print(f"Client API URL: {client.api_url}")
        
        # Test the exact URL our function would construct
        search_params = {
            'db': 'pubmed',
            'term': '23902433',
            'retmode': 'json'
        }
        
        search_url = f"{client.api_url}esearch.fcgi?{urllib.parse.urlencode(search_params)}"
        print(f"Our Search URL: {search_url}")
        
        # Test if this URL works
        response = urllib.request.urlopen(search_url)
        content = response.read().decode('utf-8')
        print(f"Our Search Response: {content[:200]}...")
        
    except Exception as e:
        print(f"Client URL Error: {e}")

def main():
    print("Testing GSE Metadata...")
    try:
        gse_result = get_gse_metadata("GSE41588")
        print(f"GSE Result: {gse_result}")
        print(f"Has gse_id: {'gse_id' in gse_result}")
        print(f"Keys: {list(gse_result.keys())}")
    except Exception as e:
        print(f"GSE Error: {e}")
    
    print("\nTesting PubMed Search...")
    test_pubmed_search()
    
    print("\nTesting Client URL...")
    test_client_url()
    
    print("\nTesting Paper Abstract...")
    try:
        paper_result = get_paper_abstract(23902433)
        print(f"Paper Result: {paper_result}")
        print(f"Title: {paper_result.get('title', 'No title')}")
        print(f"Abstract: {paper_result.get('abstract', 'No abstract')[:200]}...")
    except Exception as e:
        print(f"Paper Error: {e}")

if __name__ == "__main__":
    main() 