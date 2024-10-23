import sys
import subprocess
import pkg_resources
import os
from typing import Dict, List, Optional, Tuple
import pandas as pd
import requests
import time
import json

def install_required_packages():
    """Install required packages if they're missing."""
    required = {'requests', 'pandas'}
    installed = {pkg.key for pkg in pkg_resources.working_set}
    missing = required - installed
    
    if missing:
        print(f"Installing missing packages: {missing}")
        python = sys.executable
        try:
            subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)
            print("Successfully installed missing packages")
        except subprocess.CalledProcessError:
            print("Error: Failed to install required packages. Please install them manually:")
            print("pip install requests pandas")
            sys.exit(1)

class ComprehensiveAffiliationFinder:
    def __init__(self):
        self.crossref_url = "https://api.crossref.org/works"
        self.openalex_url = "https://api.openalex.org/works"
        self.semantic_scholar_url = "https://api.semanticscholar.org/graph/v1/paper"
        self.headers = {
            "User-Agent": "AffiliationFinder/1.0 (mailto:your-email@example.com)"
        }
    
    def clean_title(self, title: str) -> str:
        """Clean and normalize paper title."""
        if pd.isna(title):
            return ""
        return title.replace("[HTML]", "").strip()
    
    def clean_doi(self, doi: str) -> Optional[str]:
        """Clean and format DOI."""
        if pd.isna(doi):
            return None
        return str(doi).strip().lower()

    def search_crossref(self, title: str) -> Optional[Dict]:
        """Search Crossref for paper metadata."""
        try:
            params = {
                "query.title": self.clean_title(title),
                "select": "author,title,DOI,publisher",
                "rows": 1
            }
            response = requests.get(self.crossref_url, params=params, headers=self.headers)
            response.raise_for_status()
            
            data = response.json()
            if data["message"]["items"]:
                return data["message"]["items"][0]
            return None
            
        except requests.exceptions.RequestException as e:
            print(f"Error with Crossref API for title '{title}': {str(e)}")
            return None

    def search_openalex(self, doi: str) -> Optional[List[Tuple[str, str]]]:
        """Search OpenAlex for author affiliations."""
        if not doi:
            return None
            
        try:
            url = f"{self.openalex_url}/doi/{doi}"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            data = response.json()
            affiliations = []
            
            if "authorships" in data:
                for authorship in data["authorships"]:
                    author_name = authorship.get("author", {}).get("display_name", "")
                    
                    for institution in authorship.get("institutions", []):
                        inst_name = institution.get("display_name", "")
                        location_parts = []
                        
                        if institution.get("city"):
                            location_parts.append(institution["city"])
                        if institution.get("region"):
                            location_parts.append(institution["region"])
                        if institution.get("country"):
                            location_parts.append(institution["country"])
                            
                        full_affiliation = inst_name
                        if location_parts:
                            full_affiliation += f", {', '.join(location_parts)}"
                            
                        if author_name and full_affiliation:
                            affiliations.append((author_name, full_affiliation))
                            
            return affiliations if affiliations else None
            
        except requests.exceptions.RequestException as e:
            print(f"Error with OpenAlex API for DOI {doi}: {str(e)}")
            return None

    def search_semantic_scholar(self, doi: str) -> Optional[List[Tuple[str, str]]]:
        """Search Semantic Scholar for author affiliations."""
        if not doi:
            return None
            
        try:
            params = {"fields": "authors.name,authors.affiliations"}
            response = requests.get(f"{self.semantic_scholar_url}/DOI:{doi}", 
                                 headers=self.headers, 
                                 params=params)
            response.raise_for_status()
            
            data = response.json()
            affiliations = []
            
            if "authors" in data:
                for author in data["authors"]:
                    author_name = author.get("name", "")
                    for affiliation in author.get("affiliations", []):
                        if author_name and affiliation:
                            affiliations.append((author_name, affiliation))
                            
            return affiliations if affiliations else None
            
        except requests.exceptions.RequestException as e:
            print(f"Error with Semantic Scholar API for DOI {doi}: {str(e)}")
            return None

    def process_papers(self, titles: List[str], start_index: int = 0, output_file: str = 'results.csv') -> pd.DataFrame:
        """Process papers to extract metadata and affiliations with checkpointing."""
        results = []
        papers_processed = 0
        affiliations_found = 0
        
        # Load existing results if any
        if os.path.exists(output_file):
            existing_df = pd.read_csv(output_file)
            results = existing_df.to_dict('records')
            print(f"Loaded {len(results)} existing results from {output_file}")
        
        for i, title in enumerate(titles[start_index:], start=start_index):
            print(f"\nProcessing paper {i + 1}/{len(titles)}: {title}")
            
            try:
                # Step 1: Get initial metadata from Crossref
                paper_data = self.search_crossref(title)
                if paper_data:
                    doi = paper_data.get("DOI")
                    
                    # Get affiliations from paper_data
                    for author in paper_data.get("author", []):
                        name = f"{author.get('given', '')} {author.get('family', '')}".strip()
                        affiliations = []
                        
                        if "affiliation" in author:
                            for affiliation in author["affiliation"]:
                                if isinstance(affiliation, dict) and "name" in affiliation:
                                    affiliations.append(affiliation["name"])
                        
                        # If no affiliations found in Crossref, try OpenAlex
                        if not affiliations and doi:
                            openalex_affiliations = self.search_openalex(doi)
                            if openalex_affiliations:
                                for aff_author, affiliation in openalex_affiliations:
                                    if name.lower() in aff_author.lower() or aff_author.lower() in name.lower():
                                        affiliations.append(affiliation)
                        
                        # If still no affiliations, try Semantic Scholar
                        if not affiliations and doi:
                            semantic_affiliations = self.search_semantic_scholar(doi)
                            if semantic_affiliations:
                                for aff_author, affiliation in semantic_affiliations:
                                    if name.lower() in aff_author.lower() or aff_author.lower() in name.lower():
                                        affiliations.append(affiliation)
                        
                        result = {
                            "paper_title": title,
                            "author": name,
                            "affiliations": "; ".join(affiliations) if affiliations else "Not found",
                            "doi": doi or "Not found"
                        }
                        results.append(result)
                        
                        if affiliations:
                            affiliations_found += 1
                
                # Save checkpoint after each paper
                checkpoint_df = pd.DataFrame(results)
                checkpoint_df.to_csv(output_file, index=False)
                print(f"Saved checkpoint after processing paper {i + 1}")
                
            except Exception as e:
                print(f"Error processing paper {title}: {str(e)}")
                # Continue with next paper even if current one fails
                continue
            
            papers_processed += 1
            time.sleep(1)  # Be nice to APIs
            
        print(f"\nProcessed {papers_processed} papers")
        print(f"Found affiliations for {affiliations_found} authors")
        
        return pd.DataFrame(results)
    
    def main():
    # Command line arguments for start index
    #start_index = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    start_index = 0
    
    # File paths
    input_file = 'scholar_citations_{scholar_id}_{timestamp}.csv' # change this
    output_file = 'scholar_citations_{scholar_id}_{timestamp}_affliations.csv' # change this
    
    try:
        # Read titles
        df = pd.read_csv(input_file)
        titles = df['title'].tolist()
        
        if start_index >= len(titles):
            print(f"Error: Start index {start_index} is greater than number of papers {len(titles)}")
            return
        
        # Initialize finder and process papers
        finder = ComprehensiveAffiliationFinder()
        results_df = finder.process_papers(titles, start_index=start_index, output_file=output_file)
        
        # Final save (though it should already be saved from checkpoints)
        results_df.to_csv(output_file, index=False)
        
        # Print summary
        print("\nFinal Summary:")
        print(f"Total papers processed: {len(titles) - start_index}")
        print(f"Total authors found: {len(results_df)}")
        print(f"Authors with affiliations: {len(results_df[results_df['affiliations'] != 'Not found'])}")
        print(f"Results saved to: {output_file}")
        
    except FileNotFoundError:
        print(f"Error: Could not find input file: {input_file}")
    except Exception as e:
        print(f"Error processing files: {str(e)}")
        raise

if __name__ == "__main__":
    main()
