import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
from datetime import datetime
import logging
from typing import List, Dict, Optional, Tuple
import re
from urllib.parse import urlencode, urlparse, parse_qs

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GoogleScholarScraper:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }
        self.base_url = "https://scholar.google.com"
        self.session = requests.Session()

    def _get_random_delay(self) -> float:
        """Generate a random delay between requests"""
        return random.uniform(2.0, 4.0)

    def _make_request(self, url: str, retries: int = 3) -> Optional[str]:
        """Make HTTP request with retry logic"""
        for attempt in range(retries):
            try:
                logger.debug(f"Requesting URL: {url}")
                response = self.session.get(url, headers=self.headers, timeout=30)
                
                if response.status_code == 200:
                    return response.text
                elif response.status_code == 429:
                    wait_time = (attempt + 1) * 60
                    logger.warning(f"Rate limited. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.warning(f"Request failed with status: {response.status_code}")
            except Exception as e:
                logger.error(f"Request error: {str(e)}")
                if attempt < retries - 1:
                    time.sleep(self._get_random_delay())
        return None

    def _extract_cluster_id(self, href: str) -> Optional[str]:
        """Extract cluster ID from citation link"""
        try:
            parsed = urlparse(href)
            params = parse_qs(parsed.query)
            if 'cites' in params:
                return params['cites'][0]
        except Exception as e:
            logger.error(f"Error extracting cluster ID: {str(e)}")
        return None

    def _get_cited_by_url(self, paper_html) -> Optional[str]:
        """Extract and construct proper 'Cited by' URL"""
        try:
            cited_by_link = paper_html.find('a', class_='gsc_a_ac')
            if cited_by_link and 'href' in cited_by_link.attrs:
                # Extract the cluster ID from the href
                cluster_id = self._extract_cluster_id(cited_by_link['href'])
                if cluster_id:
                    # Construct proper cited by URL
                    params = {
                        'cites': cluster_id,
                        'hl': 'en',
                        'sciodt': '0,5'
                    }
                    return f"{self.base_url}/scholar?{urlencode(params)}"
            return None
        except Exception as e:
            logger.error(f"Error getting cited by URL: {str(e)}")
            return None

    def _get_total_citation_pages(self, content: str) -> int:
        """Get total number of pages from citation results"""
        try:
            soup = BeautifulSoup(content, 'html.parser')
            
            # Check for results
            results = soup.find_all('div', class_='gs_r gs_or gs_scl')
            if not results:
                return 0
                
            # Look for pagination footer
            footer = soup.find('div', id='gs_n')
            if footer:
                pages = footer.find_all('a')
                if pages:
                    try:
                        return max([int(p.text) for p in pages if p.text.isdigit()])
                    except ValueError:
                        pass
            
            # If we found results but no pagination, we have one page
            return 1
            
        except Exception as e:
            logger.error(f"Error getting total pages: {str(e)}")
            return 0

    def _parse_citing_paper(self, paper_html: BeautifulSoup) -> Dict:
        """Parse a single citing paper"""
        try:
            # Extract title and link
            title_section = paper_html.find('h3', class_='gs_rt')
            title = title_section.get_text(strip=True) if title_section else ''
            link = title_section.find('a')['href'] if title_section and title_section.find('a') else ''
            
            # Extract authors, venue, year
            byline = paper_html.find('div', class_='gs_a')
            byline_text = byline.get_text(strip=True) if byline else ''
            
            # Split byline
            parts = byline_text.split(' - ')
            authors = parts[0] if parts else ''
            venue = parts[1] if len(parts) > 1 else ''
            
            # Extract year
            year_match = re.search(r'20\d{2}|19\d{2}', byline_text)
            year = year_match.group(0) if year_match else ''
            
            # Extract snippet
            snippet = paper_html.find('div', class_='gs_rs')
            snippet_text = snippet.get_text(strip=True) if snippet else ''
            
            return {
                'title': title,
                'authors': authors,
                'venue': venue,
                'year': year,
                'link': link,
                'snippet': snippet_text
            }
            
        except Exception as e:
            logger.error(f"Error parsing citing paper: {str(e)}")
            return {}

    def _get_citations_for_paper(self, cited_by_url: str, cited_paper_title: str, min_year: Optional[int] = None) -> List[Dict]:
        """Get all citations for a single paper"""
        logger.info(f"Getting citations from URL: {cited_by_url}")
        citations = []
        
        # Get first page and total pages
        content = self._make_request(cited_by_url)
        if not content:
            return citations
            
        total_pages = self._get_total_citation_pages(content)
        if total_pages == 0:
            return citations
            
        # Process first page
        soup = BeautifulSoup(content, 'html.parser')
        papers = soup.find_all('div', class_='gs_r gs_or gs_scl')
        
        for paper in papers:
            citation_data = self._parse_citing_paper(paper)
            if citation_data:
                citation_data['cited_paper'] = cited_paper_title
                citations.append(citation_data)
        
        # Process remaining pages
        for page in range(1, total_pages):
            start_idx = page * 10
            page_url = f"{cited_by_url}&start={start_idx}"
            
            content = self._make_request(page_url)
            if not content:
                break
                
            soup = BeautifulSoup(content, 'html.parser')
            papers = soup.find_all('div', class_='gs_r gs_or gs_scl')
            
            for paper in papers:
                citation_data = self._parse_citing_paper(paper)
                if citation_data:
                    citation_data['cited_paper'] = cited_paper_title
                    citations.append(citation_data)
            
            logger.info(f"Processed page {page + 1} of {total_pages}")
            time.sleep(self._get_random_delay())
        
        return citations

    def get_all_citations(self, scholar_id: str, min_year: Optional[int] = None) -> pd.DataFrame:
        """Get all papers citing the author's work"""
        # Get author's papers
        profile_url = f"{self.base_url}/citations?user={scholar_id}&hl=en&pagesize=100"
        content = self._make_request(profile_url)
        
        if not content:
            logger.error("Could not access profile")
            return pd.DataFrame()
            
        soup = BeautifulSoup(content, 'html.parser')
        papers = soup.find_all('tr', class_='gsc_a_tr')
        
        all_citations = []
        total_papers = len(papers)
        logger.info(f"Found {total_papers} papers to process")
        
        for i, paper in enumerate(papers, 1):
            try:
                # Get paper title and citation URL
                title_elem = paper.find('a', class_='gsc_a_at')
                paper_title = title_elem.text if title_elem else 'Unknown Title'
                
                cited_by_url = self._get_cited_by_url(paper)
                if cited_by_url:
                    logger.info(f"Processing paper {i}/{total_papers}: {paper_title}")
                    citations = self._get_citations_for_paper(cited_by_url, paper_title, min_year)
                    
                    # Filter by year if needed
                    if min_year:
                        citations = [c for c in citations if c.get('year') and c['year'].isdigit() and int(c['year']) >= min_year]
                    
                    all_citations.extend(citations)
                    logger.info(f"Found {len(citations)} citations for this paper")
                
                time.sleep(self._get_random_delay())
                
            except Exception as e:
                logger.error(f"Error processing paper: {str(e)}")
                continue
        
        # Create DataFrame
        if not all_citations:
            logger.warning("No citations found")
            return pd.DataFrame()
            
        df = pd.DataFrame(all_citations)
        
        # Sort and save
        df = df.sort_values(by=['year', 'title'], ascending=[False, True]).reset_index(drop=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"scholar_citations_{scholar_id}_{timestamp}.csv"
        df.to_csv(filename, index=False)
        logger.info(f"Results saved to {filename}")
        
        return df

def main():
    """Main function to run the scraper"""
    scholar_id = "XXXXxxxxxxx" # your google scholar id
    min_year = None
    
    scraper = GoogleScholarScraper()
    logger.info(f"Starting citation scraping for Scholar ID: {scholar_id}")
    
    citations_df = scraper.get_all_citations(
        scholar_id=scholar_id,
        min_year=min_year
    )
    
    if not citations_df.empty:
        logger.info(f"\nFound {len(citations_df)} total citations")
        logger.info("\nSample of citations:")
        print(citations_df[['title', 'authors', 'year', 'cited_paper']].head())
        
        print("\nStatistics:")
        print(f"Total citing papers: {len(citations_df)}")
        print(f"Year range: {citations_df['year'].min()} - {citations_df['year'].max()}")
        print("\nCitations per paper:")
        print(citations_df['cited_paper'].value_counts())
    else:
        logger.warning("No citations found or an error occurred")

if __name__ == "__main__":
    main()