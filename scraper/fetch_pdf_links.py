"""
PDF Link Scraper for FADA Website
Extracts all relevant PDF links from the FADA press release pages.

This module preserves the original scraping logic from Full_automation.ipynb
while adding structured metadata extraction.
"""

import re
from typing import List, Dict, Optional
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import requests

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import FADA_CONFIG, MONTH_PATTERNS, MONTH_NAMES
from utils.logger import get_logger


def extract_month_year_from_filename(filename: str) -> tuple:
    """
    Extract month and year from a PDF filename.
    
    Args:
        filename: PDF filename (e.g., 'FADA releases January 2024 Vehicle Retail Data.pdf')
        
    Returns:
        Tuple of (month_number, year) or (None, None) if not found
    """
    filename_lower = filename.lower()
    
    # Find month
    month_num = None
    for month_name, month_val in MONTH_PATTERNS.items():
        if month_name in filename_lower:
            month_num = month_val
            break
    
    # Find year (4-digit year between 2018-2030)
    year_match = re.search(r'(20[1-3][0-9])', filename)
    year = int(year_match.group(1)) if year_match else None
    
    return month_num, year


def fetch_pdf_links(max_pages: int = None) -> List[Dict]:
    """
    Fetch all PDF links from FADA press release pages.
    
    Args:
        max_pages: Maximum number of pages to scrape (default from config)
        
    Returns:
        List of dicts containing PDF metadata:
        [
            {
                'url': 'https://fada.in/...',
                'filename': 'FADA releases January 2024...',
                'month': 1,
                'year': 2024
            },
            ...
        ]
    """
    logger = get_logger()
    
    if max_pages is None:
        max_pages = FADA_CONFIG['max_pages']
    
    headers = FADA_CONFIG['request_headers']
    base_page_url = FADA_CONFIG['base_page_url']
    base_site_url = FADA_CONFIG['base_site_url']
    timeout = FADA_CONFIG['request_timeout']
    
    pdf_links = []
    seen_urls = set()
    
    logger.info(f"Starting PDF link extraction from FADA website (max {max_pages} pages)")
    
    for page in range(1, max_pages + 1):
        url = base_page_url + str(page)
        
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all PDF links
            for link in soup.find_all('a', href=lambda href: href and href.lower().endswith('.pdf')):
                href = link['href']
                
                # Skip if already seen
                if href in seen_urls:
                    continue
                
                # Get filename
                filename = href.split('/')[-1] if '/' in href else href
                basename_lower = filename.lower()
                
                # Filter: only include PDFs with month names (vehicle retail data)
                if not any(month in basename_lower for month in MONTH_NAMES):
                    continue
                
                # Build full URL
                pdf_url = urljoin(base_site_url, href)
                seen_urls.add(href)
                
                # Extract month and year
                month, year = extract_month_year_from_filename(filename)
                
                pdf_links.append({
                    'url': pdf_url,
                    'filename': filename,
                    'month': month,
                    'year': year,
                    'page_found': page
                })
            
            logger.debug(f"Page {page}: Found {len(pdf_links)} total PDF links")
            
        except requests.RequestException as e:
            logger.warning(f"Error fetching page {page}: {e}")
            continue
        except Exception as e:
            logger.error(f"Unexpected error on page {page}: {e}")
            continue
    
    logger.info(f"Extraction complete: Found {len(pdf_links)} PDF links with month data")
    return pdf_links


def get_available_months(pdf_links: List[Dict] = None) -> List[Dict]:
    """
    Get list of available month/year combinations from PDF links.
    
    Args:
        pdf_links: List of PDF link dicts (will fetch if not provided)
        
    Returns:
        List of {month, year, count} dicts sorted by date
    """
    if pdf_links is None:
        pdf_links = fetch_pdf_links()
    
    month_years = {}
    for link in pdf_links:
        if link['month'] and link['year']:
            key = (link['year'], link['month'])
            month_years[key] = month_years.get(key, 0) + 1
    
    result = [
        {'year': year, 'month': month, 'count': count}
        for (year, month), count in month_years.items()
    ]
    
    # Sort by year descending, then month descending
    result.sort(key=lambda x: (x['year'], x['month']), reverse=True)
    return result


if __name__ == '__main__':
    # Test the scraper
    links = fetch_pdf_links(max_pages=3)
    print(f"\nFound {len(links)} PDF links:")
    for link in links[:10]:
        print(f"  - {link['filename']} ({link['month']}/{link['year']})")
    
    print("\nAvailable months:")
    for m in get_available_months(links)[:12]:
        print(f"  - {m['year']}-{m['month']:02d}: {m['count']} file(s)")
