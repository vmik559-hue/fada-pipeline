"""
Date Filter Module for FADA ETL Pipeline
Filters PDFs by month and year selection.
"""

import re
from typing import List, Dict, Optional
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import MONTH_PATTERNS


def parse_month_year_from_filename(filename: str) -> tuple:
    """
    Parse month and year from a PDF filename.
    
    Args:
        filename: PDF filename
        
    Returns:
        Tuple of (month_number, year) or (None, None) if not parseable
    """
    filename_lower = filename.lower()
    
    # Find month
    month_num = None
    for month_name, month_val in MONTH_PATTERNS.items():
        if month_name in filename_lower:
            month_num = month_val
            break
    
    # Find year (4-digit year)
    year_match = re.search(r'(20[1-3][0-9])', filename)
    year = int(year_match.group(1)) if year_match else None
    
    return month_num, year


def filter_by_month_year(pdf_links: List[Dict], month: int, year: int) -> List[Dict]:
    """
    Filter PDF links to only include those matching the specified month and year.
    
    Args:
        pdf_links: List of PDF link dicts with 'filename', 'month', 'year' keys
        month: Target month (1-12)
        year: Target year (e.g., 2024)
        
    Returns:
        Filtered list of PDF link dicts
    """
    filtered = []
    
    for link in pdf_links:
        link_month = link.get('month')
        link_year = link.get('year')
        
        # If month/year not in link dict, try to parse from filename
        if link_month is None or link_year is None:
            parsed_month, parsed_year = parse_month_year_from_filename(link.get('filename', ''))
            link_month = link_month or parsed_month
            link_year = link_year or parsed_year
        
        # Check if matches target month and year
        if link_month == month and link_year == year:
            filtered.append(link)
    
    return filtered


def filter_by_year(pdf_links: List[Dict], year: int) -> List[Dict]:
    """
    Filter PDF links to only include those from a specific year.
    
    Args:
        pdf_links: List of PDF link dicts
        year: Target year (e.g., 2024)
        
    Returns:
        Filtered list of PDF link dicts
    """
    filtered = []
    
    for link in pdf_links:
        link_year = link.get('year')
        
        if link_year is None:
            _, parsed_year = parse_month_year_from_filename(link.get('filename', ''))
            link_year = parsed_year
        
        if link_year == year:
            filtered.append(link)
    
    return filtered


def filter_by_date_range(pdf_links: List[Dict], 
                          start_month: int, start_year: int,
                          end_month: int, end_year: int) -> List[Dict]:
    """
    Filter PDF links within a date range.
    
    Args:
        pdf_links: List of PDF link dicts
        start_month, start_year: Start of range (inclusive)
        end_month, end_year: End of range (inclusive)
        
    Returns:
        Filtered list of PDF link dicts
    """
    filtered = []
    start_date = start_year * 12 + start_month
    end_date = end_year * 12 + end_month
    
    for link in pdf_links:
        link_month = link.get('month')
        link_year = link.get('year')
        
        if link_month is None or link_year is None:
            parsed_month, parsed_year = parse_month_year_from_filename(link.get('filename', ''))
            link_month = link_month or parsed_month
            link_year = link_year or parsed_year
        
        if link_month and link_year:
            link_date = link_year * 12 + link_month
            if start_date <= link_date <= end_date:
                filtered.append(link)
    
    return filtered


def find_latest_period(pdf_links: List[Dict]) -> tuple:
    """
    Find the latest (most recent) month/year from PDF links.
    
    Args:
        pdf_links: List of PDF link dicts with optional 'month', 'year' keys
        
    Returns:
        Tuple of (month, year) of latest period, or (None, None) if no valid periods
    """
    latest_date = 0
    latest_month, latest_year = None, None
    
    for link in pdf_links:
        link_month = link.get('month')
        link_year = link.get('year')
        
        # If month/year not in link dict, try to parse from filename
        if link_month is None or link_year is None:
            parsed_month, parsed_year = parse_month_year_from_filename(link.get('filename', ''))
            link_month = link_month or parsed_month
            link_year = link_year or parsed_year
        
        if link_month and link_year:
            date_val = link_year * 12 + link_month
            if date_val > latest_date:
                latest_date = date_val
                latest_month, latest_year = link_month, link_year
    
    return latest_month, latest_year


def get_month_name(month_num: int) -> str:
    """Get full month name from month number."""
    month_names = [
        'January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November', 'December'
    ]
    if 1 <= month_num <= 12:
        return month_names[month_num - 1]
    return str(month_num)


def format_month_year(month: int, year: int) -> str:
    """Format month and year for display (e.g., 'January 2024')."""
    return f"{get_month_name(month)} {year}"


def format_sheet_name(month: int, year: int) -> str:
    """Format month and year for Excel sheet name (e.g., '2024-01')."""
    return f"{year}-{month:02d}"


if __name__ == '__main__':
    # Test the filters
    test_links = [
        {'filename': 'FADA releases January 2024 Vehicle Retail Data.pdf', 'month': 1, 'year': 2024},
        {'filename': 'FADA releases February 2024 Vehicle Retail Data.pdf', 'month': 2, 'year': 2024},
        {'filename': 'FADA releases January 2023 Vehicle Retail Data.pdf', 'month': 1, 'year': 2023},
    ]
    
    result = filter_by_month_year(test_links, 1, 2024)
    print(f"Filter Jan 2024: {len(result)} result(s)")
    print(f"  - {result[0]['filename']}" if result else "  (none)")
