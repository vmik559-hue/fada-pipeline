"""
FADA ETL Pipeline Configuration
Centralized configuration for the FADA press release PDF scraping and processing system.
"""

import os
from pathlib import Path

# Base paths
BASE_DIR = Path(__file__).parent.resolve()
DATA_DIR = BASE_DIR / "data"
PDF_DIR = DATA_DIR / "pdfs"
EXCEL_DIR = DATA_DIR / "excel"
OUTPUT_DIR = DATA_DIR / "output"

# Create directories if they don't exist
for directory in [DATA_DIR, PDF_DIR, EXCEL_DIR, OUTPUT_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# FADA Website Configuration
FADA_CONFIG = {
    'base_page_url': 'https://fada.in/press-release-list.php?page=',
    'base_site_url': 'https://fada.in/',
    'max_pages': 10,  # Maximum pages to scrape
    'request_timeout': 30,
    'request_headers': {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
}

# Month name patterns for filtering PDFs
MONTH_PATTERNS = {
    'january': 1, 'jan': 1,
    'february': 2, 'feb': 2,
    'march': 3, 'mar': 3,
    'april': 4, 'apr': 4,
    'may': 5,
    'june': 6, 'jun': 6,
    'july': 7, 'jul': 7,
    'august': 8, 'aug': 8,
    'september': 9, 'sep': 9,
    'october': 10, 'oct': 10,
    'november': 11, 'nov': 11,
    'december': 12, 'dec': 12
}

MONTH_NAMES = list(MONTH_PATTERNS.keys())

# Category mappings preserved from original code
CATEGORY_MAPPING = {
    '2W': ['2W', 'TWO WHEELER', 'TWO-WHEELER', '2-WHEELER'],
    '3W': ['3W', 'THREE WHEELER', 'THREE-WHEELER', '3-WHEELER'],
    'E-RICKSHAW(P)': ['E-RICKSHAW(P)', 'E-RICKSHAW (P)', 'ERICKSHAW(P)'],
    'E-RICKSHAW WITH CART (G)': ['E-RICKSHAW WITH CART (G)', 'E-RICKSHAW WITH CART(G)', 'ERICKSHAW WITH CART (G)'],
    'THREE - WHEELER (GOODS)': ['THREE - WHEELER (GOODS)', 'THREE-WHEELER (GOODS)', '3W (GOODS)'],
    'THREE - WHEELER (PASSENGER)': ['THREE - WHEELER (PASSENGER)', 'THREE-WHEELER (PASSENGER)', '3W (PASSENGER)'],
    'THREE - WHEELER (PERSONAL)': ['THREE - WHEELER (PERSONAL)', 'THREE-WHEELER (PERSONAL)', '3W (PERSONAL)'],
    'CV': ['CV', 'COMMERCIAL VEHICLE', 'COMMERCIAL VEHICLES'],
    'PV': ['PV', 'PASSENGER VEHICLE', 'PASSENGER VEHICLES', 'PASSENGER CAR'],
    'TRACTOR': ['TRACTOR', 'TRACTORS', 'TRAC'],
    'LCV': ['LCV', 'LIGHT COMMERCIAL VEHICLE'],
    'MCV': ['MCV', 'MEDIUM COMMERCIAL VEHICLE'],
    'HCV': ['HCV', 'HEAVY COMMERCIAL VEHICLE'],
    'OTHERS': ['OTHERS', 'OTHER'],
    'CE': ['CE'],
    'TOTAL': ['TOTAL', 'GRAND TOTAL', 'ALL']
}

# Standard category order for output
CATEGORY_ORDER = [
    '2W', '3W', 'E-RICKSHAW(P)', 'E-RICKSHAW WITH CART (G)',
    'THREE - WHEELER (GOODS)', 'THREE - WHEELER (PASSENGER)', 'THREE - WHEELER (PERSONAL)',
    'PV', 'TRACTOR', 'CV', 'LCV', 'MCV', 'HCV', 'OTHERS', 'CE', 'TOTAL'
]

# Table order for OEM data
TABLE_ORDER = [
    'Two Wheeler (2W)',
    'Two-Wheeler EV OEM',
    'Three Wheeler (3W)',
    'Three-Wheeler EV OEM',
    'Commercial Vehicle (CV)',
    'Commercial Vehicle EV OEM',
    'Passenger Vehicle (PV)',
    'PV EV OEM',
    'Tractor (TRAC)',
    'Construction Equipment OEM'
]

# Columns to remove during processing
COLUMNS_TO_REMOVE = ['MoM%', 'YoY%', 'yoy', 'Market Share (%)', 'Growth %']

# Concurrent download settings
DOWNLOAD_CONFIG = {
    'max_workers': 5,
    'retry_attempts': 3,
    'retry_delay': 2  # seconds
}

# Logging configuration
LOG_CONFIG = {
    'log_file': DATA_DIR / 'fada_pipeline.log',
    'log_level': 'INFO',
    'log_format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
}

# Cache file for tracking processed PDFs
CACHE_FILE = DATA_DIR / 'processed_cache.json'

# Master output file name pattern
MASTER_FILE_PATTERN = 'Master_FADA_Data_{year}_{month:02d}.xlsx'

# ============== GOOGLE SHEETS CONFIGURATION ==============
# For local development: use credentials.json file
# For Railway/production: set GOOGLE_SHEETS_CREDENTIALS_JSON as base64-encoded JSON

def _get_credentials_path():
    """Get the path to Google Sheets credentials.
    
    Priority:
    1. GOOGLE_SHEETS_CREDENTIALS_JSON env var (base64-encoded JSON) - for Railway
    2. GOOGLE_SHEETS_CREDENTIALS env var (file path)
    3. Local credentials.json file
    """
    import base64
    import tempfile
    
    # Check for base64-encoded credentials (Railway)
    b64_creds = os.environ.get('GOOGLE_SHEETS_CREDENTIALS_JSON')
    if b64_creds:
        try:
            # Decode and write to temp file
            creds_json = base64.b64decode(b64_creds).decode('utf-8')
            temp_file = Path(tempfile.gettempdir()) / 'gsheets_credentials.json'
            temp_file.write_text(creds_json)
            return str(temp_file)
        except Exception as e:
            print(f"Warning: Failed to decode GOOGLE_SHEETS_CREDENTIALS_JSON: {e}")
    
    # Check for file path
    creds_path = os.environ.get('GOOGLE_SHEETS_CREDENTIALS')
    if creds_path:
        return creds_path
    
    # Default to local file
    return str(BASE_DIR / 'credentials.json')


GOOGLE_SHEETS_CONFIG = {
    'credentials_file': _get_credentials_path(),
    'spreadsheet_id': os.environ.get('GOOGLE_SHEETS_ID', '14K0eRTkf-I62-QH4IHUTPNS7UQVQgJKKWs3u5F9lVjY'),
    'worksheet_name': os.environ.get('GOOGLE_SHEETS_WORKSHEET', 'Master Data'),
    'enabled': os.environ.get('ENABLE_GOOGLE_SHEETS', 'true').lower() == 'true'
}

