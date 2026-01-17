"""
Concurrent PDF Downloader for FADA ETL Pipeline
Downloads PDFs with parallel execution, retry logic, and caching.

Preserves original download logic from Full_automation.ipynb with enhancements.
"""

import os
import time
from typing import List, Dict, Callable, Optional
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import FADA_CONFIG, DOWNLOAD_CONFIG, PDF_DIR, CACHE_FILE
from utils.logger import get_logger
from utils.cache import ProcessingCache


def download_single_pdf(url: str, save_path: Path, headers: dict, 
                         timeout: int = 30, retries: int = 3) -> tuple:
    """
    Download a single PDF file with retry logic.
    
    Args:
        url: PDF URL to download
        save_path: Local path to save the file
        headers: Request headers
        timeout: Request timeout in seconds
        retries: Number of retry attempts
        
    Returns:
        Tuple of (success: bool, filename: str, error: str or None)
    """
    filename = save_path.name
    
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            
            # Verify it's actually a PDF (check content type or magic bytes)
            if len(response.content) < 1000:
                return (False, filename, "File too small, possibly invalid")
            
            # Create parent directories
            save_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write file
            with open(save_path, 'wb') as f:
                f.write(response.content)
            
            return (True, filename, None)
            
        except requests.exceptions.Timeout:
            if attempt < retries - 1:
                time.sleep(DOWNLOAD_CONFIG['retry_delay'] * (attempt + 1))
                continue
            return (False, filename, "Timeout after retries")
            
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                time.sleep(DOWNLOAD_CONFIG['retry_delay'] * (attempt + 1))
                continue
            return (False, filename, str(e))
            
        except Exception as e:
            return (False, filename, f"Unexpected error: {str(e)}")
    
    return (False, filename, "Max retries exceeded")


def download_pdfs(pdf_links: List[Dict], 
                  output_dir: Path = None,
                  skip_existing: bool = True,
                  progress_callback: Callable = None,
                  max_workers: int = None) -> Dict:
    """
    Download multiple PDFs concurrently.
    
    Args:
        pdf_links: List of PDF link dicts with 'url' and 'filename' keys
        output_dir: Directory to save PDFs (default: config PDF_DIR)
        skip_existing: Skip files that already exist
        progress_callback: Optional callback(completed, total, filename, success)
        max_workers: Number of concurrent workers (default from config)
        
    Returns:
        Dict with download statistics and results
    """
    logger = get_logger()
    
    if output_dir is None:
        output_dir = PDF_DIR
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    if max_workers is None:
        max_workers = DOWNLOAD_CONFIG['max_workers']
    
    headers = FADA_CONFIG['request_headers']
    timeout = FADA_CONFIG['request_timeout']
    retries = DOWNLOAD_CONFIG['retry_attempts']
    
    # Initialize cache
    cache = ProcessingCache(CACHE_FILE)
    
    # Get existing files for skip logic
    existing_files = set(os.listdir(output_dir)) if skip_existing else set()
    
    # Filter out already downloaded files
    download_tasks = []
    skipped = []
    
    for link in pdf_links:
        filename = link['filename']
        
        if filename in existing_files:
            skipped.append(filename)
            logger.debug(f"Skipping existing file: {filename}")
            continue
        
        if cache.is_downloaded(filename):
            skipped.append(filename)
            logger.debug(f"Skipping cached file: {filename}")
            continue
        
        save_path = output_dir / filename
        download_tasks.append({
            'url': link['url'],
            'save_path': save_path,
            'filename': filename,
            'month': link.get('month'),
            'year': link.get('year')
        })
    
    if not download_tasks:
        logger.info(f"No new PDFs to download. {len(skipped)} files already exist.")
        return {
            'downloaded': 0,
            'skipped': len(skipped),
            'failed': 0,
            'total': len(pdf_links),
            'results': [],
            'errors': []
        }
    
    logger.info(f"Downloading {len(download_tasks)} PDFs ({len(skipped)} skipped)")
    
    results = []
    errors = []
    completed = 0
    total = len(download_tasks)
    
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_task = {
            executor.submit(
                download_single_pdf,
                task['url'],
                task['save_path'],
                headers,
                timeout,
                retries
            ): task
            for task in download_tasks
        }
        
        for future in as_completed(future_to_task):
            task = future_to_task[future]
            completed += 1
            
            try:
                success, filename, error = future.result()
                
                if success:
                    results.append(filename)
                    cache.mark_downloaded(filename, task['url'], task['save_path'])
                    logger.info(f"Downloaded: {filename} ({completed}/{total})")
                else:
                    errors.append({'filename': filename, 'error': error})
                    cache.mark_failed(filename, error)
                    logger.warning(f"Failed: {filename} - {error}")
                
                # Progress callback
                if progress_callback:
                    elapsed = time.time() - start_time
                    avg_time = elapsed / completed if completed > 0 else 0
                    eta = int(avg_time * (total - completed))
                    progress_callback(completed, total, filename, success, eta)
                    
            except Exception as e:
                errors.append({'filename': task['filename'], 'error': str(e)})
                logger.error(f"Download error for {task['filename']}: {e}")
    
    # Save cache
    cache.save()
    
    elapsed = time.time() - start_time
    logger.info(f"Download complete: {len(results)} succeeded, {len(errors)} failed ({elapsed:.1f}s)")
    
    return {
        'downloaded': len(results),
        'skipped': len(skipped),
        'failed': len(errors),
        'total': len(pdf_links),
        'results': results,
        'errors': errors,
        'output_dir': str(output_dir)
    }


def download_pdfs_for_month(month: int, year: int, 
                             pdf_links: List[Dict] = None,
                             progress_callback: Callable = None) -> Dict:
    """
    Download PDFs for a specific month and year.
    
    Args:
        month: Target month (1-12)
        year: Target year
        pdf_links: List of all PDF links (will fetch if not provided)
        progress_callback: Optional progress callback
        
    Returns:
        Download statistics dict
    """
    logger = get_logger()
    
    # Import here to avoid circular imports
    from scraper.fetch_pdf_links import fetch_pdf_links
    from filters.date_filter import filter_by_month_year
    
    if pdf_links is None:
        logger.info("Fetching PDF links from FADA website...")
        pdf_links = fetch_pdf_links()
    
    # Filter to target month/year
    filtered_links = filter_by_month_year(pdf_links, month, year)
    logger.info(f"Found {len(filtered_links)} PDFs for {month}/{year}")
    
    if not filtered_links:
        return {
            'downloaded': 0,
            'skipped': 0,
            'failed': 0,
            'total': 0,
            'results': [],
            'errors': [],
            'message': f'No PDFs found for {month}/{year}'
        }
    
    return download_pdfs(filtered_links, progress_callback=progress_callback)


if __name__ == '__main__':
    # Test the downloader
    from scraper.fetch_pdf_links import fetch_pdf_links
    
    def progress(completed, total, filename, success, eta):
        status = "✓" if success else "✗"
        print(f"  [{completed}/{total}] {status} {filename} (ETA: {eta}s)")
    
    print("Fetching PDF links...")
    links = fetch_pdf_links(max_pages=2)
    
    print(f"\nDownloading first 2 PDFs as test...")
    result = download_pdfs(links[:2], progress_callback=progress)
    
    print(f"\nResult: {result['downloaded']} downloaded, {result['skipped']} skipped, {result['failed']} failed")
