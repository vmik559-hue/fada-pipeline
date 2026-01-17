"""
Caching System for FADA ETL Pipeline
Tracks processed PDFs to avoid re-downloading and re-processing.
"""

import json
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List


class ProcessingCache:
    """
    Cache manager for tracking processed PDF files.
    Stores file hashes and processing metadata to avoid redundant work.
    """
    
    def __init__(self, cache_file: Path):
        """
        Initialize the cache.
        
        Args:
            cache_file: Path to the JSON cache file
        """
        self.cache_file = Path(cache_file)
        self.cache: Dict = self._load_cache()
    
    def _load_cache(self) -> Dict:
        """Load cache from disk or create empty cache."""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                return {'files': {}, 'metadata': {'last_updated': None}}
        return {'files': {}, 'metadata': {'last_updated': None}}
    
    def save(self) -> None:
        """Save cache to disk."""
        self.cache['metadata']['last_updated'] = datetime.now().isoformat()
        self.cache_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.cache_file, 'w', encoding='utf-8') as f:
            json.dump(self.cache, f, indent=2)
    
    def is_processed(self, filename: str) -> bool:
        """
        Check if a file has already been processed.
        
        Args:
            filename: Name of the PDF file
            
        Returns:
            True if file is in cache and marked as processed
        """
        return filename in self.cache['files'] and self.cache['files'][filename].get('processed', False)
    
    def is_downloaded(self, filename: str) -> bool:
        """
        Check if a file has already been downloaded.
        
        Args:
            filename: Name of the PDF file
            
        Returns:
            True if file is in cache and marked as downloaded
        """
        return filename in self.cache['files'] and self.cache['files'][filename].get('downloaded', False)
    
    def mark_downloaded(self, filename: str, url: str, file_path: Path) -> None:
        """
        Mark a file as downloaded.
        
        Args:
            filename: Name of the PDF file
            url: Source URL
            file_path: Local path where file is saved
        """
        if filename not in self.cache['files']:
            self.cache['files'][filename] = {}
        
        self.cache['files'][filename].update({
            'downloaded': True,
            'download_time': datetime.now().isoformat(),
            'url': url,
            'path': str(file_path)
        })
    
    def mark_processed(self, filename: str, excel_path: Path, month: int = None, year: int = None) -> None:
        """
        Mark a file as processed (converted to Excel).
        
        Args:
            filename: Name of the PDF file
            excel_path: Path to generated Excel file
            month: Extracted month (1-12)
            year: Extracted year
        """
        if filename not in self.cache['files']:
            self.cache['files'][filename] = {}
        
        self.cache['files'][filename].update({
            'processed': True,
            'process_time': datetime.now().isoformat(),
            'excel_path': str(excel_path),
            'month': month,
            'year': year
        })
    
    def mark_failed(self, filename: str, error: str) -> None:
        """
        Mark a file as failed to process.
        
        Args:
            filename: Name of the PDF file
            error: Error message
        """
        if filename not in self.cache['files']:
            self.cache['files'][filename] = {}
        
        self.cache['files'][filename].update({
            'failed': True,
            'error': error,
            'fail_time': datetime.now().isoformat()
        })
    
    def get_file_info(self, filename: str) -> Optional[Dict]:
        """Get cached info for a file."""
        return self.cache['files'].get(filename)
    
    def get_files_by_month_year(self, month: int, year: int) -> List[Dict]:
        """
        Get all cached files for a specific month/year.
        
        Args:
            month: Month (1-12)
            year: Year (e.g., 2024)
            
        Returns:
            List of file info dicts
        """
        results = []
        for filename, info in self.cache['files'].items():
            if info.get('month') == month and info.get('year') == year:
                results.append({'filename': filename, **info})
        return results
    
    def get_unprocessed_files(self) -> List[str]:
        """Get list of downloaded but not yet processed files."""
        return [
            filename for filename, info in self.cache['files'].items()
            if info.get('downloaded') and not info.get('processed') and not info.get('failed')
        ]
    
    def clear(self) -> None:
        """Clear the cache."""
        self.cache = {'files': {}, 'metadata': {'last_updated': None}}
        self.save()
    
    def get_stats(self) -> Dict:
        """Get cache statistics."""
        files = self.cache['files']
        return {
            'total_files': len(files),
            'downloaded': sum(1 for f in files.values() if f.get('downloaded')),
            'processed': sum(1 for f in files.values() if f.get('processed')),
            'failed': sum(1 for f in files.values() if f.get('failed')),
            'last_updated': self.cache['metadata'].get('last_updated')
        }
