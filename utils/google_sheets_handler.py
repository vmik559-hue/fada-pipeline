"""
Google Sheets Handler for FADA ETL Pipeline
Implements incremental append logic for Google Sheets output.

Features:
- First run: Full data population
- Subsequent runs: Append only new rows/columns
- Batch operations for performance
- Metadata-only reads (no full data reload)
"""

import gspread
from google.oauth2.service_account import Credentials
from typing import Dict, List, Optional, Tuple, Set, Any
from pathlib import Path
import re
from datetime import datetime

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logger import get_logger


# Google Sheets API scopes
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]


class GoogleSheetsHandler:
    """
    Handles Google Sheets operations with incremental append logic.
    
    Key Features:
    - Detects existing data structure (headers, row labels)
    - Appends only new columns (timepoints) and rows (items)
    - Uses batch updates for performance
    - Never overwrites existing data
    """
    
    def __init__(self, credentials_file: str, spreadsheet_id: str, worksheet_name: str = "Master Data"):
        """
        Initialize the Google Sheets handler.
        
        Args:
            credentials_file: Path to the service account JSON credentials
            spreadsheet_id: The Google Sheets spreadsheet ID
            worksheet_name: Name of the worksheet to use/create
        """
        self.logger = get_logger()
        self.credentials_file = Path(credentials_file)
        self.spreadsheet_id = spreadsheet_id
        self.worksheet_name = worksheet_name
        self.client = None
        self.spreadsheet = None
        self.worksheet = None
        
    def connect(self) -> bool:
        """
        Establish connection to Google Sheets.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            if not self.credentials_file.exists():
                self.logger.error(f"Credentials file not found: {self.credentials_file}")
                return False
            
            credentials = Credentials.from_service_account_file(
                str(self.credentials_file),
                scopes=SCOPES
            )
            
            self.client = gspread.authorize(credentials)
            self.spreadsheet = self.client.open_by_key(self.spreadsheet_id)
            
            # Get or create worksheet
            try:
                self.worksheet = self.spreadsheet.worksheet(self.worksheet_name)
                self.logger.info(f"Connected to existing worksheet: {self.worksheet_name}")
            except gspread.WorksheetNotFound:
                self.worksheet = self.spreadsheet.add_worksheet(
                    title=self.worksheet_name,
                    rows=1000,
                    cols=100
                )
                self.logger.info(f"Created new worksheet: {self.worksheet_name}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Google Sheets: {e}")
            return False
    
    def get_sheet_metadata(self) -> Dict[str, Any]:
        """
        Fetch sheet metadata (headers and row labels only).
        
        This is a lightweight read that avoids loading full data.
        
        Returns:
            dict with:
                - is_empty: bool
                - existing_timepoints: list of column headers (timepoints)
                - existing_row_labels: list of row labels (column A)
                - header_row_index: int (1-indexed)
                - data_start_row: int (1-indexed)
        """
        try:
            # Get first row (headers)
            header_row = self.worksheet.row_values(1)
            
            if not header_row or len(header_row) == 0:
                return {
                    'is_empty': True,
                    'existing_timepoints': [],
                    'existing_row_labels': [],
                    'header_row_index': 1,
                    'data_start_row': 2
                }
            
            # Get column A (row labels)
            col_a = self.worksheet.col_values(1)
            
            # Extract timepoints (skip first column which is "Item" or similar)
            timepoints = [tp for tp in header_row[1:] if tp]
            
            # Extract row labels (skip header)
            row_labels = [label for label in col_a[1:] if label]
            
            return {
                'is_empty': len(timepoints) == 0 and len(row_labels) == 0,
                'existing_timepoints': timepoints,
                'existing_row_labels': row_labels,
                'header_row_index': 1,
                'data_start_row': 2,
                'total_rows': len(col_a),
                'total_cols': len(header_row)
            }
            
        except Exception as e:
            self.logger.error(f"Error reading sheet metadata: {e}")
            return {
                'is_empty': True,
                'existing_timepoints': [],
                'existing_row_labels': [],
                'header_row_index': 1,
                'data_start_row': 2
            }
    
    def detect_new_data(self, new_data: Dict[str, Dict[str, Any]], 
                        new_timepoints: List[str],
                        metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compare new data against existing sheet structure.
        
        Args:
            new_data: {row_label: {timepoint: value}}
            new_timepoints: List of all timepoints in new data
            metadata: Sheet metadata from get_sheet_metadata()
            
        Returns:
            dict with:
                - new_columns: list of new timepoints to add
                - new_rows: list of new row labels to add
                - is_first_run: bool
        """
        existing_timepoints = set(metadata['existing_timepoints'])
        existing_rows = set(metadata['existing_row_labels'])
        
        # Find new timepoints (columns to add on the right)
        new_columns = [tp for tp in new_timepoints if tp not in existing_timepoints]
        
        # Find new row labels (rows to add at bottom)
        new_rows = [label for label in new_data.keys() if label not in existing_rows]
        
        return {
            'new_columns': new_columns,
            'new_rows': new_rows,
            'is_first_run': metadata['is_empty'],
            'existing_timepoints': list(existing_timepoints),
            'existing_rows': list(existing_rows)
        }
    
    def write_full_data(self, data: Dict[str, Dict[str, Any]], 
                        timepoints: List[str],
                        progress_callback=None) -> bool:
        """
        Write full data to empty sheet (first run).
        
        Args:
            data: {row_label: {timepoint: value}}
            timepoints: Ordered list of timepoints
            progress_callback: Optional callback for progress updates
            
        Returns:
            True if successful
        """
        try:
            if progress_callback:
                progress_callback("Preparing data for Google Sheets...")
            
            # Build the data matrix
            rows = []
            
            # Header row
            header = ["Item"] + timepoints
            rows.append(header)
            
            # Data rows - sort with TOTAL last
            sorted_labels = sorted(data.keys(), 
                                   key=lambda x: (x.upper() == 'TOTAL' or x.upper() == 'TOTALS', x))
            
            for label in sorted_labels:
                row = [label]
                for tp in timepoints:
                    value = data[label].get(tp, '')
                    row.append(value if value is not None else '')
                rows.append(row)
            
            if progress_callback:
                progress_callback(f"Writing {len(rows)} rows to Google Sheets...")
            
            # Clear and write all data in one batch
            self.worksheet.clear()
            self.worksheet.update('A1', rows, value_input_option='RAW')
            
            # Format header row (bold)
            self.worksheet.format('1:1', {'textFormat': {'bold': True}})
            
            self.logger.info(f"Wrote {len(rows)} rows × {len(header)} columns to Google Sheets")
            return True
            
        except Exception as e:
            self.logger.error(f"Error writing full data to Sheets: {e}")
            return False
    
    def append_incremental(self, new_data: Dict[str, Dict[str, Any]],
                           all_timepoints: List[str],
                           delta: Dict[str, Any],
                           metadata: Dict[str, Any],
                           progress_callback=None) -> bool:
        """
        Append only new data incrementally (subsequent runs).
        
        Args:
            new_data: {row_label: {timepoint: value}}
            all_timepoints: All timepoints in new data
            delta: Result from detect_new_data()
            metadata: Sheet metadata
            progress_callback: Optional callback
            
        Returns:
            True if successful
        """
        try:
            new_columns = delta['new_columns']
            new_rows = delta['new_rows']
            
            if not new_columns and not new_rows:
                if progress_callback:
                    progress_callback("No new data to append - sheet is up to date")
                self.logger.info("No new data to append")
                return True
            
            batch_updates = []
            
            # === APPEND NEW COLUMNS ===
            if new_columns:
                if progress_callback:
                    progress_callback(f"Adding {len(new_columns)} new columns...")
                
                start_col = metadata['total_cols'] + 1
                
                # Add column headers
                for i, tp in enumerate(new_columns):
                    col_letter = self._col_num_to_letter(start_col + i)
                    batch_updates.append({
                        'range': f'{col_letter}1',
                        'values': [[tp]]
                    })
                
                # Add values for existing rows
                existing_rows = metadata['existing_row_labels']
                for row_idx, row_label in enumerate(existing_rows, start=2):
                    if row_label in new_data:
                        for col_idx, tp in enumerate(new_columns):
                            value = new_data[row_label].get(tp, '')
                            if value is not None and value != '':
                                col_letter = self._col_num_to_letter(start_col + col_idx)
                                batch_updates.append({
                                    'range': f'{col_letter}{row_idx}',
                                    'values': [[value]]
                                })
            
            # === APPEND NEW ROWS ===
            if new_rows:
                if progress_callback:
                    progress_callback(f"Adding {len(new_rows)} new rows...")
                
                start_row = metadata['total_rows'] + 1
                
                # Get column mapping
                all_cols = metadata['existing_timepoints'] + delta.get('new_columns', [])
                
                for row_offset, row_label in enumerate(new_rows):
                    current_row = start_row + row_offset
                    
                    # Add row label
                    batch_updates.append({
                        'range': f'A{current_row}',
                        'values': [[row_label]]
                    })
                    
                    # Add values for all columns
                    if row_label in new_data:
                        for col_idx, tp in enumerate(all_cols, start=2):
                            value = new_data[row_label].get(tp, '')
                            if value is not None and value != '':
                                col_letter = self._col_num_to_letter(col_idx)
                                batch_updates.append({
                                    'range': f'{col_letter}{current_row}',
                                    'values': [[value]]
                                })
            
            # Execute batch update
            if batch_updates:
                if progress_callback:
                    progress_callback(f"Executing {len(batch_updates)} updates...")
                
                # gspread batch_update expects a specific format
                self.worksheet.batch_update(batch_updates, value_input_option='RAW')
                
                self.logger.info(f"Appended {len(new_columns)} columns, {len(new_rows)} rows")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error appending incremental data: {e}")
            return False
    
    def sync_data(self, data: Dict[str, Dict[str, Any]], 
                  timepoints: List[str],
                  progress_callback=None) -> bool:
        """
        Main entry point: Sync data to Google Sheets with incremental logic.
        
        Args:
            data: {row_label: {timepoint: value}}
            timepoints: Ordered list of timepoints
            progress_callback: Optional callback for status updates
            
        Returns:
            True if successful
        """
        if not self.connect():
            return False
        
        if progress_callback:
            progress_callback("Reading existing sheet structure...")
        
        # Get current sheet state
        metadata = self.get_sheet_metadata()
        
        if metadata['is_empty']:
            # First run - write everything
            if progress_callback:
                progress_callback("Empty sheet detected - writing full data...")
            return self.write_full_data(data, timepoints, progress_callback)
        else:
            # Incremental append
            delta = self.detect_new_data(data, timepoints, metadata)
            
            if delta['new_columns'] or delta['new_rows']:
                if progress_callback:
                    progress_callback(f"Found {len(delta['new_columns'])} new columns, {len(delta['new_rows'])} new rows")
                return self.append_incremental(data, timepoints, delta, metadata, progress_callback)
            else:
                if progress_callback:
                    progress_callback("Sheet is already up to date")
                return True
    
    def _col_num_to_letter(self, col_num: int) -> str:
        """Convert column number (1-indexed) to letter(s)."""
        result = ""
        while col_num > 0:
            col_num -= 1
            result = chr(col_num % 26 + ord('A')) + result
            col_num //= 26
        return result


# ============== CONVENIENCE FUNCTION ==============
def sync_to_google_sheets(data: Dict[str, Dict[str, Any]],
                          timepoints: List[str],
                          credentials_file: str,
                          spreadsheet_id: str,
                          worksheet_name: str = "Master Data",
                          progress_callback=None) -> bool:
    """
    Convenience function to sync data to Google Sheets.
    
    Args:
        data: {row_label: {timepoint: value}}
        timepoints: Ordered list of timepoints
        credentials_file: Path to service account JSON
        spreadsheet_id: Google Sheets spreadsheet ID
        worksheet_name: Name of worksheet
        progress_callback: Optional callback for progress
        
    Returns:
        True if sync successful
    """
    handler = GoogleSheetsHandler(credentials_file, spreadsheet_id, worksheet_name)
    return handler.sync_data(data, timepoints, progress_callback)
