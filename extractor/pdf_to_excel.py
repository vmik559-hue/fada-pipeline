"""
PDF to Excel Extractor for FADA ETL Pipeline
Extracts tables from PDF files and converts them to Excel format.

This module preserves the exact table processing logic from Full_automation.ipynb.
"""

import re
import pandas as pd
import pdfplumber
from pathlib import Path
from typing import List, Tuple, Optional

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import COLUMNS_TO_REMOVE, EXCEL_DIR, PDF_DIR, CACHE_FILE
from utils.logger import get_logger
from utils.cache import ProcessingCache
from filters.date_filter import parse_month_year_from_filename


def extract_pdf_data(pdf_path: Path) -> Tuple[str, List[pd.DataFrame]]:
    """
    Extract tables and text from a PDF file.
    
    Preserves original logic from Full_automation.ipynb.
    
    Args:
        pdf_path: Path to PDF file
        
    Returns:
        Tuple of (full_text, list_of_dataframes)
    """
    logger = get_logger()
    full_text = ""
    all_tables = []
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                # Extract text
                text = page.extract_text()
                if text:
                    full_text += text.strip() + "\n\n"
                
                # Extract tables
                tables = page.extract_tables()
                for table in tables:
                    if table:
                        df = pd.DataFrame(table)
                        all_tables.append(df)
                        
    except Exception as e:
        logger.error(f"Error processing {pdf_path}: {e}")
    
    return full_text, all_tables


def process_tables(table_list: List[pd.DataFrame]) -> List[pd.DataFrame]:
    """
    Process and clean extracted tables.
    
    Preserves original logic from Full_automation.ipynb:
    - Set first row as header
    - Remove percentage columns
    - Remove unwanted columns (MoM%, YoY%, etc.)
    - Drop empty rows/columns
    
    Args:
        table_list: List of raw DataFrames from PDF extraction
        
    Returns:
        List of cleaned DataFrames
    """
    logger = get_logger()
    processed_tables = []
    
    for table_df in table_list:
        # Skip empty or malformed tables
        if table_df is None or table_df.empty or len(table_df) < 2:
            continue
        
        try:
            # Set first row as header
            table_df.columns = table_df.iloc[0]
            df = table_df.iloc[1:].reset_index(drop=True)
            
            # Remove unwanted columns
            for col in COLUMNS_TO_REMOVE:
                if col in df.columns:
                    df = df.drop(columns=[col])
            
            # Remove percentage columns using regex
            df = df.loc[:, ~df.columns.astype(str).str.contains(r'%|\(%\)', case=False, regex=True)]
            
            # Remove empty rows/columns
            df = df.dropna(how='all', axis=1)
            df = df.dropna(how='all', axis=0)
            
            # Handle special column renaming
            if len(df.columns) > 0 and df.columns[0] is not None:
                col0 = str(df.columns[0])
                if "Motor Vehicle Road Tax Collection" in col0:
                    df = df.rename(columns={df.columns[0]: "Motor Vehicle Road Tax Collection"})
            
            if not df.empty:
                processed_tables.append(df)
                
        except Exception as e:
            logger.debug(f"Error processing table: {e}")
            continue
    
    return processed_tables


def save_tables_to_excel(tables: List[pd.DataFrame], output_path: Path) -> bool:
    """
    Save processed tables to an Excel file.
    
    Args:
        tables: List of processed DataFrames
        output_path: Path for output Excel file
        
    Returns:
        True if successful, False otherwise
    """
    logger = get_logger()
    
    if not tables:
        logger.warning(f"No tables to save for {output_path}")
        return False
    
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            for i, table in enumerate(tables):
                sheet_name = f"Table_{i+1}"
                table.to_excel(writer, sheet_name=sheet_name, index=False)
        
        logger.debug(f"Saved {len(tables)} tables to {output_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error saving Excel file {output_path}: {e}")
        return False


def process_pdf_file(pdf_path: Path, output_dir: Path = None) -> Optional[Path]:
    """
    Process a single PDF file: extract tables and save to Excel.
    
    Args:
        pdf_path: Path to PDF file
        output_dir: Directory for output Excel files (default: config EXCEL_DIR)
        
    Returns:
        Path to generated Excel file, or None if failed
    """
    logger = get_logger()
    
    if output_dir is None:
        output_dir = EXCEL_DIR
    output_dir = Path(output_dir)
    
    pdf_path = Path(pdf_path)
    
    if not pdf_path.exists():
        logger.error(f"PDF file not found: {pdf_path}")
        return None
    
    # Generate output filename
    excel_filename = pdf_path.stem + "_tables.xlsx"
    output_path = output_dir / excel_filename
    
    # Extract data
    logger.info(f"Processing: {pdf_path.name}")
    text_content, table_list = extract_pdf_data(pdf_path)
    
    if not table_list:
        logger.warning(f"No tables found in {pdf_path.name}")
        return None
    
    # Process tables
    processed_tables = process_tables(table_list)
    
    if not processed_tables:
        logger.warning(f"No valid tables after processing in {pdf_path.name}")
        return None
    
    # Save to Excel
    if save_tables_to_excel(processed_tables, output_path):
        return output_path
    
    return None


def process_all_pdfs(pdf_dir: Path = None, 
                     output_dir: Path = None,
                     month: int = None, 
                     year: int = None) -> List[Path]:
    """
    Process all PDFs in a directory (optionally filtered by month/year).
    
    Args:
        pdf_dir: Directory containing PDFs (default: config PDF_DIR)
        output_dir: Directory for output files (default: config EXCEL_DIR)
        month: Optional month filter (1-12)
        year: Optional year filter
        
    Returns:
        List of generated Excel file paths
    """
    logger = get_logger()
    cache = ProcessingCache(CACHE_FILE)
    
    if pdf_dir is None:
        pdf_dir = PDF_DIR
    if output_dir is None:
        output_dir = EXCEL_DIR
    
    pdf_dir = Path(pdf_dir)
    output_dir = Path(output_dir)
    
    # Get all PDFs
    pdf_files = list(pdf_dir.glob("*.pdf"))
    
    if not pdf_files:
        logger.warning(f"No PDF files found in {pdf_dir}")
        return []
    
    # Filter by month/year if specified
    if month is not None and year is not None:
        filtered_files = []
        for pdf_file in pdf_files:
            parsed_month, parsed_year = parse_month_year_from_filename(pdf_file.name)
            if parsed_month == month and parsed_year == year:
                filtered_files.append(pdf_file)
        pdf_files = filtered_files
        logger.info(f"Filtered to {len(pdf_files)} PDFs for {month}/{year}")
    
    results = []
    
    for pdf_file in pdf_files:
        # Generate expected Excel output path
        excel_filename = pdf_file.stem + "_tables.xlsx"
        expected_excel_path = output_dir / excel_filename
        
        # FIXED: Verify Excel file actually exists, not just in cache
        # This fixes cloud deployment where cache exists but files don't
        if cache.is_processed(pdf_file.name):
            if expected_excel_path.exists():
                logger.debug(f"Skipping already processed: {pdf_file.name}")
                results.append(expected_excel_path)
                continue
            else:
                logger.info(f"Cache says {pdf_file.name} processed but Excel not found, re-processing")
        
        try:
            excel_path = process_pdf_file(pdf_file, output_dir)
            
            if excel_path:
                parsed_month, parsed_year = parse_month_year_from_filename(pdf_file.name)
                cache.mark_processed(pdf_file.name, excel_path, parsed_month, parsed_year)
                results.append(excel_path)
            else:
                cache.mark_failed(pdf_file.name, "No valid tables extracted")
                
        except Exception as e:
            logger.error(f"Failed to process {pdf_file.name}: {e}")
            cache.mark_failed(pdf_file.name, str(e))
    
    cache.save()
    logger.info(f"Processed {len(results)} PDF files successfully")
    
    return results


def validate_pdf(pdf_path: Path) -> dict:
    """
    Validate a PDF file and check for potential issues.
    
    Args:
        pdf_path: Path to PDF file
        
    Returns:
        Dict with validation results
    """
    result = {
        'valid': False,
        'pages': 0,
        'tables': 0,
        'errors': []
    }
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            result['pages'] = len(pdf.pages)
            
            table_count = 0
            for page in pdf.pages:
                tables = page.extract_tables()
                table_count += len(tables) if tables else 0
            
            result['tables'] = table_count
            result['valid'] = table_count > 0
            
            if table_count == 0:
                result['errors'].append("No tables found in PDF")
                
    except Exception as e:
        result['errors'].append(str(e))
    
    return result


if __name__ == '__main__':
    # Test the extractor
    import os
    
    print("Testing PDF extractor...")
    
    # Check if there are any PDFs in the PDF directory
    if PDF_DIR.exists():
        pdfs = list(PDF_DIR.glob("*.pdf"))
        if pdfs:
            print(f"Found {len(pdfs)} PDFs in {PDF_DIR}")
            
            # Test with first PDF
            test_pdf = pdfs[0]
            print(f"\nProcessing: {test_pdf.name}")
            
            result = process_pdf_file(test_pdf)
            if result:
                print(f"Success! Created: {result}")
            else:
                print("Failed to extract tables")
        else:
            print(f"No PDFs found in {PDF_DIR}")
    else:
        print(f"PDF directory does not exist: {PDF_DIR}")
