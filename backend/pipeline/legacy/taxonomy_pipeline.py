#!/usr/bin/env python3
"""
Taxonomy Pipeline for CSV/XLSX files.
Combines column detection from ai_cleaner with taxonomy processing from taxonomy_categorizer.
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import argparse
import os
from pathlib import Path
import time # Added for timing API calls

# Import from existing modules
from .ai_cleaner import DataCleaner
from .taxonomy_categorizer import TaxonomyCategorizer


class TaxonomyPipeline:
    """Pipeline for taxonomy categorization of CSV/XLSX files."""
    
    def __init__(self, batch_size: int = 100, use_external_api: bool = True):
        """Initialize the taxonomy pipeline."""
        # Set up logging
        logging.basicConfig(
            level=logging.INFO, 
            format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.data_cleaner = DataCleaner()
        
        self.use_external_api = use_external_api
        
        # Initialize both categorizers for flexibility
        self.external_categorizer = None
        self.local_categorizer = None
        
        # Initialize external API categorizer if requested
        if self.use_external_api:
            try:
                # Import and initialize external Taxonomizer
                from .taxonomizer import Taxonomizer
                self.external_categorizer = Taxonomizer(batch_size=batch_size)
                self.logger.info("External API taxonomy categorizer initialized")
            except ImportError:
                self.logger.warning("External Taxonomizer not available, falling back to local")
                self.use_external_api = False
        
        # Initialize local taxonomy categorizer
        try:
            # Load the taxonomy data first
            taxonomy_file = "data/SALT Taxonomy.csv"
            if os.path.exists(taxonomy_file):
                taxonomy_df = pd.read_csv(taxonomy_file)
                self.local_categorizer = TaxonomyCategorizer(taxonomy_df)
                self.logger.info("Local taxonomy categorizer initialized")
            else:
                self.logger.warning(f"Taxonomy file not found: {taxonomy_file}")
        except Exception as e:
            self.logger.warning(f"Could not initialize local taxonomy categorizer: {e}")
        
        # Set the primary categorizer based on preference
        if self.use_external_api and self.external_categorizer:
            self.taxonomy_categorizer = self.external_categorizer
        else:
            self.taxonomy_categorizer = self.local_categorizer
        
        self.logger.info("Taxonomy Pipeline initialized successfully")
    
    def detect_columns(self, df: pd.DataFrame) -> Tuple[str, str]:
        """
        Detect name and description columns using ai_cleaner's column detection.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Tuple of (name_column, description_column)
        """
        self.logger.info("Detecting name and description columns...")
        
        try:
            # Get column names as a list
            columns = list(df.columns)
            self.logger.info(f"Available columns: {columns}")
            
            # Check if first row contains actual column headers
            first_row_values = df.iloc[0].values
            self.logger.info(f"First row values: {first_row_values}")
            
            # Look for actual column headers in the first row
            name_column = None
            description_column = None
            
            # Check if first row contains column headers
            for i, value in enumerate(first_row_values):
                if pd.notna(value):
                    value_str = str(value).strip().lower()
                    if any(keyword in value_str for keyword in ['product code', 'code', 'name', 'product', 'item', 'title']):
                        name_column = columns[i]
                        self.logger.info(f"Found name column from first row: {name_column} (value: '{value}')")
                        break  # Use the first matching column (Product Code)
                    elif any(keyword in value_str for keyword in ['description', 'desc', 'details', 'specs']):
                        description_column = columns[i]
                        self.logger.info(f"Found description column from first row: {description_column} (value: '{value}')")
            
            # If we found headers in first row, use them
            if name_column and description_column:
                self.logger.info(f"Using columns from first row - Name: {name_column}, Description: {description_column}")
                return name_column, description_column
            
            # Set the DataFrame on the DataCleaner instance for AI detection
            self.data_cleaner.df = df
            
            # Try AI-based column detection first
            try:
                name_column = self.data_cleaner._find_column_with_ai(
                    columns, "product name", "name", "product", "item", "title"
                )
                self.logger.info(f"AI detected name column: {name_column}")
            except Exception as e:
                self.logger.warning(f"AI name column detection failed: {e}")
                name_column = None
            
            try:
                description_column = self.data_cleaner._find_column_with_ai(
                    columns, "product description", "description", "desc", "details", "specs"
                )
                self.logger.info(f"AI detected description column: {description_column}")
            except Exception as e:
                self.logger.warning(f"AI description column detection failed: {e}")
                description_column = None
            
            # Fallback: look for common name column patterns
            if not name_column or name_column == "NONE":
                name_candidates = [col for col in df.columns if any(
                    keyword in col.lower() for keyword in ['name', 'product', 'item', 'title']
                )]
                if name_candidates:
                    name_column = name_candidates[0]
                    self.logger.info(f"Using fallback name column: {name_column}")
                else:
                    # Last resort: use first column that's not completely empty
                    for col in df.columns:
                        if df[col].notna().any() and df[col].astype(str).str.strip().ne('').any():
                            name_column = col
                            self.logger.info(f"Using first non-empty column as name: {name_column}")
                            break
                    
                    if not name_column:
                        raise ValueError("Could not find suitable name column")
            
            # Fallback: look for common description column patterns
            if not description_column or description_column == "NONE":
                # First priority: look for Description_Expanded column
                if 'Description_Expanded' in df.columns:
                    description_column = 'Description_Expanded'
                    self.logger.info(f"Found Description_Expanded column: {description_column}")
                else:
                    desc_candidates = [col for col in df.columns if any(
                        keyword in col.lower() for keyword in ['description', 'desc', 'details', 'specs']
                    )]
                    if desc_candidates:
                        description_column = desc_candidates[0]
                        self.logger.info(f"Using fallback description column: {description_column}")
                    else:
                        # If no description column, use name column as both
                        description_column = name_column
                        self.logger.info(f"No description column found, using name column: {description_column}")
            
            self.logger.info(f"Final detected columns - Name: {name_column}, Description: {description_column}")
            return name_column, description_column
            
        except Exception as e:
            self.logger.error(f"Error in column detection: {e}")
            raise
    
    def prepare_taxonomy_data(self, df: pd.DataFrame, name_col: str, desc_col: str) -> Tuple[List[Dict[str, str]], List[int]]:
        """
        Prepare data for taxonomy processing.
        
        Args:
            df: Input DataFrame
            name_col: Name column name
            desc_col: Description column name (should be Description_cleaned)
            
        Returns:
            Tuple of (taxonomy_data, valid_row_indices) where:
            - taxonomy_data: List of dictionaries with 'name' and 'description' keys
            - valid_row_indices: List of row indices that have valid data
        """
        self.logger.info("Preparing data for taxonomy processing...")
        self.logger.info("Using pre-cleaned descriptions from Description_cleaned column...")
        self.logger.info("Enhancing descriptions with AI for better taxonomy categorization...")
        
        taxonomy_data = []
        valid_row_indices = []  # Store indices of rows with valid data
        enhanced_descriptions = []  # Store enhanced descriptions for analysis
        enhancement_examples = []
        
        # Counters for tracking what's being processed vs skipped
        skipped_existing_taxonomy = 0
        skipped_no_name = 0
        processed_count = 0
        
        # Check if first row contains headers by looking at the values
        first_row_values = df.iloc[0].values
        has_headers_in_first_row = any(
            pd.notna(val) and any(keyword in str(val).lower() for keyword in 
                                ['product', 'pack', 'size', 'brand', 'description', 'code'])
            for val in first_row_values
        )
        
        start_row = 1 if has_headers_in_first_row else 0
        self.logger.info(f"First row contains headers: {has_headers_in_first_row}, starting from row {start_row}")
        
        for idx in range(start_row, len(df)):
            row = df.iloc[idx]
            name = str(row.get(name_col, "")).strip()
            
            # Use Description_cleaned column directly since descriptions are already cleaned
            description = str(row.get('Description_cleaned', "")).strip()
            
            # Skip rows with no name
            if not name or name.lower() in ['nan', 'none', '']:
                skipped_no_name += 1
                continue
            
            # Skip rows that already have taxonomy data from GTIN validation process
            gtin_category = str(row.get('gtin_category', "")).strip()
            gtin_subcategory = str(row.get('gtin_subcategory', "")).strip()
            gtin_subsubcategory = str(row.get('gtin_subsubcategory', "")).strip()
            
            # Check for any existing taxonomy columns (from GTIN validation or previous taxonomy processing)
            has_gtin_taxonomy = (gtin_category and gtin_category.lower() not in ['nan', 'none', ''] and
                                gtin_subcategory and gtin_subcategory.lower() not in ['nan', 'none', ''] and
                                gtin_subsubcategory and gtin_subsubcategory.lower() not in ['nan', 'none', ''])
            
            # Check for other common taxonomy column names
            has_other_taxonomy = False
            taxonomy_columns = ['category', 'subcategory', 'subsubcategory', 'taxonomy_level1', 'taxonomy_level2', 'taxonomy_level3']
            for col in taxonomy_columns:
                if col in df.columns:
                    value = str(row.get(col, "")).strip()
                    if value and value.lower() not in ['nan', 'none', '']:
                        has_other_taxonomy = True
                        break
            
            if has_gtin_taxonomy or has_other_taxonomy:
                skipped_existing_taxonomy += 1
                continue  # Skip rows that already have taxonomy data
            
            # If no cleaned description, fall back to original description column
            if not description or description.lower() in ['nan', 'none', '']:
                description = str(row.get(desc_col, "")).strip()
                if not description or description.lower() in ['nan', 'none', '']:
                    description = name
            
            # Clean and enhance the name (keep this as it's still useful)
            try:
                cleaned_name = self._clean_and_enhance_name(name)
                if cleaned_name != name:
                    self.logger.debug(f"Cleaned name: '{name}' → '{cleaned_name}'")
                    name = cleaned_name
            except Exception as e:
                self.logger.warning(f"Failed to clean name '{name}': {e}")
            
            # Skip description cleaning since it's already cleaned from GTIN pipeline
            # Just enhance the description to be more descriptive using AI
            enhanced_description = description  # Default to current cleaned description
            try:
                enhanced_description = self._enhance_description_with_ai(name, description)
                if enhanced_description != description:
                    self.logger.debug(f"Enhanced description: '{description}' → '{enhanced_description}'")
                    
                    # Collect examples of enhancement for logging (first 3 only)
                    if len(enhancement_examples) < 3:
                        enhancement_examples.append(f"'{description}' → '{enhanced_description}'")
            except Exception as e:
                self.logger.warning(f"Failed to enhance description '{description}': {e}")
                # Continue with expanded description if enhancement fails
            
            # Store enhanced description for analysis (but don't use in taxonomy data)
            enhanced_descriptions.append(enhanced_description)
            
            # Use original expanded description for taxonomy processing (not enhanced)
            taxonomy_data.append({
                'name': name,
                'description': description  # Keep original expanded description
            })
            valid_row_indices.append(idx)  # Store the row index
            processed_count += 1
        
        self.logger.info(f"Prepared {len(taxonomy_data)} items for taxonomy processing (skipped {start_row} header rows)")
        self.logger.info(f"Processing summary:")
        self.logger.info(f"  - Items processed: {processed_count}")
        self.logger.info(f"  - Items skipped (existing taxonomy): {skipped_existing_taxonomy}")
        self.logger.info(f"  - Items skipped (no name): {skipped_no_name}")
        self.logger.info(f"Name and description enhancement completed for {len(taxonomy_data)} items")
        
        # Show enhancement examples if any
        if enhancement_examples:
            self.logger.info("Sample description enhancements:")
            for example in enhancement_examples:
                self.logger.info(f"  {example}")
        else:
            self.logger.info("No descriptions needed enhancement")
        
        # Log enhanced descriptions for analysis (first 3 only)
        if enhanced_descriptions:
            self.logger.info("Enhanced descriptions for analysis (first 3):")
            for i, enhanced_desc in enumerate(enhanced_descriptions[:3]):
                self.logger.info(f"  {i+1}. {enhanced_desc[:100]}{'...' if len(enhanced_desc) > 100 else ''}")
        
        return taxonomy_data, valid_row_indices
    
    def process_file(self, input_file: str, output_file: str = None, limit: int = None) -> pd.DataFrame:
        """
        Process a CSV or XLSX file for taxonomy categorization.
        
        Args:
            input_file: Path to input CSV or XLSX file
            output_file: Path to output CSV file (optional)
            limit: Maximum number of rows to process (optional)
            
        Returns:
            DataFrame with taxonomy results
        """
        self.logger.info(f"Processing file: {input_file}")
        
        try:
            # Load the file
            if input_file.lower().endswith('.csv'):
                # Try to detect the CSV format and handle potential issues
                try:
                    df = pd.read_csv(input_file)
                except Exception as e:
                    self.logger.warning(f"Standard CSV loading failed: {e}")
                    # Try with different parameters
                    try:
                        df = pd.read_csv(input_file, encoding='utf-8', on_bad_lines='skip')
                        self.logger.info("CSV loaded with encoding='utf-8' and on_bad_lines='skip'")
                    except Exception as e2:
                        self.logger.warning(f"UTF-8 CSV loading failed: {e2}")
                        # Last resort: try with latin-1 encoding
                        df = pd.read_csv(input_file, encoding='latin-1', on_bad_lines='skip')
                        self.logger.info("CSV loaded with encoding='latin-1' and on_bad_lines='skip'")
            elif input_file.lower().endswith(('.xlsx', '.xls')):
                df = pd.read_excel(input_file)
            else:
                raise ValueError("Unsupported file format. Please use CSV or XLSX.")
            
            self.logger.info(f"Loaded file with {len(df)} rows and {len(df.columns)} columns")
            self.logger.info(f"Columns: {list(df.columns)}")
            
            # Check for header rows and skip them
            header_rows_to_skip = 0
            for i, row in df.iterrows():
                first_cell = str(row.iloc[0]).strip()
                if any(keyword in first_cell.lower() for keyword in ['report', 'drill down', 'only the top', 'sum of']):
                    header_rows_to_skip = i + 1
                    continue
                elif first_cell.lower() == 'product':
                    # Found the actual header row
                    header_rows_to_skip = i
                    break
                else:
                    # Found actual data
                    break
            
            if header_rows_to_skip > 0:
                self.logger.info(f"Skipping {header_rows_to_skip} header rows")
                df = df.iloc[header_rows_to_skip:].reset_index(drop=True)
                self.logger.info(f"After skipping headers: {len(df)} rows")
            
            # Show sample data for debugging
            self.logger.info("Sample data (first 3 rows):")
            for i in range(min(3, len(df))):
                self.logger.info(f"Row {i}: {dict(df.iloc[i])}")
            
            # Apply row limit if specified
            if limit:
                original_count = len(df)
                df = df.head(limit)
                self.logger.info(f"Limited processing to first {len(df)} rows (from {original_count} total)")
                print(f"Processing limited to first {len(df)} rows (from {original_count} total)")
            
            # Detect name and description columns
            name_col, desc_col = self.detect_columns(df)
            
            # Prepare data for taxonomy
            taxonomy_data, valid_row_indices = self.prepare_taxonomy_data(df, name_col, desc_col)
            
            if not taxonomy_data:
                raise ValueError("No valid data found for taxonomy processing")
            
            # Process taxonomy
            self.logger.info("Processing taxonomy categorization...")
            print(f"Taxonomy Processing Method: {'EXTERNAL API' if self.use_external_api else 'LOCAL PROCESSING'}")
            
            # Initialize timing variables
            api_duration = None
            local_duration = None
            final_taxonomy_results = None
            taxonomy_results = None  # Initialize taxonomy_results
            
            if self.use_external_api:
                # Check if we have an external API categorizer available
                if hasattr(self.taxonomy_categorizer, 'request_url'):
                    try:
                        self.logger.info("ATTEMPTING EXTERNAL API CALL...")
                        self.logger.info(f"API Endpoint: {self.taxonomy_categorizer.request_url}")
                        self.logger.info(f"Batch Size: {getattr(self.taxonomy_categorizer, 'batch_size', 'N/A')}")
                        self.logger.info(f"Items to Process: {len(taxonomy_data)}")
                        
                        # Log the first few items being sent to API
                        sample_items = taxonomy_data[:3]
                        self.logger.info(f"Sample items being sent to API:")
                        for i, item in enumerate(sample_items):
                            self.logger.info(f"  Item {i+1}: Name='{item.get('name', '')[:50]}...', Description='{item.get('description', '')[:50]}...'")
                        
                        # Make the API call
                        start_time = time.time()
                        taxonomy_results = self.taxonomy_categorizer.taxonomize_batch(taxonomy_data)
                        end_time = time.time()
                        
                        api_duration = end_time - start_time
                        self.logger.info(f"EXTERNAL API CALL SUCCESSFUL!")
                        self.logger.info(f"API Response Time: {api_duration:.2f} seconds")
                        self.logger.info(f"API Results Received: {len(taxonomy_results)} items")
                        
                        # Log sample API results
                        sample_results = taxonomy_results[:3]
                        self.logger.info(f"Sample API Results:")
                        for i, result in enumerate(sample_results):
                            self.logger.info(f"  Result {i+1}: Category='{result.get('category', '')}', Subcategory='{result.get('subcategory', '')}', Subsubcategory='{result.get('subsubcategory', '')}'")
                        
                        # Check if any results are N/A or empty and need fallback to local processing
                        na_count = sum(1 for result in taxonomy_results if any(
                            value == "N/A" or value == "" for value in result.values()
                        ))
                        
                        if na_count == 0:
                            # All results are valid - use external API results directly
                            print(f"EXTERNAL API SUCCESS: Processed {len(taxonomy_results)} items in {api_duration:.2f}s")
                            final_taxonomy_results = taxonomy_results
                        else:
                            # Some results are N/A, use selective fallback - only process N/A items locally
                            self.logger.warning(f"{na_count}/{len(taxonomy_results)} items returned N/A from external API, using selective local fallback")
                            print(f"{na_count}/{len(taxonomy_results)} items returned N/A from external API, using selective local fallback")
                            
                            # Try to process N/A items locally while keeping successful API results
                            try:
                                # Identify items that need local processing
                                items_needing_local = [i for i, result in enumerate(taxonomy_results) if any(value == "N/A" for value in result.values())]
                                
                                if items_needing_local and self.local_categorizer:
                                    print(f"Processing {len(items_needing_local)} N/A items locally while keeping {len(taxonomy_results) - len(items_needing_local)} successful API results")
                                    
                                    # Process only the N/A items locally
                                    try:
                                        # Get the data for items that need local processing
                                        local_taxonomy_data = [taxonomy_data[i] for i in items_needing_local]
                                        
                                        # Process these items locally
                                        local_results = self.process_taxonomy_local(local_taxonomy_data)
                                        
                                        # Merge local results back into the external API results
                                        print(f"DEBUG: Starting merge of {len(local_results)} local results into taxonomy_results")
                                        print(f"DEBUG: First local result structure: {local_results[0] if local_results else 'No results'}")
                                        print(f"DEBUG: First local result keys: {list(local_results[0].keys()) if local_results else 'No results'}")
                                        
                                        for i, local_result in zip(items_needing_local, local_results):
                                            # Convert local result format to external API format
                                            old_value = taxonomy_results[i]
                                            print(f"DEBUG: Processing item {i}, local_result: {local_result}")
                                            print(f"DEBUG: local_result keys: {list(local_results[0].keys())}")
                                            print(f"DEBUG: Taxo1 value: {local_result.get('Taxo1', 'NOT_FOUND')}")
                                            print(f"DEBUG: Taxo2 value: {local_result.get('Taxo2', 'NOT_FOUND')}")
                                            print(f"DEBUG: Taxo3 value: {local_result.get('Taxo3', 'NOT_FOUND')}")
                                            
                                            new_value = {
                                                'category': local_result.get('Taxo1', ''),
                                                'subcategory': local_result.get('Taxo2', ''),
                                                'subsubcategory': local_result.get('Taxo3', '')
                                            }
                                            taxonomy_results[i] = new_value
                                            print(f"DEBUG: Merged item {i}: {old_value} → {new_value}")
                                        
                                        print(f"Selective fallback completed: {len(items_needing_local)} items processed locally, {len(taxonomy_results) - len(items_needing_local)} items kept from API")
                                        
                                        # Debug: Check what we have after selective fallback
                                        non_na_count = sum(1 for result in taxonomy_results if not any(value == "N/A" or value == "" for value in result.values()))
                                        print(f"DEBUG: After selective fallback - {non_na_count} non-N/A results, {len(taxonomy_results)} total results")
                                        
                                        final_taxonomy_results = taxonomy_results
                                        
                                    except Exception as e:
                                        self.logger.warning(f"Selective local fallback failed: {e}, using external API results as-is")
                                        print(f"Selective local fallback failed: {e}, using external API results as-is")
                                        final_taxonomy_results = taxonomy_results
                                else:
                                    # No local categorizer available, use API results as-is
                                    print(f"No local categorizer available, using API results with {na_count} N/A items")
                                    final_taxonomy_results = taxonomy_results
                            except Exception as e:
                                self.logger.warning(f"Selective fallback processing failed: {e}, using external API results as-is")
                                print(f"Selective fallback processing failed: {e}, using external API results as-is")
                                final_taxonomy_results = taxonomy_results
                        
                        # Ensure taxonomy_results is available for DataFrame creation
                        if final_taxonomy_results is not None:
                            taxonomy_results = final_taxonomy_results
                        else:
                            # Only fall back to local processing if external API completely failed
                            if self.local_categorizer:
                                self.logger.info("FALLING BACK TO LOCAL TAXONOMY PROCESSING...")
                                print(f"FALLING BACK TO LOCAL TAXONOMY PROCESSING...")
                                
                                local_start_time = time.time()
                                try:
                                    taxonomy_results = self.process_taxonomy_local(taxonomy_data)
                                    local_end_time = time.time()
                                    local_duration = local_end_time - local_start_time
                                    
                                    self.logger.info("LOCAL TAXONOMY PROCESSING COMPLETED!")
                                    print(f"LOCAL TAXONOMY PROCESSING COMPLETED!")
                                    print(f"Local Processing Time: {local_duration:.2f} seconds")
                                    print(f"Local Results Generated: {len(taxonomy_results)} items")
                                    
                                    # Show sample results
                                    if taxonomy_results:
                                        print(f"Sample Local Results:")
                                        for i, result in enumerate(taxonomy_results[:3]):
                                            print(f"  Result {i+1}: Taxo1='{result.get('Taxo1', '')}', Taxo2='{result.get('Taxo2', '')}', Taxo3='{result.get('Taxo3', '')}'")
                                    
                                    print(f"LOCAL PROCESSING COMPLETE: Processed {len(taxonomy_results)} items in {local_duration:.2f}s")
                                    
                                    # Use local results
                                    final_taxonomy_results = taxonomy_results
                                    
                                except Exception as e:
                                    self.logger.error(f"Local taxonomy processing failed: {e}")
                                    print(f"LOCAL PROCESSING FAILED: {e}")
                                    # Return empty results if both external and local fail
                                    final_taxonomy_results = [{"Taxo1": "", "Taxo2": "", "Taxo3": ""} for _ in range(len(taxonomy_data))]
                            else:
                                self.logger.error("No fallback taxonomy categorizer available")
                                print(f"NO FALLBACK AVAILABLE: Both external API and local processing failed")
                                # Return empty results
                                final_taxonomy_results = [{"Taxo1": "", "Taxo2": "", "Taxo3": ""} for _ in range(len(taxonomy_data))]
                    
                    except Exception as e:
                        self.logger.warning(f"EXTERNAL API FAILED: {e}")
                        print(f"EXTERNAL API FAILED: {e}")
                        # Fall through to local processing
                        final_taxonomy_results = None
                        
                        # Try local processing as fallback
                        if self.local_categorizer:
                            self.logger.info("FALLING BACK TO LOCAL TAXONOMY PROCESSING...")
                            print(f"FALLING BACK TO LOCAL TAXONOMY PROCESSING...")
                            
                            local_start_time = time.time()
                            try:
                                taxonomy_results = self.process_taxonomy_local(taxonomy_data)
                                local_end_time = time.time()
                                local_duration = local_end_time - local_start_time
                                
                                self.logger.info("LOCAL TAXONOMY PROCESSING COMPLETED!")
                                print(f"LOCAL TAXONOMY PROCESSING COMPLETED!")
                                print(f"Local Processing Time: {local_duration:.2f} seconds")
                                print(f"Local Results Generated: {len(taxonomy_results)} items")
                                
                                # Show sample results
                                if taxonomy_results:
                                    print(f"Sample Local Results:")
                                    for i, result in enumerate(taxonomy_results[:3]):
                                        print(f"  Result {i+1}: Taxo1='{result.get('Taxo1', '')}', Taxo2='{result.get('Taxo2', '')}', Taxo3='{result.get('Taxo3', '')}'")
                                
                                print(f"LOCAL PROCESSING COMPLETE: Processed {len(taxonomy_results)} items in {local_duration:.2f}s")
                                
                                # Use local results
                                final_taxonomy_results = taxonomy_results
                                
                            except Exception as local_e:
                                self.logger.error(f"Local taxonomy processing failed: {local_e}")
                                print(f"LOCAL PROCESSING FAILED: {local_e}")
                                # Return empty results if both external and local fail
                                final_taxonomy_results = [{"Taxo1": "", "Taxo2": "", "Taxo3": ""} for _ in range(len(taxonomy_data))]
                        else:
                            self.logger.error("No fallback taxonomy categorizer available")
                            print(f"NO FALLBACK AVAILABLE: Both external API and local processing failed")
                            # Return empty results
                            final_taxonomy_results = [{"Taxo1": "", "Taxo2": "", "Taxo3": ""} for _ in range(len(taxonomy_data))]
            else:
                # Use local processing directly
                if self.taxonomy_categorizer:
                    self.logger.info("USING LOCAL TAXONOMY PROCESSING (API disabled)")
                    print(f"USING LOCAL TAXONOMY PROCESSING (API disabled)")
                    start_time = time.time()
                    taxonomy_results = self.process_taxonomy_local(taxonomy_data)
                    end_time = time.time()
                    local_duration = end_time - start_time
                else:
                    raise ValueError("Local taxonomy categorizer not available")
            
            # Create result DataFrame by preserving original structure and adding taxonomy columns
            result_df = df.copy()  # Keep all original columns and data
            
            # Ensure GTIN columns exist in result_df
            if 'gtin_category' not in result_df.columns:
                result_df['gtin_category'] = ''
            if 'gtin_subcategory' not in result_df.columns:
                result_df['gtin_subcategory'] = ''
            if 'gtin_subsubcategory' not in result_df.columns:
                result_df['gtin_subsubcategory'] = ''
            
            # Ensure taxonomy_results is available
            if taxonomy_results is None:
                raise ValueError("No taxonomy results available - processing failed")
            
            print(f"DEBUG: Creating DataFrame with {len(taxonomy_results)} taxonomy results")
            
            # Debug: Check the actual content of taxonomy_results
            non_na_count = sum(1 for result in taxonomy_results if not any(value == "N/A" or value == "" for value in result.values()))
            print(f"DEBUG: Final taxonomy_results contains {non_na_count} non-N/A results out of {len(taxonomy_results)} total")
            print(f"DEBUG: First 3 results:")
            for i, result in enumerate(taxonomy_results[:3]):
                print(f"  Result {i}: {result}")
            
            # Create a mapping from valid row indices to taxonomy results
            # Check if GTIN columns exist, create them if they don't
            if 'gtin_category' not in df.columns:
                df['gtin_category'] = ''
                df['gtin_subcategory'] = ''
                df['gtin_subsubcategory'] = ''
                self.logger.info("Created missing GTIN columns for taxonomy processing")
            
            # Initialize all rows with existing GTIN taxonomy data (don't overwrite existing data)
            taxo1_list = df['gtin_category'].fillna('').astype(str).tolist()
            taxo2_list = df['gtin_subcategory'].fillna('').astype(str).tolist()
            taxo3_list = df['gtin_subsubcategory'].fillna('').astype(str).tolist()
            
            # Fill in the taxonomy results for all rows
            print(f"DEBUG: Mapping {len(taxonomy_results)} taxonomy results to {len(df)} rows")
            print(f"DEBUG: First 3 taxonomy results:")
            for i, result in enumerate(taxonomy_results[:3]):
                print(f"  Result {i}: {result}")
            
            for i, result in enumerate(taxonomy_results):
                if i < len(df):
                    # Handle both external API format (category, subcategory, subsubcategory) 
                    # and local format (Taxo1, Taxo2, Taxo3)
                    taxo1 = result.get('category', result.get('Taxo1', ''))
                    taxo2 = result.get('subcategory', result.get('Taxo2', ''))
                    taxo3 = result.get('subsubcategory', result.get('Taxo3', ''))
                    
                    print(f"DEBUG: Row {i}: mapping {taxo1} | {taxo2} | {taxo3}")
                    
                    taxo1_list[i] = taxo1
                    taxo2_list[i] = taxo2
                    taxo3_list[i] = taxo3
            
            # Assign the lists to the DataFrame using GTIN column names
            result_df['gtin_category'] = taxo1_list
            result_df['gtin_subcategory'] = taxo2_list
            result_df['gtin_subsubcategory'] = taxo3_list
            
            # Save output if specified
            if output_file:
                result_df.to_csv(output_file, index=False)
                self.logger.info(f"Results saved to: {output_file}")
            
            # Print final summary of which method was used
            if self.use_external_api and api_duration is not None:
                print(f"\nTAXONOMY PROCESSING SUMMARY:")
                print(f"   Method: EXTERNAL API")
                print(f"   Time: {api_duration:.2f} seconds")
                print(f"   Items: {len(taxonomy_results)} processed")
                if hasattr(self.taxonomy_categorizer, 'request_url'):
                    print(f"   Endpoint: {self.taxonomy_categorizer.request_url}")
                else:
                    print(f"   Endpoint: External API (no URL available)")
                print(f"   Output: {output_file if output_file else 'Not saved'}")
            elif not self.use_external_api and local_duration is not None:
                print(f"\nTAXONOMY PROCESSING SUMMARY:")
                print(f"   Method: LOCAL PROCESSING")
                print(f"   Time: {local_duration:.2f} seconds")
                print(f"   Items: {len(taxonomy_results)} processed")
                print(f"   Source: data/SALT Taxonomy.csv")
                print(f"   Output: {output_file if output_file else 'Not saved'}")
            else:
                print(f"\nTAXONOMY PROCESSING SUMMARY:")
                print(f"   Method: UNKNOWN (check logs)")
                print(f"   Items: {len(taxonomy_results)} processed")
                print(f"   Output: {output_file if output_file else 'Not saved'}")
            
            self.logger.info("Taxonomy processing completed successfully")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error processing file: {e}")
            raise
    
    def process_dataframe(self, df: pd.DataFrame, output_file: str = None, limit: int = None) -> pd.DataFrame:
        """
        Process a DataFrame directly for taxonomy categorization.
        
        Args:
            df: Input DataFrame
            output_file: Path to output CSV file (optional)
            limit: Maximum number of rows to process (optional)
            
        Returns:
            DataFrame with taxonomy results
        """
        self.logger.info("Processing DataFrame for taxonomy categorization...")
        
        try:
            # Apply row limit if specified
            if limit:
                original_count = len(df)
                df = df.head(limit)
                self.logger.info(f"Limited processing to first {len(df)} rows (from {original_count} total)")
                print(f"Processing limited to first {len(df)} rows (from {original_count} total)")
            
            # Detect name and description columns
            name_col, desc_col = self.detect_columns(df)
            
            # Prepare data for taxonomy
            taxonomy_data, valid_row_indices = self.prepare_taxonomy_data(df, name_col, desc_col)
            
            if not taxonomy_data:
                raise ValueError("No valid data found for taxonomy processing")
            
            # Process taxonomy
            self.logger.info("Processing taxonomy categorization...")
            taxonomy_results = self.taxonomy_categorizer.taxonomize_batch(taxonomy_data)
            
            # Create result DataFrame by preserving original structure and adding taxonomy columns
            result_df = df.copy()  # Keep all original columns and data
            
            # Create a mapping from valid row indices to taxonomy results
            # Check if GTIN columns exist, create them if they don't
            if 'gtin_category' not in df.columns:
                df['gtin_category'] = ''
                df['gtin_subcategory'] = ''
                df['gtin_subsubcategory'] = ''
                self.logger.info("Created missing GTIN columns for taxonomy processing")
            
            # Initialize all rows with existing GTIN taxonomy data (don't overwrite existing data)
            taxo1_list = df['gtin_category'].fillna('').astype(str).tolist()
            taxo2_list = df['gtin_subcategory'].fillna('').astype(str).tolist()
            taxo3_list = df['gtin_subsubcategory'].fillna('').astype(str).tolist()
            
            # Fill in the taxonomy results for all rows
            for i, result in enumerate(taxonomy_results):
                if i < len(df):
                    # Handle both external API format (category, subcategory, subsubcategory) 
                    # and local format (Taxo1, Taxo2, Taxo3)
                    taxo1 = result.get('category', result.get('Taxo1', ''))
                    taxo2 = result.get('subcategory', result.get('Taxo2', ''))
                    taxo3 = result.get('subsubcategory', result.get('Taxo3', ''))
                    
                    taxo1_list[i] = taxo1
                    taxo2_list[i] = taxo2
                    taxo3_list[i] = taxo3
            
            # Assign the lists to the DataFrame using GTIN column names
            result_df['gtin_category'] = taxo1_list
            result_df['gtin_subcategory'] = taxo2_list
            result_df['gtin_subsubcategory'] = taxo3_list
            
            # Save output if specified
            if output_file:
                result_df.to_csv(output_file, index=False)
                self.logger.info(f"Results saved to: {output_file}")
            
            self.logger.info("Taxonomy processing completed successfully")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error processing DataFrame: {e}")
            raise

    def process_taxonomy_local(self, taxonomy_data: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """
        Process taxonomy using local categorizer as fallback.
        
        Args:
            taxonomy_data: List of dictionaries with 'name' and 'description' keys
            
        Returns:
            List of dictionaries with taxonomy results
        """
        if not self.local_categorizer:
            raise ValueError("Local taxonomy categorizer not available")
        
        self.logger.info("Processing taxonomy using LOCAL categorizer...")
        print(f"LOCAL TAXONOMY PROCESSING: Using SALT Taxonomy CSV file")
        
        try:
            # Extract descriptions for batch processing
            descriptions = [item['description'] for item in taxonomy_data]
            
            # Get the model from the data_cleaner instance
            model = getattr(self.data_cleaner, 'model', None)
            if not model:
                raise ValueError("No AI model available for taxonomy categorization")
            
            self.logger.info(f"Local processing: {len(descriptions)} items using SALT Taxonomy")
            self.logger.info(f"Local taxonomy file: data/SALT Taxonomy.csv")
            
            # Use the main categorization method
            start_time = time.time()
            taxonomy_results = self.local_categorizer.categorize_taxonomy(
                descriptions=descriptions,
                model=model,
                track_tokens_func=self.data_cleaner._track_tokens if hasattr(self.data_cleaner, '_track_tokens') else None,
                batch_size=len(descriptions)
            )
            end_time = time.time()
            
            local_duration = end_time - start_time
            self.logger.info(f"LOCAL TAXONOMY PROCESSING COMPLETED!")
            self.logger.info(f"Local Processing Time: {local_duration:.2f} seconds")
            self.logger.info(f"Local Results Generated: {len(taxonomy_results)} items")
            
            # Log sample local results
            sample_results = taxonomy_results[:3]
            self.logger.info(f"Sample Local Results:")
            for i, result in enumerate(sample_results):
                self.logger.info(f"  Result {i+1}: Taxo1='{result.get('Taxo1', '')}', Taxo2='{result.get('Taxo2', '')}', Taxo3='{result.get('Taxo3', '')}'")
            
            print(f"LOCAL PROCESSING COMPLETE: Processed {len(taxonomy_results)} items in {local_duration:.2f}s")
            
            # The method returns a list of dictionaries, not separate lists
            results = []
            for i, result in enumerate(taxonomy_results):
                if isinstance(result, dict):
                    results.append({
                        'category': result.get('Taxo1', ''),
                        'subcategory': result.get('Taxo2', ''),
                        'subsubcategory': result.get('Taxo3', '')
                    })
                else:
                    # Fallback if result is not a dictionary
                    results.append({
                        'category': '',
                        'subcategory': '',
                        'subsubcategory': ''
                    })
            
            self.logger.info(f"Local taxonomy processing completed for {len(results)} items")
            return results
            
        except Exception as e:
            self.logger.error(f"Local taxonomy processing failed: {e}")
            print(f"LOCAL PROCESSING FAILED: {e}")
            # Return empty results as fallback
            return [{'category': '', 'subcategory': '', 'subsubcategory': ''} for _ in taxonomy_data]
    
    def _clean_and_enhance_name(self, name: str) -> str:
        """
        Clean and enhance product names for better readability.
        
        Args:
            name: Raw product name
            
        Returns:
            Cleaned and enhanced product name
        """
        if not name or pd.isna(name):
            return name
        
        try:
            # Use AI to clean and enhance the name
            prompt = f"""
            Clean and enhance this product name to make it more readable and professional.
            Follow these rules:
            1. Standardize letter case (use Title Case)
            2. Remove unnecessary punctuation and symbols
            - IMPORTANT: Remove ALL quotes (") from the text - both outer quotes and inner quotes EXCEPT for indicating inches (")
            3. Fix common typos and formatting issues
            4. Make it clear and professional
            5. DO NOT change the core meaning
            6. DO NOT add new information
            7. DO NOT add "Cleaned name:" prefix - just return the cleaned name
            
            Product name: "{name}"
            
            Cleaned name:"""
            
            response = self.data_cleaner.model.generate_content(prompt)
            cleaned_name = response.text.strip().strip('"')
            
            # Remove any "Cleaned name:" prefix if it appears
            if cleaned_name and cleaned_name.startswith("Cleaned name:"):
                cleaned_name = cleaned_name.replace("Cleaned name:", "").strip()
            
            return cleaned_name if cleaned_name else name
            
        except Exception as e:
            self.logger.warning(f"AI name cleaning failed: {e}")
            # Fallback to basic cleaning
            return name.strip().title()
    
    def _clean_and_enhance_description(self, description: str) -> str:
        """
        Clean and enhance product descriptions for better readability.
        
        Args:
            description: Raw product description
            
        Returns:
            Cleaned and enhanced product description
        """
        if not description or pd.isna(description):
            return description
        
        try:
            # Use AI to clean and enhance the description
            prompt = f"""
            Clean and enhance this product description to make it more readable and professional.
            Follow these rules:
            1. Standardize letter case (use Title Case for key terms)
            2. Remove unnecessary punctuation and symbols
            3. Fix common typos and formatting issues
            4. Make it clear and professional
            5. DO NOT change the core meaning
            6. DO NOT add new information
            
            Product description: "{description}"
            
            Cleaned description:"""
            
            response = self.data_cleaner.model.generate_content(prompt)
            cleaned_description = response.text.strip().strip('"')
            
            # Post-process to enforce "oz" standardization
            cleaned_description = self._enforce_abbreviation_standardization(cleaned_description)
            
            return cleaned_description if cleaned_description else description
            
        except Exception as e:
            self.logger.warning(f"AI description cleaning failed: {e}")
            # Fallback to basic cleaning
            return description.strip()
    
    def _enhance_description_with_ai(self, name: str, description: str) -> str:
        """
        Enhance product description to be more descriptive and informative using AI.
        Keep it concise (1-2 sentences) for better analysis.
        
        Args:
            name: Product name
            description: Current description
            
        Returns:
            Enhanced, concise description (1-2 sentences)
        """
        if not description or pd.isna(description):
            return description
        
        try:
            # Use AI to enhance the description with concise output
            prompt = f"""
            Enhance this product description to be more descriptive and informative.
            Make it clear what the product is in 1-2 sentences maximum.
            
            Rules:
            1. Add relevant details about the product type, form, and characteristics
            2. Keep it concise - maximum 2 sentences
            3. Keep the original meaning and information
            4. DO NOT add false or speculative information
            5. Focus on making it clear what the product is
            6. Use clear, professional language
            
            Product name: "{name}"
            Current description: "{description}"
            
            Enhanced description (1-2 sentences):"""
            
            response = self.data_cleaner.model.generate_content(prompt)
            enhanced_description = response.text.strip().strip('"')
            
            # Post-process to enforce "oz" standardization
            enhanced_description = self._enforce_abbreviation_standardization(enhanced_description)
            
            return enhanced_description if enhanced_description else description
            
        except Exception as e:
            self.logger.warning(f"AI description enhancement failed: {e}")
            # Return original description if enhancement fails
            return description
    
    def _enforce_abbreviation_standardization(self, text: str) -> str:
        """
        Post-process text to enforce abbreviation standardization.
        This ensures that all measurement units use standard abbreviations.
        
        Args:
            text: Text to standardize
            
        Returns:
            Text with abbreviation standardization enforced
        """
        if not text or pd.isna(text):
            return text
        
        # Convert text to string if it isn't already
        text = str(text)
        
        # Import re for regex operations
        import re
        
        # Replace various forms of measurement units with standard abbreviations
        # Use regex to match word boundaries to avoid partial replacements
        
        # Weight units
        text = re.sub(r'\bounce\b', 'Oz', text, flags=re.IGNORECASE)
        text = re.sub(r'\bounces\b', 'Oz', text, flags=re.IGNORECASE)
        text = re.sub(r'\bpound\b', 'lb', text, flags=re.IGNORECASE)
        text = re.sub(r'\bpounds\b', 'lbs', text, flags=re.IGNORECASE)
        text = re.sub(r'\bkilogram\b', 'kg', text, flags=re.IGNORECASE)
        text = re.sub(r'\bkilograms\b', 'kg', text, flags=re.IGNORECASE)
        text = re.sub(r'\bgram\b', 'g', text, flags=re.IGNORECASE)
        text = re.sub(r'\bgrams\b', 'g', text, flags=re.IGNORECASE)
        
        # Length units
        text = re.sub(r'\binch\b', 'in', text, flags=re.IGNORECASE)
        text = re.sub(r'\binches\b', 'in', text, flags=re.IGNORECASE)
        
        # Volume units
        text = re.sub(r'\bliter\b', 'lt', text, flags=re.IGNORECASE)
        text = re.sub(r'\bliters\b', 'lt', text, flags=re.IGNORECASE)
        text = re.sub(r'\bmilliliter\b', 'ml', text, flags=re.IGNORECASE)
        text = re.sub(r'\bmilliliters\b', 'ml', text, flags=re.IGNORECASE)
        
        # Special case: # symbol for pounds
        text = re.sub(r'#(\d+)', r'\1 lb', text)
        
        return text


def main():
    """Main function for command-line usage."""
    parser = argparse.ArgumentParser(description="Taxonomy Pipeline for CSV/XLSX files")
    parser.add_argument("input_file", help="Input CSV or XLSX file path")
    parser.add_argument("-o", "--output", help="Output CSV file path (optional)")
    parser.add_argument("-b", "--batch-size", type=int, default=100, help="Batch size for processing (default: 100)")
    parser.add_argument("-l", "--limit", type=int, help="Maximum number of rows to process (optional)")
    parser.add_argument("--local", action="store_true", help="Use local taxonomy processing instead of external API")
    
    args = parser.parse_args()
    
    # Validate input file
    if not os.path.exists(args.input_file):
        print(f"Error: Input file '{args.input_file}' not found.")
        return 1
    
    # Generate output filename if not specified
    if not args.output:
        input_path = Path(args.input_file)
        args.output = input_path.parent / f"{input_path.stem}_taxonomy.csv"
    
    try:
        # Initialize pipeline
        use_external_api = not args.local
        pipeline = TaxonomyPipeline(batch_size=args.batch_size, use_external_api=use_external_api)
        
        if args.local:
            print("Using local taxonomy processing (no external API calls)")
        else:
            print("Using external API for taxonomy processing")
        
        # Process file
        result_df = pipeline.process_file(args.input_file, args.output, limit=args.limit)
        
        # Print summary
        print(f"\nProcessing completed successfully!")
        print(f"Input file: {args.input_file}")
        print(f"Output file: {args.output}")
        print(f"Processed {len(result_df)} items")
        if args.limit:
            print(f"Row limit applied: {args.limit}")
        print(f"Columns: {list(result_df.columns)}")
        
        # Show sample results
        print(f"\nSample results:")
        print(result_df.head().to_string(index=False))
        
        return 0
        
    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    exit(main()) 
