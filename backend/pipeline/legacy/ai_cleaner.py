#!/usr/bin/env python3
"""
AI-Powered Data Cleaner
Cleans and standardizes CSV and Excel data using Google Gemini AI.
Automatically detects columns for cleaning regardless of column names.
Focuses on standardizing descriptions, pack sizes, and brands to be human-readable.
"""

import os
import json
import logging
from typing import List, Dict, Any, Optional
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
import google.generativeai as genai
import re
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from .taxonomy_categorizer import TaxonomyCategorizer
import concurrent.futures
from functools import lru_cache
import hashlib
from .prompts import *

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_cleaning.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DataCleaner:
    """Main data cleaner class that handles everything."""
    
    def __init__(self):
        """Initialize the data cleaner with Google Gemini."""
        self._setup_gemini()
        self._load_taxonomy()
        # Initialize token tracking
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self.api_calls = 0
        # Initialize performance tracking
        self.processing_times = {}
        self.start_time = None
        # Configuration options - All features enabled by default for maximum accuracy
        self.use_ai_taxonomy_filtering = True  # Enabled by default for better accuracy
        self.taxonomy_batch_size = 100  # Smaller default for better reliability
        self.enable_gemini_validation = True  # Enable Gemini validation layer by default
        self.enable_hierarchical_consistency = True  # Enable hierarchical consistency checks
        self.validation_batch_size = 50  # Batch size for validation
        self.enable_taxonomy_debug = True  # Enable debug output for taxonomy categorization
        self.enable_web_search = True  # Enable web search for better product understanding (balanced approach)
        self.enable_product_consistency = True  # Enable product consistency enforcement across similar items
        self.enable_recategorization = True  # Enable re-categorization of unsure or incorrect items
        self.enable_categorization = False  # Disable taxonomy categorization by default
        self.enable_enhanced_brands = True  # Enable enhanced brand search and cleaning by default
    
    def _start_timing(self, operation: str):
        """Start timing an operation."""
        if self.start_time is None:
            self.start_time = {}
        self.start_time[operation] = pd.Timestamp.now()
    
    def _end_timing(self, operation: str):
        """End timing an operation and store the duration."""
        if self.start_time and operation in self.start_time:
            duration = pd.Timestamp.now() - self.start_time[operation]
            self.processing_times[operation] = duration
            print(f"  ⏱️  {operation} completed in {duration.total_seconds():.2f} seconds")
            return duration
        return None
    
    def _setup_gemini(self):
        """Setup Google Gemini client."""
        api_key = os.getenv('GEMINI_API_KEY')
        if not api_key:
            raise ValueError("GEMINI_API_KEY not found in environment variables")
        
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('models/gemini-2.0-flash')
        logger.info("Google Gemini client initialized")
    
    def _track_tokens(self, response):
        """Track token usage from Gemini API response."""
        try:
            if hasattr(response, 'usage_metadata'):
                input_tokens = getattr(response.usage_metadata, 'prompt_token_count', 0)
                output_tokens = getattr(response.usage_metadata, 'candidates_token_count', 0)
                
                self.total_input_tokens += input_tokens
                self.total_output_tokens += output_tokens
                self.api_calls += 1
                
                logger.debug(f"API call {self.api_calls}: {input_tokens} input + {output_tokens} output tokens")
            else:
                # Fallback if usage metadata not available
                self.api_calls += 1
                logger.debug(f"API call {self.api_calls}: Usage metadata not available")
        except Exception as e:
            logger.warning(f"Could not track tokens: {e}")
            self.api_calls += 1
    
    def _load_taxonomy(self):
        """Load the SALT Taxonomy for categorization."""
        try:
            taxonomy_file = Path("data/SALT Taxonomy.csv")
            if taxonomy_file.exists():
                # Try to load with explicit string dtype to avoid mixed types
                self.taxonomy_df = pd.read_csv(taxonomy_file, dtype=str)
                # Clean up any empty or whitespace-only values
                for col in ['Level I', 'Level II', 'Level III']:
                    if col in self.taxonomy_df.columns:
                        self.taxonomy_df[col] = self.taxonomy_df[col].astype(str).str.strip()
                        # Remove rows where the column is empty or just whitespace
                        self.taxonomy_df = self.taxonomy_df[self.taxonomy_df[col] != '']
                
                # Initialize the taxonomy categorizer
                self.taxonomy_categorizer = TaxonomyCategorizer(self.taxonomy_df)
                
                logger.info(f"Loaded SALT Taxonomy with {len(self.taxonomy_df)} categories")
                logger.debug(f"Taxonomy columns: {list(self.taxonomy_df.columns)}")
                logger.debug(f"Sample Level I: {self.taxonomy_df['Level I'].head(3).tolist()}")
                
                # Debug output to console
                print(f"TAXONOMY LOADED:")
                print(f"  Total rows: {len(self.taxonomy_df)}")
                print(f"  Level I categories: {len(self.taxonomy_categorizer.level1_categories)}")
                print(f"  Sample Level I: {', '.join(self.taxonomy_categorizer.level1_categories[:5])}")
                print(f"  Level II total: {sum(len(level2s) for level2s in self.taxonomy_categorizer.level2_lookup.values())}")
                print(f"  Level III total: {sum(len(level3s) for level3s in self.taxonomy_categorizer.level3_lookup.values())}")
            else:
                logger.warning("SALT Taxonomy file not found, taxonomy categorization disabled")
                self.taxonomy_df = None
                self.taxonomy_categorizer = None
        except Exception as e:
            logger.error(f"Could not load SALT Taxonomy: {e}")
            self.taxonomy_df = None
            self.taxonomy_categorizer = None
    
    def _load_food_abbreviations(self):
        """Load food abbreviations dictionary."""
        try:
            abbreviations_path = Path(__file__).parent / "gtin_validation" / "food_abbreviations.json"
            with open(abbreviations_path, 'r') as f:
                self.food_abbreviations = json.load(f)['food_abbreviations']
            logger.info(f"Loaded {len(self.food_abbreviations)} food abbreviations")
        except Exception as e:
            logger.warning(f"Failed to load food abbreviations: {e}")
            self.food_abbreviations = {}
    
    def process_abbreviations_hybrid(self, text: str) -> str:
        """
        Hybrid method: abbreviate measurement units and expand food/product abbreviations.
        Preserves the original case pattern of the text.
        
        Args:
            text: Input text that may contain full words to abbreviate or abbreviations to expand
            
        Returns:
            Text with measurement units abbreviated and food terms expanded, maintaining original case style
        """
        if not text or not isinstance(text, str):
            return text
        
        # Load abbreviations if not already loaded
        if not hasattr(self, 'food_abbreviations'):
            self._load_food_abbreviations()
        
        if not self.food_abbreviations:
            return text
        
        processed_text = text
        
        # Detect the case pattern of the original text
        def detect_case_pattern(text: str) -> str:
            """Detect if text is ALL CAPS, Title Case, or mixed case."""
            if not text or not text.strip():
                return "mixed"
            
            # Check if it's ALL CAPS (excluding numbers and punctuation)
            alpha_chars = [c for c in text if c.isalpha()]
            if alpha_chars and all(c.isupper() for c in alpha_chars):
                return "all_caps"
            
            # Check if it's Title Case (first letter of each word capitalized)
            words = text.split()
            if words and all(word[0].isupper() if word and word[0].isalpha() else True for word in words):
                return "title_case"
            
            return "mixed"
        
        # Detect the case pattern of the original text
        original_case_pattern = detect_case_pattern(text)
        
        # Separate measurement units from food terms for different handling
        measurement_units = {'kilogram', 'kilograms', 'gram', 'grams', 'liter', 'liters', 
                           'milliliter', 'milliliters', 'ounce', 'ounces', 'pound', 'pounds',
                           'inch', 'inches'}
        
        # First pass: expand food/product abbreviations (abbreviation → full word)
        for full_word, abbreviation in sorted(
            self.food_abbreviations.items(), 
            key=lambda x: len(x[0]), 
            reverse=True
        ):
            if full_word.lower() not in measurement_units:
                # Use word boundaries to avoid partial matches
                pattern = r'\b' + re.escape(abbreviation) + r'\b'
                
                def replace_with_case_preservation(match):
                    """Replace abbreviation with full word, preserving case pattern."""
                    if original_case_pattern == "all_caps":
                        return full_word.upper()
                    elif original_case_pattern == "title_case":
                        return full_word.title()
                    else:
                        # For mixed case, use the original full_word as-is
                        return full_word
                
                processed_text = re.sub(pattern, replace_with_case_preservation, processed_text, flags=re.IGNORECASE)
        
        # Second pass: abbreviate measurement units (full word → abbreviation) 
        for full_word, abbreviation in sorted(
            self.food_abbreviations.items(), 
            key=lambda x: len(x[0]), 
            reverse=True
        ):
            if full_word.lower() in measurement_units:
                pattern = r'\b' + re.escape(full_word) + r'\b'
                
                def replace_with_case_preservation(match):
                    """Replace full word with abbreviation, preserving case pattern."""
                    if original_case_pattern == "all_caps":
                        return abbreviation.upper()
                    elif original_case_pattern == "title_case":
                        return abbreviation.title()
                    else:
                        # For mixed case, use the original abbreviation as-is
                        return abbreviation
                
                processed_text = re.sub(pattern, replace_with_case_preservation, processed_text, flags=re.IGNORECASE)
        
        return processed_text
    
    def process_abbreviations_hybrid_in_dataframe(self, df: pd.DataFrame, text_columns: List[str] = None) -> pd.DataFrame:
        """
        Process abbreviations in DataFrame: abbreviate measurement units and expand food/product abbreviations.
        Preserves the original case pattern of each text entry.
        
        Args:
            df: Input DataFrame
            text_columns: List of column names to process. If None, auto-detect text columns.
            
        Returns:
            DataFrame with measurement units abbreviated and food terms expanded in specified columns,
            maintaining original case styles
        """
        if text_columns is None:
            # Auto-detect text columns (object/string type columns)
            text_columns = df.select_dtypes(include=['object']).columns.tolist()
        
        logger.info(f"Processing abbreviations in {len(text_columns)} columns: {text_columns}")
        
        # Create a copy to avoid modifying the original
        df_expanded = df.copy()
        
        for column in text_columns:
            if column in df_expanded.columns:
                logger.info(f"Processing column: {column}")
                
                # Process each row individually to preserve case patterns
                for idx in df_expanded.index:
                    original_text = str(df_expanded.at[idx, column])
                    if original_text and original_text.strip():
                        # Detect case pattern for this specific text
                        alpha_chars = [c for c in original_text if c.isalpha()]
                        if alpha_chars and all(c.isupper() for c in alpha_chars):
                            case_pattern = "ALL CAPS"
                        elif original_text.split() and all(word[0].isupper() if word and word[0].isalpha() else True for word in original_text.split()):
                            case_pattern = "Title Case"
                        else:
                            case_pattern = "Mixed Case"
                        
                        # Process abbreviations while preserving case
                        processed_text = self.process_abbreviations_hybrid(original_text)
                        
                        # Log case preservation for debugging
                        if idx < 5:  # Only log first 5 rows to avoid spam
                            logger.info(f"  Row {idx}: '{original_text[:30]}...' ({case_pattern}) → '{processed_text[:30]}...'")
                        
                        df_expanded.at[idx, column] = processed_text
        
        logger.info("Hybrid abbreviation processing completed with case preservation")
        return df_expanded
    
    def _categorize_taxonomy(self, descriptions: List[str]) -> List[Dict[str, str]]:
        """Categorize items using SALT Taxonomy with improved hierarchical approach for better accuracy."""
        if not self.taxonomy_categorizer:
            return [{"Taxo1": "", "Taxo2": "", "Taxo3": ""} for _ in descriptions]
        
        # Use the taxonomy categorizer to handle all categorization logic
        return self.taxonomy_categorizer.categorize_taxonomy(
            descriptions=descriptions,
            model=self.model,
            track_tokens_func=self._track_tokens,
            batch_size=self.taxonomy_batch_size,
            enable_debug=self.enable_taxonomy_debug
        )
    
    def _get_column_hash(self, df: pd.DataFrame) -> str:
        """Generate a hash for the DataFrame columns to enable caching."""
        columns_str = str(tuple(sorted(df.columns)))
        return hashlib.md5(columns_str.encode()).hexdigest()
    
    @lru_cache(maxsize=100)
    def _cached_column_detection(self, column_hash: str, columns: tuple) -> Dict[str, str]:
        """Cache column detection results for repeated file types."""
        # Convert tuple back to list for processing
        columns_list = list(columns)
        columns_lower = [col.lower().strip() for col in columns_list]
        
        # Description patterns
        desc_patterns = [
            r'description', r'desc', r'product.*desc', r'item.*desc', r'name.*desc',
            r'product.*name', r'item.*name', r'title', r'product.*title',
            r'comment', r'notes', r'details', r'specification', r'specs',
            r'product.*info', r'item.*info', r'label', r'caption'
        ]
        
        # Pack size patterns  
        size_patterns = [
            r'pack.*size', r'size', 
            r'unit.*size', r'package.*size', r'weight', r'volume',
            r'amount', r'measurement', r'dimension', r'capacity',
            r'count', r'pieces', r'units', r'pack.*count'
        ]
        
        # Brand patterns
        brand_patterns = [
            r'brand', r'manufacturer', r'maker', r'company', r'vendor',
            r'supplier', r'producer', r'label', r'trade.*name',
            r'product.*brand', r'item.*brand', r'brand.*name',
            r'manufacturer.*name', r'company.*name', r'vendor.*name'
        ]
        
        detected = {}
        
        # Find description column
        for pattern in desc_patterns:
            for col, col_lower in zip(columns_list, columns_lower):
                if re.search(pattern, col_lower):
                    detected['description'] = col
                    break
            if 'description' in detected:
                break
        
        # Find pack size column
        for pattern in size_patterns:
            for col, col_lower in zip(columns_list, columns_lower):
                if re.search(pattern, col_lower):
                    detected['packsize'] = col
                    break
            if 'packsize' in detected:
                break
        
        # Find brand column
        for pattern in brand_patterns:
            for col, col_lower in zip(columns_list, columns_lower):
                if re.search(pattern, col_lower):
                    detected['brand'] = col
                    break
            if 'brand' in detected:
                break
        
        return detected
    
    def _process_batches_parallel(self, descriptions: List[str], batch_size: int = 100):
        """Process batches in parallel for better speed."""
        print(f"  Using parallel processing with batch size {batch_size}")
        
        # Create batches
        batches = [descriptions[i:i + batch_size] for i in range(0, len(descriptions), batch_size)]
        print(f"  Created {len(batches)} batches for parallel processing")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            # Submit all three operations in parallel
            future_desc = executor.submit(self._clean_and_standardize_descriptions, descriptions)
            future_pack = executor.submit(self._extract_packsize_from_descriptions, descriptions)
            future_brand = executor.submit(self._extract_brand_from_descriptions, descriptions)
            
            # Wait for all to complete
            cleaned_descriptions = future_desc.result()
            pack_sizes = future_pack.result()
            brands = future_brand.result()
            
            print(f"  Parallel processing completed for {len(descriptions)} items")
            return cleaned_descriptions, pack_sizes, brands
    
    def _quick_validation(self, descriptions: List[str]) -> bool:
        """Quick check if complex processing is needed."""
        # TEMPORARILY DISABLED: Always process to ensure quote cleaning works
        return False  # Always need processing
        
        # Note: We want to process descriptions with quotes to clean them up
        simple_pattern = r'^[A-Za-z0-9\s,\.\-]+$'  # Simple alphanumeric without quotes
        
        # Check sample of descriptions
        sample_size = min(10, len(descriptions))
        for desc in descriptions[:sample_size]:
            desc_str = str(desc)
            # If description contains quotes, we definitely need to process it
            if '"' in desc_str or '"' in desc_str or '"' in desc_str:
                return False  # Need processing for quotes
            if not re.match(simple_pattern, desc_str):
                return False  # Need processing for other reasons
        return True  # Skip processing
    
    def _fast_file_loading(self, input_file: str) -> pd.DataFrame:
        """Optimize file loading for large files."""
        if input_file.endswith('.csv'):
            # Use faster CSV engine
            return pd.read_csv(input_file, engine='c', dtype=str)
        elif input_file.endswith(('.xlsx', '.xls')):
            # Read only necessary columns
            return pd.read_excel(input_file, dtype=str)
        else:
            raise ValueError("Unsupported file format. Please use CSV or Excel files.")
    
    def _detect_columns(self, df: pd.DataFrame) -> Dict[str, str]:
        """Auto-detect columns for cleaning using both pattern matching and AI intelligence."""
        # Store DataFrame reference for AI analysis
        self.df = df
        
        columns = list(df.columns)
        columns_lower = [col.lower().strip() for col in columns]
        
        # Try caching first for better performance
        column_hash = self._get_column_hash(df)
        cached_result = self._cached_column_detection(column_hash, tuple(columns))
        
        if cached_result:
            print(f"\nColumn Detection (CACHED):")
            print(f"Total columns in file: {len(columns)}")
            print(f"Using cached column detection results")
            
            # Check if we need AI for missing columns
            if 'description' not in cached_result:
                print(f"  Description: No pattern match - using AI to find possible columns...")
                ai_desc_col = self._find_column_with_ai(columns, "product description", "description", "name", "title", "item")
                if ai_desc_col:
                    cached_result['description'] = ai_desc_col
                    print(f"  Description found by AI: '{ai_desc_col}'")
                else:
                    print(f"  Description: AI could not identify a suitable column")
                    print(f"  Please specify the description column manually:")
                    print(f"    --description-col 'COLUMN_NAME'")
                    print(f"  Available columns:")
                    for i, col in enumerate(columns):
                        print(f"    {i+1}. {col}")
                    raise ValueError("Description column not found. Please specify it manually using --description-col.")
            
            if 'packsize' not in cached_result:
                print(f"  Pack Size: No pattern match - using AI to find possible columns...")
                ai_size_col = self._find_column_with_ai(columns, "pack size", "size", "weight", "quantity")
                if ai_size_col:
                    cached_result['packsize'] = ai_size_col
                    print(f"  Pack Size found by AI: '{ai_size_col}'")
                else:
                    print(f"  Pack Size: AI could not identify a suitable column")
                    print(f"    (This is optional - will extract from description if needed)")
            
            if 'brand' not in cached_result:
                print(f"  Brand: No pattern match - using AI to find possible columns...")
                ai_brand_col = self._find_column_with_ai(columns, "brand", "manufacturer", "company", "vendor")
                if ai_brand_col:
                    cached_result['brand'] = ai_brand_col
                    print(f"  Brand found by AI: '{ai_brand_col}'")
                else:
                    print(f"  Brand: AI could not identify a suitable column")
                    print(f"    (This is optional - will extract from description if needed)")
            
            return cached_result
        
        # Fallback to original detection logic if no cache hit
        print(f"\nColumn Detection Debug:")
        print(f"Total columns in file: {len(columns)}")
        print(f"Looking for patterns in: {', '.join(columns)}")
        
        # Description patterns
        desc_patterns = [
            r'description', r'desc', r'product.*desc', r'item.*desc', r'name.*desc',
            r'product.*name', r'item.*name', r'title', r'product.*title',
            r'comment', r'notes', r'details', r'specification', r'specs',
            r'product.*info', r'item.*info', r'label', r'caption'
        ]
        
        # Pack size patterns  
        size_patterns = [
            r'pack.*size', r'size', 
            r'unit.*size', r'package.*size', r'weight', r'volume',
            r'amount', r'measurement', r'dimension', r'capacity',
            r'count', r'pieces', r'units', r'pack.*count'
        ]
        
        # Brand patterns
        brand_patterns = [
            r'brand', r'manufacturer', r'maker', r'company', r'vendor',
            r'supplier', r'producer', r'label', r'trade.*name',
            r'product.*brand', r'item.*brand', r'brand.*name',
            r'manufacturer.*name', r'company.*name', r'vendor.*name'
        ]
        
        detected = {}
        
        # Find description column
        for pattern in desc_patterns:
            for col, col_lower in zip(columns, columns_lower):
                if re.search(pattern, col_lower):
                    detected['description'] = col
                    print(f"  Description found: '{col}' (matched pattern: {pattern})")
                    break
            if 'description' in detected:
                break
        
        # If no description found with patterns, use AI to find it
        if 'description' not in detected:
            print(f"  Description: No pattern match - using AI to find possible columns...")
            ai_desc_col = self._find_column_with_ai(columns, "product description", "description", "name", "title", "item")
            if ai_desc_col:
                detected['description'] = ai_desc_col
                print(f"  Description found by AI: '{ai_desc_col}'")
            else:
                print(f"  Description: AI could not identify a suitable column")
                print(f"  Please specify the description column manually:")
                print(f"    --description-col 'COLUMN_NAME'")
                print(f"  Available columns:")
                for i, col in enumerate(columns):
                    print(f"    {i+1}. {col}")
                raise ValueError("Description column not found. Please specify it manually using --description-col.")
        
        # Find pack size column using patterns first
        for pattern in size_patterns:
            for col, col_lower in zip(columns, columns_lower):
                if re.search(pattern, col_lower):
                    detected['packsize'] = col
                    print(f"  Pack Size found: '{col}' (matched pattern: {pattern})")
                    break
            if 'packsize' in detected:
                break
        
        # If no pack size found with patterns, use AI to find it
        if 'packsize' not in detected:
            print(f"  Pack Size: No pattern match - using AI to find possible columns...")
            ai_size_col = self._find_column_with_ai(columns, "pack size", "size", "weight", "quantity")
            if ai_size_col:
                detected['packsize'] = ai_size_col
                print(f"  Pack Size found by AI: '{ai_size_col}'")
            else:
                print(f"  Pack Size: AI could not identify a suitable column")
                print(f"    (This is optional - will extract from description if needed)")
        
        # Find brand column using patterns first
        for pattern in brand_patterns:
            for col, col_lower in zip(columns, columns_lower):
                if re.search(pattern, col_lower):
                    detected['brand'] = col
                    print(f"  Brand found: '{col}' (matched pattern: {pattern})")
                    break
            if 'brand' in detected:
                break
        
        # If no brand found with patterns, use AI to find it
        if 'brand' not in detected:
            print(f"  Brand: No pattern match - using AI to find possible columns...")
            ai_brand_col = self._find_column_with_ai(columns, "brand", "manufacturer", "company", "vendor")
            if ai_brand_col:
                detected['brand'] = ai_brand_col
                print(f"  Brand found by AI: '{ai_brand_col}'")
            else:
                print(f"  Brand: AI could not identify a suitable column")
                print(f"    (This is optional - will extract from description if needed)")
        
        return detected
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((Exception,)),
        reraise=True
    )
    def _find_column_with_ai(self, columns: List[str], column_type: str, *keywords) -> Optional[str]:
        """Use AI to find the most likely column for a specific data type by examining column values."""
        if not columns:
            return None
        
        # Get sample data from each column to help AI make better decisions
        df_sample = self._get_column_samples()
        
        prompt = COLUMN_DETECTION_PROMPT.format(
            column_type=column_type,
            column_data=json.dumps(df_sample, indent=2),
            keywords=', '.join(keywords)
        )
        
        try:
            response = self.model.generate_content(prompt)
            self._track_tokens(response)  # Track token usage
            suggested_column = response.text.strip()
            
            # Clean up the response
            if suggested_column.startswith('"') and suggested_column.endswith('"'):
                suggested_column = suggested_column[1:-1]
            
            # Check if the suggested column exists in our list
            if suggested_column in columns:
                return suggested_column
            elif suggested_column.upper() == "NONE":
                return None
            else:
                # Try to find a close match
                for col in columns:
                    if suggested_column.lower() in col.lower() or col.lower() in suggested_column.lower():
                        return col
                return None
                
        except Exception as e:
            logger.error(f"Error using AI to find {column_type} column: {e}")
            return None
    
    def _get_column_samples(self) -> Dict[str, List[str]]:
        """Get sample data from each column to help AI make better column detection decisions."""
        try:
            # Get a sample of rows (first 10 non-empty rows)
            sample_data = {}
            
            for col in self.df.columns:
                # Get first 10 non-empty values from this column
                non_empty_values = self.df[col].dropna().head(10).astype(str).tolist()
                if non_empty_values:
                    sample_data[col] = non_empty_values
                else:
                    sample_data[col] = ["(empty)"]
            
            return sample_data
            
        except Exception as e:
            logger.error(f"Error getting column samples: {e}")
            # Fallback: just return column names
            return {col: ["(sample data unavailable)"] for col in self.df.columns}
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((Exception,)),
        reraise=True
    )
    def _extract_packsize_from_descriptions(self, descriptions: List[str]) -> List[str]:
        """Extract pack size information from product descriptions using AI."""
        if not descriptions:
            return []
        
        prompt = PACK_SIZE_EXTRACTION_PROMPT.format(
            descriptions=json.dumps(descriptions, indent=2)
        )
        
        try:
            response = self.model.generate_content(prompt)
            self._track_tokens(response)  # Track token usage
            cleaned_text = response.text.strip()
            
            # Remove markdown code blocks if present
            if cleaned_text.startswith('```'):
                lines = cleaned_text.split('\n')
                if len(lines) >= 3:
                    cleaned_text = '\n'.join(lines[1:-1])
                cleaned_text = cleaned_text.strip()
            
            extracted_sizes = json.loads(cleaned_text)
            if not isinstance(extracted_sizes, list):
                raise ValueError("Response is not a list")
            
            # Ensure correct number of values
            if len(extracted_sizes) != len(descriptions):
                logger.warning(f"Expected {len(descriptions)} sizes, got {len(extracted_sizes)}")
                if len(extracted_sizes) < len(descriptions):
                    extracted_sizes.extend([""] * (len(descriptions) - len(extracted_sizes)))
                else:
                    extracted_sizes = extracted_sizes[:len(descriptions)]
            
            return extracted_sizes
            
        except Exception as e:
            logger.error(f"Error extracting pack sizes: {e}")
            return ["" for _ in descriptions]
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((Exception,)),
        reraise=True
    )
    def _extract_brand_from_descriptions(self, descriptions: List[str]) -> List[str]:
        """Extract brand information from product descriptions using AI."""
        if not descriptions:
            return []
        
        prompt = BRAND_EXTRACTION_PROMPT.format(
            descriptions=json.dumps(descriptions, indent=2)
        )
        
        try:
            response = self.model.generate_content(prompt)
            self._track_tokens(response)  # Track token usage
            cleaned_text = response.text.strip()
            
            # Remove markdown code blocks if present
            if cleaned_text.startswith('```'):
                lines = cleaned_text.split('\n')
                if len(lines) >= 3:
                    cleaned_text = '\n'.join(lines[1:-1])
                cleaned_text = cleaned_text.strip()
            
            # Try to extract JSON from the response
            json_start = cleaned_text.find('[')
            json_end = cleaned_text.rfind(']') + 1
            if json_start != -1 and json_end > json_start:
                json_text = cleaned_text[json_start:json_end]
            else:
                json_text = cleaned_text
            
            extracted_brands = json.loads(json_text)
            if not isinstance(extracted_brands, list):
                raise ValueError("Response is not a list")
            
            # Ensure correct number of values
            if len(extracted_brands) != len(descriptions):
                logger.warning(f"Expected {len(descriptions)} brands, got {len(extracted_brands)}")
                if len(extracted_brands) < len(descriptions):
                    extracted_brands.extend([""] * (len(descriptions) - len(extracted_brands)))
                else:
                    extracted_brands = extracted_brands[:len(descriptions)]
            
            return extracted_brands
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error in brand extraction: {e}")
            logger.error(f"Response text: {cleaned_text[:500]}...")
            return ["" for _ in descriptions]
        except Exception as e:
            logger.error(f"Error extracting brands: {e}")
            return ["" for _ in descriptions]
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((Exception,)),
        reraise=True
    )
    def _clean_and_standardize_brands(self, brands: List[str]) -> List[str]:
        """Clean and standardize brands in a single AI call for efficiency."""
        if not brands:
            return []
        
        # Filter out empty values
        non_empty_data = [(i, str(val)) for i, val in enumerate(brands) if pd.notna(val) and str(val).strip()]
        
        if not non_empty_data:
            return [""] * len(brands)
        
        prompt = BRAND_CLEANING_PROMPT.format(
            brands=json.dumps([val for _, val in non_empty_data], indent=2)
        )
        
        try:
            response = self.model.generate_content(prompt)
            self._track_tokens(response)
            cleaned_text = response.text.strip()
            
            # Remove markdown code blocks if present
            if cleaned_text.startswith('```'):
                lines = cleaned_text.split('\n')
                if len(lines) >= 3:
                    cleaned_text = '\n'.join(lines[1:-1])
                cleaned_text = cleaned_text.strip()
            
            # Try to extract JSON from the response
            json_start = cleaned_text.find('[')
            json_end = cleaned_text.rfind(']') + 1
            if json_start != -1 and json_end > json_start:
                json_text = cleaned_text[json_start:json_end]
            else:
                json_text = cleaned_text
            
            cleaned_values = json.loads(json_text)
            if not isinstance(cleaned_values, list):
                raise ValueError("Response is not a list")
            
            # Ensure correct number of values
            if len(cleaned_values) != len(non_empty_data):
                logger.warning(f"Expected {len(non_empty_data)} values, got {len(cleaned_values)}")
                if len(cleaned_values) < len(non_empty_data):
                    cleaned_values.extend([""] * (len(non_empty_data) - len(cleaned_values)))
                else:
                    cleaned_values = cleaned_values[:len(non_empty_data)]
            
            # Create result array
            result = [""] * len(brands)
            for (i, original_val), cleaned_val in zip(non_empty_data, cleaned_values):
                result[i] = str(cleaned_val) if cleaned_val else ""
            
            return result
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error in brand standardization: {e}")
            logger.error(f"Response text: {cleaned_text[:500]}...")
            return [str(val) if pd.notna(val) else "" for val in brands]
        except Exception as e:
            logger.error(f"Error in brand cleaning and standardization: {e}")
            return [str(val) if pd.notna(val) else "" for val in brands]
    
    def _enhanced_brand_search_and_clean(self, item_names: List[str], descriptions: List[str], existing_brands: List[str]) -> List[str]:
        """
        Enhanced brand cleaning with search capability to find full brand names and confirm spelling.
        
        Args:
            item_names: List of product names
            descriptions: List of product descriptions  
            existing_brands: List of existing brand values
            
        Returns:
            List of cleaned and verified brand names
        """
        if not item_names or not descriptions or not existing_brands:
            return [""] * len(item_names) if item_names else []
        
        logger.info("Starting enhanced brand search and cleaning...")
        
        # First pass: Standardize existing brands for consistency
        standardized_brands = self._standardize_brand_variations(existing_brands)
        
        # Create enhanced prompts for brand research
        enhanced_prompts = []
        for i, (name, desc, brand) in enumerate(zip(item_names, descriptions, standardized_brands)):
            if pd.notna(brand) and str(brand).strip():
                if brand and str(brand).strip():
                    # If we have an existing brand, research it
                    prompt = ENHANCED_BRAND_RESEARCH_PROMPT.format(
                        product_name=name,
                        product_description=desc,
                        existing_brand=brand
                    )
                else:
                    # If no existing brand, extract from description
                    prompt = ENHANCED_BRAND_EXTRACTION_PROMPT.format(
                        product_name=name,
                        product_description=desc
                    )
            
            enhanced_prompts.append(prompt)
        
        # Process brands in batches to avoid overwhelming the AI
        batch_size = 10
        cleaned_brands = []
        
        for i in range(0, len(enhanced_prompts), batch_size):
            batch_end = min(i + batch_size, len(enhanced_prompts))
            batch_prompts = enhanced_prompts[i:batch_end]
            
            logger.info(f"Processing brand batch {i//batch_size + 1}/{(len(enhanced_prompts) + batch_size - 1)//batch_size}")
            
            try:
                # Create a combined prompt for the batch
                combined_prompt = COMPREHENSIVE_BRAND_PROCESSING_PROMPT.format(
                    items_data=json.dumps([{
                        'index': j,
                        'product_name': item_names[i + j],
                        'product_description': descriptions[i + j],
                        'existing_brand': standardized_brands[i + j] if pd.notna(standardized_brands[i + j]) else ''
                    } for j in range(len(batch_prompts))], indent=2)
                )
                
                response = self.model.generate_content(combined_prompt)
                self._track_tokens(response)
                cleaned_text = response.text.strip()
                
                # Remove markdown code blocks if present
                if cleaned_text.startswith('```'):
                    lines = cleaned_text.split('\n')
                    if len(lines) >= 3:
                        cleaned_text = '\n'.join(lines[1:-1])
                    cleaned_text = cleaned_text.strip()
                
                batch_results = json.loads(cleaned_text)
                if not isinstance(batch_results, list):
                    raise ValueError("Response is not a list")
                
                # Ensure correct number of values
                if len(batch_results) != len(batch_prompts):
                    logger.warning(f"Expected {len(batch_prompts)} brands, got {len(batch_results)}")
                    if len(batch_results) < len(batch_prompts):
                        batch_results.extend([""] * (len(batch_prompts) - len(batch_prompts)))
                    else:
                        batch_results = batch_results[:len(batch_prompts)]
                
                cleaned_brands.extend(batch_results)
                
            except Exception as e:
                logger.error(f"Error processing brand batch {i//batch_size + 1}: {e}")
                # Fallback to original brands for this batch
                fallback_brands = []
                for j in range(len(batch_prompts)):
                    original_brand = standardized_brands[i + j]
                    fallback_brands.append(str(original_brand) if pd.notna(original_brand) else "")
                cleaned_brands.extend(fallback_brands)
        
        # Ensure we have the right number of results
        if len(cleaned_brands) != len(item_names):
            logger.warning(f"Brand cleaning result count mismatch: expected {len(item_names)}, got {len(cleaned_brands)}")
            if len(cleaned_brands) < len(item_names):
                cleaned_brands.extend([""] * (len(item_names) - len(cleaned_brands)))
            else:
                cleaned_brands = cleaned_brands[:len(item_names)]
        
        # Final pass: Validate brand consistency and data integrity
        final_brands = self._validate_brand_consistency(cleaned_brands)
        
        logger.info(f"Enhanced brand cleaning completed for {len(final_brands)} items")
        return final_brands
    
    def _standardize_brand_variations(self, brands: List[str]) -> List[str]:
        """
        Pre-process brands to standardize basic variations before AI processing.
        
        Args:
            brands: List of brand names
            
        Returns:
            List of pre-standardized brand names
        """
        if not brands:
            return []
        
        standardized = []
        for brand in brands:
            if pd.notna(brand) and str(brand).strip():
                brand_str = str(brand).strip()
                
                # Enhanced standardization for common issues
                brand_variations = {
                    # La Française variations
                    'LA FRANCAI': 'La Francai',  # Fix missing 's' but let AI handle final standardization
                    'LA FRANCAIS': 'La Francais',  # Basic capitalization
                    'la francai': 'La Francai',   # Basic capitalization
                    'la francais': 'La Francais',  # Basic capitalization
                    'LA FRANCAISE': 'La Française',  # Proper French spelling
                    'la francaise': 'La Française',  # Proper French spelling
                    
                    # Otis Spunk variations
                    'OTIS SPUNK': 'Otis Spunk',    # Proper capitalization
                    'otis spunk': 'Otis Spunk',    # Proper capitalization
                    'OTIS SPUNK ': 'Otis Spunk',   # Remove trailing space
                    
                    # Lactantia variations
                    'LACTANTIA': 'Lactantia',      # Proper capitalization
                    'lactantia': 'Lactantia',      # Proper capitalization
                    'LACTANTIA ': 'Lactantia',     # Remove trailing space
                    
                    # Basic formatting fixes
                    'WISE BY NA': 'Wise by NA',    # Basic capitalization
                    'wise by na': 'Wise by NA',    # Basic capitalization
                    'MOLLY B\'S': 'Molly B\'s',    # Basic capitalization
                    'molly b\'s': 'Molly B\'s',    # Basic capitalization
                    'SWIPES': 'Swipes',            # Basic capitalization
                    'swipes': 'Swipes',            # Basic capitalization
                    
                    # Additional common food service brands
                    'HY FIVE': 'HY FIVE',          # Keep as is (brand preference)
                    'HY PAX': 'HY PAX',            # Keep as is (brand preference)
                    '511': '511',                  # Keep as is (brand preference)
                }
                
                # Apply enhanced standardization
                if brand_str.upper() in brand_variations:
                    standardized.append(brand_variations[brand_str.upper()])
                else:
                    # Apply smart capitalization for unknown brands
                    if brand_str.isupper() and len(brand_str) > 2:
                        # Convert ALL CAPS to Title Case for multi-character brands
                        # But preserve single letters and numbers
                        if not brand_str.isdigit() and not (len(brand_str) == 1 and brand_str.isalpha()):
                            # Handle special cases like "HY FIVE" (keep as is)
                            if not any(word in brand_str.upper() for word in ['HY FIVE', 'HY PAX', '511']):
                                standardized.append(brand_str.title())
                            else:
                                standardized.append(brand_str)
                        else:
                            standardized.append(brand_str)
                    else:
                        # Keep original for AI to process
                        standardized.append(brand_str)
            else:
                standardized.append("")
        
        return standardized
    
    def _validate_brand_consistency(self, brands: List[str]) -> List[str]:
        """
        Post-process brands to ensure consistency across similar brand names.
        Since we now use a comprehensive prompt that handles consistency,
        this method primarily validates and ensures data integrity.
        
        Args:
            brands: List of AI-processed brand names
            
        Returns:
            List of validated brand names
        """
        if not brands:
            return []
        
        # Filter out empty brands
        non_empty_brands = [(i, brand) for i, brand in enumerate(brands) 
                           if pd.notna(brand) and str(brand).strip()]
        
        if not non_empty_brands:
            return [""] * len(brands)
        
        logger.info("Validating brand consistency and data integrity...")
        
        try:
            validated_brands = [""] * len(brands)
            
            # Create a mapping of standardized brands to ensure consistency
            brand_standardization = {}
            
            for i, brand in non_empty_brands:
                if pd.notna(brand) and str(brand).strip():
                    brand_str = str(brand).strip()
                    
                    # Apply final capitalization validation
                    validated_brand = self._apply_final_brand_capitalization(brand_str)
                    
                    # Check if we've seen a similar brand before
                    if validated_brand in brand_standardization:
                        # Use the previously standardized version for consistency
                        validated_brands[i] = brand_standardization[validated_brand]
                    else:
                        # First time seeing this brand, store it
                        brand_standardization[validated_brand] = validated_brand
                        validated_brands[i] = validated_brand
                else:
                    validated_brands[i] = ""
            
            logger.info(f"Brand validation completed. Found {len(brand_standardization)} unique brands.")
            return validated_brands
                
        except Exception as e:
            logger.error(f"Error in brand validation: {e}")
            logger.info("Falling back to original brands")
            return brands
    
    def _apply_final_brand_capitalization(self, brand: str) -> str:
        """
        Apply final capitalization rules to ensure consistent brand formatting.
        
        Args:
            brand: Brand name to format
            
        Returns:
            Properly capitalized brand name
        """
        if not brand or not isinstance(brand, str):
            return brand
        
        brand = brand.strip()
        
        # Special cases that should remain in ALL CAPS
        all_caps_brands = {
            'HY FIVE', 'HY PAX', '511', 'HY FIVE ECO', 'HY PAX ECO'
        }
        
        # French brands with proper spelling
        french_brands = {
            'LA FRANCAI': 'La Française',
            'LA FRANCAIS': 'La Française',
            'LA FRANCAISE': 'La Française'
        }
        
        # Check special cases first
        if brand.upper() in all_caps_brands:
            return brand.upper()
        
        if brand.upper() in french_brands:
            return french_brands[brand.upper()]
        
        # Apply smart capitalization for other brands
        if brand.isupper() and len(brand) > 2:
            # Convert ALL CAPS to Title Case for multi-character brands
            # But preserve single letters and numbers
            if not brand.isdigit() and not (len(brand) == 1 and brand.isalpha()):
                # Handle special cases that should stay ALL CAPS
                if not any(word in brand.upper() for word in ['HY FIVE', 'HY PAX', '511']):
                    return brand.title()
        
        return brand
    
    def _process_brands_with_enhanced_search(self, df: pd.DataFrame, name_col: str, desc_col: str, brand_col: str) -> pd.DataFrame:
        """
        Process brands using enhanced search and cleaning capabilities.
        
        Args:
            df: Input DataFrame
            name_col: Name column name
            desc_col: Description column name
            brand_col: Brand column name
            
        Returns:
            DataFrame with enhanced brand processing
        """
        logger.info(f"Processing brands with enhanced search and cleaning...")
        
        try:
            # Extract the data for processing
            item_names = df[name_col].fillna("").astype(str).tolist()
            descriptions = df[desc_col].fillna("").astype(str).tolist()
            existing_brands = df[brand_col].fillna("").astype(str).tolist()
            
            # Use enhanced brand search and cleaning
            cleaned_brands = self._enhanced_brand_search_and_clean(
                item_names, descriptions, existing_brands
            )
            
            # Add the cleaned brands to the DataFrame
            df[f"{brand_col}_Enhanced"] = cleaned_brands
            
            # Show some examples of the improvements
            improvements = []
            for i, (original, enhanced) in enumerate(zip(existing_brands, cleaned_brands)):
                if original != enhanced and enhanced and enhanced != "Private Label":
                    improvements.append(f"Row {i+1}: '{original}' → '{enhanced}'")
                    if len(improvements) >= 5:  # Show max 5 examples
                        break
            
            if improvements:
                logger.info("Brand improvements examples:")
                for improvement in improvements:
                    logger.info(f"   {improvement}")
            
            logger.info(f"Enhanced brand processing completed. Added '{brand_col}_Enhanced' column.")
            return df
            
        except Exception as e:
            logger.error(f"Error in enhanced brand processing: {e}")
            # Fallback to original brand column
            df[f"{brand_col}_Enhanced"] = df[brand_col].fillna("")
            return df
    
    def _clean_and_standardize_descriptions(self, descriptions: List[str]) -> List[str]:
        """Clean and standardize product descriptions using abbreviation processing with case preservation."""
        if not descriptions:
            return []
        
        print(f"  Using abbreviation processing for description cleaning with case preservation")
        
        result = [""] * len(descriptions)
        for i, val in enumerate(descriptions):
            if pd.notna(val) and str(val).strip():
                # Apply hybrid abbreviation processing with case preservation
                original_text = str(val).strip()
                
                # Detect case pattern for debugging
                alpha_chars = [c for c in original_text if c.isalpha()]
                if alpha_chars and all(c.isupper() for c in alpha_chars):
                    case_pattern = "ALL CAPS"
                elif original_text.split() and all(word[0].isupper() if word and word[0].isalpha() else True for word in original_text.split()):
                    case_pattern = "Title Case"
                else:
                    case_pattern = "Mixed Case"
                
                final_cleaned = self.process_abbreviations_hybrid(original_text)
                
                # Log case preservation for first few items
                if i < 5:
                    print(f"    Row {i}: '{original_text[:40]}...' ({case_pattern}) → '{final_cleaned[:40]}...'")
                
                result[i] = final_cleaned
            else:
                result[i] = ""
        
        return result
    
    def process_file(self, input_file: str, output_file: str = None, description_col: str = None, 
                    packsize_col: str = None, brand_col: str = None, limit: int = None) -> str:
        """Main method to process a CSV or Excel file with AI-powered cleaning."""
        try:
            self._start_timing("total_processing")
            
            # Load the file
            print(f"\nLoading file: {input_file}")
            df = self._fast_file_loading(input_file)
            
            print(f"File loaded successfully: {len(df)} rows, {len(df.columns)} columns")
            
            # Apply row limit if specified
            if limit:
                df = df.head(limit)
                print(f"Processing limited to first {len(df)} rows")
            
            # Auto-detect columns if not specified
            if not description_col or not packsize_col or not brand_col:
                print(f"\nAuto-detecting columns...")
                detected_cols = self._detect_columns(df)
                
                if not description_col:
                    description_col = detected_cols.get('description')
                if not packsize_col:
                    packsize_col = detected_cols.get('packsize')
                if not brand_col:
                    brand_col = detected_cols.get('brand')
            
            print(f"\nUsing columns:")
            print(f"  Description: {description_col}")
            print(f"  Pack Size: {packsize_col}")
            print(f"  Brand: {brand_col}")
            
            # Show what will be generated
            print(f"\nProcessing operations:")
            print(f"  Descriptions: Clean and standardize → 'Description_cleaned'")
            if not packsize_col:
                print(f"  Pack Sizes: Extract from descriptions → 'PackSize_extracted'")
            if not brand_col:
                print(f"  Brands: Extract from descriptions → 'Brand_extracted'")
            print(f"  Taxonomy: Categorize using SALT Taxonomy → 'Taxo1', 'Taxo2', 'Taxo3'")
            
            # Estimate web search costs if enabled
            if self.enable_web_search and description_col:
                print(f"\nWEB SEARCH COST ESTIMATION:")
                cost_estimates = self._estimate_web_search_costs(df[description_col].tolist())
                
                # Count products needing research
                products_needing_research = sum(1 for desc in df[description_col] 
                                             if any(word in desc.lower() for word in ['day spot', 'day', 'mask', 'crayon', 'pet', 'cup', 'container']))
                
                print(f"  Products needing research: ~{products_needing_research}")
                print(f"  Estimated cost: ${cost_estimates['balanced']:.4f} (balanced approach)")
                print(f"  Web search uses balanced approach for consistent accuracy")
                
                # Show available taxonomy options for common product types
                if self.taxonomy_df is not None:
                    print(f"  Taxonomy loaded with {len(self.taxonomy_categorizer.level1_categories)} Level I categories")
            
            # Process descriptions
            if description_col:
                print(f"\nProcessing descriptions...")
                self._start_timing("description_processing")
                
                descriptions = df[description_col].tolist()
                
                # Check if processing is needed
                if self._quick_validation(descriptions):
                    print(f"  Descriptions already clean, skipping processing")
                    df['Description_cleaned'] = descriptions
                else:
                    # Use parallel processing for better speed
                    cleaned_descriptions, pack_sizes, brands = self._process_batches_parallel(descriptions, batch_size=100)
                    df['Description_cleaned'] = cleaned_descriptions
                    
                    if not packsize_col:
                        df['PackSize_extracted'] = pack_sizes
                    if not brand_col:
                        df['Brand_extracted'] = brands
                        # Apply enhanced brand processing to extracted brands if enabled
                        if self.enable_enhanced_brands:
                            print(f"  Applying enhanced brand processing to extracted brands...")
                            df = self._process_brands_with_enhanced_search(
                                df,
                                name_col=df.columns[0] if len(df.columns) > 0 else None,
                                desc_col=description_col if description_col else df.columns[0],
                                brand_col='Brand_extracted'
                            )
                        else:
                            print(f"  Using standard brand processing for extracted brands")
                    
                    print(f"  Parallel processing completed for {len(descriptions)} items")
                
                self._end_timing("description_processing")
            
            # Process pack sizes (if not already extracted in parallel)
            if packsize_col and packsize_col not in ['PackSize_extracted']:
                print(f"\nProcessing pack sizes...")
                self._start_timing("packsize_processing")
                
                # Clean and standardize pack sizes
                if packsize_col in df.columns:
                    # Use the existing pack size column
                    df['PackSize_standardized'] = df[packsize_col]
                    print(f"  Using existing pack size column: {packsize_col}")
            else:
                    print(f"  Pack size column '{packsize_col}' not found in file")
                
            self._end_timing("packsize_processing")
            
            # Process brands (if not already extracted in parallel)
            if brand_col and brand_col not in ['Brand_extracted']:
                print(f"\nProcessing brands...")
                self._start_timing("brand_processing")
                
                if brand_col in df.columns:
                        # Use enhanced brand search and cleaning if enabled
                        if self.enable_enhanced_brands:
                            print(f"  Using enhanced brand search and cleaning...")
                            df = self._process_brands_with_enhanced_search(
                                df, 
                                name_col=df.columns[0] if len(df.columns) > 0 else None,  # Use first column as name
                                desc_col=description_col if description_col else df.columns[0],
                                brand_col=brand_col
                            )
                            print(f"  Enhanced brand processing completed for {len(df)} items")
                        else:
                            # Use standard brand cleaning
                            brands = df[brand_col].tolist()
                            cleaned_brands = self._clean_and_standardize_brands(brands)
                            df['Brand_standardized'] = cleaned_brands
                            print(f"  Brands cleaned and standardized for {len(cleaned_brands)} items")
                else:
                    print(f"  Brand column '{brand_col}' not found in file")
                
                self._end_timing("brand_processing")
            
            # Process taxonomy categorization
            if self.taxonomy_df is not None and description_col and self.enable_categorization:
                print(f"\nProcessing taxonomy categorization...")
                self._start_timing("taxonomy_categorization")
                
                # Get descriptions for categorization
                descriptions = df[description_col].tolist()
                
                # CRITICAL: Reset DataFrame index to ensure proper alignment
                print(f"  Debug: DataFrame index before processing:")
                print(f"    Index range: {df.index.min()} to {df.index.max()}")
                print(f"    Index type: {type(df.index)}")
                
                # Reset index to ensure proper alignment
                df = df.reset_index(drop=True)
                print(f"  Debug: DataFrame index after reset:")
                print(f"    Index range: {df.index.min()} to {df.index.max()}")
                
                # Debug: Check descriptions before categorization
                print(f"  Debug: First 5 descriptions for taxonomy:")
                for i, desc in enumerate(descriptions[:5]):
                    print(f"    {i}: {desc[:60]}...")
                
                # Categorize using improved hierarchical approach
                categories = self._categorize_taxonomy(descriptions)
                
                # Debug: Check categories after categorization
                print(f"  Debug: First 5 categories returned:")
                for i, cat in enumerate(categories[:5]):
                    print(f"    {i}: Taxo1='{cat.get('Taxo1', '')}', Taxo2='{cat.get('Taxo2', '')}', Taxo3='{cat.get('Taxo3', '')}'")
                
                # Validate lengths match
                if len(descriptions) != len(categories):
                    print(f"  CRITICAL ERROR: Length mismatch!")
                    print(f"    Descriptions: {len(descriptions)}")
                    print(f"    Categories: {len(categories)}")
                    # Pad or truncate to match
                    if len(categories) < len(descriptions):
                        print(f"    Padding categories with empty values...")
                        while len(categories) < len(descriptions):
                            categories.append({"Taxo1": "", "Taxo2": "", "Taxo3": ""})
                    else:
                        print(f"    Truncating categories to match...")
                        categories = categories[:len(descriptions)]
                
                # CRITICAL: Use iloc for proper row-by-row assignment
                print(f"  Debug: Assigning taxonomy using iloc for proper alignment...")
                
                # Ensure taxonomy columns exist
                if 'Taxo1' not in df.columns:
                    df['Taxo1'] = ''
                if 'Taxo2' not in df.columns:
                    df['Taxo2'] = ''
                if 'Taxo3' not in df.columns:
                    df['Taxo3'] = ''
                
                # Assign taxonomy row by row to ensure perfect alignment
                for i in range(len(categories)):
                    df.iloc[i, df.columns.get_loc('Taxo1')] = categories[i].get('Taxo1', '')
                    df.iloc[i, df.columns.get_loc('Taxo2')] = categories[i].get('Taxo2', '')
                    df.iloc[i, df.columns.get_loc('Taxo3')] = categories[i].get('Taxo3', '')
                
                print(f"  Taxonomy assignment completed for {len(categories)} rows")
                
                # Debug: Verify the first few rows to ensure alignment
                print(f"  Debug: Verifying first 5 rows after taxonomy assignment:")
                for i in range(min(5, len(df))):
                    desc = df.iloc[i][description_col]
                    taxo1 = df.iloc[i]['Taxo1']
                    print(f"    Row {i}: '{desc[:50]}...' → Taxo1='{taxo1}'")
                
                print(f"  Taxonomy categorization completed for {len(categories)} items")
                
                self._end_timing("taxonomy_categorization")
            elif self.taxonomy_df is not None and description_col and not self.enable_categorization:
                print(f"\nTaxonomy categorization skipped (disabled by --enable-categorization flag)")
            else:
                print(f"\nTaxonomy categorization skipped (no taxonomy file or description column)")
            
            # Determine output filename
            if not output_file:
                input_path = Path(input_file)
                output_file = f"{input_path.stem}_cleaned{input_path.suffix}"
            
            # Save the cleaned data
            print(f"\nSaving cleaned data...")
            self._start_timing("file_saving")
            
            if output_file.endswith('.csv'):
                df.to_csv(output_file, index=False)
            else:
                df.to_excel(output_file, index=False)
            
            print(f"  Cleaned data saved to: {output_file}")
            self._end_timing("file_saving")
            
            # Final summary
            self._end_timing("total_processing")
            
            print(f"\nDATA CLEANING COMPLETED SUCCESSFULLY!")
            print(f"  Input rows: {len(df)}")
            print(f"  Output columns: {len(df.columns)}")
            print(f"  Descriptions cleaned and standardized")
            print(f"  Pack sizes extracted/standardized")
            print(f"  Brands extracted/standardized")
            if self.enable_categorization:
                print(f"  Taxonomy categorization completed")
            else:
                print(f"  Taxonomy categorization skipped (disabled)")
            print(f"  Total API calls: {self.api_calls}")
            print(f"  Total tokens used: {self.total_input_tokens + self.total_output_tokens}")
            
            # Show performance metrics
            self._monitor_performance()
            
            return output_file
            
        except Exception as e:
            logger.error(f"Error processing file: {e}")
            raise
    
    def _estimate_web_search_costs(self, descriptions: List[str]) -> Dict[str, float]:
        """Estimate the cost impact of web search based on the number of products that need it."""
        if not self.enable_web_search:
            return {"balanced": 0.0}
        
        # Estimate how many products will need web search (items that are unclear or incorrectly categorized)
        products_needing_research = 0
        
        for desc in descriptions:
            desc_lower = desc.lower()
            # Products that commonly need clarification
            if any(word in desc_lower for word in ['day spot', 'day', 'mask', 'crayon', 'pet', 'cup', 'container']):
                products_needing_research += 1
        
        # Cost per product for balanced approach (estimated based on token usage)
        cost_per_product = 0.0035  # ~40-60 tokens for balanced approach
        
        # Calculate total cost
        total_cost = products_needing_research * cost_per_product
        
        return {"balanced": total_cost}
    
    def _monitor_performance(self):
        """Monitor and log performance metrics."""
        if self.processing_times:
            print(f"\nPERFORMANCE SUMMARY:")
            total_time = sum(duration.total_seconds() for duration in self.processing_times.values() if duration)
            if hasattr(self, 'df') and self.df is not None:
                items_per_second = len(self.df) / total_time if total_time > 0 else 0
                print(f"  Total processing time: {total_time:.2f} seconds")
                print(f"  Processing speed: {items_per_second:.1f} items/second")
                print(f"  Total items processed: {len(self.df)}")
            print(f"  API calls: {self.api_calls}")
            print(f"  Tokens used: {self.total_input_tokens + self.total_output_tokens}")
            
            # Show individual operation times
            print(f"  Operation breakdown:")
            for operation, duration in self.processing_times.items():
                if duration:
                    print(f"    - {operation}: {duration.total_seconds():.2f} seconds")
            
            # Show optimization benefits
            if self.api_calls > 0:
                avg_tokens_per_call = (self.total_input_tokens + self.total_output_tokens) / self.api_calls
                print(f"  Average tokens per API call: {avg_tokens_per_call:.1f}")


def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description="AI-Powered Data Cleaner - Fast bulk processing with auto-detection and AI taxonomy filtering enabled by default!")
    parser.add_argument("input_file", help="Path to input CSV or Excel file")
    parser.add_argument("--limit", "-l", type=int, help="Maximum number of rows to process")
    parser.add_argument("--description-col", "-d", help="Name of description column")
    parser.add_argument("--packsize-col", "-s", help="Name of pack size column")
    parser.add_argument("--brand-col", "-b", help="Name of brand column")
    parser.add_argument("--output", "-o", help="Path to output CSV file")
    parser.add_argument("--validation-batch-size", type=int, default=50, help="Batch size for validation (default: 50)")
    parser.add_argument("--taxonomy-batch-size", type=int, default=100, help="Batch size for taxonomy processing (default: 100)")
    parser.add_argument("--enable-categorization", action="store_true", help="Enable taxonomy categorization (disabled by default)")
    parser.add_argument("--enhanced-brands", action="store_true", default=True, help="Enable enhanced brand search and cleaning (enabled by default)")
    
    args = parser.parse_args()
    
    try:
        cleaner = DataCleaner()
        
        # All features are always enabled for maximum accuracy
        cleaner.enable_gemini_validation = True
        cleaner.enable_hierarchical_consistency = True
        cleaner.validation_batch_size = args.validation_batch_size
        cleaner.taxonomy_batch_size = args.taxonomy_batch_size
        cleaner.enable_taxonomy_debug = True
        cleaner.enable_web_search = True
        cleaner.enable_product_consistency = True
        cleaner.enable_recategorization = True
        cleaner.enable_categorization = args.enable_categorization
        cleaner.enable_enhanced_brands = args.enhanced_brands
        
        print(f"Validation batch size: {args.validation_batch_size}, Taxonomy batch size: {args.taxonomy_batch_size})")
        if args.enable_categorization:
            print("Taxonomy categorization enabled")
        else:
            print("Taxonomy categorization disabled (use --enable-categorization to enable)")
        
        output_file = cleaner.process_file(
            input_file=args.input_file,
            output_file=args.output,
            description_col=args.description_col,
            packsize_col=args.packsize_col,
            brand_col=args.brand_col,
            limit=args.limit
        )
        
        logger.info(f"Data cleaning completed! Output: {output_file}")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        raise


if __name__ == "__main__":
    main() 
