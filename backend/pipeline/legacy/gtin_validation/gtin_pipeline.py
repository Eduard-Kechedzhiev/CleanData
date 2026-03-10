#!/usr/bin/env python3
"""
GTIN CSV/Excel Processor with Perplexity API Integration
Processes CSV and Excel files to check GTINs against MongoDB database and add existence status
Enhanced with Perplexity API for additional data enrichment and AI-powered data cleaning
"""

import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import google.generativeai as genai
import pandas as pd
from dotenv import load_dotenv
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from ..ai_cleaner import DataCleaner
from .mongodb_lookup import MongoDBGTINLookup
from .perplexity_search import PerplexityProductSearch

DATA_CLEANING_AVAILABLE = True
PERPLEXITY_AVAILABLE = True

# Load environment variables - check multiple locations like perplexity_api.py does
if os.path.exists("../.env"):
    load_dotenv("../.env")
elif os.path.exists(".env"):
    load_dotenv(".env")
else:
    load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# AI Prompt for GTIN Column Detection
GTIN_COLUMN_DETECTION_PROMPT = """
You are an AI assistant helping to identify the correct column for GTIN (Global Trade Item Number) data in a data file.

Available columns and sample data:
{column_data}

Please analyze the sample data and identify which column contains GTIN information.

GTINs are typically:
- Numeric codes (8-14 digits)
- May contain hyphens or spaces
- Common formats: GTIN, barcode, EAN, UPC, product code, SKU
- Used for product identification in retail and supply chain

Consider:
- Column names that suggest product identification
- Sample data that looks like numeric product codes
- Common variations and abbreviations
- The most logical choice based on the data content

Return only the exact column name, or "NONE" if no suitable column is found.
"""

# AI Prompt for Item Name Clarity Rating
ITEM_CLARITY_RATING_PROMPT = """
You are an AI assistant evaluating the clarity and quality of product item names.

Rate each item name on a scale of 1-10 based on the following criteria:

10 (Excellent): Clear, descriptive, follows standard naming conventions
- Contains product type, brand, size, and pack information
- Uses standard terminology and abbreviations
- Easy to understand what the product is

7-9 (Good): Generally clear with minor issues
- Mostly descriptive but may have some unclear abbreviations
- Contains most key information (product type, size, etc.)
- Minor formatting or terminology issues

4-6 (Fair): Somewhat unclear or incomplete
- Missing some key information (brand, size, pack details)
- Uses non-standard abbreviations or terminology
- May be confusing but still identifiable

1-3 (Poor): Unclear or poorly structured
- Very abbreviated or cryptic naming
- Missing important product information
- Difficult to understand what the product is
- Uses internal codes or non-standard terminology

0 (Very Poor): Extremely unclear
- Just codes or numbers
- No descriptive information
- Impossible to understand without additional context

Examples:
- "Borden Vanilla Soft Serve Ice Cream 2.5 Gallon" → 10 (clear, descriptive)
- "SOFT SERVE BORDEN 6% VAN 2/2.5GL" → 7 (good, some abbreviations)
- "Foam Ex-Squat White 8oz 20/50" → 3 (poor, unclear terminology)
- "AC0200" → 0 (very poor, just a code)

Please rate the following item names:

{item_names}

Return only a JSON array of numbers (ratings from 0-10), one for each item name.
"""

class GTINValidationPipeline:
    """Processes CSV and Excel files to check GTINs against MongoDB database with enhanced data cleaning"""
    
    def __init__(self, enable_data_cleaning: bool = True, enable_perplexity: bool = True):
        """Initialize the GTIN CSV processor"""
        self.query_obj = None
        self.gtin_column = None
        self.model = None
        self.data_cleaner = None
        self.perplexity_api = None
        self.enable_data_cleaning = enable_data_cleaning and DATA_CLEANING_AVAILABLE
        self.enable_perplexity = enable_perplexity and PERPLEXITY_AVAILABLE
        
        self._setup_ai()
        self._setup_data_cleaning()
        self._setup_perplexity()
        
    def _setup_ai(self):
        """Setup Google Gemini AI for column detection"""
        try:
            api_key = os.getenv('GEMINI_API_KEY')
            if api_key:
                genai.configure(api_key=api_key)
                self.model = genai.GenerativeModel('models/gemini-2.0-flash')
                logger.info("Google Gemini AI initialized for column detection")
            else:
                logger.warning("GEMINI_API_KEY not found - AI column detection will be disabled")
                self.model = None
        except Exception as e:
            logger.warning(f"Failed to initialize AI: {e} - AI column detection will be disabled")
            self.model = None
    
    def _setup_data_cleaning(self):
        """Setup data cleaning functionality"""
        if self.enable_data_cleaning:
            try:
                self.data_cleaner = DataCleaner()
                logger.info("Data cleaning functionality initialized")
            except Exception as e:
                logger.warning(f"Failed to initialize data cleaning: {e}")
                self.enable_data_cleaning = False
        else:
            logger.info("Data cleaning disabled")
    
    def _setup_perplexity(self):
        """Setup Perplexity API integration"""
        if self.enable_perplexity:
            try:
                api_key = os.getenv('PERPLEXITY_API_KEY')
                print(f"PERPLEXITY_API_KEY found: {'Yes' if api_key else 'No'}")
                if api_key:
                    print(f"API key length: {len(api_key)} characters")
                    self.perplexity_api = PerplexityProductSearch()
                    logger.info("Perplexity API integration initialized")
                else:
                    logger.warning("PERPLEXITY_API_KEY not found - Perplexity API disabled")
                    self.enable_perplexity = False
            except Exception as e:
                logger.warning(f"Failed to initialize Perplexity API: {e}")
                self.enable_perplexity = False
        else:
            logger.info("Perplexity API integration disabled")
    
    def _auto_detect_gtin_column(self, df: pd.DataFrame) -> str:
        """
        Auto-detect GTIN column using pattern matching, data analysis, and AI
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Name of the detected GTIN column
        """
        columns = list(df.columns)
        columns_lower = [col.lower().strip() for col in columns]
        
        print(f"\nAuto-detecting GTIN column...")
        print(f"Total columns in file: {len(columns)}")
        print(f"Looking for patterns in: {', '.join(columns)}")
        
        # GTIN patterns - more specific to avoid false matches
        gtin_patterns = [
            # UPCode patterns first (highest priority)
            r'upcode', r'unit.*code', r'case.*code',
            # Exact matches
            r'^gtin$', r'^g\.t\.i\.n$', r'^global.*trade.*item.*number$',
            r'^barcode$', r'^bar.*code$', r'^bar-code$',
            r'^ean$', r'^e\.a\.n$', r'^european.*article.*number$',
            r'^upc$', r'^u\.p\.c$', r'^universal.*product.*code$',
            # Product code patterns (lower priority)
            r'^product.*code$', r'^item.*code$', r'^sku$',
            r'^identifier$', r'^id$', r'^code$',
            # More specific patterns
            r'product.*id', r'item.*id'
        ]
        
        detected = None
        
        # Find GTIN column using patterns first
        for pattern in gtin_patterns:
            for col, col_lower in zip(columns, columns_lower):
                if re.search(pattern, col_lower):
                    detected = col
                    print(f"  GTIN column found: '{col}' (matched pattern: {pattern})")
                    break
            if detected:
                break
        
        # If no GTIN found with patterns, try to identify by data characteristics
        if not detected:
            print(f"  No pattern match - analyzing column data characteristics...")
            
            best_column = None
            best_score = 0
            
            for col in columns:
                # Sample some values to check if they look like GTINs
                sample_values = df[col].dropna().astype(str).head(100)
                
                # Check if values look like GTINs (numeric, reasonable length)
                gtin_like = 0
                total_checked = 0
                
                for val in sample_values:
                    if pd.isna(val) or val == '':
                        continue
                    
                    # Clean the value
                    clean_val = str(val).strip().replace('-', '').replace(' ', '')
                    
                    # Check if it looks like a GTIN
                    if clean_val.isdigit() and 8 <= len(clean_val) <= 14:
                        gtin_like += 1
                    
                    total_checked += 1
                
                # Calculate score (percentage of GTIN-like values)
                if total_checked > 0:
                    score = gtin_like / total_checked
                    if score > best_score and score > 0.3:  # Lower threshold to 30%
                        best_score = score
                        best_column = col
                        print(f"    Column '{col}': {gtin_like}/{total_checked} values look like GTINs (score: {score:.2f})")
            
            if best_column and best_score > 0.3:
                detected = best_column
                print(f"  GTIN column found by data analysis: '{best_column}' (score: {best_score:.2f})")
        
        # If still no GTIN found, try AI-powered detection
        if not detected and self.model:
            print(f"  No pattern or data match - using AI to find possible columns...")
            ai_detected = self._ai_detect_gtin_column(df, columns)
            if ai_detected:
                detected = ai_detected
                print(f"  GTIN column found by AI: '{ai_detected}'")
        
        # If still no GTIN found, show available columns and ask user
        if not detected:
            print(f"  GTIN column could not be automatically detected")
            print(f"  Available columns:")
            for i, col in enumerate(columns):
                print(f"     {i+1}. {col}")
            print(f"  Please specify the GTIN column manually or ensure your file has a column with GTIN data")
            raise ValueError("GTIN column not found. Please check your file or specify the column manually.")
        
        return detected
    
    def _search_perplexity_for_gtin(self, row: pd.Series, gtin_clean: str) -> Tuple[bool, Optional[str], Optional[str], Optional[str], Optional[str], str, float, str]:
        """
        Try to find product information using Perplexity API as fallback
        
        Args:
            row: DataFrame row with product information
            gtin_clean: Cleaned GTIN
            
        Returns:
            Tuple with (exists, category, subcategory, subsubcategory, query_name, ai_decision, ai_confidence, ai_reasoning)
        """
        if not self.perplexity_api:
            return False, None, None, None, None, 'No Fallback Available', 0.0, 'Perplexity API not configured'
        
        try:
            print(f"  Trying Perplexity API fallback for GTIN: {gtin_clean}")
            
            # Search Perplexity API for the GTIN
            product_info = self.perplexity_api.search_by_gtin(gtin_clean)
            
            if product_info and product_info.get('product_name'):
                # Product found via Perplexity API
                print(f"  Product found via Perplexity API: {product_info.get('product_name', 'N/A')}")
                
                # Try to categorize using AI or basic logic
                category = self._categorize_perplexity_product(product_info)
                
                return (
                    True,  # exists
                    category,  # category
                    None,  # subcategory (not available from Perplexity)
                    None,  # subsubcategory (not available from Perplexity)
                    product_info.get('product_name'),  # query_name
                    'Perplexity API',  # ai_decision
                    0.8,  # ai_confidence (lower than SALT database)
                    f'Product found via Perplexity API: {product_info.get("product_name")}'  # ai_reasoning
                )
            else:
                # No product found via Perplexity API
                print(f"  No product found via Perplexity API for GTIN: {gtin_clean}")
                return (
                    False,  # exists
                    None,  # category
                    None,  # subcategory
                    None,  # subsubcategory
                    None,  # query_name
                    'Perplexity API',  # ai_decision
                    0.0,  # ai_confidence
                    'No product information found via Perplexity API'  # ai_reasoning
                )
                
        except Exception as e:
            print(f"  WARNING: Perplexity API fallback failed: {e}")
            return (
                False,  # exists
                None,  # category
                None,  # subcategory
                None,  # subsubcategory
                None,  # query_name
                'Perplexity API',  # ai_decision
                0.0,  # ai_confidence
                f'Perplexity API fallback failed: {str(e)}'  # ai_reasoning
            )
    
    def _categorize_perplexity_product(self, product_info: Dict) -> Optional[str]:
        """
        Basic categorization of product found via Perplexity API
        
        Args:
            product_info: Product information from Perplexity API
            
        Returns:
            Basic category string
        """
        try:
            product_name = product_info.get('product_name', '').lower()
            description = product_info.get('description', '').lower()
            brand = product_info.get('brand', '').lower()
            
            # Basic categorization logic
            if any(word in product_name or word in description for word in ['food', 'snack', 'beverage', 'drink', 'meal']):
                return 'Food & Beverage'
            elif any(word in product_name or word in description for word in ['cleaning', 'soap', 'detergent', 'cleaner']):
                return 'Cleaning & Household'
            elif any(word in product_name or word in description for word in ['paper', 'tissue', 'napkin', 'towel']):
                return 'Paper & Disposables'
            elif any(word in product_name or word in description for word in ['equipment', 'tool', 'machine', 'device']):
                return 'Equipment & Supplies'
            else:
                return 'Other'
                
        except Exception as e:
            logger.warning(f"Error categorizing Perplexity product: {e}")
            return 'Uncategorized'
    
    def _ai_detect_gtin_column(self, df: pd.DataFrame, columns: List[str]) -> Optional[str]:
        """Use AI to find the most likely GTIN column by examining column values."""
        if not columns or not self.model:
            return None
        
        try:
            # Get sample data from each column to help AI make better decisions
            df_sample = self._get_column_samples_for_ai(df)
            
            prompt = GTIN_COLUMN_DETECTION_PROMPT.format(
                column_data=json.dumps(df_sample, indent=2)
            )
            
            response = self.model.generate_content(prompt)
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
            logger.error(f"Error using AI to find GTIN column: {e}")
            return None
    
    def _get_column_samples_for_ai(self, df: pd.DataFrame) -> Dict[str, List[str]]:
        """Get sample data from each column to help AI make better column detection decisions."""
        try:
            # Get a sample of rows (first 10 non-empty rows)
            sample_data = {}
            
            for col in df.columns:
                # Get first 10 non-empty values from this column
                non_empty_values = df[col].dropna().head(10).astype(str).tolist()
                if non_empty_values:
                    sample_data[col] = non_empty_values
                else:
                    sample_data[col] = ["(empty)"]
            
            return sample_data
            
        except Exception as e:
            logger.error(f"Error getting column samples: {e}")
            # Fallback: just return column names
            return {col: ["(sample data unavailable)"] for col in df.columns}
    
    def _run_data_cleaning_pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enhance data using AI-powered data cleaning and Perplexity API"""
        if not self.enable_data_cleaning and not self.enable_perplexity:
            return df
        
        print(f"\nEnhancing data with AI-powered cleaning and enrichment...")
        
        # Create a copy to avoid modifying original
        enhanced_df = df.copy()
        
        # Data cleaning with AI
        if self.enable_data_cleaning and self.data_cleaner:
            try:
                print(f"  Running AI-powered data cleaning...")
                
                # Auto-detect description column for cleaning
                description_col = None
                # Check for display_name first (common in Pepper/VP data, contains actual product descriptions)
                # This should be checked before 'name' to avoid using supplier names
                if 'display_name' in enhanced_df.columns:
                    description_col = 'display_name'
                    print(f"  Found display_name column (prioritized over name column)")
                else:
                    # Check for various description column patterns (including quoted columns)
                    # Note: 'display_name' is checked first above, 'name' is lower priority
                    description_candidates = [
                        'Description', 'description', 'Product Description', 'Item Description', 'Product',
                        'DESC', 'desc', 'DESCRIPTION', 'Item_Description',
                        'Product Name', 'Item Name',  # Product/Item names before generic 'name'
                        'F02',  # Field code for product descriptions in Pos_Load files
                        'NAME', 'name'  # Generic 'name' last, as it may be supplier name
                    ]
                    
                    for candidate in description_candidates:
                        # Check exact match
                        if candidate in enhanced_df.columns:
                            description_col = candidate
                            break
                        # Check quoted version (single quotes around double quotes)
                        quoted_candidate = f'"{candidate}"'
                        if quoted_candidate in enhanced_df.columns:
                            description_col = quoted_candidate
                            break
                        # Check triple quoted version
                        triple_quoted_candidate = f'"""{candidate}"""'
                        if triple_quoted_candidate in enhanced_df.columns:
                            description_col = triple_quoted_candidate
                            break
                
                if description_col:
                    print(f"  Using description column: {description_col}")
                    
                    # Stage 1: Use food abbreviations to expand abbreviations but abbreviate units
                    print(f"  Stage 1: Expanding food abbreviations and abbreviating units...")
                    stage1_descriptions = self._process_food_abbreviations(enhanced_df[description_col].tolist())
                    
                    # Stage 2: Use AI to restructure descriptions into consistent format
                    print(f"  Stage 2: AI restructuring descriptions...")
                    stage2_descriptions = self._ai_restructure_descriptions(stage1_descriptions)
                    
                    # Add cleaned columns
                    enhanced_df['Description_cleaned'] = stage2_descriptions
                    
                    # Stage 3: Enhanced brand search and cleaning
                    print(f"  Stage 3: Enhanced brand search and cleaning...")
                    enhanced_df = self._clean_brands_and_sizes(enhanced_df, description_col)
                    
                    # Stage 4: Rate item clarity
                    print(f"  Stage 4: Rating item clarity...")
                    enhanced_df = self._add_clarity_ratings(enhanced_df, description_col)
                    
                    # Stage 5: Generate enhanced descriptions
                    print(f"  Stage 5: Generating enhanced descriptions...")
                    enhanced_df = self._add_enhanced_descriptions(enhanced_df, description_col)
                    
                    # Stage 6: Parse brand, size, and pack size from product names
                    print(f"  Stage 6: Parsing brand, size, and pack size...")
                    enhanced_df = self._add_parsed_product_info(enhanced_df, description_col)
                    
                    print(f"  Six-stage data cleaning completed")
                    print(f"  Stage 1: Food abbreviation expansion + unit abbreviation")
                    print(f"  Stage 2: AI restructuring to PRODUCT TYPE DESCRIPTION SIZE PACK format")
                    print(f"  Stage 3: Enhanced brand search + pack/size extraction")
                    print(f"  Stage 4: Item clarity rating (0-10 scale)")
                    print(f"  Stage 5: Enhanced description generation")
                    print(f"  Stage 6: Brand, size, and pack size parsing")
                else:
                    print(f"  WARNING: No description column found for cleaning")
                    
            except Exception as e:
                logger.warning(f"Data cleaning failed: {e}")
        
        # Perplexity API enrichment
        if self.enable_perplexity and self.perplexity_api:
            try:
                print(f"  Enriching data with Perplexity API...")
                # This would integrate with your existing Perplexity API functionality
                # enhanced_df = self.perplexity_api.enrich_dataframe(enhanced_df)
                print(f"  Perplexity API enrichment completed")
            except Exception as e:
                logger.warning(f"Perplexity API enrichment failed: {e}")
        
        return enhanced_df
    
    def _process_food_abbreviations(self, descriptions: List[str]) -> List[str]:
        """Stage 1: Apply food abbreviations to expand abbreviations but abbreviate units"""
        try:
            # Load food abbreviations
            abbreviations_file = os.path.join(os.path.dirname(__file__), 'food_abbreviations.json')
            with open(abbreviations_file, 'r') as f:
                abbreviations_data = json.load(f)
            
            food_abbreviations = abbreviations_data.get('food_abbreviations', {})
            
            # Create reverse mapping for units (to abbreviate them)
            unit_abbreviations = {
                'ounce': 'Oz', 'ounces': 'Oz', 'OUNCE': 'Oz', 'OUNCES': 'Oz',
                'pound': 'lb', 'pounds': 'lbs', 'POUND': 'lb', 'POUNDS': 'lbs',
                'inch': 'in', 'inches': 'in', 'INCH': 'in', 'INCHES': 'in',
                'gram': 'g', 'grams': 'g', 'GRAM': 'g', 'GRAMS': 'g',
                'kilogram': 'kg', 'kilograms': 'kg', 'KILOGRAM': 'kg', 'KILOGRAMS': 'kg',
                'liter': 'lt', 'liters': 'lt', 'LITER': 'lt', 'LITERS': 'lt',
                'milliliter': 'ml', 'milliliters': 'ml', 'MILLILITER': 'ml', 'MILLILITERS': 'ml'
            }
            
            processed_descriptions = []
            
            for desc in descriptions:
                if pd.isna(desc) or desc == '':
                    processed_descriptions.append('')
                    continue
                
                processed_desc = str(desc)
                
                # Step 1: Expand food abbreviations
                for abbreviation, full_word in food_abbreviations.items():
                    # Use word boundaries to avoid partial matches
                    pattern = r'\b' + re.escape(abbreviation) + r'\b'
                    processed_desc = re.sub(pattern, full_word, processed_desc, flags=re.IGNORECASE)
                
                # Step 2: Abbreviate units
                for full_unit, abbreviation in unit_abbreviations.items():
                    pattern = r'\b' + re.escape(full_unit) + r'\b'
                    processed_desc = re.sub(pattern, abbreviation, processed_desc, flags=re.IGNORECASE)
                
                processed_descriptions.append(processed_desc)
            
            print(f"    Applied food abbreviations and unit abbreviations to {len(descriptions)} descriptions")
            return processed_descriptions
            
        except Exception as e:
            print(f"    WARNING: Food abbreviation processing failed: {e}")
            return descriptions
    
    def _ai_restructure_descriptions(self, descriptions: List[str]) -> List[str]:
        """Stage 2: Use AI to restructure descriptions into consistent format"""
        try:
            if not self.model:
                print(f"    WARNING: AI model not available, skipping restructuring")
                return descriptions
            
            print(f"    Using AI to restructure {len(descriptions)} descriptions...")
            
            # AI prompt for restructuring descriptions
            prompt = """You are an AI assistant that restructures product descriptions into a consistent format.

The goal is to reorganize descriptions so the core product comes first, followed by type, description, size, and pack information.

Current format examples:
- "COD FISH" → "Fish Cod"
- "tomato paste" → "Paste Tomato" 
- "MASK FACE 3PLY" → "Mask Face 3Ply"
- "FREEZE DRIED DRAGON FRUIT-RASP" → "Dragon Fruit Freeze Dried Raspberry"

Rules:
1. Put the main product/ingredient first
2. NO separators (|) - just use spaces between components
3. Use Standard Case (Title Case) - first letter of each word capitalized
4. Keep important measurements and specifications
5. Make it clear and readable
6. Reorder logically: PRODUCT TYPE DESCRIPTION SIZE PACK
7. Remove all the " and ', replace with appropriate units if needed.

Please restructure this description: "{description}"

Return only the restructured description, no quotes, no explanations."""
            
            restructured_descriptions = []
            
            for i, desc in enumerate(descriptions):
                if pd.isna(desc) or desc == '':
                    restructured_descriptions.append('')
                    continue
                
                try:
                    # Use AI to restructure
                    ai_prompt = prompt.format(description=desc)
                    response = self.model.generate_content(ai_prompt)
                    restructured = response.text.strip()
                    
                    # No quote handling needed - just use the AI response as-is
                    
                    restructured_descriptions.append(restructured)
                    
                    # Show progress for first few items
                    if i < 3:
                        print(f"      {i+1}: '{desc[:50]}...' → {restructured[:50]}...")
                    
                except Exception as e:
                    print(f"      WARNING: AI restructuring failed for item {i+1}: {e}")
                    # Keep original description as fallback
                    restructured_descriptions.append(desc)
            
            print(f"    AI restructuring completed for {len(descriptions)} descriptions")
            return restructured_descriptions
            
        except Exception as e:
            print(f"    WARNING: AI restructuring failed: {e}")
            return descriptions
    
    def _clean_brands_and_sizes(self, df: pd.DataFrame, description_col: str) -> pd.DataFrame:
        """Stage 3: Enhanced brand search and pack/size cleaning using ai_cleaner methods"""
        try:
            print(f"    Cleaning and standardizing existing Pack and Size columns...")
            
            # Clean and standardize the Pack column
            if 'Pack' in df.columns:
                print(f"      Cleaning Pack column...")
                pack_values = df['Pack'].tolist()
                cleaned_pack = self._standardize_pack_sizes(pack_values)
                df['Pack_cleaned'] = cleaned_pack
                print(f"        Pack column cleaned: {len([p for p in cleaned_pack if p])} values processed")
            
            # Clean and standardize the Size column
            if 'Size' in df.columns:
                print(f"      Cleaning Size column...")
                size_values = df['Size'].tolist()
                cleaned_size = self._standardize_sizes(size_values)
                df['Size_cleaned'] = cleaned_size
                print(f"        Size column cleaned: {len([s for s in cleaned_size if s])} values processed")
            
            # Handle brand processing: clean existing brand column OR extract from descriptions
            print(f"    Processing brands...")
            
            if 'Brand' in df.columns:
                # Clean existing brand column
                print(f"      Cleaning existing Brand column...")
                if hasattr(self.data_cleaner, '_clean_and_standardize_brands'):
                    try:
                        brand_values = df['Brand'].tolist()
                        standardized_brands = self.data_cleaner._clean_and_standardize_brands(brand_values)
                        
                        # Clean extra spaces from standardized brands
                        cleaned_brands = []
                        for brand in standardized_brands:
                            if pd.notna(brand) and str(brand).strip():
                                # Remove extra spaces and normalize to single spaces
                                cleaned_brand = re.sub(r'\s+', ' ', str(brand).strip())
                                cleaned_brands.append(cleaned_brand)
                            else:
                                cleaned_brands.append('')
                        
                        df['extracted_brand'] = cleaned_brands
                        print(f"        Brand column cleaned: {len([b for b in cleaned_brands if b])} brands processed")
                    except Exception as e:
                        print(f"        WARNING: Brand cleaning failed: {e}")
                        # Fallback: use original brand values
                        df['extracted_brand'] = [str(b).strip() if pd.notna(b) and str(b).strip() else '' for b in df['Brand']]
                else:
                    # Fallback: just clean spaces from original brand column
                    df['extracted_brand'] = [str(b).strip() if pd.notna(b) and str(b).strip() else '' for b in df['Brand']]
                    print(f"        Brand column cleaned (basic): {len([b for b in df['extracted_brand'] if b])} brands processed")
            else:
                # Extract brands from descriptions
                print(f"      Extracting brands from descriptions...")
                descriptions = df[description_col].tolist()
                
                if hasattr(self.data_cleaner, '_extract_brand_from_descriptions'):
                    try:
                        print(f"        Extracting brands from descriptions using AI...")
                        
                        # Process in batches to avoid overwhelming the AI
                        batch_size = 50  # Process 50 descriptions at a time
                        all_extracted_brands = []
                        
                        for i in range(0, len(descriptions), batch_size):
                            batch = descriptions[i:i + batch_size]
                            batch_num = (i // batch_size) + 1
                            total_batches = (len(descriptions) + batch_size - 1) // batch_size
                            
                            print(f"          Processing brand batch {batch_num}/{total_batches} ({len(batch)} descriptions)...")
                            
                            # Extract brands from this batch
                            batch_brands = self.data_cleaner._extract_brand_from_descriptions(batch)
                            all_extracted_brands.extend(batch_brands)
                            
                            # Small delay between batches
                            if i + batch_size < len(descriptions):
                                import time
                                time.sleep(0.5)
                        
                        extracted_brands = all_extracted_brands
                        
                        # Clean and standardize the extracted brands
                        if hasattr(self.data_cleaner, '_clean_and_standardize_brands'):
                            print(f"        Standardizing extracted brands...")
                            
                            # Process standardization in batches too
                            batch_size = 50
                            all_standardized_brands = []
                            
                            for i in range(0, len(extracted_brands), batch_size):
                                batch = extracted_brands[i:i + batch_size]
                                batch_num = (i // batch_size) + 1
                                total_batches = (len(extracted_brands) + batch_size - 1) // batch_size
                                
                                print(f"          Processing standardization batch {batch_num}/{total_batches} ({len(batch)} brands)...")
                                
                                # Standardize this batch
                                batch_standardized = self.data_cleaner._clean_and_standardize_brands(batch)
                                all_standardized_brands.extend(batch_standardized)
                                
                                # Small delay between batches
                                if i + batch_size < len(extracted_brands):
                                    import time
                                    time.sleep(0.5)
                            
                            standardized_brands = all_standardized_brands
                            
                            # Clean extra spaces from standardized brands
                            cleaned_brands = []
                            for brand in standardized_brands:
                                if pd.isna(brand) or brand == '':
                                    cleaned_brands.append('')
                                else:
                                    # Remove extra spaces and normalize to single spaces
                                    cleaned_brand = re.sub(r'\s+', ' ', str(brand).strip())
                                    cleaned_brands.append(cleaned_brand)
                            
                            df['cleaned_brand'] = cleaned_brands
                            print(f"        Brand extraction and cleaning completed: {len([b for b in cleaned_brands if b])} brands extracted")
                        else:
                            # Fallback: just use extracted brands without standardization
                            df['cleaned_brand'] = extracted_brands
                            print(f"        Brand extraction completed (no standardization): {len([b for b in extracted_brands if b])} brands extracted")
                            
                    except Exception as e:
                        print(f"        WARNING: Brand extraction failed: {e}")
                        # Fallback: create empty brand column
                        df['cleaned_brand'] = [''] * len(df)
                else:
                    # Fallback: create empty brand column
                    df['cleaned_brand'] = [''] * len(df)
                    print(f"        No brand extraction available, creating empty brand column")
            
            return df
            
        except Exception as e:
            print(f"    WARNING: Enhanced brand and pack processing failed: {e}")
            return df
    
    def _add_clarity_ratings(self, df: pd.DataFrame, description_col: str) -> pd.DataFrame:
        """Add clarity ratings for item names using AI."""
        try:
            print(f"    Rating clarity of item names...")
            
            # Get the original item names (before cleaning)
            # Use Item Name if description is empty
            if description_col in df.columns and df[description_col].notna().any() and df[description_col].str.strip().ne('').any():
                item_names = df[description_col].tolist()
                print(f"      Using {description_col} for clarity rating")
            elif 'Item Name' in df.columns:
                item_names = df['Item Name'].tolist()
                print(f"      Using Item Name for clarity rating (description column empty)")
            else:
                # Fallback to first column
                item_names = df.iloc[:, 0].tolist()
                print(f"      Using first column for clarity rating")
            
            # Process in batches to avoid overwhelming the AI with too many items
            batch_size = 50  # Process 50 items at a time
            all_ratings = []
            
            for i in range(0, len(item_names), batch_size):
                batch = item_names[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (len(item_names) + batch_size - 1) // batch_size
                
                print(f"      Processing clarity batch {batch_num}/{total_batches} ({len(batch)} items)...")
                
                # Rate the clarity of this batch
                batch_ratings = self._rate_item_clarity(batch)
                all_ratings.extend(batch_ratings)
                
                # Small delay between batches to avoid overwhelming the API
                if i + batch_size < len(item_names):
                    import time
                    time.sleep(0.5)
            
            clarity_ratings = all_ratings
            
            # Add the ratings to the dataframe
            df['clarity_rating'] = clarity_ratings
            
            # Add clarity category labels
            clarity_categories = []
            for rating in clarity_ratings:
                if rating >= 9:
                    clarity_categories.append("Excellent")
                elif rating >= 7:
                    clarity_categories.append("Good")
                elif rating >= 5:
                    clarity_categories.append("Fair")
                elif rating >= 3:
                    clarity_categories.append("Poor")
                elif rating >= 1:
                    clarity_categories.append("Very Poor")
                else:
                    clarity_categories.append("Extremely Poor")
            
            df['clarity_category'] = clarity_categories
            
            # Generate AI-powered explanations for each rating
            print(f"      Generating AI explanations for clarity ratings...")
            explanations = self._generate_clarity_explanations(item_names, clarity_ratings, clarity_categories)
            df['clarity_explanation'] = explanations
            
            # Log summary statistics
            total_items = len(clarity_ratings)
            excellent_count = sum(1 for r in clarity_ratings if r >= 9)
            good_count = sum(1 for r in clarity_ratings if 7 <= r < 9)
            fair_count = sum(1 for r in clarity_ratings if 5 <= r < 7)
            poor_count = sum(1 for r in clarity_ratings if 3 <= r < 5)
            very_poor_count = sum(1 for r in clarity_ratings if 1 <= r < 3)
            extremely_poor_count = sum(1 for r in clarity_ratings if r == 0)
            
            print(f"        Clarity ratings completed: {total_items} items rated")
            print(f"        Excellent (9-10): {excellent_count} items ({excellent_count/total_items*100:.1f}%)")
            print(f"        Good (7-8): {good_count} items ({good_count/total_items*100:.1f}%)")
            print(f"        Fair (5-6): {fair_count} items ({fair_count/total_items*100:.1f}%)")
            print(f"        Poor (3-4): {poor_count} items ({poor_count/total_items*100:.1f}%)")
            print(f"        Very Poor (1-2): {very_poor_count} items ({very_poor_count/total_items*100:.1f}%)")
            print(f"        Extremely Poor (0): {extremely_poor_count} items ({extremely_poor_count/total_items*100:.1f}%)")
            
            return df
            
        except Exception as e:
            print(f"    WARNING: Clarity rating failed: {e}")
            # Fallback: create empty clarity columns
            df['clarity_rating'] = [0] * len(df)
            df['clarity_category'] = ['Extremely Poor'] * len(df)
            df['clarity_explanation'] = ['Just codes/numbers'] * len(df)
            return df
    
    def _add_enhanced_descriptions(self, df: pd.DataFrame, description_col: str) -> pd.DataFrame:
        """Add enhanced descriptions using AI."""
        try:
            print(f"    Generating enhanced descriptions...")
            
            # Get the original item names and descriptions
            item_names = df[description_col].tolist()
            
            # Generate enhanced descriptions
            enhanced_descriptions = []
            for i, (name, description) in enumerate(zip(item_names, df[description_col].tolist())):
                try:
                    enhanced_desc = self._enhance_description_with_ai(name, description)
                    enhanced_descriptions.append(enhanced_desc)
                    
                    # Log progress for every 10 items
                    if (i + 1) % 10 == 0:
                        print(f"        Enhanced {i + 1}/{len(item_names)} descriptions...")
                        
                except Exception as e:
                    print(f"        WARNING: Failed to enhance description for item {i+1}: {e}")
                    enhanced_descriptions.append(description)  # Fallback to original
            
            # Add the enhanced descriptions to the dataframe
            df['enhanced_description'] = enhanced_descriptions
            
            print(f"        Enhanced descriptions completed: {len(enhanced_descriptions)} descriptions generated")
            
            return df
            
        except Exception as e:
            print(f"    WARNING: Enhanced description generation failed: {e}")
            # Fallback: use original descriptions
            df['enhanced_description'] = df[description_col]
            return df
    
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
            
            response = self.model.generate_content(prompt)
            enhanced_description = response.text.strip().strip('"')
            
            # Post-process to enforce abbreviation standardization
            enhanced_description = self._enforce_abbreviation_standardization(enhanced_description)
            
            return enhanced_description if enhanced_description else description
            
        except Exception as e:
            logger.warning(f"AI description enhancement failed: {e}")
            # Return original description if enhancement fails
            return description
    
    def _enforce_abbreviation_standardization(self, text: str) -> str:
        """
        Enforce abbreviation standardization in text.
        
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
    
    def _standardize_pack_sizes(self, pack_values: List[str]) -> List[str]:
        """Clean and standardize pack size values - add space between number and unit, standardize units"""
        cleaned_packs = []
        
        for pack in pack_values:
            if pd.isna(pack) or pack == '':
                cleaned_packs.append('')
                continue
            
            pack_str = str(pack).strip()
            
            # Add space between number and unit if missing
            # Pattern: number followed immediately by letters (e.g., 50CT -> 50 CT)
            pack_str = re.sub(r'(\d)([A-Za-z])', r'\1 \2', pack_str)
            
            # Standardize common unit variations
            unit_mappings = {
                # Count variations
                r'\bCT\b': 'CT',
                r'\bCOUNT\b': 'CT',
                r'\bPCS\b': 'PCS',
                r'\bPACK\b': 'PACK',
                r'\bUNIT\b': 'UNIT',
                r'\bEACH\b': 'EA',
                r'\bEA\b': 'EA',
                
                # Weight variations
                r'\bLB\b': 'LB',
                r'\bLBS\b': 'LB',
                r'\bPOUND\b': 'LB',
                r'\bPOUNDS\b': 'LB',
                r'\b#\b': 'LB',
                
                r'\bOZ\b': 'OZ',
                r'\bOUNCE\b': 'OZ',
                r'\bOUNCES\b': 'OZ',
                
                r'\bG\b': 'G',
                r'\bGRAM\b': 'G',
                r'\bGRAMS\b': 'G',
                
                r'\bKG\b': 'KG',
                r'\bKILOGRAM\b': 'KG',
                r'\bKILOGRAMS\b': 'KG',
                
                # Volume variations
                r'\bML\b': 'ML',
                r'\bMILLILITER\b': 'ML',
                r'\bMILLILITERS\b': 'ML',
                
                r'\bLT\b': 'LT',
                r'\bLITER\b': 'LT',
                r'\bLITERS\b': 'LT',
                
                # Length variations
                r'\bIN\b': 'IN',
                r'\bINCH\b': 'IN',
                r'\bINCHES\b': 'IN',
            }
            
            # Apply unit standardization
            for pattern, replacement in unit_mappings.items():
                pack_str = re.sub(pattern, replacement, pack_str, flags=re.IGNORECASE)
            
            # Remove extra spaces - normalize multiple spaces to single space
            pack_str = re.sub(r'\s+', ' ', pack_str)
            
            # Final trim to remove leading/trailing spaces
            pack_str = pack_str.strip()
            
            cleaned_packs.append(pack_str)
        
        return cleaned_packs
    
    def _standardize_sizes(self, size_values: List[str]) -> List[str]:
        """Clean and standardize size values - add space between number and unit, standardize units"""
        cleaned_sizes = []
        
        for size in size_values:
            if pd.isna(size) or size == '':
                cleaned_sizes.append('')
                continue
            
            size_str = str(size).strip()
            
            # Add space between number and unit if missing
            # Pattern: number followed immediately by letters (e.g., 120G -> 120 G)
            size_str = re.sub(r'(\d)([A-Za-z])', r'\1 \2', size_str)
            
            # Standardize common unit variations (same as pack sizes)
            unit_mappings = {
                # Weight variations
                r'\bLB\b': 'LB',
                r'\bLBS\b': 'LB',
                r'\bPOUND\b': 'LB',
                r'\bPOUNDS\b': 'LB',
                r'\b#\b': 'LB',
                
                r'\bOZ\b': 'OZ',
                r'\bOUNCE\b': 'OZ',
                r'\bOUNCES\b': 'OZ',
                
                r'\bG\b': 'G',
                r'\bGRAM\b': 'G',
                r'\bGRAMS\b': 'G',
                
                r'\bKG\b': 'KG',
                r'\bKILOGRAM\b': 'KG',
                r'\bKILOGRAMS\b': 'KG',
                
                # Volume variations
                r'\bML\b': 'ML',
                r'\bMILLILITER\b': 'ML',
                r'\bMILLILITERS\b': 'ML',
                
                r'\bLT\b': 'LT',
                r'\bLITER\b': 'LT',
                r'\bLITERS\b': 'LT',
                
                # Length variations
                r'\bIN\b': 'IN',
                r'\bINCH\b': 'IN',
                r'\bINCHES\b': 'IN',
            }
            
            # Apply unit standardization
            for pattern, replacement in unit_mappings.items():
                size_str = re.sub(pattern, replacement, size_str, flags=re.IGNORECASE)
            
            # Remove extra spaces - normalize multiple spaces to single space
            size_str = re.sub(r'\s+', ' ', size_str)
            
            # Final trim to remove leading/trailing spaces
            size_str = size_str.strip()
            
            cleaned_sizes.append(size_str)
        
        return cleaned_sizes
    
    def _rate_item_clarity(self, item_names: List[str]) -> List[int]:
        """Rate the clarity of item names using AI on a scale of 0-10."""
        if not item_names or not self.model:
            return [0] * len(item_names) if item_names else []
        
        # Try up to 3 times with different approaches
        for attempt in range(3):
            try:
                # Prepare the prompt with item names
                prompt = ITEM_CLARITY_RATING_PROMPT.format(
                    item_names=json.dumps(item_names, indent=2)
                )
                
                # Generate response from AI
                response = self.model.generate_content(prompt)
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
                
                # Parse the JSON response
                ratings = json.loads(json_text)
                
                # Ensure we have the correct number of ratings
                if len(ratings) != len(item_names):
                    logger.warning(f"Expected {len(item_names)} ratings, got {len(ratings)}")
                    if len(ratings) < len(item_names):
                        ratings.extend([0] * (len(item_names) - len(ratings)))
                    else:
                        ratings = ratings[:len(item_names)]
                
                # Validate ratings are within 0-10 range
                validated_ratings = []
                for rating in ratings:
                    try:
                        rating_int = int(rating)
                        if 0 <= rating_int <= 10:
                            validated_ratings.append(rating_int)
                        else:
                            validated_ratings.append(0)
                    except (ValueError, TypeError):
                        validated_ratings.append(0)
                
                return validated_ratings
                
            except json.JSONDecodeError as e:
                logger.warning(f"JSON parsing error on attempt {attempt + 1}: {e}")
                if attempt < 2:  # Try again with simpler prompt
                    continue
                else:
                    logger.error(f"Failed to parse JSON after 3 attempts, using fallback ratings")
                    return [0] * len(item_names)
            except Exception as e:
                logger.error(f"Error rating item clarity on attempt {attempt + 1}: {e}")
                if attempt < 2:
                    continue
                else:
                    return [0] * len(item_names)
        
        return [0] * len(item_names)
    
    def _generate_clarity_explanations(self, item_names: List[str], ratings: List[int], categories: List[str]) -> List[str]:
        """Generate AI-powered explanations for clarity ratings."""
        if not item_names or not self.model:
            return ["AI explanation not available"] * len(item_names)
        
        try:
            # Process in smaller batches for explanation generation
            batch_size = 20
            all_explanations = []
            
            for i in range(0, len(item_names), batch_size):
                batch_end = min(i + batch_size, len(item_names))
                batch_items = item_names[i:batch_end]
                batch_ratings = ratings[i:batch_end]
                batch_categories = categories[i:batch_end]
                
                # Create prompt for this batch
                items_data = []
                for j, (item, rating, category) in enumerate(zip(batch_items, batch_ratings, batch_categories)):
                    items_data.append(f"{j+1}. Item: '{item}' | Rating: {rating} ({category})")
                
                prompt = f"""You are an AI assistant that explains product name clarity ratings.

For each item below, provide a brief, specific explanation (max 60 characters) of why it received its clarity rating.

Rating Scale:
- 9-10 (Excellent): Clear, descriptive, includes key details
- 7-8 (Good): Generally clear with minor issues
- 5-6 (Fair): Missing some info or uses unclear terms
- 3-4 (Poor): Abbreviated/cryptic, hard to understand
- 1-2 (Very Poor): Minimal context, mostly codes
- 0 (Extremely Poor): Just codes/numbers, no description

Items to explain:
{chr(10).join(items_data)}

Provide explanations as a JSON array of strings, one for each item. Keep each explanation under 60 characters and be specific about what makes the item clear or unclear.

Example format: ["Missing brand info", "Good size details", "Too abbreviated"]"""

                try:
                    response = self.model.generate_content(prompt)
                    response_text = response.text.strip()
                    
                    # Extract JSON from response
                    json_start = response_text.find('[')
                    json_end = response_text.rfind(']') + 1
                    if json_start != -1 and json_end > json_start:
                        json_text = response_text[json_start:json_end]
                        batch_explanations = json.loads(json_text)
                        
                        # Ensure we have the right number of explanations
                        if len(batch_explanations) == len(batch_items):
                            all_explanations.extend(batch_explanations)
                        else:
                            # Fallback: create basic explanations
                            for rating, category in zip(batch_ratings, batch_categories):
                                if category == "Excellent":
                                    all_explanations.append("Clear and descriptive")
                                elif category == "Good":
                                    all_explanations.append("Minor format issues")
                                elif category == "Fair":
                                    all_explanations.append("Missing key details")
                                elif category == "Poor":
                                    all_explanations.append("Too abbreviated")
                                elif category == "Very Poor":
                                    all_explanations.append("Minimal context")
                                else:
                                    all_explanations.append("Just codes/numbers")
                    else:
                        # Fallback: create basic explanations
                        for rating, category in zip(batch_ratings, batch_categories):
                            if category == "Excellent":
                                all_explanations.append("Clear and descriptive")
                            elif category == "Good":
                                all_explanations.append("Minor format issues")
                            elif category == "Fair":
                                all_explanations.append("Missing key details")
                            elif category == "Poor":
                                all_explanations.append("Too abbreviated")
                            elif category == "Very Poor":
                                all_explanations.append("Minimal context")
                            else:
                                all_explanations.append("Just codes/numbers")
                
                except Exception as e:
                    print(f"        WARNING: AI explanation generation failed for batch {i//batch_size + 1}: {e}")
                    # Fallback: create basic explanations for this batch
                    for rating, category in zip(batch_ratings, batch_categories):
                        if category == "Excellent":
                            all_explanations.append("Clear and descriptive")
                        elif category == "Good":
                            all_explanations.append("Minor format issues")
                        elif category == "Fair":
                            all_explanations.append("Missing key details")
                        elif category == "Poor":
                            all_explanations.append("Too abbreviated")
                        elif category == "Very Poor":
                            all_explanations.append("Minimal context")
                        else:
                            all_explanations.append("Just codes/numbers")
            
            return all_explanations
            
        except Exception as e:
            print(f"        WARNING: AI explanation generation failed: {e}")
            # Fallback: create basic explanations
            explanations = []
            for category in categories:
                if category == "Excellent":
                    explanations.append("Clear and descriptive")
                elif category == "Good":
                    explanations.append("Minor format issues")
                elif category == "Fair":
                    explanations.append("Missing key details")
                elif category == "Poor":
                    explanations.append("Too abbreviated")
                elif category == "Very Poor":
                    explanations.append("Minimal context")
                else:
                    explanations.append("Just codes/numbers")
            return explanations

    
    def _validate_gtin_format(self, gtin: str) -> bool:
        """
        Basic validation that a string looks like a GTIN
        
        Args:
            gtin: String to validate
            
        Returns:
            True if it looks like a valid GTIN format
        """
        if pd.isna(gtin) or gtin == '':
            return False
        
        # Clean the GTIN
        clean_gtin = self._normalize_gtin(gtin)
        
        # Check if it's numeric and has reasonable length for GTINs
        if clean_gtin.isdigit() and 8 <= len(clean_gtin) <= 14:
            return True
        
        return False
    
    def _normalize_gtin(self, gtin: str) -> str:
        """
        Clean GTIN string for consistent processing
        
        Args:
            gtin: Raw GTIN string
            
        Returns:
            Cleaned GTIN string
        """
        if pd.isna(gtin) or gtin == '':
            return ""
        
        # Clean the GTIN - remove all whitespace, hyphens, and other non-digit characters
        clean_gtin = str(gtin).strip()
        
        # Remove common non-digit characters that might appear in GTINs
        clean_gtin = re.sub(r'[^\d]', '', clean_gtin)
        
        return clean_gtin
    
    def process_file(self, file_path: str, output_path: str = None, gtin_column: str = None, 
                    row_limit: int = None, enable_enhancement: bool = True, chunk_size: int = 100) -> str:
        """
        Process CSV or Excel file to check GTINs against MongoDB database with enhanced features
        
        Args:
            file_path: Path to input file (CSV or Excel)
            output_path: Path for output file (optional)
            gtin_column: Name of GTIN column (optional, will auto-detect if not provided)
            row_limit: Limit processing to first N rows (optional, for testing)
            enable_enhancement: Enable AI-powered data enhancement (optional)
            
        Returns:
            Path to output file
        """
        print(f"Starting GTIN file processing with enhanced features...")
        print(f"Input file: {file_path}")
        if row_limit:
            print(f"Row limit: {row_limit} rows (for testing)")
        print(f"AI Enhancement: {'Enabled' if enable_enhancement else 'Disabled'}")
        
        # Determine file type and read accordingly
        file_ext = Path(file_path).suffix.lower()
        
        try:
            if file_ext in ['.xlsx', '.xls']:
                print(f"Reading Excel file...")
                df = pd.read_excel(file_path)
            elif file_ext == '.csv':
                print(f"Reading CSV file...")
                # Use more robust CSV reading to handle malformed quotes
                df = pd.read_csv(file_path, quoting=3, on_bad_lines='skip')  # QUOTE_NONE, skip bad lines
                print(f"  CSV read with robust parsing (quoting=3, skip bad lines)")
            else:
                raise ValueError(f"Unsupported file type: {file_ext}. Please use .csv, .xlsx, or .xls files")
            
            # Apply row limit if specified
            if row_limit and len(df) > row_limit:
                df = df.head(row_limit)
                print(f"Limited to first {row_limit} rows for testing")
            
            print(f"File loaded successfully: {len(df)} rows, {len(df.columns)} columns")
        except Exception as e:
            print(f"Error reading file: {e}")
            raise
        
        # AI-powered data enhancement
        if enable_enhancement:
            df = self._run_data_cleaning_pipeline(df)
        
        # Process in chunks if dataset is large
        if len(df) > chunk_size:
            print(f"Large dataset detected ({len(df)} rows). Processing in chunks of {chunk_size}...")
            return self._process_large_dataset(df, file_path, output_path, gtin_column, chunk_size)
        
        # Detect GTIN column if not provided
        if gtin_column:
            if gtin_column not in df.columns:
                raise ValueError(f"Specified GTIN column '{gtin_column}' not found in file")
            self.gtin_column = gtin_column
            print(f"Using specified GTIN column: '{gtin_column}'")
        else:
            self.gtin_column = self._auto_detect_gtin_column(df)
            print(f"Auto-detected GTIN column: '{self.gtin_column}'")
        
        # Connect to MongoDB
        print(f"\nConnecting to MongoDB...")
        self.query_obj = MongoDBGTINLookup()
        
        if not self.query_obj.connect():
            print(f"Failed to connect to MongoDB")
            raise ConnectionError("Could not connect to MongoDB database")
        
        print(f"Connected to MongoDB successfully")
        
        # Process GTINs
        print(f"\nProcessing GTINs...")
        
        # Check for multiple GTIN columns (UPC1, UPC2, UPC3)
        gtin_columns = []
        for col in ['UPC1', 'UPC2', 'UPC3']:
            quoted_col = f'"{col}"'
            if quoted_col in df.columns:
                gtin_columns.append(quoted_col)
        
        if not gtin_columns:
            # Fallback to the detected GTIN column
            gtin_columns = [self.gtin_column]
        
        print(f"Found GTIN columns: {gtin_columns}")
        
        # Process all GTIN columns
        all_valid_gtins = set()
        gtin_data = []
        
        for gtin_col in gtin_columns:
            print(f"Processing {gtin_col}...")
            df[f'{gtin_col}_clean'] = df[gtin_col].apply(self._normalize_gtin)
            df[f'{gtin_col}_valid'] = df[f'{gtin_col}_clean'].apply(self._validate_gtin_format)
            
            valid_gtins_col = df[df[f'{gtin_col}_valid']][f'{gtin_col}_clean'].dropna().unique().tolist()
            print(f"  Found {len(valid_gtins_col)} valid GTINs in {gtin_col}")
            all_valid_gtins.update(valid_gtins_col)
            
            # Store GTIN data for each row
            for idx, row in df.iterrows():
                if row[f'{gtin_col}_valid'] and row[f'{gtin_col}_clean']:
                    gtin_data.append({
                        'row_idx': idx,
                        'gtin': row[f'{gtin_col}_clean'],
                        'column': gtin_col
                    })
        
        valid_gtins = list(all_valid_gtins)
        print(f"Found {len(valid_gtins)} unique valid GTINs across all columns")
        
        if len(valid_gtins) == 0:
            print(f"WARNING: No valid GTINs found in the file")
            # Add empty results columns
            df['gtin_exists'] = False
            df['gtin_category'] = None
            df['gtin_subcategory'] = None
            df['gtin_subsubcategory'] = None
            df['gtin_query_name'] = None
            df['ai_decision'] = 'No GTINs Found'
            df['ai_confidence'] = 0.0
            df['ai_reasoning'] = 'No valid GTINs found in file'
        else:
            # Check GTINs against database
            print(f"Checking GTINs against database...")
            batch_results = self.query_obj.batch_query_gtins(valid_gtins)
            
            # Process GTIN data for each row
            print(f"Processing {len(df)} rows with GTIN information...")
            
            # Initialize result columns
            df['gtin_exists'] = False
            df['gtin_category'] = None
            df['gtin_subcategory'] = None
            df['gtin_subsubcategory'] = None
            df['gtin_query_name'] = None
            df['ai_decision'] = None
            df['ai_confidence'] = 0.0
            df['ai_reasoning'] = None
            
            # Process each row's GTINs
            for row_data in gtin_data:
                row_idx = row_data['row_idx']
                gtin = row_data['gtin']
                column = row_data['column']
                
                if gtin in batch_results:
                    exists, info = batch_results[gtin]
                    if exists:
                        taxonomy = info.get('taxonomy', {})
                        search = info.get('search', {})
                        
                        # Update row with GTIN info (prioritize first successful match)
                        if not df.iloc[row_idx]['gtin_exists']:
                            df.iloc[row_idx, df.columns.get_loc('gtin_exists')] = True
                            df.iloc[row_idx, df.columns.get_loc('gtin_category')] = taxonomy.get('level1')
                            df.iloc[row_idx, df.columns.get_loc('gtin_subcategory')] = taxonomy.get('level2')
                            df.iloc[row_idx, df.columns.get_loc('gtin_subsubcategory')] = taxonomy.get('level3')
                            df.iloc[row_idx, df.columns.get_loc('gtin_query_name')] = search.get('product_name')
                            df.iloc[row_idx, df.columns.get_loc('ai_decision')] = 'SALT Database'
                            df.iloc[row_idx, df.columns.get_loc('ai_confidence')] = search.get('confidence', 0.8)
                            df.iloc[row_idx, df.columns.get_loc('ai_reasoning')] = f"Product found in SALT database: {search.get('product_name', 'Unknown')}"
                        continue
                
                # Try Perplexity API as fallback for GTINs not found in database
                if not df.iloc[row_idx]['gtin_exists']:
                    try:
                        perplexity_result = self._search_perplexity_for_gtin(df.iloc[row_idx], gtin)
                        if perplexity_result[0]:  # If Perplexity found something
                            df.iloc[row_idx, df.columns.get_loc('gtin_exists')] = perplexity_result[0]
                            df.iloc[row_idx, df.columns.get_loc('gtin_category')] = perplexity_result[1]
                            df.iloc[row_idx, df.columns.get_loc('gtin_subcategory')] = perplexity_result[2]
                            df.iloc[row_idx, df.columns.get_loc('gtin_subsubcategory')] = perplexity_result[3]
                            df.iloc[row_idx, df.columns.get_loc('gtin_query_name')] = perplexity_result[4]
                            df.iloc[row_idx, df.columns.get_loc('ai_decision')] = perplexity_result[5]
                            df.iloc[row_idx, df.columns.get_loc('ai_confidence')] = perplexity_result[6]
                            df.iloc[row_idx, df.columns.get_loc('ai_reasoning')] = perplexity_result[7]
                    except Exception as e:
                        print(f"  Error processing GTIN {gtin} for row {row_idx}: {e}")
            
            print(f"GTIN processing completed, results shape: {len(df)}")
            
            print(f"Added GTIN columns to DataFrame")
            print(f"   Columns: {list(df.columns)}")
            
            # Print summary
            existing_count = df['gtin_exists'].sum()
            print(f"Database lookup complete: {existing_count}/{len(valid_gtins)} GTINs found in database")
        
        # Clean up temporary columns
        # Clean up temporary GTIN columns (keep the individual UPC columns)
        temp_columns_to_drop = []
        for col in df.columns:
            if col.endswith('_clean') or col.endswith('_valid'):
                temp_columns_to_drop.append(col)
        
        if temp_columns_to_drop:
            df = df.drop(temp_columns_to_drop, axis=1)
            print(f"Cleaned up temporary columns: {temp_columns_to_drop}")
        
        # Generate output path if not provided
        if not output_path:
            output_path = "gtin_lookup_output.csv"
        
        # Save results (always save as CSV for consistency)
        try:
            df.to_csv(output_path, index=False)
            print(f"Results saved to: {output_path}")
        except Exception as e:
            print(f"Error saving results: {e}")
            raise
        
        # Print summary
        print(f"\nProcessing Summary:")
        print(f"  Total rows processed: {len(df)}")
        print(f"  GTINs found in database: {df['gtin_exists'].sum()}")
        print(f"  GTINs not found: {(~df['gtin_exists']).sum()}")
        
        # Show AI decision breakdown
        print(f"\nAI Decision Summary:")
        ai_decisions = df['ai_decision'].value_counts()
        for decision, count in ai_decisions.items():
            if pd.notna(decision):
                print(f"  {decision}: {count}")
        
        # Show confidence distribution
        if df['ai_confidence'].notna().any():
            avg_confidence = df['ai_confidence'].mean()
            print(f"  Average AI Confidence: {avg_confidence:.2f}")
        
        if df['gtin_exists'].sum() > 0:
            print(f"\nSample categories found:")
            categories = df[df['gtin_exists']]['gtin_category'].value_counts().head(5)
            for category, count in categories.items():
                if pd.notna(category):
                    print(f"    {category}: {count}")
        
        # Clean up MongoDB connection
        if self.query_obj:
            self.query_obj.disconnect()
            print(f"Disconnected from MongoDB")
        
        return str(output_path)
    
    def _process_large_dataset(self, df: pd.DataFrame, file_path: str, output_path: str, 
                              gtin_column: str, chunk_size: int) -> str:
        """Process large datasets in chunks to avoid memory/timeout issues."""
        import time
        
        print(f"Processing {len(df)} rows in chunks of {chunk_size}")
        
        # Detect GTIN column once
        if gtin_column:
            if gtin_column not in df.columns:
                raise ValueError(f"Specified GTIN column '{gtin_column}' not found in file")
            self.gtin_column = gtin_column
            print(f"Using specified GTIN column: '{gtin_column}'")
        else:
            self.gtin_column = self._auto_detect_gtin_column(df)
        
        # Process chunks
        all_results = []
        total_chunks = (len(df) + chunk_size - 1) // chunk_size
        
        for chunk_num in range(total_chunks):
            start_idx = chunk_num * chunk_size
            end_idx = min(start_idx + chunk_size, len(df))
            chunk_df = df.iloc[start_idx:end_idx].copy()
            
            print(f"Processing chunk {chunk_num + 1}/{total_chunks} (rows {start_idx+1}-{end_idx})")
            
            try:
                # Process this chunk
                chunk_results = self._process_chunk(chunk_df)
                all_results.append(chunk_results)
                
                # Small delay between chunks to avoid overwhelming APIs
                if chunk_num < total_chunks - 1:
                    time.sleep(1)
                    
            except Exception as e:
                print(f"Error processing chunk {chunk_num + 1}: {e}")
                # Continue with next chunk
                continue
        
        # Combine all results
        if all_results:
            final_df = pd.concat(all_results, ignore_index=True)
            
            # Save results
            if not output_path:
                output_path = file_path.replace('.csv', '_processed.csv').replace('.xlsx', '_processed.csv')
            
            final_df.to_csv(output_path, index=False)
            print(f"Chunked processing completed. Results saved to: {output_path}")
            print(f"Final dataset: {len(final_df)} rows, {len(final_df.columns)} columns")
            
            return str(output_path)
        else:
            raise Exception("No chunks were processed successfully")
    
    def _process_chunk(self, chunk_df: pd.DataFrame) -> pd.DataFrame:
        """Process a single chunk of data."""
        # Connect to MongoDB for this chunk
        if not self.query_obj:
            from .mongodb_lookup import MongoDBGTINLookup

            self.query_obj = MongoDBGTINLookup()
        
        # Clean and validate GTINs for this chunk
        chunk_df['gtin_clean'] = chunk_df[self.gtin_column].apply(self._normalize_gtin)
        chunk_df['gtin_valid'] = chunk_df['gtin_clean'].apply(self._validate_gtin_format)
        
        # Get unique valid GTINs
        valid_gtins = [gtin for gtin in chunk_df['gtin_clean'].unique() 
                      if gtin and self._validate_gtin_format(gtin)]
        
        if valid_gtins:
            print(f"  Found {len(valid_gtins)} unique valid GTINs to check")
            batch_results = self.query_obj.batch_query_gtins(valid_gtins)
            
            # Define the function to get GTIN info
            def get_gtin_info(row):
                gtin_clean = row['gtin_clean']
                if not gtin_clean or not row['gtin_valid']:
                    return False, None, None, None, None, None, None, None
                
                if gtin_clean in batch_results:
                    exists, info = batch_results[gtin_clean]
                    if exists:
                        taxonomy = info.get('taxonomy', {})
                        search = info.get('search', {})
                        return (
                            True,
                            taxonomy.get('category'),
                            taxonomy.get('subcategory'),
                            taxonomy.get('subsubcategory'),
                            search.get('query_name'),
                            'SALT Database',
                            1.0,
                            'Product found in SALT database'
                        )
                    else:
                        # Try Perplexity API as fallback
                        return self._search_perplexity_for_gtin(row, gtin_clean)
                else:
                    # Try Perplexity API as fallback
                    return self._search_perplexity_for_gtin(row, gtin_clean)
            
            # Apply the function to get results
            results = chunk_df.apply(get_gtin_info, axis=1)
            
            # Unpack results into separate columns
            chunk_df['gtin_exists'] = [r[0] for r in results]
            chunk_df['gtin_category'] = [r[1] for r in results]
            chunk_df['gtin_subcategory'] = [r[2] for r in results]
            chunk_df['gtin_subsubcategory'] = [r[3] for r in results]
            chunk_df['gtin_query_name'] = [r[4] for r in results]
            chunk_df['ai_decision'] = [r[5] for r in results]
            chunk_df['ai_confidence'] = [r[6] for r in results]
            chunk_df['ai_reasoning'] = [r[7] for r in results]
        else:
            # No valid GTINs, add empty columns
            for col in ['gtin_exists', 'gtin_category', 'gtin_subcategory', 'gtin_subsubcategory', 
                       'gtin_query_name', 'ai_decision', 'ai_confidence', 'ai_reasoning']:
                chunk_df[col] = False if col == 'gtin_exists' else None
        
        return chunk_df

    def _add_parsed_product_info(self, df: pd.DataFrame, description_col: str) -> pd.DataFrame:
        """Parse brand, size, and pack size using existing columns first, then AI parsing as fallback."""
        try:
            print(f"    Parsing brand, size, and pack size from existing columns and product names...")
            
            # Initialize result columns
            parsed_brands = []
            parsed_sizes = []
            parsed_pack_sizes = []
            
            # Process each row
            for idx, row in df.iterrows():
                # Try to extract from existing columns first
                brand = self._extract_brand_from_existing_columns(row)
                size = self._extract_size_from_existing_columns(row)
                pack_size = self._extract_pack_size_from_existing_columns(row)
                
                # If we didn't get complete info from existing columns, use AI parsing as fallback
                if not brand or not size or not pack_size:
                    product_name = row[description_col]
                    ai_parsed = self._parse_single_product_info(product_name)
                    
                    # Use AI results to fill in missing information
                    if not brand and ai_parsed.get('brand'):
                        brand = ai_parsed['brand']
                    if not size and ai_parsed.get('size'):
                        size = ai_parsed['size']
                    if not pack_size and ai_parsed.get('pack_size'):
                        pack_size = ai_parsed['pack_size']
                
                parsed_brands.append(brand or '')
                parsed_sizes.append(size or '')
                parsed_pack_sizes.append(pack_size or '')
            
            # Add the new columns to the dataframe
            df['parsed_brand'] = parsed_brands
            df['parsed_size'] = parsed_sizes
            df['parsed_pack_size'] = parsed_pack_sizes
            
            # Print summary statistics
            brands_found = sum(1 for brand in parsed_brands if brand.strip())
            sizes_found = sum(1 for size in parsed_sizes if size.strip())
            pack_sizes_found = sum(1 for pack in parsed_pack_sizes if pack.strip())
            
            print(f"        Parsing completed: {len(parsed_brands)} items processed")
            print(f"        Brands found: {brands_found} items ({brands_found/len(parsed_brands)*100:.1f}%)")
            print(f"        Sizes found: {sizes_found} items ({sizes_found/len(parsed_brands)*100:.1f}%)")
            print(f"        Pack sizes found: {pack_sizes_found} items ({pack_sizes_found/len(parsed_brands)*100:.1f}%)")
            
            return df
            
        except Exception as e:
            print(f"    WARNING: Error parsing product info: {e}")
            # Fallback: create empty columns
            df['parsed_brand'] = [''] * len(df)
            df['parsed_size'] = [''] * len(df)
            df['parsed_pack_size'] = [''] * len(df)
            return df

    def _extract_brand_from_existing_columns(self, row) -> str:
        """Extract brand from existing columns like BRAND."""
        # Check BRAND column first
        brand_cols = ['BRAND', '"BRAND"']
        for col in brand_cols:
            if col in row.index and pd.notna(row[col]) and str(row[col]).strip():
                brand = str(row[col]).strip()
                if brand and brand != '      ':  # Skip empty/spaces-only brands
                    return brand
        return ''

    def _extract_size_from_existing_columns(self, row) -> str:
        """Extract size from existing columns like PACK_SIZE, WEIGHT, etc."""
        # Check PACK_SIZE column first
        size_cols = ['PACK_SIZE', '"PACK_SIZE"', 'WEIGHT', '"WEIGHT"']
        for col in size_cols:
            if col in row.index and pd.notna(row[col]) and str(row[col]).strip():
                size = str(row[col]).strip()
                if size and size != '          ':  # Skip empty/spaces-only sizes
                    return size
        return ''

    def _extract_pack_size_from_existing_columns(self, row) -> str:
        """Extract pack size from existing columns like STOCK_UNIT, SELL_UNIT, CASE_UNIT."""
        # Check unit columns in order of preference
        unit_cols = ['STOCK_UNIT', '"STOCK_UNIT"', 'SELL_UNIT', '"SELL_UNIT"', 'CASE_UNIT', '"CASE_UNIT"']
        for col in unit_cols:
            if col in row.index and pd.notna(row[col]) and str(row[col]).strip():
                unit = str(row[col]).strip()
                if unit and unit != '   ':  # Skip empty/spaces-only units
                    return unit
        return ''

    def _parse_single_product_info(self, product_name: str) -> dict:
        """Parse brand, size, and pack size from a single product name using AI."""
        try:
            prompt = f"""
You are a product data parser. For the product name below, extract the brand, size, and pack size information.

Return your response as a JSON object with these fields:
- "brand": The brand name (e.g., "LD", "GOLD CREST", "SMOKER FRIENDLY")
- "size": The size information (e.g., "100's", "100", "20", "12oz", "1L")
- "pack_size": The pack/container type (e.g., "BOX", "BX", "CTN", "PK", "CASE")

Product name to parse: {product_name}

Examples:
- "LD BLUE 100'S BOX" → {{"brand": "LD", "size": "100's", "pack_size": "BOX"}}
- "GOLD CREST YELLOW 100'S BOX" → {{"brand": "GOLD CREST", "size": "100's", "pack_size": "BOX"}}
- "SMOKER FRIENDLY FILT VANILLA 100BX" → {{"brand": "SMOKER FRIENDLY", "size": "100", "pack_size": "BX"}}
- "CROWN RED BOX" → {{"brand": "CROWN", "size": "", "pack_size": "BOX"}}
- "E-CIG BLU MENTHOL" → {{"brand": "BLU", "size": "", "pack_size": ""}}

Return only the JSON object, no additional text.
"""

            # Call the AI service
            response = self.data_cleaner.model.generate_content(prompt)
            
            if not response or not response.text:
                return {"brand": "", "size": "", "pack_size": ""}
            
            # Parse the JSON response
            response_text = response.text.strip()
            
            # Try to extract JSON object from the response
            try:
                # Find the JSON object in the response
                start_idx = response_text.find('{')
                end_idx = response_text.rfind('}')
                
                if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
                    json_text = response_text[start_idx:end_idx + 1]
                    parsed_data = json.loads(json_text)
                    return parsed_data
                else:
                    return {"brand": "", "size": "", "pack_size": ""}
                    
            except json.JSONDecodeError:
                return {"brand": "", "size": "", "pack_size": ""}
                
        except Exception:
            return {"brand": "", "size": "", "pack_size": ""}

    def _parse_product_info_batch(self, product_names: List[str]) -> List[dict]:
        """Parse brand, size, and pack size from a batch of product names using AI."""
        try:
            # Create the prompt for parsing product information
            prompt = f"""
You are a product data parser. For each product name below, extract the brand, size, and pack size information.

Return your response as a JSON array where each item is an object with these fields:
- "brand": The brand name (e.g., "LD", "GOLD CREST", "SMOKER FRIENDLY")
- "size": The size information (e.g., "100's", "100", "20", "12oz", "1L")
- "pack_size": The pack/container type (e.g., "BOX", "BX", "CTN", "PK", "CASE")

Product names to parse:
{chr(10).join([f"{i+1}. {name}" for i, name in enumerate(product_names)])}

Examples:
- "LD BLUE 100'S BOX" → {{"brand": "LD", "size": "100's", "pack_size": "BOX"}}
- "GOLD CREST YELLOW 100'S BOX" → {{"brand": "GOLD CREST", "size": "100's", "pack_size": "BOX"}}
- "SMOKER FRIENDLY FILT VANILLA 100BX" → {{"brand": "SMOKER FRIENDLY", "size": "100", "pack_size": "BX"}}
- "CROWN RED BOX" → {{"brand": "CROWN", "size": "", "pack_size": "BOX"}}
- "E-CIG BLU MENTHOL" → {{"brand": "BLU", "size": "", "pack_size": ""}}

Return only the JSON array, no additional text.
"""

            # Call the AI service
            response = self.data_cleaner.model.generate_content(prompt)
            
            if not response or not response.text:
                print(f"        WARNING: Empty AI response for product parsing")
                return [{"brand": "", "size": "", "pack_size": ""} for _ in product_names]
            
            # Parse the JSON response
            response_text = response.text.strip()
            
            # Try to extract JSON array from the response
            try:
                # Find the JSON array in the response
                start_idx = response_text.find('[')
                end_idx = response_text.rfind(']')
                
                if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
                    json_text = response_text[start_idx:end_idx + 1]
                    parsed_data = json.loads(json_text)
                    
                    # Ensure we have the right number of items
                    if len(parsed_data) == len(product_names):
                        return parsed_data
                    else:
                        print(f"        WARNING: AI returned {len(parsed_data)} items, expected {len(product_names)}")
                        # Pad or truncate as needed
                        while len(parsed_data) < len(product_names):
                            parsed_data.append({"brand": "", "size": "", "pack_size": ""})
                        return parsed_data[:len(product_names)]
                else:
                    print(f"        WARNING: Could not find JSON array in AI response")
                    return [{"brand": "", "size": "", "pack_size": ""} for _ in product_names]
                    
            except json.JSONDecodeError as e:
                print(f"        WARNING: JSON parsing error: {e}")
                return [{"brand": "", "size": "", "pack_size": ""} for _ in product_names]
                
        except Exception as e:
            print(f"        WARNING: Error in AI parsing: {e}")
            return [{"brand": "", "size": "", "pack_size": ""} for _ in product_names]

def main():
    """Main function for command line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Process CSV or Excel file to check GTINs against MongoDB database with enhanced features')
    parser.add_argument('file_path', help='Path to input file (CSV, XLSX, or XLS)')
    parser.add_argument('-o', '--output', help='Output file path (optional, defaults to CSV)')
    parser.add_argument('-g', '--gtin-column', help='Name of GTIN column (optional, will auto-detect)')
    parser.add_argument('-l', '--row-limit', type=int, help='Limit processing to first N rows (optional, for testing)')
    parser.add_argument('--no-enhancement', action='store_true', help='Disable AI-powered data enhancement')
    parser.add_argument('--cleaning-only', action='store_true', help='Only run data cleaning without GTIN processing')
    parser.add_argument('--chunk-size', type=int, default=100, help='Process data in chunks of specified size (default: 100)')
    
    args = parser.parse_args()
    
    try:
        processor = GTINValidationPipeline(
            enable_data_cleaning=True,
            enable_perplexity=True
        )
        if args.cleaning_only:
            # Only run data cleaning without GTIN processing
            print(f"Running data cleaning only...")
            
            # Load the file for cleaning
            file_ext = Path(args.file_path).suffix.lower()
            if file_ext in ['.xlsx', '.xls']:
                print(f"Reading Excel file...")
                df = pd.read_excel(args.file_path)
            elif file_ext == '.csv':
                print(f"Reading CSV file...")
                df = pd.read_csv(args.file_path, quoting=3, on_bad_lines='skip')
            else:
                raise ValueError(f"Unsupported file type: {file_ext}")
            
            # Apply row limit if specified
            if args.row_limit and len(df) > args.row_limit:
                df = df.head(args.row_limit)
                print(f"Limited to first {args.row_limit} rows for testing")
            
            print(f"File loaded successfully: {len(df)} rows, {len(df.columns)} columns")
            
            # Run data cleaning only
            enhanced_df = processor._run_data_cleaning_pipeline(df)
            
            # Save cleaned data
            output_file = args.output or "cleaned_data.csv"
            enhanced_df.to_csv(output_file, index=False)
            print(f"Data cleaning completed and saved to: {output_file}")
            print(f"Output columns: {list(enhanced_df.columns)}")
        else:
            # Run full GTIN processing
            output_file = processor.process_file(
                file_path=args.file_path,
                output_path=args.output,
                gtin_column=args.gtin_column,
                row_limit=args.row_limit,
                enable_enhancement=not args.no_enhancement,
                chunk_size=args.chunk_size
            )
        print(f"\nProcessing complete! Results saved to: {output_file}")
        
    except Exception as e:
        print(f"\nProcessing failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
