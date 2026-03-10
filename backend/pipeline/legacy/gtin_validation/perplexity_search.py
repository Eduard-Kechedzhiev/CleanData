#!/usr/bin/env python3
"""
Perplexity GTIN API Client
Uses Perplexity API to search for product information as a fallback
for GTINs that don't exist in GS1 or MongoDB.
"""

import time
import json
import logging
import requests
from typing import Dict, Optional
import google.generativeai as genai
import os
from dotenv import load_dotenv

# Load environment variables
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

# AI Prompt for Perplexity Product Validation
PERPLEXITY_PRODUCT_VALIDATION_PROMPT = """
You are an AI assistant helping to validate whether a product found through Perplexity search matches the product information from a CSV file.

CSV Product Information:
- Product Name: {csv_product_name}
- Description: {csv_description}
- Brand: {csv_brand}
- Size/Pack: {csv_size_pack}

Perplexity Search Results:
- Product Name: {perplexity_product_name}
- Description: {perplexity_description}
- Size/Weight: {perplexity_size}
- Additional Info: {perplexity_additional}

Please analyze whether these two products are likely the same item, considering:

1. **Product Identity**: Are the names describing the same core product?
2. **Brand Consistency**: Do the brands match or are they related?
3. **Size/Pack Compatibility**: Are the sizes/packs compatible?
4. **Description Similarity**: Do the descriptions refer to the same product?

Common scenarios to consider:
- Product names may use different terminology (e.g., "Corn Kernel" vs "Sweet Corn")
- Brands may have variations (e.g., "ALASKO" vs "Alasko Foods")
- Sizes may be expressed differently (e.g., "100g" vs "3.5 oz")
- Descriptions may focus on different aspects of the same product

IMPORTANT: Return ONLY a valid JSON object with no extra text, newlines, or formatting:

{{"is_same_product": true, "confidence": 0.9, "reasoning": "Products match based on brand and size", "mismatch_details": ""}}

or

{{"is_same_product": false, "confidence": 0.7, "reasoning": "Different product categories", "mismatch_details": "CSV shows food item, Perplexity shows cleaning product"}}
"""

class PerplexityProductSearch:
    """Uses Perplexity API to search for product information as GTIN fallback"""
    
    def __init__(self):
        """Initialize Perplexity API client"""
        self.api_key = os.getenv('PERPLEXITY_API_KEY')
        self.model = None
        self._setup_ai()
        
        if not self.api_key:
            logger.warning("PERPLEXITY_API_KEY not found - Perplexity API will be disabled")
    
    def _setup_ai(self):
        """Setup Google Gemini AI for product validation"""
        try:
            api_key = os.getenv('GEMINI_API_KEY')
            if api_key:
                genai.configure(api_key=api_key)
                self.model = genai.GenerativeModel('models/gemini-2.0-flash')
                logger.info("Google Gemini AI initialized for Perplexity product validation")
            else:
                logger.warning("GEMINI_API_KEY not found - AI validation will be disabled")
                self.model = None
        except Exception as e:
            logger.warning(f"Failed to initialize AI: {e} - AI validation will be disabled")
            self.model = None
    
    def search_by_gtin(self, gtin: str) -> Optional[Dict]:
        """
        Search for product information using GTIN via Perplexity API
        
        Args:
            gtin: GTIN to search for
            
        Returns:
            Dictionary with product information or None if not found
        """
        if not self.api_key:
            logger.error("Perplexity API key not configured")
            return None
        
        try:
            logger.info(f"Searching Perplexity API for GTIN: {gtin}")
            
            # Construct search query
            search_query = f"Search for product information, product name, brand, description, and size/weight for the product with barcode/GTIN {gtin}. Look in product databases, retail catalogs, and manufacturer websites. If you cannot find specific product details, please indicate that no product information was found."
            
            # Prepare API request
            headers = {
                'Authorization': f'Bearer {self.api_key}',
                'Content-Type': 'application/json'
            }
            
            payload = {
                'model': 'sonar',
                'messages': [
                    {
                        'role': 'user',
                        'content': search_query
                    }
                ],
                'max_tokens': 1000,
                'temperature': 0.1
            }
            
            # Make API call
            response = requests.post(
                'https://api.perplexity.ai/chat/completions',
                headers=headers,
                json=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                response_data = response.json()
                content = response_data['choices'][0]['message']['content']
                logger.info(f"Successfully received response from Perplexity API for GTIN: {gtin}")
                logger.info(f"Raw Perplexity response (first 500 chars): {content[:500]}")
                
                # Parse the response to extract product information
                product_info = self._parse_perplexity_response(content, gtin)
                
                if product_info:
                    logger.info(f"Successfully extracted product info from Perplexity API for GTIN: {gtin}")
                    logger.info(f"Extracted product info: {product_info}")
                    return product_info
                else:
                    logger.warning(f"No product information could be parsed from Perplexity API response for GTIN: {gtin}")
                    return None
            else:
                logger.error(f"Perplexity API request failed with status {response.status_code}: {response.text}")
                return None
                
        except requests.exceptions.Timeout:
            logger.error(f"Perplexity API request timed out for GTIN: {gtin}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Perplexity API request failed for GTIN {gtin}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error searching Perplexity API for GTIN {gtin}: {e}")
            return None
    
    def _parse_perplexity_response(self, response_text: str, gtin: str) -> Optional[Dict]:
        """Parse Perplexity API response to extract product information"""
        try:
            logger.info(f"Parsing Perplexity response for GTIN: {gtin}")
            logger.info(f"Response length: {len(response_text)} characters")
            
            # Check if the response indicates no product found
            negative_indicators = [
                "not found", "no product", "no information", "unknown", "invalid",
                "doesn't exist", "does not exist", "no results", "no data",
                "could not find", "unable to find", "no match", "do not provide any direct information",
                "does not include a lookup", "no product database entry", "mainly explain what a gtin is",
                "explain what a gtin is", "how to find or verify gtin", "where to obtain or check gtin"
            ]
            
            response_lower = response_text.lower()
            if any(indicator in response_lower for indicator in negative_indicators):
                logger.info(f"Perplexity API response indicates no product found for GTIN: {gtin}")
                return None
            
            # Additional check: if response is mostly about GTIN explanation rather than product info
            explanation_keywords = ["gtin is", "what is a gtin", "how to find", "how to verify", "where to obtain", "gtin database", "gtin tools"]
            explanation_count = sum(1 for keyword in explanation_keywords if response_text.lower().count(keyword))
            
            if explanation_count >= 2:  # If response contains 2+ explanation keywords, it's likely not product info
                logger.info(f"Perplexity API response appears to be GTIN explanation, not product info for GTIN: {gtin}")
                return None
            
            logger.info(f"No negative indicators found, proceeding with extraction")
            
            # Extract product information using AI if available
            if self.model:
                logger.info(f"Using AI to extract product info")
                return self._ai_extract_product_info(response_text)
            else:
                logger.info(f"Using basic text parsing fallback")
                # Fallback to basic text parsing
                return self._basic_extract_product_info(response_text)
                
        except Exception as e:
            logger.error(f"Error parsing Perplexity API response: {e}")
            return None
    
    def _ai_extract_product_info(self, response_text: str) -> Optional[Dict]:
        """Use AI to extract product information from Perplexity API response"""
        try:
            prompt = f"""
            Extract product information from this Perplexity API response. You are looking for SPECIFIC PRODUCT DETAILS, not general explanations about GTINs.
            
            IMPORTANT: Only return product information if the response actually contains details about a specific product. If the response is just explaining what GTINs are or how to find them, return null for all fields.
            
            Look for:
            - product_name: The actual name of the product (e.g., "Sticky's Traditional Strawberry Jam")
            - description: Product description (e.g., "Traditional Strawberry Jam with low sugar content")
            - brand: Brand name if mentioned (e.g., "Sticky's")
            - size: Size/weight information if available (e.g., "500 grams")
            
            Response text:
            {response_text[:2000]}  # Limit to first 2000 chars
            
            If the response contains actual product information, return a JSON object. If it's just GTIN explanation, return:
            {{"product_name": null, "description": null, "brand": null, "size": null}}
            
            Return only valid JSON, nothing else.
            """
            
            response = self.model.generate_content(prompt)
            ai_response = response.text.strip()
            
            # Parse JSON response
            try:
                # Clean up the response
                ai_response = ai_response.replace('\n', ' ').replace('  ', ' ').strip()
                
                # Extract JSON content
                start_idx = ai_response.find('{')
                end_idx = ai_response.rfind('}')
                
                if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
                    json_content = ai_response[start_idx:end_idx + 1]
                    product_info = json.loads(json_content)
                    
                    # Add source information
                    product_info['source'] = 'Perplexity API'
                    return product_info
                else:
                    logger.warning("AI response did not contain valid JSON")
                    return None
                    
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse AI response as JSON: {e}")
                return None
                
        except Exception as e:
            logger.error(f"Error using AI to extract product info: {e}")
            return None
    
    def _basic_extract_product_info(self, response_text: str) -> Optional[Dict]:
        """Basic text parsing fallback for product information"""
        try:
            # Check if response is mostly about GTIN explanation rather than product info
            explanation_keywords = ["gtin is", "what is a gtin", "how to find", "how to verify", "where to obtain", "gtin database", "gtin tools"]
            explanation_count = sum(1 for keyword in explanation_keywords if response_text.lower().count(keyword))
            
            if explanation_count >= 2:  # If response contains 2+ explanation keywords, it's likely not product info
                logger.info("Basic parser: Response appears to be GTIN explanation, not product info")
                return None
            
            # Simple extraction logic
            lines = response_text.split('\n')
            
            product_name = ""
            description = ""
            brand = ""
            size = ""
            
            for line in lines:
                line_lower = line.lower()
                if any(keyword in line_lower for keyword in ['product', 'item', 'name']):
                    product_name = line.strip()
                elif any(keyword in line_lower for keyword in ['brand', 'manufacturer', 'company']):
                    brand = line.strip()
                elif any(keyword in line_lower for keyword in ['size', 'weight', 'grams', 'ounces', 'kg', 'lb']):
                    size = line.strip()
                elif len(line.strip()) > 20 and not description:  # First long line as description
                    description = line.strip()
            
            if product_name or description:
                return {
                    'product_name': product_name,
                    'description': description,
                    'brand': brand,
                    'size': size,
                    'source': 'Perplexity API (Basic Parse)'
                }
            else:
                logger.info("Basic parser: No product information found in response")
                return None
                
        except Exception as e:
            logger.error(f"Error in basic text parsing: {e}")
            return None
    
    def _validate_product_match(self, csv_data: Dict, perplexity_data: Dict) -> Dict:
        """
        Use AI to validate whether Perplexity product matches CSV product
        
        Args:
            csv_data: Dictionary containing CSV product information
            perplexity_data: Dictionary containing Perplexity product information
            
        Returns:
            Dictionary with validation results
        """
        if not self.model:
            return {
                "is_same_product": True,  # Default to True if AI not available
                "confidence": 0.5,
                "reasoning": "AI validation not available",
                "mismatch_details": ""
            }
        
        try:
            # Prepare the prompt with product information
            prompt = PERPLEXITY_PRODUCT_VALIDATION_PROMPT.format(
                csv_product_name=csv_data.get('product_name', 'N/A'),
                csv_description=csv_data.get('description', 'N/A'),
                csv_brand=csv_data.get('brand', 'N/A'),
                csv_size_pack=csv_data.get('size_pack', 'N/A'),
                perplexity_product_name=perplexity_data.get('product_name', 'N/A'),
                perplexity_description=perplexity_data.get('description', 'N/A'),
                perplexity_size=perplexity_data.get('size', 'N/A'),
                perplexity_additional=perplexity_data.get('brand', 'N/A')
            )
            
            # Get AI response
            response = self.model.generate_content(prompt)
            ai_response = response.text.strip()
            
            # Parse JSON response
            try:
                # Clean up the response
                ai_response = ai_response.replace('\n', ' ').replace('  ', ' ').strip()
                
                # Try to extract JSON from the response
                start_idx = ai_response.find('{')
                end_idx = ai_response.rfind('}')
                
                if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
                    json_content = ai_response[start_idx:end_idx + 1]
                    validation_result = json.loads(json_content)
                else:
                    # Fallback: try to parse the entire response
                    validation_result = json.loads(ai_response)
                
                return validation_result
                
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse AI response as JSON: {e}")
                logger.error(f"AI response: {ai_response}")
                
                # Try to manually extract key information from the response
                try:
                    is_same = "true" in ai_response.lower() or "same" in ai_response.lower()
                    confidence = 0.5  # Default confidence
                    reasoning = "Parsed from text response"
                    mismatch_details = ""
                    
                    return {
                        "is_same_product": is_same,
                        "confidence": confidence,
                        "reasoning": reasoning,
                        "mismatch_details": mismatch_details
                    }
                except:
                    return {
                        "is_same_product": True,  # Default to True on error
                        "confidence": 0.5,
                        "reasoning": f"AI response parsing failed: {e}",
                        "mismatch_details": ""
                    }
                
        except Exception as e:
            logger.error(f"Error in AI Perplexity product validation: {e}")
            return {
                "is_same_product": True,  # Default to True on error
                "confidence": 0.5,
                "reasoning": f"AI validation error: {e}",
                "mismatch_details": ""
            }
    


def main():
    """Test the Perplexity API client with a sample GTIN"""
    client = PerplexityProductSearch()
    
    try:
        # Test with a sample GTIN
        test_gtin = "10784300264689"  # Test GTIN from user's dataset
        print(f"Testing Perplexity API client with GTIN: {test_gtin}")
        
        product_info = client.search_by_gtin(test_gtin)
        if product_info:
            print(f"Product found via Perplexity API:")
            print(f"  Product Name: {product_info.get('product_name', 'N/A')}")
            print(f"  Description: {product_info.get('description', 'N/A')}")
            print(f"  Brand: {product_info.get('brand', 'N/A')}")
            print(f"  Size: {product_info.get('size', 'N/A')}")
            
            # Test AI validation
            csv_data = {
                'product_name': 'Test Product',
                'description': 'Test Description',
                'brand': 'Test Brand',
                'size_pack': '100g'
            }
            
            validation = client._validate_product_match(csv_data, product_info)
            print(f"\nAI Validation Results:")
            print(f"  Same Product: {validation['is_same_product']}")
            print(f"  Confidence: {validation['confidence']}")
            print(f"  Reasoning: {validation['reasoning']}")
        else:
            print(f"Product not found via Perplexity API for GTIN: {test_gtin}")
            
    except Exception as e:
        print(f"Error in main execution: {e}")

if __name__ == "__main__":
    main()
