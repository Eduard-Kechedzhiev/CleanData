#!/usr/bin/env python3
"""
Taxonomy Categorizer Module
Handles all SALT Taxonomy categorization logic for the AI Data Cleaner.
"""

import json
import logging
from typing import List, Dict, Any, Optional
import pandas as pd
from .prompts import (
    TAXONOMY_LEVEL1_IMPROVED_PROMPT,
    TAXONOMY_LEVEL2_IMPROVED_PROMPT,
    TAXONOMY_LEVEL3_IMPROVED_PROMPT,
    ENHANCED_PRODUCT_DESCRIPTION_TEMPLATES,
)

logger = logging.getLogger(__name__)


class TaxonomyCategorizer:
    """Handles all taxonomy categorization logic."""
    
    def __init__(self, taxonomy_df: pd.DataFrame):
        """Initialize the taxonomy categorizer with taxonomy data."""
        self.taxonomy_df = taxonomy_df
        self._build_taxonomy_tree()
    
    def _build_taxonomy_tree(self):
        """Build a robust taxonomy tree for faster lookups and validation."""
        try:
            self.taxonomy_tree = {}
            
            # Clean and validate the taxonomy data first
            cleaned_taxonomy = self.taxonomy_df.copy()
            
            # Clean up any empty or whitespace-only values
            for col in ['Level I', 'Level II', 'Level III']:
                if col in cleaned_taxonomy.columns:
                    cleaned_taxonomy[col] = cleaned_taxonomy[col].astype(str).str.strip()
                    # Remove rows where the column is empty or just whitespace
                    cleaned_taxonomy = cleaned_taxonomy[cleaned_taxonomy[col] != '']
                    cleaned_taxonomy = cleaned_taxonomy[cleaned_taxonomy[col] != 'nan']
                    cleaned_taxonomy = cleaned_taxonomy[cleaned_taxonomy[col] != 'None']
            
            # Build Level I → Level II → Level III tree
            for _, row in cleaned_taxonomy.iterrows():
                level1 = row['Level I'].strip()
                level2 = row['Level II'].strip()
                level3 = row['Level III'].strip()
                
                # Skip invalid entries
                if not level1 or level1 in ['nan', 'None', '']:
                    continue
                
                if level1 not in self.taxonomy_tree:
                    self.taxonomy_tree[level1] = {}
                
                if level2 and level2 not in ['nan', 'None', '']:
                    if level2 not in self.taxonomy_tree[level1]:
                        self.taxonomy_tree[level1][level2] = []
                    if level3 and level3 not in ['nan', 'None', '']:
                        self.taxonomy_tree[level1][level2].append(level3)
            
            # Build reverse lookup maps for validation
            self.level1_categories = sorted(list(self.taxonomy_tree.keys()))
            self.level2_lookup = {}  # Level I → [Level II]
            self.level3_lookup = {}  # (Level I, Level II) → [Level III]
            
            for level1, level2_dict in self.taxonomy_tree.items():
                self.level2_lookup[level1] = sorted(list(level2_dict.keys()))
                
                for level2, level3_list in level2_dict.items():
                    key = (level1, level2)
                    self.level3_lookup[key] = sorted(level3_list)
            
            # Validate the tree structure
            if not self.level1_categories:
                logger.error("No valid Level I categories found in taxonomy")
                raise ValueError("Taxonomy tree is empty - no valid categories found")
            
            logger.info(f"Built robust taxonomy tree with {len(self.level1_categories)} Level I categories")
            logger.debug(f"Sample Level I: {self.level1_categories[:5]}")
            
            # Log some statistics for debugging
            total_level2 = sum(len(level2s) for level2s in self.level2_lookup.values())
            total_level3 = sum(len(level3s) for level3s in self.level3_lookup.values())
            logger.info(f"Taxonomy tree contains {total_level2} Level II categories and {total_level3} Level III categories")
            
        except Exception as e:
            logger.error(f"Error building taxonomy tree: {e}")
            self.taxonomy_tree = {}
            self.level1_categories = []
            self.level2_lookup = {}
            self.level3_lookup = {}
            raise 
    
    def _is_valid_hierarchy(self, level1: str, level2: str = "", level3: str = "") -> bool:
        """Check if a taxonomy hierarchy is valid with improved error handling."""
        try:
            if not level1 or level1 not in self.level1_categories:
                return False
            if level2:
                if level2 not in self._get_available_level2(level1):
                    return False
            if level3:
                if not level2:  # Level III requires Level II
                    return False
                if level3 not in self._get_available_level3(level1, level2):
                    return False
            return True
        except Exception as e:
            logger.warning(f"Error validating hierarchy {level1} > {level2} > {level3}: {e}")
            return False
    
    def _get_available_level2(self, level1: str) -> List[str]:
        """Get available Level II categories for a given Level I category with error handling."""
        try:
            if not level1 or level1 not in self.level2_lookup:
                return []
            return self.level2_lookup.get(level1, [])
        except Exception as e:
            logger.warning(f"Error getting Level II categories for '{level1}': {e}")
            return []
    
    def _get_available_level3(self, level1: str, level2: str) -> List[str]:
        """Get available Level III categories for a given Level I + Level II combination with error handling."""
        try:
            if not level1 or not level2:
                return []
            key = (level1, level2)
            return self.level3_lookup.get(key, [])
        except Exception as e:
            logger.warning(f"Error getting Level III categories for '{level1} > {level2}': {e}")
            return []
    
    def get_taxonomy_categories(self) -> Dict[str, List[str]]:
        """Get all available taxonomy categories for maximum accuracy."""
        if self.taxonomy_df is None:
            return {"Level I": [], "Level II": [], "Level III": []}
        
        try:
            # Return ALL available categories from each level for maximum accuracy
            filtered_taxonomy = {}
            for level in ['Level I', 'Level II', 'Level III']:
                if level in self.taxonomy_df.columns:
                    available_categories = [str(x).strip() for x in self.taxonomy_df[level].dropna().unique() if str(x).strip()]
                    # Use ALL categories for maximum accuracy
                    filtered_taxonomy[level] = sorted(available_categories)
                else:
                    filtered_taxonomy[level] = []
            
            logger.info(f"Using all taxonomy categories: Level I: {len(filtered_taxonomy.get('Level I', []))}, Level II: {len(filtered_taxonomy.get('Level II', []))}, Level III: {len(filtered_taxonomy.get('Level III', []))}")
            return filtered_taxonomy
            
        except Exception as e:
            logger.error(f"Simple taxonomy filtering also failed: {e}")
            # Last resort: return empty categories
            return {
                "Level I": [],
                "Level II": [],
                "Level III": []
            } 

    def _categorize_batch_improved_hierarchical(self, batch_descriptions: List[str], model, track_tokens_func) -> List[Dict[str, str]]:
        """Categorize a batch using rule-based approach first, then AI fallback for better accuracy."""
        print(f"  Processing batch with {len(batch_descriptions)} items using rule-based + AI fallback...")
        
        # Step 1: Rule-based categorization first (more accurate)
        print(f"    Step 1: Rule-based categorization...")
        rule_based_results = self._rule_based_categorization(batch_descriptions)
        
        # Step 2: Identify items that need AI fallback (empty categories)
        items_needing_ai = []
        ai_indices = []
        
        for i, result in enumerate(rule_based_results):
            if not result['Taxo1']:  # No rule matched
                items_needing_ai.append(batch_descriptions[i])
                ai_indices.append(i)
        
        print(f"      Rule-based categorization: {len(batch_descriptions) - len(items_needing_ai)}/{len(batch_descriptions)} items categorized")
        
        # Step 3: AI fallback for items that don't match rules
        if items_needing_ai:
            print(f"    Step 2: AI fallback for {len(items_needing_ai)} items...")
            ai_results = self._ai_categorization_fallback(items_needing_ai)
            
            # Merge AI results back into rule-based results
            for ai_idx, result_idx in enumerate(ai_indices):
                if ai_idx < len(ai_results):
                    rule_based_results[result_idx] = ai_results[ai_idx]
        
        # Step 4: Convert to the format expected by validation methods
        level1_categories = [r['Taxo1'] for r in rule_based_results]
        level2_categories = [r['Taxo2'] for r in rule_based_results]
        level3_categories = [r['Taxo3'] for r in rule_based_results]
        
        # Step 5: Comprehensive validation and consistency check
        print(f"    Step 3: Comprehensive validation and consistency check...")
        validated_categories = self._comprehensive_validation(batch_descriptions, level1_categories, level2_categories, level3_categories)
        print(f"      Validation results: {len(validated_categories)} categories")
        
        # Step 6: Final consistency enforcement
        print(f"    Step 4: Final consistency enforcement...")
        final_categories = self._enforce_hierarchical_consistency(validated_categories)
        print(f"      Consistency results: {len(final_categories)} categories")
        
        # Step 7: Enforce product consistency across similar items
        print(f"    Step 5: Enforcing product consistency across similar items...")
        product_consistent_categories = self._enforce_product_consistency(batch_descriptions, final_categories)
        print(f"      Product consistency results: {len(product_consistent_categories)} categories")
        
        # Step 8: Re-categorize unsure or incorrectly categorized items
        print(f"    Step 6: Re-categorizing unsure or incorrectly categorized items...")
        final_recategorized_categories = self._recategorize_unsure_items(batch_descriptions, product_consistent_categories, model, track_tokens_func)
        print(f"      Final results: {len(final_recategorized_categories)} categories")
        
        print(f"    Rule-based + AI fallback categorization completed for batch")
        return final_recategorized_categories

    def categorize_taxonomy(self, descriptions: List[str], model, track_tokens_func, 
                           batch_size: int = 50, enable_debug: bool = True) -> List[Dict[str, str]]:
        """Categorize items using SALT Taxonomy with improved hierarchical approach for better accuracy."""
        if self.taxonomy_df is None:
            return [{"Taxo1": "", "Taxo2": "", "Taxo3": ""} for _ in descriptions]
        
        # Validate taxonomy DataFrame structure
        required_columns = ['Level I', 'Level II', 'Level III']
        if not all(col in self.taxonomy_df.columns for col in required_columns):
            logger.error(f"Taxonomy DataFrame missing required columns. Found: {list(self.taxonomy_df.columns)}")
            return [{"Taxo1": "", "Taxo2": "", "Taxo3": ""} for _ in descriptions]
        
        if len(self.taxonomy_df) == 0:
            logger.error("Taxonomy DataFrame is empty")
            return [{"Taxo1": "", "Taxo2": "", "Taxo3": ""} for _ in descriptions]
        
        # Check if taxonomy tree is properly built
        if not hasattr(self, 'level1_categories') or not self.level1_categories:
            logger.error("Taxonomy tree not properly built - no Level I categories available")
            print(f"TAXONOMY ERROR: No Level I categories available!")
            print(f"   Taxonomy DataFrame rows: {len(self.taxonomy_df)}")
            print(f"   Taxonomy tree keys: {list(self.taxonomy_tree.keys()) if hasattr(self, 'taxonomy_tree') else 'No tree'}")
            print(f"   Level1 categories: {self.level1_categories if hasattr(self, 'level1_categories') else 'None'}")
            return [{"Taxo1": "", "Taxo2": "", "Taxo3": ""} for _ in descriptions]
        
        print(f"Categorizing {len(descriptions)} items using SALT Taxonomy (Improved Hierarchical)...")
        print(f"   Available Level I categories: {len(self.level1_categories)}")
        print(f"   Sample categories: {', '.join(self.level1_categories[:5])}")
        
        # Use smaller batch size for better reliability and consistency
        batch_size = min(50, batch_size)  # Reduced for better accuracy
        all_categories = []
        
        # Process sequentially for better consistency (async was causing issues)
        for i in range(0, len(descriptions), batch_size):
            batch_end = min(i + batch_size, len(descriptions))
            batch_descriptions = descriptions[i:batch_end]
            batch_num = (i // batch_size) + 1
            total_batches = (len(descriptions) + batch_size - 1) // batch_size
            
            print(f"  Processing batch {batch_num}/{total_batches} ({len(batch_descriptions)} items)")
            print(f"    Batch range: {i} to {batch_end-1}")
            print(f"    First item in batch: {batch_descriptions[0][:50]}...")
            
            try:
                batch_categories = self._categorize_batch_improved_hierarchical(
                    batch_descriptions, model, track_tokens_func)
                print(f"  Batch {batch_num} returned {len(batch_categories)} categories")
                
                # Debug: Check first and last items in batch
                if batch_categories:
                    print(f"    First category: {batch_categories[0]}")
                    print(f"    Last category: {batch_categories[-1]}")
                
                all_categories.extend(batch_categories)
                print(f"  Batch {batch_num} completed successfully, total categories: {len(all_categories)}")
            except Exception as e:
                print(f"  Batch {batch_num} failed: {e}")
                logger.error(f"Taxonomy categorization failed for batch {batch_num}: {e}")
                # Create empty categories for this batch as fallback
                fallback_categories = [{"Taxo1": "", "Taxo2": "", "Taxo3": ""} for _ in range(batch_end - i)]
                all_categories.extend(fallback_categories)
                print(f"  Added {len(fallback_categories)} fallback categories, total: {len(all_categories)}")
        
        print(f"Taxonomy categorization completed: {len(all_categories)} items processed")
        print(f"   Expected: {len(descriptions)} items")
        print(f"   Actual: {len(all_categories)} categories")
        
        # Debug output if enabled
        if enable_debug:
            self._debug_taxonomy_categorization(descriptions, all_categories)
            self._analyze_taxonomy_matching_issues(descriptions, all_categories)
        
        return all_categories 

    def _categorize_level1_improved(self, descriptions: List[str], model, track_tokens_func) -> List[str]:
        """Categorize items into Level I categories with enhanced AI retry and validation."""
        level1_options = self.level1_categories
        sample_descriptions = descriptions[:min(5, len(descriptions))]
        
        # Enhanced prompt with better food categorization rules
        enhanced_prompt = TAXONOMY_LEVEL1_IMPROVED_PROMPT.format(
            level1_options=json.dumps(level1_options, indent=2),
            sample_descriptions=json.dumps(sample_descriptions, indent=2),
            descriptions=json.dumps(descriptions, indent=2)
        )
        
        max_attempts = 5  # Increased from 3 to 5
        for attempt in range(max_attempts):
            try:
                print(f"          Level I categorization attempt {attempt+1}/{max_attempts}")
                
                response = model.generate_content(enhanced_prompt)
                track_tokens_func(response)
                cleaned_text = response.text.strip()
                if cleaned_text.startswith('```'):
                    lines = cleaned_text.split('\n')
                    if len(lines) >= 3:
                        cleaned_text = '\n'.join(lines[1:-1])
                    cleaned_text = cleaned_text.strip()
                try:
                    categories = json.loads(cleaned_text)
                except json.JSONDecodeError:
                    fixed_text = self._fix_json_response(cleaned_text)
                    categories = json.loads(fixed_text)
                if not isinstance(categories, list):
                    raise ValueError("Response is not a list")
                
                # Normalize length
                if len(categories) < len(descriptions):
                    categories.extend([level1_options[0] if level1_options else ""] * (len(descriptions) - len(categories)))
                if len(categories) > len(descriptions):
                    categories = categories[:len(descriptions)]
                
                # Enhanced validation with food item detection
                final = []
                validation_passed = True
                
                for i, (cat, desc) in enumerate(zip(categories, descriptions)):
                    cat = cat.strip() if isinstance(cat, str) else ""
                    
                    # Check if food item was incorrectly categorized as "Disposables"
                    if cat == "Disposables":
                        desc_lower = desc.lower()
                        if any(word in desc_lower for word in ['muffin', 'bagel', 'cookie', 'waffle', 'brownie', 'bread', 'english', 'dough', 'soft serve', 'calamari', 'squid']):
                            print(f"          WARNING: Item {i+1} incorrectly categorized as 'Disposables': {desc[:50]}...")
                            validation_passed = False
                            break
                    
                    # Check if container/equipment was incorrectly categorized as food
                    if cat in ["Seafood", "Fish & Seafood"] and any(word in desc.lower() for word in ['crate', 'box', 'container', 'empty']):
                        print(f"          WARNING: Item {i+1} incorrectly categorized as food: {desc[:50]}...")
                        validation_passed = False
                        break
                    
                    final.append(cat if cat in level1_options else (level1_options[0] if level1_options else ""))
                
                if validation_passed and all(bool(c) for c in final):
                    print(f"          Level I categorization successful on attempt {attempt+1}")
                    return final
                else:
                    print(f"          Validation failed on attempt {attempt+1}, retrying...")
                    continue
                    
            except Exception as e:
                print(f"          Attempt {attempt+1} failed: {e}")
                if attempt < max_attempts - 1:
                    print(f"          Retrying...")
                    continue
                else:
                    print(f"          All Level I attempts failed, using enhanced fallback")
                    break
        
        # Final fallback with better defaults
        print(f"          🚨 All Level I attempts failed, using enhanced fallback")
        return self._enhanced_level1_fallback(descriptions, level1_options)
    
    def _enhanced_level1_fallback(self, descriptions: List[str], level1_options: List[str]) -> List[str]:
        """Enhanced fallback categorization with better food item detection."""
        print(f"          🔧 Using enhanced fallback categorization...")
        
        fallback_categories = []
        for desc in descriptions:
            desc_lower = desc.lower()
            
            # Food items - never "Disposables"
            if any(word in desc_lower for word in ['muffin', 'bagel', 'cookie', 'waffle', 'brownie', 'bread', 'english']):
                if 'Bread & Dough' in level1_options:
                    fallback_categories.append('Bread & Dough')
                elif 'Grocery, Refrigerated & Frozen' in level1_options:
                    fallback_categories.append('Grocery, Refrigerated & Frozen')
                else:
                    fallback_categories.append(level1_options[0] if level1_options else "")
            
            # Cookie dough - food category
            elif 'dough' in desc_lower:
                if 'Bread & Dough' in level1_options:
                    fallback_categories.append('Bread & Dough')
                elif 'Grocery, Refrigerated & Frozen' in level1_options:
                    fallback_categories.append('Grocery, Refrigerated & Frozen')
                else:
                    fallback_categories.append(level1_options[0] if level1_options else "")
            
            # Soft serve mix - dairy
            elif 'soft serve' in desc_lower:
                if 'Dairy' in level1_options:
                    fallback_categories.append('Dairy')
                else:
                    fallback_categories.append(level1_options[0] if level1_options else "")
            
            # Seafood items
            elif any(word in desc_lower for word in ['calamari', 'squid', 'fish', 'seafood']):
                if 'Fish & Seafood' in level1_options:
                    fallback_categories.append('Fish & Seafood')
                else:
                    fallback_categories.append(level1_options[0] if level1_options else "")
            
            # Empty crates, containers - equipment or disposables
            elif any(word in desc_lower for word in ['crate', 'box', 'container', 'empty']):
                if 'Equipment & Supplies' in level1_options:
                    fallback_categories.append('Equipment & Supplies')
                elif 'Disposables' in level1_options:
                    fallback_categories.append('Disposables')
                else:
                    fallback_categories.append(level1_options[0] if level1_options else "")
            
            # Default fallback
            else:
                fallback_categories.append(level1_options[0] if level1_options else "")
        
        print(f"          Enhanced fallback completed for {len(fallback_categories)} items")
        return fallback_categories
    
    def _fix_json_response(self, text: str) -> str:
        """Attempt to fix common JSON formatting issues in AI responses."""
        try:
            # Find the JSON array start and end
            start_idx = text.find('[')
            end_idx = text.rfind(']')
            
            if start_idx == -1 or end_idx == -1:
                # No array brackets found, try to wrap the content
                return f"[{text}]"
            
            # Extract the array content
            json_content = text[start_idx:end_idx + 1]
            
            # Fix common issues
            # 1. Replace single quotes with double quotes
            json_content = json_content.replace("'", '"')
            
            # 2. Fix unquoted property names
            import re
            # Find property names that aren't quoted and quote them
            json_content = re.sub(r'(\s*)(\w+)(\s*):', r'\1"\2"\3:', json_content)
            
            # 3. Fix trailing commas
            json_content = re.sub(r',(\s*[}\]])', r'\1', json_content)
            
            # 4. Fix missing quotes around string values
            # This is more complex, so we'll try a simple approach
            json_content = re.sub(r':\s*([^"][^,}\]]*[^"\s,}\]])', r': "\1"', json_content)
            
            return json_content
            
        except Exception as e:
            logger.warning(f"JSON fixing failed: {e}")
            return text 

    def _categorize_level2_improved(self, descriptions: List[str], level1_categories: List[str], model, track_tokens_func) -> List[str]:
        """Categorize items into Level II categories with strict parent validation and persistent retries."""
        level2_results = []
        
        for i, (desc, level1) in enumerate(zip(descriptions, level1_categories)):
            if not level1:  # No Level I category, skip Level II
                level2_results.append("")
                continue
            
            # Get available Level II categories for this Level I using taxonomy tree
            available_level2 = self._get_available_level2(level1)
            
            if not available_level2:
                level2_results.append("")
                continue
            
            # Categorize this single item with persistent retries
            level2_category = self._categorize_single_level2_persistent(desc, level1, available_level2, model, track_tokens_func)
            level2_results.append(level2_category)
        
        return level2_results
    
    def _categorize_single_level2_persistent(self, description: str, level1_category: str, available_level2: List[str], model, track_tokens_func) -> str:
        """Categorize a single item into Level II category with persistent retries."""
        max_attempts = 3
        
        for attempt in range(max_attempts):
            try:
                prompt = TAXONOMY_LEVEL2_IMPROVED_PROMPT.format(
                    description=description,
                    level1_category=level1_category,
                    available_level2=json.dumps(available_level2, indent=2)
                )

                response = model.generate_content(prompt)
                track_tokens_func(response)
                suggested_category = response.text.strip().strip('"')
                
                # Validate the suggested category exists
                if suggested_category in available_level2:
                    return suggested_category
                if suggested_category == "" and attempt < max_attempts - 1:
                    # Try again with more aggressive prompting
                    continue
                # Fallback: return first available
                return available_level2[0] if available_level2 else ""
            except Exception as e:
                logger.warning(f"Error categorizing Level II for '{description}' on attempt {attempt+1}: {e}")
                if attempt == max_attempts - 1:
                    return available_level2[0] if available_level2 else ""
        
        # Should never reach here, but just in case
        return available_level2[0] if available_level2 else ""
    
    def _categorize_level3_improved(self, descriptions: List[str], level1_categories: List[str], level2_categories: List[str], model, track_tokens_func) -> List[str]:
        """Categorize items into Level III categories with strict parent validation and persistent retries."""
        level3_results = []
        
        for i, (desc, level1, level2) in enumerate(zip(descriptions, level1_categories, level2_categories)):
            if not level1 or not level2:  # Missing Level I or II, skip Level III
                level3_results.append("")
                continue
            
            # Get available Level III categories for this Level I + II combination using taxonomy tree
            available_level3 = self._get_available_level3(level1, level2)
            
            if not available_level3:
                level3_results.append("")
                continue
            
            # Categorize this single item with persistent retries
            level3_category = self._categorize_single_level3_persistent(desc, level1, level2, available_level3, model, track_tokens_func)
            level3_results.append(level3_category)
        
        return level3_results
    
    def _categorize_single_level3_persistent(self, description: str, level1_category: str, level2_category: str, available_level3: List[str], model, track_tokens_func) -> str:
        """Categorize a single item into Level III category with persistent retries."""
        max_attempts = 3
        
        for attempt in range(max_attempts):
            try:
                prompt = TAXONOMY_LEVEL3_IMPROVED_PROMPT.format(
                    description=description,
                    level1_category=level1_category,
                    level2_category=level2_category,
                    available_level3=json.dumps(available_level3, indent=2)
                )

                response = model.generate_content(prompt)
                track_tokens_func(response)
                suggested_category = response.text.strip().strip('"')
                
                # Validate the suggested category exists
                if suggested_category in available_level3:
                    return suggested_category
                if suggested_category == "" and attempt < max_attempts - 1:
                    # Try again with more aggressive prompting
                    continue
                # Fallback: return first available
                return available_level3[0] if available_level3 else ""
            except Exception as e:
                logger.warning(f"Error categorizing Level III for '{description}' on attempt {attempt+1}: {e}")
                if attempt == max_attempts - 1:
                    return available_level3[0] if available_level3 else ""
        
        # Should never reach here, but just in case
        return available_level3[0] if available_level3 else "" 

    def _comprehensive_validation(self, descriptions: List[str], level1_categories: List[str], 
                                level2_categories: List[str], level3_categories: List[str]) -> List[Dict[str, str]]:
        """Comprehensive validation of all taxonomy levels with strict hierarchy enforcement."""
        print(f"      Comprehensive validation with strict hierarchy enforcement...")
        print(f"        Input: {len(descriptions)} descriptions, {len(level1_categories)} Level I, {len(level2_categories)} Level II, {len(level3_categories)} Level III")
        
        validated_categories = []
        validation_errors = 0
        
        for i, (desc, taxo1, taxo2, taxo3) in enumerate(zip(descriptions, level1_categories, level2_categories, level3_categories)):
            # Clean up whitespace
            taxo1 = taxo1.strip() if taxo1 else ""
            taxo2 = taxo2.strip() if taxo2 else ""
            taxo3 = taxo3.strip() if taxo3 else ""
            
            # Debug output for first few items
            if i < 3:
                print(f"        Row {i+1}: '{desc[:40]}{'...' if len(desc) > 40 else ''}'")
                print(f"          → Taxo1: '{taxo1}' (valid: {taxo1 in self.level1_categories if taxo1 else 'N/A'})")
                if taxo2:
                    print(f"          → Taxo2: '{taxo2}' (valid for {taxo1}: {taxo2 in self._get_available_level2(taxo1) if taxo1 else 'N/A'})")
                if taxo3:
                    print(f"          → Taxo3: '{taxo3}' (valid for {taxo1}>{taxo2}: {taxo3 in self._get_available_level3(taxo1, taxo2) if taxo1 and taxo2 else 'N/A'})")
                    # Additional debug for specific combination
                    if taxo1 == "Disposables" and taxo2 == "Register Tape, Labels, Trays":
                        available_level3 = self._get_available_level3(taxo1, taxo2)
                        print(f"          DEBUG: Available Level III for 'Disposables > Register Tape, Labels, Trays': {available_level3}")
                        print(f"          DEBUG: Looking for '{taxo3}' in: {available_level3}")
            
            # Validate the taxonomy hierarchy
            is_valid = True
            
            # Level I must exist and be valid
            if not taxo1 or taxo1 not in self.level1_categories:
                is_valid = False
                if i < 3:  # Debug output for first few items
                    print(f"          → INVALID: Level I '{taxo1}' not found in available categories")
            
            # Level II must be valid for Level I if it exists
            if taxo2 and taxo1:
                available_level2 = self._get_available_level2(taxo1)
                if taxo2 not in available_level2:
                    is_valid = False
                    if i < 3:  # Debug output for first few items
                        print(f"          → INVALID: Level II '{taxo2}' not valid for Level I '{taxo1}'")
            
            # Level III must be valid for Level I+II if it exists
            if taxo3 and taxo1 and taxo2:
                available_level3 = self._get_available_level3(taxo1, taxo2)
                if taxo3 not in available_level3:
                    is_valid = False
                    if i < 3:  # Debug output for first few items
                        print(f"          → INVALID: Level III '{taxo3}' not valid for Level I+II '{taxo1}' > '{taxo2}'")
            
            # Add to validated categories if valid
            if is_valid:
                validated_categories.append({
                    'Taxo1': taxo1,
                    'Taxo2': taxo2,
                    'Taxo3': taxo3
                })
            else:
                validation_errors += 1
                if i < 3:  # Debug output for first few items
                    print(f"          → REJECTED: Invalid taxonomy hierarchy")
        
        print(f"        Output: {len(validated_categories)} validated categories")
        print(f"        Validation errors: {validation_errors}")
        
        return validated_categories
    
    def _enforce_hierarchical_consistency(self, categories: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """Enforce hierarchical consistency across all categories."""
        print(f"      Enforcing hierarchical consistency...")
        
        consistent_categories = []
        consistency_fixes = 0
        
        for i, cat in enumerate(categories):
            taxo1 = cat.get('Taxo1', '').strip()
            taxo2 = cat.get('Taxo2', '').strip()
            taxo3 = cat.get('Taxo3', '').strip()
            
            # Ensure proper hierarchy: Level I must exist for Level II to exist
            if not taxo1 and taxo2:
                print(f"        Row {i+1}: Level II '{taxo2}' without Level I - clearing Level II")
                taxo2 = ""
                consistency_fixes += 1
            
            # Ensure proper hierarchy: Level II must exist for Level III to exist
            if not taxo2 and taxo3:
                print(f"        Row {i+1}: Level III '{taxo3}' without Level II - clearing Level III")
                taxo3 = ""
                consistency_fixes += 1
            
            # Ensure proper hierarchy: Level I must exist for Level III to exist
            if not taxo1 and taxo3:
                print(f"        Row {i+1}: Level III '{taxo3}' without Level I - clearing Level III")
                taxo3 = ""
                consistency_fixes += 1
            
            consistent_categories.append({
                'Taxo1': taxo1,
                'Taxo2': taxo2,
                'Taxo3': taxo3
            })
        
        if consistency_fixes > 0:
            print(f"        Fixed {consistency_fixes} consistency issues")
        else:
            print(f"        All categories are hierarchically consistent")
        
        return consistent_categories 

    def _enforce_product_consistency(self, descriptions: List[str], categories: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """Enforce consistency across similar products to ensure identical product types get identical categories."""
        print(f"      Enforcing product consistency across similar items...")
        
        # Create a mapping of product patterns to categories
        product_patterns = {}
        consistency_fixes = 0
        
        for i, (desc, cat) in enumerate(zip(descriptions, categories)):
            # Extract the base product pattern (remove day-specific or size-specific variations)
            base_pattern = self._extract_base_product_pattern(desc)
            
            if base_pattern in product_patterns:
                # We've seen this product type before, check for consistency
                existing_cat = product_patterns[base_pattern]
                current_cat = cat
                
                # Check if categories are different
                if (existing_cat.get('Taxo1') != current_cat.get('Taxo1') or
                    existing_cat.get('Taxo2') != current_cat.get('Taxo2') or
                    existing_cat.get('Taxo3') != current_cat.get('Taxo3')):
                    
                    # Inconsistency detected - use the most common category for this product type
                    consistency_fixes += 1
                    print(f"        Row {i+1}: Inconsistent categorization detected for '{desc}'")
                    print(f"          Previous: {existing_cat.get('Taxo1', '')} > {existing_cat.get('Taxo2', '')} > {existing_cat.get('Taxo3', '')}")
                    print(f"          Current:  {current_cat.get('Taxo1', '')} > {current_cat.get('Taxo2', '')} > {current_cat.get('Taxo3', '')}")
                    print(f"          Using consistent categorization from previous items")
                    
                    # Use the existing category for consistency
                    categories[i] = existing_cat.copy()
            else:
                # First time seeing this product type, store the category
                product_patterns[base_pattern] = cat.copy()
        
        if consistency_fixes > 0:
            print(f"        Fixed {consistency_fixes} consistency issues across similar products")
        else:
            print(f"        All similar products already have consistent categorization")
        
        return categories
    
    def _extract_base_product_pattern(self, description: str) -> str:
        """Extract the base product pattern by removing day, size, and other variations."""
        # Convert to lowercase for pattern matching
        desc_lower = description.lower()
        
        # Remove day variations
        days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
        for day in days:
            desc_lower = desc_lower.replace(day, '')
        
        # Remove size variations (common patterns)
        size_patterns = [
            r'\d+\.?\d*\s*["""]',  # 3/4", 1.5", etc.
            r'\d+\s*ounce',        # 16 ounce, etc.
            r'\d+\s*lb',           # 2000 lb, etc.
            r'\d+\s*count',        # 24 count, etc.
            r'\d+\s*pack',         # 12 pack, etc.
        ]
        
        import re
        for pattern in size_patterns:
            desc_lower = re.sub(pattern, '', desc_lower)
        
        # Remove common variations
        variations = [
            'use first',
            'use first',
            'first',
            'inch',
            'pound',
            'ounce',
            'count',
            'pack',
            'size',
            'large',
            'small',
            'medium',
        ]
        
        for variation in variations:
            desc_lower = desc_lower.replace(variation, '')
        
        # Clean up extra spaces and normalize
        desc_lower = re.sub(r'\s+', ' ', desc_lower).strip()
        
        return desc_lower
    
    def _recategorize_unsure_items(self, descriptions: List[str], categories: List[Dict[str, str]], model, track_tokens_func) -> List[Dict[str, str]]:
        """Re-categorize items that were initially categorized as unsure or incorrectly."""
        print(f"      Re-categorizing unsure or incorrectly categorized items...")
        
        # Identify items that need re-categorization
        items_to_recategorize = []
        for i, (desc, cat) in enumerate(zip(descriptions, categories)):
            taxo1 = cat.get('Taxo1', '').strip()
            taxo2 = cat.get('Taxo2', '').strip()
            taxo3 = cat.get('Taxo3', '').strip()
            
            # Check if item needs re-categorization
            needs_recategorization = False
            
            # Items with no Level I categorization
            if not taxo1:
                needs_recategorization = True
            
            # Items that are clearly food service labels but got wrong categories
            desc_lower = desc.lower()
            if any(word in desc_lower for word in ['day spot', 'day', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']):
                if taxo1 != "Disposables" or "label" not in taxo2.lower():
                    needs_recategorization = True
            
            # Items that are clearly PPE but got wrong categories
            if any(word in desc_lower for word in ['mask', 'face mask', '3-ply']):
                if taxo1 != "Equipment & Supplies":
                    needs_recategorization = True
            
            if needs_recategorization:
                items_to_recategorize.append((i, desc, cat))
        
        if not items_to_recategorize:
            print(f"        No items need re-categorization")
            return categories
        else:
            print(f"        Found {len(items_to_recategorize)} items that need re-categorization")
        
        for i, desc, original_cat in items_to_recategorize:
            print(f"        Re-categorizing item {i+1}: '{desc[:60]}{'...' if len(desc) > 60 else ''}'")
            
            # Get enhanced product description
            enhanced_description = self._get_enhanced_product_description(desc)
            
            # Re-categorize with enhanced context
            new_categories = self._recategorize_single_item(desc, enhanced_description, model, track_tokens_func)
            
            # Debug output
            print(f"          Enhanced description length: {len(enhanced_description)} characters")
            print(f"          Original: {original_cat.get('Taxo1', '')} > {original_cat.get('Taxo2', '')} > {original_cat.get('Taxo3', '')}")
            print(f"          New:      {new_categories.get('Taxo1', '')} > {new_categories.get('Taxo2', '')} > {new_categories.get('Taxo3', '')}")
            
            # Validate that we actually got categories
            if not new_categories.get('Taxo1', '').strip():
                    print(f"          WARNING: Still no Taxo1 after re-categorization!")
                    print(f"          Debug info: enhanced_description preview: {enhanced_description[:200]}...")
            
            # Update the categories
            categories[i] = new_categories
        
        print(f"        Re-categorization completed for {len(items_to_recategorize)} items")
        return categories 

    def _get_enhanced_product_description(self, product_description: str) -> str:
        """Get an enhanced product description with usage details and context using imported templates."""
        desc_lower = product_description.lower()
        
        # Determine the appropriate template based on product keywords
        if any(word in desc_lower for word in ['day spot', 'day', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']):
            template_key = "day_spot"
        elif any(word in desc_lower for word in ['mask', 'face mask', '3-ply']):
            template_key = "face_mask"
        elif any(word in desc_lower for word in ['crayon', 'art', 'educational']):
            template_key = "crayon_art"
        elif any(word in desc_lower for word in ['cup', 'container', 'pet']):
            template_key = "food_container"
        else:
            template_key = "default"
        
        # Use the imported template
        template = ENHANCED_PRODUCT_DESCRIPTION_TEMPLATES[template_key]
        return template.format(product_description=product_description)
    
    def _recategorize_single_item(self, product_description: str, enhanced_description: str, model, track_tokens_func) -> Dict[str, str]:
        """Re-categorize a single item using persistent AI retry until correct categorization."""
        try:
            print(f"          Re-categorizing with persistent AI retry: '{product_description[:50]}{'...' if len(product_description) > 50 else ''}'")
            
            # CRITICAL CHECK: DAY SPOT items must be food safety labels, not food!
            product_lower = product_description.lower()
            if any(word in product_lower for word in ['day spot', 'day', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday', 'use first', 'use by']):
                print(f"          CRITICAL: DAY SPOT item detected - forcing correct food safety label categorization")
                return {
                    'Taxo1': 'Disposables',
                    'Taxo2': 'Register Tape, Labels, Trays',
                    'Taxo3': 'Labels, Miscellaneous'
                }
            
            # Use persistent AI retry with enhanced context
            return self._ai_retry_categorization(product_description, enhanced_description, model, track_tokens_func)
            
        except Exception as e:
            logger.error(f"Error re-categorizing '{product_description}': {e}")
            print(f"          Error in re-categorization: {e}")
            # Even on error, try AI retry instead of fallback
            return self._ai_retry_categorization(product_description, enhanced_description, model, track_tokens_func)
    
    def _ai_retry_categorization(self, product_description: str, enhanced_description: str, model, track_tokens_func) -> Dict[str, str]:
        """Persistently retry AI categorization until correct category is found."""
        max_attempts = 5  # Allow up to 5 attempts
        last_error = None
        
        for attempt in range(1, max_attempts + 1):
            try:
                print(f"          AI Retry Attempt {attempt}/{max_attempts}")
                
                # Create a comprehensive prompt for re-categorization
                prompt = f"""You are a food service taxonomy expert. Categorize this product using the SALT Taxonomy.

PRODUCT DESCRIPTION: {product_description}

ENHANCED CONTEXT: {enhanced_description}

AVAILABLE TAXONOMY CATEGORIES:
Level I: {', '.join(self.level1_categories[:10])}... (and {len(self.level1_categories)-10} more)

CRITICAL RULES:
- ENGLISH MUFFINS, BAGELS, MUFFINS, COOKIES, WAFFLES, BROWNIES are FOOD ITEMS, NOT "Disposables"
- Food items should be categorized as:
  * "Bread & Dough" for bread products
  * "Grocery, Refrigerated & Frozen" for baked goods
  * "Dairy" for dairy-based products
- Only truly disposable items (plates, cups, containers) should be "Disposables"
- IQF (Individually Quick Frozen) products = "Grocery, Refrigerated & Frozen" NOT "Appetizers, Entrees, & Potatoes Refrigerated & Frozen"
- Frozen fruits/vegetables = "Grocery, Refrigerated & Frozen" > "Fruits & Vegetables"
- Use the enhanced context to understand the product type

Return a JSON object with the correct taxonomy:
{{
  "Taxo1": "Level I category",
  "Taxo2": "Level II category", 
  "Taxo3": "Level III category"
}}

Return only the JSON object, nothing else."""

                response = model.generate_content(prompt)
                track_tokens_func(response)
                
                # Parse the response
                cleaned_text = response.text.strip()
                if cleaned_text.startswith('```'):
                    lines = cleaned_text.split('\n')
                    if len(lines) >= 3:
                        cleaned_text = '\n'.join(lines[1:-1])
                    cleaned_text = cleaned_text.strip()
                
                result = json.loads(cleaned_text)
                
                # Validate the result structure
                if not isinstance(result, dict) or 'Taxo1' not in result:
                    raise ValueError("Invalid response structure")
                
                # Validate that we didn't get a generic "Disposables" for food items
                if (result.get('Taxo1') == 'Disposables' and 
                    any(word in product_description.lower() for word in ['muffin', 'bagel', 'cookie', 'waffle', 'brownie', 'bread', 'english'])):
                    print(f"          WARNING: AI incorrectly categorized food item as 'Disposables' - retrying...")
                    last_error = "Food item incorrectly categorized as Disposables"
                    continue
                
                # Validate that IQF products are not incorrectly categorized as "Appetizers, Entrees, & Potatoes Refrigerated & Frozen"
                if (result.get('Taxo1') == 'Appetizers, Entrees, & Potatoes Refrigerated & Frozen' and 
                    'iqf' in product_description.lower()):
                    print(f"          WARNING: AI incorrectly categorized IQF product as 'Appetizers, Entrees, & Potatoes Refrigerated & Frozen' - retrying...")
                    last_error = "IQF product incorrectly categorized as Entrees"
                    continue
                
                print(f"          AI categorization successful on attempt {attempt}")
                return result
                
            except Exception as e:
                last_error = str(e)
                print(f"          AI attempt {attempt} failed: {e}")
                if attempt < max_attempts:
                    print(f"          Retrying...")
                    continue
                else:
                    print(f"          All AI attempts failed, using emergency fallback")
                    break
        
        # Emergency fallback - only for truly critical cases
        print(f"          Emergency fallback for: {product_description}")
        return self._emergency_fallback_categorization(product_description)
    
    def _emergency_fallback_categorization(self, product_description: str) -> Dict[str, str]:
        """Emergency fallback categorization for critical cases only."""
        product_lower = product_description.lower()
        
        # Only handle truly critical cases
        if any(word in product_lower for word in ['day spot', 'day', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']):
            return {
                'Taxo1': 'Disposables',
                'Taxo2': 'Register Tape, Labels, Trays',
                'Taxo3': 'Labels, Miscellaneous'
            }
        
        if any(word in product_lower for word in ['mask', 'face mask', '3-ply']):
            return {
                'Taxo1': 'Equipment & Supplies',
                'Taxo2': 'Safety Equipment',
                'Taxo3': 'Personal Protective Equipment'
            }
        
        # For food items, use a reasonable default instead of "Disposables"
        if any(word in product_lower for word in ['muffin', 'bagel', 'cookie', 'waffle', 'brownie', 'bread', 'english']):
            return {
                'Taxo1': 'Bread & Dough',
                'Taxo2': 'Bread & Dough, Refrigerated & Frozen',
                'Taxo3': 'Bread & Dough, Other'
            }
        
        # Default to a reasonable food category instead of generic "Disposables"
        return {
            'Taxo1': 'Grocery, Refrigerated & Frozen',
            'Taxo2': 'Bread & Dough, Refrigerated & Frozen',
            'Taxo3': 'Bread & Dough, Other'
        }
    
    def _rule_based_categorization(self, descriptions: List[str]) -> List[Dict[str, str]]:
        """Rule-based categorization using keyword matching for more accurate results."""
        print(f"  Using rule-based categorization for {len(descriptions)} items...")
        
        results = []
        
        for desc in descriptions:
            desc_lower = desc.lower()
            
            # Initialize with empty categories
            taxo1 = ""
            taxo2 = ""
            taxo3 = ""
            
            # RULE 1: BAKERY BOXES
            if any(keyword in desc_lower for keyword in ['bakery box', 'cake box', 'pastry box', 'bread box', 'kraft window box']):
                taxo1 = "Disposables"
                taxo2 = "Boxes"
                taxo3 = "Paper, Bakery & Cake"
            
            # RULE 2: LABELS
            elif any(keyword in desc_lower for keyword in ['label', 'day glo', 'dayglo', 'day-glo', 'cajun', 'bbq', 'boneless', 'turkey', 'lamb', 'veal', 'chicken', 'beef', 'foil oval']):
                taxo1 = "Disposables"
                taxo2 = "Register Tape, Labels, Trays"
                taxo3 = "Labels, Miscellaneous"
            
            # RULE 3: ALUMINUM CONTAINERS & PANS
            elif any(keyword in desc_lower for keyword in ['aluminum', 'alum', 'alum.']):
                if any(keyword in desc_lower for keyword in ['pan', 'tin', 'roaster', 'container']):
                    taxo1 = "Disposables"
                    taxo2 = "Carry-Out Container"
                    taxo3 = "Aluminum, Disposable"
                else:
                    taxo1 = "Disposables"
                    taxo2 = "Carry-Out Container"
                    taxo3 = "Aluminum, Disposable"
            
            # RULE 4: PLASTIC CONTAINERS
            elif any(keyword in desc_lower for keyword in ['plastic', 'pet', 'poly']):
                if any(keyword in desc_lower for keyword in ['container', 'lid', 'bowl', 'cup']):
                    taxo1 = "Disposables"
                    taxo2 = "Carry-Out Container"
                    taxo3 = "Plastic, Disposable"
                else:
                    taxo1 = "Disposables"
                    taxo2 = "Carry-Out Container"
                    taxo3 = "Plastic, Disposable"
            
            # RULE 5: FOAM CONTAINERS
            elif any(keyword in desc_lower for keyword in ['foam', 'hfa']):
                if any(keyword in desc_lower for keyword in ['container', 'tray', 'steam']):
                    taxo1 = "Disposables"
                    taxo2 = "Carry-Out Container"
                    taxo3 = "Foam, Hinged"
                else:
                    taxo1 = "Disposables"
                    taxo2 = "Carry-Out Container"
                    taxo3 = "Foam, Other"
            
            # RULE 6: PAPER PRODUCTS
            elif any(keyword in desc_lower for keyword in ['paper', 'kraft']):
                if any(keyword in desc_lower for keyword in ['plate', 'bowl', 'cup']):
                    taxo1 = "Disposables"
                    taxo2 = "Bowls, Cups, Plates & Lids,Disposable"
                    if 'plate' in desc_lower:
                        taxo3 = "Plates & Platters, Paper"
                    elif 'bowl' in desc_lower:
                        taxo3 = "Bowls, Paper"
                    elif 'cup' in desc_lower:
                        taxo3 = "Cups, Paper"
                elif 'napkin' in desc_lower:
                    taxo1 = "Disposables"
                    taxo2 = "Napkins, Tablcovrs, Traycovrs & Placemats, Dispble"
                    taxo3 = "Napkins, Paper, Dinner, Luncheon"
                elif 'towel' in desc_lower:
                    taxo1 = "Disposables"
                    taxo2 = "Tissue, Towels, & Personal Hygiene, Disposable"
                    taxo3 = "Towels, Paper, Disposable"
                else:
                    taxo1 = "Disposables"
                    taxo2 = "Film, Foil, & Paper Wraps"
                    taxo3 = "Butcher Paper"
            
            # RULE 7: BAGS
            elif any(keyword in desc_lower for keyword in ['bag', 'polybag', 'poly bag']):
                taxo1 = "Disposables"
                taxo2 = "Bags"
                if any(keyword in desc_lower for keyword in ['food', 'storage', 'produce', 'deli']):
                    taxo3 = "Food Storage"
                else:
                    taxo3 = "Plastic, Other"
            
            # RULE 8: ALUMINUM FOIL & WRAPS
            elif any(keyword in desc_lower for keyword in ['foil', 'aluminum foil', 'alum foil']):
                taxo1 = "Disposables"
                taxo2 = "Film, Foil, & Paper Wraps"
                if 'roll' in desc_lower:
                    taxo3 = "Foil, Roll"
                else:
                    taxo3 = "Wraps & Sheets, Foil"
            
            # RULE 9: PLASTIC WRAP
            elif any(keyword in desc_lower for keyword in ['plastic wrap', 'wrap', 'film']):
                taxo1 = "Disposables"
                taxo2 = "Film, Foil, & Paper Wraps"
                if 'roll' in desc_lower:
                    taxo3 = "Film, Roll, Plastic"
                else:
                    taxo3 = "Wraps, Plastic (Not Roll)"
            
            # RULE 10: GLOVES
            elif 'glove' in desc_lower:
                taxo1 = "Disposables"
                taxo2 = "Aprons, Bibs, Gloves & Headware Disposable"
                if 'nitrile' in desc_lower:
                    taxo3 = "Gloves, Disposable, Nitrile"
                elif 'vinyl' in desc_lower:
                    taxo3 = "Gloves, Disposable Vinyl"
                elif 'poly' in desc_lower:
                    taxo3 = "Gloves, Disposable Poly"
                else:
                    taxo3 = "Gloves, Disposable, Synthetic/Other"
            
            # RULE 11: CUTLERY
            elif any(keyword in desc_lower for keyword in ['fork', 'spoon', 'knife', 'cutlery']):
                taxo1 = "Disposables"
                taxo2 = "Cutlery Kits & Diet Kits, Disposable"
                if 'fork' in desc_lower:
                    taxo3 = "Forks & Sporks, Disposable"
                elif 'spoon' in desc_lower:
                    taxo3 = "Spoons, Disposable"
                elif 'knife' in desc_lower:
                    taxo3 = "Knives, Disposable"
            
            # RULE 12: FILTERS
            elif any(keyword in desc_lower for keyword in ['filter', 'coffee filter', 'fryer filter']):
                taxo1 = "Disposables"
                taxo2 = "Filters, Beverage & Fryer"
                taxo3 = "Filters, Disposable, Coffee, Tea, Fryer"
            
            # RULE 13: CLEANING CHEMICALS
            elif any(keyword in desc_lower for keyword in ['soap', 'detergent', 'cleaner', 'sanitizer', 'degreaser']):
                taxo1 = "Chemicals & Cleaning Agents"
                if any(keyword in desc_lower for keyword in ['soap', 'sanitizer']):
                    taxo2 = "Detergnt, Santizr, Freshnrs, Toiletries,Manual Use"
                    if 'soap' in desc_lower:
                        taxo3 = "Soaps & Lotions, Hand & Body And Other Toiletries"
                    else:
                        taxo3 = "Sanitizers"
                elif 'detergent' in desc_lower:
                    taxo2 = "Detergent, Soap & Additives For Machines"
                    taxo3 = "Detergents, Dish Machine"
                else:
                    taxo2 = "Cleaners, Polishes & Waxes"
                    if 'degreaser' in desc_lower:
                        taxo3 = "Degreasers"
                    elif 'oven' in desc_lower or 'grill' in desc_lower:
                        taxo3 = "Oven & Grill"
                    elif 'glass' in desc_lower or 'window' in desc_lower:
                        taxo3 = "Glass & Window"
                    else:
                        taxo3 = "Other"
            
            # RULE 14: EQUIPMENT & TOOLS
            elif any(keyword in desc_lower for keyword in ['equipment', 'tool', 'utensil', 'scraper', 'duster']):
                taxo1 = "Equipment & Supplies"
                taxo2 = "Smallwares, Kitchen & Bar"
                if any(keyword in desc_lower for keyword in ['utensil', 'scraper', 'duster']):
                    taxo3 = "Kitchen Utensils"
                else:
                    taxo3 = "Food Storage Containers & Lids (W/O Wheels)"
            
            # RULE 15: CHINA & GLASSWARE
            elif any(keyword in desc_lower for keyword in ['china', 'plate', 'bowl', 'mug', 'glass']):
                taxo1 = "Equipment & Supplies"
                if any(keyword in desc_lower for keyword in ['plate', 'saucer']):
                    taxo2 = "China"
                    taxo3 = "Plates & Saucers"
                elif any(keyword in desc_lower for keyword in ['bowl', 'dish', 'ovenware']):
                    taxo2 = "China"
                    taxo3 = "(Bowls, Dishes, Ovenware)"
                elif 'mug' in desc_lower:
                    taxo2 = "China"
                    taxo3 = "Mugs"
                elif 'glass' in desc_lower:
                    taxo2 = "Glassware"
                    taxo3 = "Dinnerware, Glass"
            
            # RULE 16: SKEWERS & TOOTHPICKS
            elif any(keyword in desc_lower for keyword in ['skewer', 'toothpick']):
                taxo1 = "Disposables"
                taxo2 = "Straws, Stirrers, Steak Markers, Skewers & Other"
                if 'skewer' in desc_lower:
                    taxo3 = "Skewers"
                else:
                    taxo3 = "Toothpicks"
            
            # RULE 17: STRAWS & STIRRERS
            elif any(keyword in desc_lower for keyword in ['straw', 'stirrer']):
                taxo1 = "Disposables"
                taxo2 = "Straws, Stirrers, Steak Markers, Skewers & Other"
                if 'stirrer' in desc_lower:
                    taxo3 = "Stirrers, Plastic"
                else:
                    taxo3 = "Straws, Plastic"
            
            # RULE 18: REGISTER TAPE
            elif any(keyword in desc_lower for keyword in ['register tape', 'cash register', 'thermal']):
                taxo1 = "Disposables"
                taxo2 = "Register Tape, Labels, Trays"
                taxo3 = "Cash Register Tape & Ribbons"
            
            # RULE 19: TRAYS
            elif 'tray' in desc_lower:
                taxo1 = "Disposables"
                if 'foam' in desc_lower:
                    taxo2 = "Register Tape, Labels, Trays"
                    taxo3 = "Trays, Foam"
                else:
                    taxo2 = "Carry-Out Container"
                    taxo3 = "Plastic, Disposable"
            
            # RULE 20: LINERS
            elif any(keyword in desc_lower for keyword in ['liner', 'trash can', 'wastebasket']):
                taxo1 = "Disposables"
                taxo2 = "Liners, Can (Trash) & Wastebasket"
                taxo3 = "Liner, Trash Can & Wastebasket"
            
            # RULE 21: NAPKINS & DOILIES
            elif any(keyword in desc_lower for keyword in ['napkin', 'doily', 'doilie']):
                taxo1 = "Disposables"
                taxo2 = "Napkins, Tablcovrs, Traycovrs & Placemats, Dispble"
                if 'doily' in desc_lower or 'doilie' in desc_lower:
                    taxo3 = "Doilies & Coasters, Disposable"
                elif 'dispenser' in desc_lower:
                    taxo3 = "Napkins, Paper, Dispenser"
                else:
                    taxo3 = "Napkins, Paper, Dinner, Luncheon"
            
            # RULE 22: CUPS
            elif 'cup' in desc_lower:
                taxo1 = "Disposables"
                taxo2 = "Bowls, Cups, Plates & Lids,Disposable"
                if 'foam' in desc_lower:
                    taxo3 = "Cups, Foam"
                elif 'paper' in desc_lower:
                    taxo3 = "Cups, Paper"
                elif 'portion' in desc_lower or 'souffle' in desc_lower:
                    taxo3 = "Cups, Portion / Souffle, Plastic"
                else:
                    taxo3 = "Cups & Tumblers, Plastic Disposable"
            
            # RULE 23: LIDS
            elif 'lid' in desc_lower:
                taxo1 = "Disposables"
                if any(keyword in desc_lower for keyword in ['bowl', 'container']):
                    taxo2 = "Bowls, Cups, Plates & Lids,Disposable"
                    taxo3 = "Lids, Disposable For Bowls"
                else:
                    taxo2 = "Bowls, Cups, Plates & Lids,Disposable"
                    taxo3 = "Lids, Disposable, For Cups & Tumblers"
            
            # RULE 24: APRON & HEADWARE
            elif any(keyword in desc_lower for keyword in ['apron', 'hairnet', 'beard cover']):
                taxo1 = "Disposables"
                taxo2 = "Aprons, Bibs, Gloves & Headware Disposable"
                if 'apron' in desc_lower:
                    taxo3 = "Aprons And Bibs, Disposable"
                elif 'hairnet' in desc_lower:
                    taxo3 = "Hairnets"
                elif 'beard' in desc_lower:
                    taxo3 = "Beard/Shoe Covers"
            
            # RULE 25: BAKING ACCESSORIES
            elif any(keyword in desc_lower for keyword in ['muffin tin', 'pie tin', 'baking pan']):
                taxo1 = "Equipment & Supplies"
                taxo2 = "Smallwares, Kitchen & Bar"
                taxo3 = "Baking Pans & Molds"
            
            # RULE 26: STEAM TABLES
            elif 'steam' in desc_lower and 'table' in desc_lower:
                taxo1 = "Equipment & Supplies"
                taxo2 = "Equipment"
                taxo3 = "Equipment Stands/Not Tabletop"
            
            # RULE 27: FUEL & CHAFING
            elif any(keyword in desc_lower for keyword in ['fuel', 'chafing']):
                taxo1 = "Equipment & Supplies"
                taxo2 = "Fuel, Candles, Charcoal & Logs"
                taxo3 = "Fuel, Chafing"
            
            # RULE 28: FLATWARE
            elif any(keyword in desc_lower for keyword in ['fork', 'spoon', 'knife']) and 'stainless' in desc_lower:
                taxo1 = "Equipment & Supplies"
                taxo2 = "Flatware, Stainless Steel, Metal"
                if 'fork' in desc_lower:
                    taxo3 = "Forks, Stainless Steel Metal"
                elif 'spoon' in desc_lower:
                    taxo3 = "Spoons, Stainless Steel Metal"
                elif 'knife' in desc_lower:
                    taxo3 = "Knives, Stainless Steel Metal"
            
            # RULE 29: MEAT & POULTRY
            elif any(keyword in desc_lower for keyword in ['chicken', 'beef', 'poultry', 'meat']):
                if 'chicken' in desc_lower:
                    taxo1 = "Poultry"
                    if 'wing' in desc_lower:
                        taxo2 = "Chicken, Not Further Processed, Frozen"
                        taxo3 = "Wings & Wing Sections, Raw, Unflavored, Frozen"
                    elif 'breast' in desc_lower:
                        taxo2 = "Chicken, Not Further Processed, Frozen"
                        taxo3 = "Breasts, Raw, Unflavored, Frozen"
                    elif 'thigh' in desc_lower:
                        taxo2 = "Chicken, Not Further Processed, Frozen"
                        taxo3 = "Thighs, Raw, Unflavored, Frozen"
                    else:
                        taxo2 = "Chicken, Not Further Processed, Frozen"
                        taxo3 = "Whole Birds, Frozen"
                elif 'beef' in desc_lower:
                    if 'ground' in desc_lower:
                        taxo1 = "Beef"
                        taxo2 = "Ground Beef"
                        taxo3 = "Ground Beef, Raw, Frozen"
                    elif 'pastrami' in desc_lower:
                        taxo1 = "Beef"
                        taxo2 = "Pastrami"
                        taxo3 = "Pastrami, Cooked, Sliced, Frozen"
                    else:
                        taxo1 = "Beef"
                        taxo2 = "Boxed, Loins"
                        taxo3 = "Loin, Other, Raw, Refrigerated"
            
            # RULE 30: PRODUCE
            elif any(keyword in desc_lower for keyword in ['fruit', 'vegetable', 'produce']):
                taxo1 = "Produce, Fresh"
                if 'fruit' in desc_lower:
                    taxo2 = "Fruits, Other, Fresh"
                    taxo3 = "Fruit, Other Fresh"
                else:
                    taxo2 = "Vegetables, Other, Fresh"
                    taxo3 = "Vegetable, Other Fresh"
            
            # RULE 31: GROCERY DRY
            elif any(keyword in desc_lower for keyword in ['pasta', 'dry', 'shelf stable']):
                taxo1 = "Grocery, Dry"
                if 'pasta' in desc_lower:
                    taxo2 = "Pasta, Shelf Stable"
                    taxo3 = "Pasta, Other, Dry"
                else:
                    taxo2 = "Bakery Mixes & Ingredients, Shelf Stable"
                    taxo3 = "Bakery Ingredient, Specialty"
            
            # RULE 32: GROCERY REFRIGERATED
            elif any(keyword in desc_lower for keyword in ['frozen', 'refrigerated', 'chilled']):
                taxo1 = "Grocery, Refrigerated & Frozen"
                taxo2 = "Grocery, Other, Refrigerated & Frozen"
                if 'bakery' in desc_lower:
                    taxo3 = "Bakery Ingredient, Specialty, Refrigerated Or Frozen"
                else:
                    taxo3 = "Specialty Snack Foods, Bulk, Refrigerated/Frozen"
            
            # RULE 33: ENTREES
            elif any(keyword in desc_lower for keyword in ['entree', 'kabob', 'skewer']):
                taxo1 = "Appetizers, Entrees, & Potatoes Refrigerated & Frozen"
                taxo2 = "Entrees, Refrigerated & Frozen"
                if any(keyword in desc_lower for keyword in ['kabob', 'skewer']):
                    taxo3 = "Entree, Kabobs/Skewers, Refrigerated (Not Seafood)"
                else:
                    taxo3 = "Entrees, Prepared, Other, Frozen"
            
            # RULE 34: APPETIZERS
            elif any(keyword in desc_lower for keyword in ['appetizer', 'hors d', 'canape']):
                taxo1 = "Appetizers, Entrees, & Potatoes Refrigerated & Frozen"
                if 'hors d' in desc_lower or 'canape' in desc_lower:
                    taxo2 = "Hors D'Oeuvres & Canapes"
                    taxo3 = "(Not For Fryer) Frozen"
                else:
                    taxo2 = "Appetizers And Coated Vegetables"
                    taxo3 = "Asian, Other"
            
            # RULE 35: CONDIMENTS
            elif any(keyword in desc_lower for keyword in ['ketchup', 'teriyaki', 'condiment']):
                taxo1 = "Grocery, Dry"
                if 'ketchup' in desc_lower:
                    taxo2 = "Condiments, Bulk, Shelf Stable"
                    taxo3 = "Ketchup, Bulk"
                elif 'teriyaki' in desc_lower:
                    taxo2 = "Condiments, Bulk, Sauce"
                    taxo3 = "Teriyaki, Bulk, Ready To Serve"
            
            # RULE 36: OILS & SHORTENING
            elif any(keyword in desc_lower for keyword in ['oil', 'shortening', 'mineral oil']):
                taxo1 = "Oils & Shortening"
                if 'mineral' in desc_lower:
                    taxo2 = "Oil, Specialty"
                    taxo3 = "Specialty, Other"
                else:
                    taxo2 = "Oil, Specialty"
                    taxo3 = "Specialty, Other"
            
            # RULE 37: DAIRY & ICE CREAM
            elif any(keyword in desc_lower for keyword in ['dairy', 'milk', 'cream', 'ice cream', 'soft serve']):
                if 'ice cream' in desc_lower or 'soft serve' in desc_lower:
                    taxo1 = "Dairy"
                    taxo2 = "Ice Cream And Frozen Novelties"
                    if 'soft serve' in desc_lower:
                        taxo3 = "Ice Cream, Soft Serve, Mix, Frozen"
                    else:
                        taxo3 = "Ice Cream, Bulk, Frozen"
                elif 'milk' in desc_lower:
                    taxo1 = "Dairy"
                    taxo2 = "Milk"
                    taxo3 = "Milk, Whole, Refrigerated"
                elif 'cream' in desc_lower:
                    taxo1 = "Dairy"
                    taxo2 = "Cream"
                    taxo3 = "Cream, Heavy, Refrigerated"
                else:
                    taxo1 = "Dairy"
                    taxo2 = "Dairy, Other"
                    taxo3 = "Dairy, Other, Refrigerated"
            
            # RULE 38: WAFFLE & PANCAKE MIXES
            elif any(keyword in desc_lower for keyword in ['waffle', 'pancake', 'jacket']):
                taxo1 = "Grocery, Dry"
                taxo2 = "Bakery Mixes & Ingredients, Shelf Stable"
                if 'waffle' in desc_lower:
                    taxo3 = "Mix, Waffle"
                elif 'pancake' in desc_lower:
                    taxo3 = "Mix, Pancake"
                else:
                    taxo3 = "Bakery Ingredient, Specialty"
            
            # RULE 39: CANDY & SNACKS
            elif any(keyword in desc_lower for keyword in ['candy', 'chocolate', 'butterfinger', 'snack', 'chips']):
                taxo1 = "Grocery, Dry"
                taxo2 = "Chips, Snacks & Candy, Shelf Stable"
                if any(keyword in desc_lower for keyword in ['chocolate', 'candy', 'butterfinger']):
                    taxo3 = "Candy & Confections"
                elif 'chips' in desc_lower:
                    taxo3 = "Chips, Potato"
                else:
                    taxo3 = "Snacks, Other"
            
            # RULE 37: PROCESSED MEAT
            elif any(keyword in desc_lower for keyword in ['deli', 'luncheon', 'processed']):
                taxo1 = "Processed Meat"
                taxo2 = "Deli & Luncheon Meats"
                taxo3 = "Luncheon Meat, Other, Bulk Or Sliced, Frozen"
            
            # RULE 38: DISPENSERS
            elif 'dispenser' in desc_lower:
                taxo1 = "Equipment & Supplies"
                if any(keyword in desc_lower for keyword in ['soap', 'sanitizer']):
                    taxo2 = "Equipment"
                    taxo3 = "Dispensers, Dish"
                else:
                    taxo2 = "Equipment"
                    taxo3 = "Dispensers, Dish"
            
            # RULE 39: CLEANING AIDS
            elif any(keyword in desc_lower for keyword in ['brush', 'mop', 'duster']):
                taxo1 = "Equipment & Supplies"
                if 'mop' in desc_lower:
                    taxo2 = "Cleaning Aids, Janitorial & Floor Matting"
                    taxo3 = "Mops/Dusters & Handles"
                else:
                    taxo2 = "Cleaning Aids, Janitorial & Floor Matting"
                    taxo3 = "Brushes & Brush Handles"
            
            # RULE 40: SUPPLIES MISCELLANEOUS
            elif any(keyword in desc_lower for keyword in ['supply', 'miscellaneous', 'accessory']):
                taxo1 = "Equipment & Supplies"
                taxo2 = "Supplies, Miscellaneous"
                taxo3 = "Supplies, Miscellaneous"
            
            # RULE 41: EQUIPMENT PARTS
            elif any(keyword in desc_lower for keyword in ['part', 'accessory', 'component']):
                taxo1 = "Equipment & Supplies"
                taxo2 = "Equipment & Supplies, Parts"
                taxo3 = "E & S Misc Parts And Accessories"
            
            # RULE 42: CHINA MISC
            elif any(keyword in desc_lower for keyword in ['china', 'dish', 'bowl', 'plate']):
                taxo1 = "Equipment & Supplies"
                taxo2 = "China"
                if 'plate' in desc_lower:
                    taxo3 = "Plates & Saucers"
                elif 'bowl' in desc_lower:
                    taxo3 = "(Bowls, Dishes, Ovenware)"
                elif 'mug' in desc_lower:
                    taxo3 = "Mugs"
                else:
                    taxo3 = "(Bowls, Dishes, Ovenware)"
            
            # RULE 43: SMALLWARES MISC
            elif any(keyword in desc_lower for keyword in ['smallware', 'kitchen', 'bar']):
                taxo1 = "Equipment & Supplies"
                taxo2 = "Smallwares, Kitchen & Bar"
                if any(keyword in desc_lower for keyword in ['utensil', 'tool']):
                    taxo3 = "Kitchen Utensils"
                elif 'storage' in desc_lower:
                    taxo3 = "Food Storage Containers & Lids (W/O Wheels)"
                elif 'bar' in desc_lower:
                    taxo3 = "Bar Supplies"
                else:
                    taxo3 = "Kitchen Utensils"
            
            # RULE 44: EQUIPMENT MISC
            elif any(keyword in desc_lower for keyword in ['equipment', 'stand', 'tabletop']):
                taxo1 = "Equipment & Supplies"
                taxo2 = "Equipment"
                if 'stand' in desc_lower or 'tabletop' in desc_lower:
                    taxo3 = "Equipment Stands/Not Tabletop"
                else:
                    taxo3 = "Equipment, Countertop, Other"
            
            # RULE 45: FURNITURE
            elif any(keyword in desc_lower for keyword in ['furniture', 'table', 'shelving']):
                taxo1 = "Equipment & Supplies"
                taxo2 = "Furniture & Shelving"
                if 'table' in desc_lower:
                    taxo3 = "Tables & Tabletops"
                else:
                    taxo3 = "Shelving & Storage"
            
            # RULE 46: GLASSWARE
            elif any(keyword in desc_lower for keyword in ['glass', 'glassware']):
                taxo1 = "Equipment & Supplies"
                taxo2 = "Glassware"
                taxo3 = "Dinnerware, Glass"
            
            # RULE 47: FLATWARE
            elif any(keyword in desc_lower for keyword in ['flatware', 'fork', 'spoon', 'knife']):
                taxo1 = "Equipment & Supplies"
                if 'stainless' in desc_lower:
                    taxo2 = "Flatware, Stainless Steel, Metal"
                    if 'fork' in desc_lower:
                        taxo3 = "Forks, Stainless Steel Metal"
                    elif 'spoon' in desc_lower:
                        taxo3 = "Spoons, Stainless Steel Metal"
                    elif 'knife' in desc_lower:
                        taxo3 = "Knives, Stainless Steel Metal"
                else:
                    taxo2 = "Smallwares, Kitchen & Bar"
                    taxo3 = "Preparation Cutlery/Racks & Knife Blocks"
            
            # RULE 48: MENU & SIGNS
            elif any(keyword in desc_lower for keyword in ['menu', 'sign', 'board']):
                taxo1 = "Equipment & Supplies"
                taxo2 = "Smallwares, Dining Room"
                if 'menu' in desc_lower:
                    taxo3 = "Menu Covers & Inserts"
                else:
                    taxo3 = "Menu Boards & Signs"
            
            # RULE 49: TISSUE & TOWELS
            elif any(keyword in desc_lower for keyword in ['tissue', 'towel', 'wipes']):
                taxo1 = "Disposables"
                taxo2 = "Tissue, Towels, & Personal Hygiene, Disposable"
                if 'tissue' in desc_lower:
                    taxo3 = "Tissue, Toilet/Bathroom"
                elif 'towel' in desc_lower:
                    taxo3 = "Towels, Paper, Disposable"
                elif 'wipes' in desc_lower:
                    taxo3 = "Towelettes, Moist & Baby Wipes"
            
            # RULE 50: DEFAULT - If no rules match, use AI fallback
            else:
                # This will be handled by the AI fallback
                taxo1 = ""
                taxo2 = ""
                taxo3 = ""
            
            results.append({
                'Taxo1': taxo1,
                'Taxo2': taxo2,
                'Taxo3': taxo3
            })
        
        # Count how many were categorized by rules
        rule_categorized = sum(1 for r in results if r['Taxo1'])
        print(f"  Rule-based categorization completed: {rule_categorized}/{len(descriptions)} items categorized by rules")
        
        return results

    def _ai_categorization_fallback(self, descriptions: List[str]) -> List[Dict[str, str]]:
        """AI fallback categorization for items that don't match rules."""
        print(f"  Using AI fallback for {len(descriptions)} items...")
        
        # Initialize empty results
        results = []
        
        for desc in descriptions:
            # Use simple keyword-based categorization as fallback
            desc_lower = desc.lower()
            
            # Basic fallback rules for common items using CORRECT SALT categories
            if any(keyword in desc_lower for keyword in ['food', 'ingredient', 'spice', 'seasoning']):
                taxo1 = "Grocery, Dry"
                taxo2 = "Spices, Seasonings & Flavorings"
                taxo3 = "Spices, Seasonings & Flavorings, Other"
            elif any(keyword in desc_lower for keyword in ['beverage', 'drink', 'juice', 'soda']):
                taxo1 = "Beverages"
                taxo2 = "Beverages, Other"
                taxo3 = "Beverages, Other, Non-Alcoholic"
            elif any(keyword in desc_lower for keyword in ['dairy', 'milk', 'cheese', 'yogurt']):
                taxo1 = "Dairy"
                taxo2 = "Dairy, Other"
                taxo3 = "Dairy, Other, Refrigerated"
            elif any(keyword in desc_lower for keyword in ['meat', 'protein', 'fish', 'seafood']):
                taxo1 = "Beef"  # Default to beef for meat
                taxo2 = "Boxed, Loins"
                taxo3 = "Loin, Other, Raw, Refrigerated"
            elif any(keyword in desc_lower for keyword in ['produce', 'fruit', 'vegetable']):
                taxo1 = "Produce, Fresh"
                taxo2 = "Fruits, Other, Fresh"
                taxo3 = "Fruit, Other Fresh"
            elif any(keyword in desc_lower for keyword in ['bakery', 'bread', 'pastry', 'dessert']):
                taxo1 = "Grocery, Dry"
                taxo2 = "Bakery Mixes & Ingredients, Shelf Stable"
                taxo3 = "Bakery Ingredient, Specialty"
            elif any(keyword in desc_lower for keyword in ['frozen', 'ice cream', 'popsicle']):
                taxo1 = "Dairy"
                taxo2 = "Ice Cream And Frozen Novelties"
                taxo3 = "Ice Cream, Bulk, Frozen"
            elif any(keyword in desc_lower for keyword in ['canned', 'jar', 'preserved']):
                taxo1 = "Grocery, Dry"
                taxo2 = "Grocery, Other"
                taxo3 = "Grocery, Other, Shelf Stable"
            elif any(keyword in desc_lower for keyword in ['cleaning', 'chemical', 'soap', 'detergent']):
                taxo1 = "Chemicals & Cleaning Agents"
                taxo2 = "Detergent, Soap & Additives For Machines"
                taxo3 = "Detergents, Dish Machine"
            elif any(keyword in desc_lower for keyword in ['equipment', 'tool', 'machine', 'appliance']):
                taxo1 = "Equipment & Supplies"
                taxo2 = "Equipment"
                taxo3 = "Equipment, Countertop, Other"
            elif any(keyword in desc_lower for keyword in ['disposable', 'single use', 'throw away']):
                taxo1 = "Disposables"
                taxo2 = "Register Tape, Labels, Trays"
                taxo3 = "Labels, Miscellaneous"
            else:
                # Default fallback to Equipment & Supplies
                taxo1 = "Equipment & Supplies"
                taxo2 = "Supplies, Miscellaneous"
                taxo3 = "Supplies, Miscellaneous"
                taxo1 = "Equipment & Supplies"
                taxo2 = "Supplies, Miscellaneous"
                taxo3 = "Supplies, Miscellaneous"
            
            results.append({
                'Taxo1': taxo1,
                'Taxo2': taxo2,
                'Taxo3': taxo3
            })
        
        print(f"  AI fallback completed: {len(results)} items categorized")
        return results

    def _debug_taxonomy_categorization(self, descriptions: List[str], categories: List[Dict[str, str]]):
        """Provide detailed debugging information for taxonomy categorization."""
        print(f"\nTAXONOMY CATEGORIZATION DEBUG INFO:")
        print(f"  Total items: {len(descriptions)}")
        print(f"  Total categories: {len(categories)}")
        
        # Check for empty categories
        empty_level1 = sum(1 for cat in categories if not cat.get('Taxo1', '').strip())
        empty_level2 = sum(1 for cat in categories if not cat.get('Taxo2', '').strip())
        empty_level3 = sum(1 for cat in categories if not cat.get('Taxo3', '').strip())
        
        if len(categories) > 0:
            print(f"  Empty Level I: {empty_level1}/{len(categories)} ({empty_level1/len(categories)*100:.1f}%)")
            print(f"  Empty Level II: {empty_level2}/{len(categories)} ({empty_level2/len(categories)*100:.1f}%)")
            print(f"  Empty Level III: {empty_level3}/{len(categories)} ({empty_level3/len(categories)*100:.1f}%)")
        else:
            print(f"  Empty Level I: {empty_level1}/0 (0.0%)")
            print(f"  Empty Level II: {empty_level2}/0 (0.0%)")
            print(f"  Empty Level III: {empty_level3}/0 (0.0%)")
        
        # Show sample categorizations
        print(f"\nSAMPLE CATEGORIZATIONS:")
        for i in range(min(3, len(categories))):
            cat = categories[i]
            desc = descriptions[i] if i < len(descriptions) else "N/A"
            print(f"  {i+1}. '{desc[:50]}{'...' if len(desc) > 50 else ''}'")
            print(f"     → Taxo1: '{cat.get('Taxo1', '')}'")
            print(f"     → Taxo2: '{cat.get('Taxo2', '')}'")
            print(f"     → Taxo3: '{cat.get('Taxo3', '')}'")
        
        # Show taxonomy tree statistics
        print(f"\nTAXONOMY TREE STATISTICS:")
        print(f"  Available Level I categories: {len(self.level1_categories)}")
        total_level2 = sum(len(level2s) for level2s in self.level2_lookup.values())
        total_level3 = sum(len(level3s) for level3s in self.level3_lookup.values())
        print(f"  Available Level II categories: {total_level2}")
        print(f"  Available Level III categories: {total_level3}")
        
        print("=" * 60)
    
    def _analyze_taxonomy_matching_issues(self, descriptions: List[str], categories: List[Dict[str, str]]):
        """Analyze specific taxonomy matching issues and provide suggestions."""
        print(f"\nTAXONOMY MATCHING ISSUE ANALYSIS:")
        
        # Find items with no Level I categorization
        no_level1_items = []
        for i, (desc, cat) in enumerate(zip(descriptions, categories)):
            if not cat.get('Taxo1', '').strip():
                no_level1_items.append((i, desc))
        
        if no_level1_items:
            print(f"  Items with NO Level I categorization: {len(no_level1_items)}")
            print(f"    These items couldn't be matched to any top-level category")
            print(f"    Possible reasons:")
            print(f"      - Product type is unclear or ambiguous")
            print(f"      - Product doesn't fit existing taxonomy categories")
            print(f"      - AI couldn't determine the best category")
            print(f"      - Product description is too generic or unclear")
            
            # Show examples
            print(f"    Examples of items without Level I:")
            for i, (row_idx, desc) in enumerate(no_level1_items[:5]):
                print(f"      {i+1}. Row {row_idx+1}: '{desc[:80]}{'...' if len(desc) > 80 else ''}'")
            if len(no_level1_items) > 5:
                print(f"      ... and {len(no_level1_items) - 5} more")
        
        # Find items with Level I but no Level II
        level1_only_items = []
        for i, (desc, cat) in enumerate(zip(descriptions, categories)):
            if cat.get('Taxo1', '').strip() and not cat.get('Taxo2', '').strip():
                level1_only_items.append((i, desc, cat.get('Taxo1', '').strip()))
        
        if level1_only_items:
            print(f"\n  Items with Level I but NO Level II: {len(level1_only_items)}")
            print(f"    These items have a top-level category but couldn't be subcategorized")
            print(f"    Possible reasons:")
            print(f"      - No suitable Level II categories exist for the Level I category")
            print(f"      - Product is too generic for subcategorization")
            print(f"      - AI couldn't determine the best subcategory")
            
            # Group by Level I category
            level1_groups = {}
            for row_idx, desc, level1 in level1_only_items:
                if level1 not in level1_groups:
                    level1_groups[level1] = []
                level1_groups[level1].append((row_idx, desc))
            
            print(f"    Breakdown by Level I category:")
            for level1, items in level1_groups.items():
                print(f"      - {level1}: {len(items)} items")
                # Check if Level II categories exist for this Level I
                available_level2 = self._get_available_level2(level1)
                if available_level2:
                    print(f"        Available Level II options: {', '.join(available_level2[:5])}{'...' if len(available_level2) > 5 else ''}")
                else:
                    print(f"        WARNING: No Level II categories available for '{level1}'")
        
        # Find items with Level I+II but no Level III
        level2_only_items = []
        for i, (desc, cat) in enumerate(zip(descriptions, categories)):
            if (cat.get('Taxo1', '').strip() and 
                cat.get('Taxo2', '').strip() and 
                not cat.get('Taxo3', '').strip()):
                level2_only_items.append((i, desc, cat.get('Taxo1', '').strip(), cat.get('Taxo2', '').strip()))
        
        if level2_only_items:
            print(f"\n  Items with Level I+II but NO Level III: {len(level2_only_items)}")
            print(f"    These items have main and subcategories but couldn't be further categorized")
            print(f"    Possible reasons:")
            print(f"      - No suitable Level III categories exist for the Level I+II combination")
            print(f"      - Product is too generic for further subcategorization")
            print(f"      - AI couldn't determine the best sub-subcategory")
            
            # Group by Level I+II combination
            level2_groups = {}
            for row_idx, desc, level1, level2 in level2_only_items:
                key = f"{level1} > {level2}"
                if key not in level2_groups:
                    level2_groups[key] = []
                level2_groups[key].append((row_idx, desc))
            
            print(f"    Breakdown by Level I+II combination:")
            if level2_groups:  # Only process if we have groups
                for combo, items in level2_groups.items():
                    print(f"      - {combo}: {len(items)} items")
                    # Check if Level III categories exist for this combination
                    level1, level2 = combo.split(" > ")
                    available_level3 = self._get_available_level3(level1, level2)
                    if available_level3:
                        print(f"        Available Level III options: {', '.join(available_level3[:5])}{'...' if len(available_level3) > 5 else ''}")
                    else:
                        print(f"        WARNING: No Level III categories available for '{combo}'")
            else:
                print(f"      - No Level I+II combinations found")
        
        # Provide overall suggestions
        print(f"\nIMPROVEMENT SUGGESTIONS:")
        
        if no_level1_items:
            print(f"  For items without Level I:")
            print(f"    - Review product descriptions for clarity")
            print(f"    - Consider adding new top-level categories to taxonomy if needed")
            print(f"    - Check if product types are too generic or ambiguous")
        
        if level1_only_items:
            print(f"  For items with Level I only:")
            print(f"    - Review if Level II categories are missing from taxonomy")
            print(f"    - Consider if products are too generic for subcategorization")
            print(f"    - Check if AI prompts need improvement for subcategory selection")
        
        if level2_only_items:
            print(f"  For items with Level I+II only:")
            print(f"    - Review if Level III categories are missing from taxonomy")
            print(f"    - Consider if products are too generic for further subcategorization")
            print(f"    - Check if AI prompts need improvement for sub-subcategory selection")
        
        print("=" * 60) 
