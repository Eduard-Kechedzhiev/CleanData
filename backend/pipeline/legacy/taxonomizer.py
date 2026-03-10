import logging
from typing import Any, Optional

import numpy as np
import pandas as pd
import requests


class Taxonomizer:
    def __init__(self, request_url: Optional[str] = None, batch_size: int = 100) -> None:
        # Set up logging
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

        # API endpoint
        self.request_url = (
            request_url
            or "https://salt-dev-ai-service-api.greedyants.com/api/v1/validation/taxonomize_items"
        )

        # Batch size for API requests
        self.batch_size = batch_size

    def taxonomize_batch(self, items: list[dict[str, Any]]) -> list[dict[str, str]]:
        """Taxonomize a batch of items with proper batching and rate limiting for large datasets."""
        if not items:
            return []
        
        # Determine optimal batch size based on dataset size
        total_items = len(items)
        if total_items > 500:
            batch_size = 25  # Smaller batches for very large datasets
        elif total_items > 200:
            batch_size = 50  # Medium batches for large datasets
        else:
            batch_size = min(self.batch_size, 100)  # Default batch size
        
        self.logger.info(f"Processing {total_items} items in batches of {batch_size}")
        
        all_results = []
        total_batches = (total_items + batch_size - 1) // batch_size
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, total_items)
            batch_items = items[start_idx:end_idx]
            
            self.logger.info(f"Processing batch {batch_num + 1}/{total_batches} ({len(batch_items)} items)")
            
            # Process this batch with retries
            batch_results = self._process_batch_with_retries(batch_items, batch_num + 1, total_batches)
            all_results.extend(batch_results)
            
            # Rate limiting between batches (except for the last batch)
            if batch_num < total_batches - 1:
                import time
                time.sleep(0.5)  # 500ms delay between batches
        
        self.logger.info(f"Completed processing {len(all_results)} items in {total_batches} batches")
        return all_results
    
    def _process_batch_with_retries(self, batch_items: list[dict[str, Any]], batch_num: int, total_batches: int) -> list[dict[str, str]]:
        """Process a single batch with retries for N/A responses."""
        max_retries = 5  # Increased retries for N/A handling
        retry_count = 0
        backoff_time = 1  # Initial backoff time in seconds
        
        while retry_count < max_retries:
            try:
                taxonomy_requests = [
                    {
                        "item_name": item.get("name", ""),
                        "item_description": item.get("description", ""),
                    }
                    for item in batch_items
                ]

                self.logger.info(
                    f"Attempt {retry_count + 1}/{max_retries}: Sending batch {batch_num}/{total_batches} of {len(taxonomy_requests)} items for taxonomization"
                )
                response = requests.post(
                    self.request_url, json=taxonomy_requests, timeout=30
                )
                response.raise_for_status()
                
                results = response.json()
                
                # Check if any results are N/A and need retrying
                na_count = sum(1 for result in results if any(
                    value == "N/A" for value in result.values()
                ))
                
                if na_count == 0:
                    # All results are valid, return them
                    self.logger.info(f"Batch {batch_num}/{total_batches}: All {len(results)} items categorized successfully")
                    return results
                else:
                    # Some results are N/A, retry with enhanced descriptions
                    self.logger.warning(f"Batch {batch_num}/{total_batches}: {na_count}/{len(results)} items returned N/A, retrying with enhanced descriptions...")
                    
                    # Enhance descriptions for N/A items
                    enhanced_requests = []
                    for i, item in enumerate(batch_items):
                        if any(value == "N/A" for value in results[i].values()):
                            # Create more detailed description for N/A items
                            enhanced_name = item.get("name", "")
                            enhanced_desc = item.get("description", "")
                            
                            # Add context keywords that might help categorization
                            if "mask" in enhanced_name.lower() or "mask" in enhanced_desc.lower():
                                # Try different approaches for masks
                                if retry_count == 1:
                                    enhanced_desc += " Personal protective equipment, disposable face covering, safety gear"
                                elif retry_count == 2:
                                    enhanced_desc += " Medical supplies, healthcare equipment, safety products"
                                elif retry_count == 3:
                                    enhanced_desc += " Restaurant supplies, food service equipment, disposable products"
                                else:
                                    enhanced_desc += " Cleaning supplies, paper products, disposable items"
                            elif "glove" in enhanced_name.lower() or "glove" in enhanced_desc.lower():
                                # Try different approaches for gloves
                                if retry_count == 1:
                                    enhanced_desc += " Hand protection, disposable gloves, food service equipment"
                                elif retry_count == 2:
                                    enhanced_desc += " Restaurant supplies, kitchen equipment, safety gear"
                                else:
                                    enhanced_desc += " Cleaning supplies, disposable products, commercial equipment"
                            elif "towel" in enhanced_name.lower() or "towel" in enhanced_desc.lower():
                                # Try different approaches for towels
                                if retry_count == 1:
                                    enhanced_desc += " Cleaning supplies, paper products, disposable towels"
                                elif retry_count == 2:
                                    enhanced_desc += " Restaurant supplies, kitchen equipment, cleaning products"
                                else:
                                    enhanced_desc += " Food service, commercial kitchen, disposable supplies"
                            else:
                                # Generic enhancement with different approaches
                                if retry_count == 1:
                                    enhanced_desc += " Food service, restaurant supplies, commercial kitchen equipment"
                                elif retry_count == 2:
                                    enhanced_desc += " Healthcare supplies, medical equipment, safety products"
                                elif retry_count == 3:
                                    enhanced_desc += " Cleaning supplies, paper products, disposable items"
                                else:
                                    enhanced_desc += " Commercial supplies, business equipment, industrial products"
                            
                            enhanced_requests.append({
                                "item_name": enhanced_name,
                                "item_description": enhanced_desc,
                            })
                        else:
                            # Keep original request for already categorized items
                            enhanced_requests.append({
                                "item_name": item.get("name", ""),
                                "item_description": item.get("description", ""),
                            })
                    
                    # Retry with enhanced descriptions
                    retry_count += 1
                    if retry_count < max_retries:
                        self.logger.info(f"Retrying batch {batch_num}/{total_batches} with enhanced descriptions (attempt {retry_count + 1}/{max_retries})")
                        import time
                        time.sleep(backoff_time)
                        backoff_time *= 1.5  # Gentler backoff for N/A retries
                        continue
                    else:
                        # Final attempt failed, return what we have
                        self.logger.warning(f"Batch {batch_num}/{total_batches}: Max retries reached, returning results with {na_count} N/A items")
                        return results
                        
            except Exception as e:
                # Network/API error handling
                retry_count += 1
                if retry_count < max_retries:
                    self.logger.warning(
                        f"Batch {batch_num}/{total_batches}: Attempt {retry_count}/{max_retries}: API error, retrying after {backoff_time}s: {str(e)}"
                    )
                    import time
                    time.sleep(backoff_time)
                    backoff_time *= 2  # Exponential backoff for errors
                else:
                    # If all retries failed, log the error and raise an exception to trigger fallback
                    self.logger.exception(f"Batch {batch_num}/{total_batches}: All retries failed: {str(e)}")
                    raise Exception(f"External API failed for batch {batch_num}/{total_batches} after all retries: {str(e)}")

    def extend_with_taxonomy(
        self, df: pd.DataFrame, taxonomies: list[dict[str, str]]
    ) -> pd.DataFrame:
        """Extend the dataframe with the taxonomies."""
        categories, subcategories, subsubcategories = [], [], []
        for taxonomy in taxonomies:
            categories.append(taxonomy.get("category", ""))
            subcategories.append(taxonomy.get("subcategory", ""))
            subsubcategories.append(taxonomy.get("subsubcategory", ""))

        df["category"] = categories
        df["subcategory"] = subcategories
        df["subsubcategory"] = subsubcategories
        return df

    def process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process a DataFrame to add taxonomy information.
        This is the main function to use in notebooks.

        Args:
            df: Input DataFrame containing 'name' and 'description' columns

        Returns:
            DataFrame with additional taxonomy columns
        """
        try:
            # Make a copy to avoid modifying the original
            df_copy = df.copy()

            # Replace NaN values with empty strings
            df_copy = df_copy.replace(np.nan, "")

            # Log info about the data
            self.logger.info(f"Processing {len(df_copy)} rows for taxonomization")

            # Process in batches
            batch = []
            taxonomies = []

            for i, row in df_copy.iterrows():
                row_dict = row.to_dict()
                batch.append(row_dict)

                if len(batch) == self.batch_size:
                    batch_taxonomies = self.taxonomize_batch(batch)
                    taxonomies.extend(batch_taxonomies)
                    self.logger.info(f"Processed batch {i//self.batch_size + 1}")
                    batch = []

            # Process any remaining items
            if batch:
                batch_taxonomies = self.taxonomize_batch(batch)
                taxonomies.extend(batch_taxonomies)

            # Add taxonomy data to the dataframe
            return self.extend_with_taxonomy(df_copy, taxonomies)

        except Exception as e:
            self.logger.exception(f"Error in process_dataframe: {str(e)}")
            raise
