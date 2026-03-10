#!/usr/bin/env python3
"""
MongoDB Query Script for DocumentDB
GTIN lookup functionality to check if GTINs exist in database and return taxonomy/search info
"""

import pymongo
from pymongo import MongoClient
import json
from datetime import datetime
import os
from typing import Dict, List, Any, Optional, Tuple
import ssl
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
# Try parent directory first, then current directory
if os.path.exists("../.env"):
    load_dotenv("../.env")
elif os.path.exists(".env"):
    load_dotenv(".env")
else:
    load_dotenv()  # Try default location

class MongoDBGTINLookup:
    def __init__(self, connection_string: str = None, database_name: str = "salt", collection_name: str = "ds_gtin_metadata", 
                 username: str = None, password: str = None, cert_bundle_path: str = None):
        """
        Initialize connection to DocumentDB
        
        Args:
            connection_string: DocumentDB connection string (optional if using individual params)
            database_name: Database name (default: salt)
            collection_name: Collection name (default: ds_gtin_metadata)
            username: Database username (default: engineering)
            password: Database password (from .env file)
            cert_bundle_path: Path to certificate bundle file (default: global-bundle.pem)
        """
        self.database_name = database_name
        self.collection_name = collection_name
        self.username = username or "engineering"
        self.password = password
        self.cert_bundle_path = self._resolve_cert_bundle_path(cert_bundle_path)
        self.client = None
        self.db = None
        self.collection = None
        
        # Build connection string if not provided
        if connection_string:
            self.connection_string = connection_string
        else:
            self.connection_string = self._build_mongodb_connection()

    def _resolve_cert_bundle_path(self, cert_bundle_path: str | None) -> str:
        backend_root = Path(__file__).resolve().parents[3]

        if cert_bundle_path:
            candidate = Path(cert_bundle_path).expanduser()
            if candidate.is_absolute():
                return str(candidate)

            scoped_candidates = [
                backend_root / candidate,
                Path.cwd() / candidate,
            ]
            for scoped in scoped_candidates:
                if scoped.exists():
                    return str(scoped.resolve())
            return str((backend_root / candidate).resolve())

        for candidate in (
            backend_root / "global-bundle.pem",
            backend_root / "global-bundle.pem 01-05-10-697.pem",
        ):
            if candidate.exists():
                return str(candidate.resolve())

        return str((backend_root / "global-bundle.pem").resolve())
    
    def _build_mongodb_connection(self) -> str:
        """Build the MongoDB connection string with proper parameters"""
        # Get password from environment if not provided
        if not self.password:
            self.password = os.getenv('MONGODB_PASSWORD')
            if not self.password:
                raise ValueError("MONGODB_PASSWORD environment variable not set")
        
        # Verify certificate bundle exists
        cert_bundle_path = Path(self.cert_bundle_path).expanduser()
        print(f"Looking for certificate bundle at: {cert_bundle_path}")
        if not cert_bundle_path.exists():
            raise FileNotFoundError(f"Certificate bundle not found: {self.cert_bundle_path}")
        self.cert_bundle_path = str(cert_bundle_path.resolve())
        print(f"Certificate bundle found at: {self.cert_bundle_path}")
        
        # Build connection string with all required parameters
        connection_string = (
            f"mongodb://{self.username}:{self.password}@"
            f"pepper-production-docdb.cluster-cxciwycm3oeg.us-east-1.docdb.amazonaws.com/"
            f"?tls=true"
            f"&tlsCAFile={self.cert_bundle_path}"
            f"&replicaSet=rs0"
            f"&readPreference=secondaryPreferred"
            f"&retryWrites=false"
            f"&authSource=admin"
        )
        
        return connection_string
    
    def connect(self, timeout_ms: int = 60000):
        """
        Establish connection to DocumentDB with the specific configuration
        
        Args:
            timeout_ms: Connection timeout in milliseconds
        """
        try:
            # Enhanced connection options for DocumentDB
            connection_options = {
                'serverSelectionTimeoutMS': timeout_ms,
                'connectTimeoutMS': timeout_ms,
                'socketTimeoutMS': timeout_ms,
                'maxPoolSize': 10,
                'retryWrites': False,  # DocumentDB doesn't support retryWrites
                'retryReads': False,   # DocumentDB doesn't support retryReads
            }
            
            print(f"Attempting to connect to DocumentDB...")
            print(f"Username: {self.username}")
            print(f"Database: {self.database_name}")
            print(f"Collection: {self.collection_name}")
            print(f"Certificate bundle: {self.cert_bundle_path}")
            print(f"Timeout: {timeout_ms}ms")
            
            # Connect to DocumentDB with the specific connection string
            self.client = MongoClient(self.connection_string, **connection_options)
            self.db = self.client[self.database_name]
            self.collection = self.db[self.collection_name]
            
            # Test connection with longer timeout
            print("Testing connection...")
            self.client.admin.command('ping', serverSelectionTimeoutMS=timeout_ms)
            print(f"Successfully connected to {self.database_name}.{self.collection_name}")
            return True
            
        except Exception as e:
            print(f"Error connecting to DocumentDB: {e}")
            if "Authorization failure" in str(e) or "code: 13" in str(e):
                print("\nWARNING: AUTHORIZATION ERROR DETECTED")
                print("This means the connection is working but you need proper credentials.")
                print("Please check your .env file for MONGODB_PASSWORD")
                print("And ensure global-bundle.pem exists in the parent directory")
            return False
    
    def disconnect(self):
        """Close connection to DocumentDB"""
        if self.client:
            self.client.close()
            print("Disconnected from DocumentDB")
    
    def query_gtin(self, gtin: str) -> Tuple[bool, Dict]:
        """
        Look up a GTIN in the database and return match status and taxonomy/search info
        
        Args:
            gtin: GTIN string to look up
            
        Returns:
            Tuple of (exists: bool, info: Dict)
            If exists=True, info contains taxonomy and search.query_name
            If exists=False, info is empty dict
        """
        if self.collection is None:
            print("Not connected to DocumentDB. Call connect() first.")
            return False, {}
        
        try:
            # Simple query using dot notation to search identifiers.value
            query = {
                'identifiers.value': gtin
            }
            
            # Find document with projection for only needed fields
            projection = {
                "_id": 0,
                "taxonomy": 1,
                "search": 1,
                "item_info": 1
            }
            
            doc = self.collection.find_one(query, projection)
            
            if doc:
                # Extract taxonomy and search info
                taxonomy = doc.get("taxonomy", {})
                search_info = doc.get("search", {})
                
                result_info = {
                    "taxonomy": {
                        "category": taxonomy.get("category"),
                        "subcategory": taxonomy.get("subcategory"),
                        "subsubcategory": taxonomy.get("subsubcategory")
                    },
                    "search": {
                        "query_name": search_info.get("query_name"),
                        "query_description": search_info.get("query_description"),
                        "confidence": search_info.get("confidence")
                    },
                    "product_info": {
                        "product_name": self._extract_product_name(doc),
                        "description": self._extract_description(doc),
                        "brand": self._extract_brand(doc)
                    }
                }
                
                return True, result_info
            else:
                return False, {}
                
        except Exception as e:
            print(f"Error looking up GTIN {gtin}: {e}")
            return False, {}
    
    def _extract_product_name(self, doc: Dict) -> str:
        """Extract product name from document"""
        try:
            # Try to get from item_info first
            item_info = doc.get("item_info", {})
            basic_info = item_info.get("basic_information", {})
            names = basic_info.get("names", [])
            
            if names:
                for name_obj in names:
                    if name_obj.get("name") == "Name":
                        return name_obj.get("value", "")
            
            # Fallback to search query_name
            search_info = doc.get("search", {})
            return search_info.get("query_name", "")
            
        except Exception:
            return ""
    
    def _extract_description(self, doc: Dict) -> str:
        """Extract product description from document"""
        try:
            # Try to get from search first
            search_info = doc.get("search", {})
            description = search_info.get("query_description", "")
            
            if description:
                return description
            
            # Fallback to item_info descriptions
            item_info = doc.get("item_info", {})
            basic_info = item_info.get("basic_information", {})
            descriptions = basic_info.get("descriptions", [])
            
            if descriptions:
                return " ".join([desc.get("value", "") for desc in descriptions])
            
            return ""
            
        except Exception:
            return ""
    
    def _extract_brand(self, doc: Dict) -> str:
        """Extract brand information from document"""
        try:
            item_info = doc.get("item_info", {})
            brand_info = item_info.get("brand_information", {})
            return brand_info.get("brand_name", "")
            
        except Exception:
            return ""
    
    def batch_query_gtins(self, gtins: List[str]) -> Dict[str, Tuple[bool, Dict]]:
        """
        Look up multiple GTINs in batch for efficiency
        
        Args:
            gtins: List of GTIN strings to look up
            
        Returns:
            Dict mapping GTIN to (exists, info) tuple
        """
        if self.collection is None:
            print("Not connected to DocumentDB. Call connect() first.")
            return {}
        
        results = {}
        
        try:
            # Process GTINs in batches for efficiency
            batch_size = 100
            for i in range(0, len(gtins), batch_size):
                batch = gtins[i:i + batch_size]
                
                # Build query for batch
                batch_query = {
                    'identifiers.value': {"$in": batch}
                }
                
                # Find documents with projection
                projection = {
                    "_id": 0,
                    "identifiers": 1,
                    "taxonomy": 1,
                    "search": 1,
                    "item_info": 1
                }
                
                cursor = self.collection.find(batch_query, projection)
                
                # Process results
                for doc in cursor:
                    # Find which GTIN matched - use the identifiers.value structure
                    matched_gtin = None
                    for identifier in doc.get("identifiers", []):
                        if "value" in identifier and identifier["value"] in batch:
                            matched_gtin = identifier["value"]
                            break
                    
                    if matched_gtin:
                        # Extract taxonomy and search info
                        taxonomy = doc.get("taxonomy", {})
                        search_info = doc.get("search", {})
                        
                        result_info = {
                            "taxonomy": {
                                "category": taxonomy.get("category"),
                                "subcategory": taxonomy.get("subcategory"),
                                "subsubcategory": taxonomy.get("subsubcategory")
                            },
                            "search": {
                                "query_name": search_info.get("query_name"),
                                "query_description": search_info.get("query_description"),
                                "confidence": search_info.get("confidence")
                            },
                            "product_info": {
                                "product_name": self._extract_product_name(doc),
                                "description": self._extract_description(doc),
                                "brand": self._extract_brand(doc)
                            }
                        }
                        
                        results[matched_gtin] = (True, result_info)
                
                # Mark unmatched GTINs as False
                for gtin in batch:
                    if gtin not in results:
                        results[gtin] = (False, {})
            
            return results
                
        except Exception as e:
            print(f"Error in batch GTIN lookup: {e}")
            # Fall back to individual lookups
            for gtin in gtins:
                results[gtin] = self.query_gtin(gtin)
            
            return results

def main():
    """Main function to demonstrate GTIN lookup functionality"""
    
    # Initialize query object
    lookup_client = MongoDBGTINLookup()
    
    try:
        # Try to connect with enhanced options
        print("\n=== Attempting Main Connection ===")
        if not query_obj.connect(timeout_ms=60000):
            print("\nMain connection failed. Please check the troubleshooting tips above.")
            return
        
        print("\n=== Connection Successful! Testing GTIN Lookup ===")
        
        # Test individual GTIN lookup
        test_gtin = "1234567890123"  # Replace with actual GTIN for testing
        print(f"\n--- Testing Individual GTIN Lookup ---")
        print(f"Looking up GTIN: {test_gtin}")
        
        exists, info = lookup_client.query_gtin(test_gtin)
        if exists:
            print(f"GTIN found in database")
            print(f"Category: {info['taxonomy']['category']}")
            print(f"Subcategory: {info['taxonomy']['subcategory']}")
            print(f"Sub-subcategory: {info['taxonomy']['subsubcategory']}")
            print(f"Query Name: {info['search']['query_name']}")
        else:
            print(f"GTIN not found in database")
        
        # Test batch lookup
        test_gtins = ["1234567890123", "9876543210987", "5555555555555"]  # Replace with actual GTINs
        print(f"\n--- Testing Batch GTIN Lookup ---")
        print(f"Looking up {len(test_gtins)} GTINs")
        
        batch_results = query_obj.batch_lookup_gtins(test_gtins)
        for gtin, (exists, info) in batch_results.items():
            status = "Found" if exists else "Not Found"
            print(f"{gtin}: {status}")
            if exists:
                print(f"  Category: {info['taxonomy']['category']}")
        
        print("\n=== GTIN Lookup Testing Complete ===")
        
    except Exception as e:
        print(f"Error in main execution: {e}")
    
    finally:
        # Clean up connection
        lookup_client.disconnect()

if __name__ == "__main__":
    main()
