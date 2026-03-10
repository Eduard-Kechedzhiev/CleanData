"""
AI Prompts for Data Cleaner
Contains all the prompts used for AI-powered data cleaning and categorization.
"""

# AI Prompts for Data Cleaning and Taxonomy Categorization

# Column Detection
COLUMN_DETECTION_PROMPT = """
You are an AI assistant helping to identify the correct column for {column_type} data in a CSV file.

Available columns and sample data:
{column_data}

Keywords to look for: {keywords}

Please analyze the sample data and identify which column contains {column_type} information.
Return only the exact column name, or "NONE" if no suitable column is found.

Consider:
- Column names that contain the keywords
- Sample data that matches the expected format
- Common variations and abbreviations
- The most logical choice based on the data content
"""

# Pack Size Extraction
PACK_SIZE_EXTRACTION_PROMPT = """
Extract and standardize pack size information from these product descriptions.

Product Descriptions:
{descriptions}

For each description, extract the pack size information and return it in a standardized format.

Guidelines:
- Extract weight (oz, lb, kg, g)
- Extract volume (ml, l, gal, qt)
- Extract count (CT, count, pieces, units)
- Extract dimensions (inches, cm, mm)
- Standardize units to consistent abbreviations
- Processing info: "IQF" (Individually Quick Frozen), "frozen", "fresh"
- If no pack size found, return empty string

Return a JSON array with the extracted pack sizes in the same order as the descriptions.
Example: ["16 oz", "1 lb", "500 CT", "12x8x2 in"]

Return only the JSON array, nothing else.
"""

# Brand Extraction
BRAND_EXTRACTION_PROMPT = """
Extract brand names from these product descriptions.

Product Descriptions:
{descriptions}

For each description, extract the brand name if present.

Guidelines:
- Look for company names, brand names, or manufacturer names
- Common food service brands (Kraft, Heinz, Sysco, etc.)
- Private label indicators (store brands, generic brands)
- If no brand found, return empty string
- Return the most recognizable brand name format

Return a JSON array with the extracted brand names in the same order as the descriptions.
Example: ["Kraft", "Heinz", "", "Sysco"]

Return only the JSON array, nothing else.
"""

# Brand Cleaning
BRAND_CLEANING_PROMPT = """
Clean and standardize these brand names for professional use.

Brand Names:
{brands}

For each brand name, clean and standardize it according to these rules:

1. **Capitalization**: Use proper capitalization (e.g., "Kraft" not "KRAFT")
2. **Spacing**: Remove extra spaces, standardize spacing
3. **Abbreviations**: Expand common abbreviations (e.g., "PHIL" → "Philadelphia")
4. **Formatting**: Maintain professional appearance
5. **Consistency**: Ensure consistent formatting across similar brands

Examples:
- "KRAFT      " → "Kraft"
- "beyond meat" → "Beyond Meat"
- "PHIL" → "Philadelphia"
- "HEIN" → "Heinz"
- "SYSCO" → "Sysco"

Return a JSON array with the cleaned brand names in the same order.
Example: ["Kraft", "Beyond Meat", "Philadelphia", "Heinz", "Sysco"]

Return only the JSON array, nothing else.
"""

# Description Cleaning
DESCRIPTION_CLEANING_PROMPT = """
Clean and standardize these product descriptions for professional use.

Product Descriptions:
{descriptions}

For each description, clean and standardize it according to these rules:

1. **Spacing**: Remove extra spaces, standardize spacing
2. **Abbreviations**: Expand common abbreviations
3. **Formatting**: Maintain professional appearance
4. **Clarity**: Make descriptions clear and understandable
5. **Consistency**: Ensure consistent formatting

Specific Rules:
- Use standard abbreviations: "pound" → "lb", "ounce" → "Oz", "inch" → "in"
- Remove excessive punctuation and special characters
- Standardize common terms (e.g., "Oz" for ounces, "lb" for pounds, "in" for inches)
- Maintain product-specific terminology
- Keep important technical specifications
- Manual abbreviation replacement will handle measurement units automatically

Examples:
- "SHRIMP IQF 16/20" → "Shrimp, IQF (Individually Quick Frozen), 16/20 count"
- "BEYOND MEAT PATTYS FOODSERVICE" → "Beyond Meat Patties, Foodservice"
- "PAPER PLACEMAT AUTUMN         *SPEC.ORD*" → "Paper Placemat, Autumn, Special Order"
- "FISH COD TAIL 4 OUNCE" → "Fish Cod Tail 4 Oz" (manual replacement: OUNCE → Oz)
- "FISH HADDOCK 2 POUND" → "Fish Haddock 2 lb" (manual replacement: POUND → lb)
- "FISH SOLE 5 INCH" → "Fish Sole 5 in" (manual replacement: INCH → in)

Return a JSON array with the cleaned descriptions in the same order.
Example: ["Shrimp, IQF (Individually Quick Frozen), 16/20 count", "Beyond Meat Patties, Foodservice"]

Return only the JSON array, nothing else.
"""



# Enhanced Brand Research Prompt (Single Brand)
ENHANCED_BRAND_RESEARCH_PROMPT = """
Research and verify this brand name for accuracy and completeness.

Product Name: {product_name}
Product Description: {product_description}
Existing Brand: {existing_brand}

Please:
1. Research the brand name to find the full, correct company name
2. Confirm the proper spelling and capitalization
3. Check if this is a known brand or if it might be a private label
4. Return the most accurate, professional brand name

Consider:
- Is this a major food service brand?
- Is it a private label or store brand?
- Are there common misspellings or abbreviations?
- What's the most professional way to represent this brand?
- For French brands like "La Francai/La Francais", ensure correct spelling

Return only the cleaned brand name, nothing else.
"""

# Enhanced Brand Research Prompt (No Existing Brand)
ENHANCED_BRAND_EXTRACTION_PROMPT = """
Extract and research the brand name from this product information.

Product Name: {product_name}
Product Description: {product_description}

Please:
1. Look for any brand indicators in the name or description
2. Research to find the full, correct company name if possible
3. Check if this is a known brand or private label
4. Return the most accurate, professional brand name

Consider:
- Company names mentioned in the description
- Brand indicators like "by [Company]", "from [Company]"
- Common food service brands
- Private label indicators

If no brand can be identified, return "Private Label" or empty string.

Return only the brand name or "Private Label" or empty string, nothing else.
"""

# Comprehensive Brand Processing Prompt
COMPREHENSIVE_BRAND_PROCESSING_PROMPT = """
Research, clean, and standardize these brand names for accuracy, completeness, and consistency.

For each item, analyze the product information and return the most accurate, professional brand name.

IMPORTANT REQUIREMENTS:
1. **Research & Accuracy**: Find the correct, official company name when possible
2. **Consistency**: Ensure all variations of the same brand use identical spelling and formatting
3. **Professional Format**: Use proper capitalization and professional appearance
4. **Variation Handling**: Standardize similar brand names to consistent formats

CRITICAL CAPITALIZATION RULES:
- Convert ALL CAPS to Title Case for all brands (e.g., "LACTANTIA" → "Lactantia")
- Use proper Title Case: first letter of each word capitalized, rest lowercase
- French brands: "LA FRANCAI" → "La Française" (proper French spelling)
- Remove trailing spaces and extra whitespace

SPECIFIC BRAND STANDARDIZATION:
- "LA FRANCAI" → "La Française" (proper French spelling)
- "OTIS SPUNK" → "Otis Spunk" (Title Case)
- "LACTANTIA" → "Lactantia" (Title Case)
- "WISE BY NA" → "Wise by NA" (Title Case)
- "MOLLY B'S" → "Molly B's" (Title Case)
- "511" → "511" (keep as is)

Items to process:
{items_data}

Return a JSON array with the cleaned and standardized brand names in the same order.
Example format: ["Brand1", "Brand2", "Brand3"]

Return only the JSON array, nothing else.
"""

# ============================================================================
# TAXONOMY CATEGORIZATION PROMPTS
# ============================================================================

# Taxonomy Level I Categorization (Improved)
TAXONOMY_LEVEL1_IMPROVED_PROMPT = """
You are a food service taxonomy expert. Categorize the following product descriptions into the most appropriate Level I category from the SALT Taxonomy.

AVAILABLE LEVEL I CATEGORIES:
{level1_options}

CRITICAL CATEGORIZATION RULES - READ CAREFULLY:

**CONTAINERS & PACKAGING:**
- BAKERY BOXES (cake boxes, pastry boxes, bread boxes) → "Disposables"
- CARRY-OUT CONTAINERS (food containers, takeout boxes) → "Disposables"
- ALUMINUM PANS & CONTAINERS → "Disposables"
- PLASTIC CONTAINERS & LIDS → "Disposables"
- FOAM CONTAINERS & TRAYS → "Disposables"

**LABELS & TAPE:**
- FOOD SAFETY LABELS (day labels, date labels) → "Disposables"
- PRICE LABELS, PRODUCT LABELS → "Disposables"
- REGISTER TAPE, CASH REGISTER TAPE → "Disposables"

**BAGS & WRAPS:**
- PLASTIC BAGS (food storage, produce bags) → "Disposables"
- ALUMINUM FOIL, PLASTIC WRAP → "Disposables"
- WAX PAPER, BUTCHER PAPER → "Disposables"

**CLEANING & CHEMICALS:**
- SOAPS, DETERGENTS, SANITIZERS → "Chemicals & Cleaning Agents"
- CLEANERS, POLISHES, DEGREASERS → "Chemicals & Cleaning Agents"

**EQUIPMENT & SUPPLIES:**
- KITCHEN EQUIPMENT, TOOLS → "Equipment & Supplies"
- SMALLWARES, UTENSILS → "Equipment & Supplies"
- CHINA, GLASSWARE, FLATWARE → "Equipment & Supplies"

**FOOD ITEMS:**
- DRY PASTA (shelf stable) → "Grocery, Dry"
- FROZEN/REFRIGERATED FOOD → "Appetizers, Entrees, & Potatoes Refrigerated & Frozen"
- MEAT, POULTRY, BEEF → Use specific meat categories
- PRODUCE, FRESH FRUITS/VEGETABLES → "Produce, Fresh"

**DISPOSABLES (Most Common):**
- PAPER PLATES, CUPS, BOWLS → "Disposables"
- NAPKINS, TOWELS, TISSUES → "Disposables"
- CUTLERY, FORKS, SPOONS, KNIVES → "Disposables"
- GLOVES, APRONS, HAIRNETS → "Disposables"
- FILTERS, COFFEE FILTERS → "Disposables"

EXAMPLES OF CORRECT CATEGORIZATION:
- "9 X 9 X 2.5 KRAFT WINDOW BOX" → "Disposables" (bakery box)
- "CAJUN DAY GLO LABEL 1000" → "Disposables" (food safety label)
- "ALUMINUM FOIL HEAVY DUTY" → "Disposables" (aluminum foil)
- "GOJO HAND SOAP" → "Chemicals & Cleaning Agents" (soap)
- "BAKING PAN LINER" → "Equipment & Supplies" (baking accessory)
- "DRY PASTA RIGATONI" → "Grocery, Dry" (dry pasta)

PRODUCT DESCRIPTIONS TO CATEGORIZE:
{descriptions}

Return a JSON list of Level I categories, one for each description:
["Category1", "Category2", "Category3", ...]
"""

# Taxonomy Level II Categorization (Improved)
TAXONOMY_LEVEL2_IMPROVED_PROMPT = """
You are a food service taxonomy expert. Categorize the following product into a Level II category based on its Level I category.

LEVEL I CATEGORY: {level1_category}

AVAILABLE LEVEL II CATEGORIES FOR "{level1_category}":
{available_level2}

CRITICAL CATEGORIZATION RULES - READ CAREFULLY:

**IF Level I is "Disposables":**
- BAKERY BOXES, CAKE BOXES → "Boxes"
- CARRY-OUT CONTAINERS, FOOD CONTAINERS → "Carry-Out Container"
- ALUMINUM PANS, ALUMINUM CONTAINERS → "Carry-Out Container"
- PLASTIC CONTAINERS, PLASTIC LIDS → "Carry-Out Container"
- FOAM CONTAINERS, FOAM TRAYS → "Carry-Out Container"
- PAPER PLATES, CUPS, BOWLS → "Bowls, Cups, Plates & Lids,Disposable"
- NAPKINS, TOWELS, TISSUES → "Napkins, Tablcovrs, Traycovrs & Placemats, Dispble"
- CUTLERY, FORKS, SPOONS, KNIVES → "Cutlery Kits & Diet Kits, Disposable"
- GLOVES, APRONS, HAIRNETS → "Aprons, Bibs, Gloves & Headware Disposable"
- FILTERS, COFFEE FILTERS → "Filters, Beverage & Fryer"
- BAGS, PLASTIC BAGS → "Bags"
- ALUMINUM FOIL, PLASTIC WRAP → "Film, Foil, & Paper Wraps"
- LABELS, FOOD SAFETY LABELS → "Register Tape, Labels, Trays"
- REGISTER TAPE, CASH REGISTER TAPE → "Register Tape, Labels, Trays"
- STRAWS, STIRRERS, TOOTHPICKS → "Straws, Stirrers, Steak Markers, Skewers & Other"

**IF Level I is "Chemicals & Cleaning Agents":**
- SOAPS, HAND SOAPS → "Detergnt, Santizr, Freshnrs, Toiletries,Manual Use"
- DETERGENTS, DISH SOAPS → "Detergent, Soap & Additives For Machines"
- CLEANERS, DEGREASERS → "Cleaners, Polishes & Waxes"
- SANITIZERS → "Detergnt, Santizr, Freshnrs, Toiletries,Manual Use"

**IF Level I is "Equipment & Supplies":**
- KITCHEN TOOLS, UTENSILS → "Smallwares, Kitchen & Bar"
- CHINA, DISHES, PLATES → "China"
- GLASSWARE, CUPS, MUGS → "Glassware"
- FLATWARE, FORKS, SPOONS → "Flatware, Stainless Steel, Metal"

**IF Level I is "Grocery, Dry":**
- DRY PASTA → "Pasta, Shelf Stable"
- BAKERY MIXES → "Bakery Mixes & Ingredients, Shelf Stable"

EXAMPLES OF CORRECT CATEGORIZATION:
- "9 X 9 X 2.5 KRAFT WINDOW BOX" (Disposables) → "Boxes"
- "CAJUN DAY GLO LABEL 1000" (Disposables) → "Register Tape, Labels, Trays"
- "ALUMINUM FOIL HEAVY DUTY" (Disposables) → "Film, Foil, & Paper Wraps"
- "GOJO HAND SOAP" (Chemicals & Cleaning Agents) → "Detergnt, Santizr, Freshnrs, Toiletries,Manual Use"

PRODUCT DESCRIPTION:
{description}

Return only the Level II category name from the available options above.
"""

# Taxonomy Level III Categorization (Improved)
TAXONOMY_LEVEL3_IMPROVED_PROMPT = """
You are a food service taxonomy expert. Categorize the following product into a Level III category based on its Level I and Level II categories.

LEVEL I CATEGORY: {level1_category}
LEVEL II CATEGORY: {level2_category}

AVAILABLE LEVEL III CATEGORIES FOR "{level1_category}" > "{level2_category}":
{available_level3}

CRITICAL CATEGORIZATION RULES - READ CAREFULLY:

**IF Level I is "Disposables":**
- **Boxes** → Choose "Paper, Bakery & Cake" for bakery boxes, "Paper, Pizza" for pizza boxes
- **Carry-Out Container** → Choose "Aluminum, Disposable" for aluminum pans, "Plastic, Disposable" for plastic containers, "Foam, Hinged" for foam containers, "Paper, Hinged" for paper containers
- **Bowls, Cups, Plates & Lids,Disposable** → Choose "Bowls, Paper" for paper bowls, "Cups, Paper" for paper cups, "Plates & Platters, Paper" for paper plates, "Lids, Disposable, For Cups & Tumblers" for lids
- **Filters, Beverage & Fryer** → Choose "Filters, Disposable, Coffee, Tea, Fryer" for coffee filters, fryer filters
- **Bags** → Choose "Food Storage" for food storage bags, "Plastic, Other" for other plastic bags
- **Film, Foil, & Paper Wraps** → Choose "Wraps & Sheets, Foil" for aluminum foil, "Film, Roll, Plastic" for plastic wrap, "Butcher Paper" for butcher paper
- **Register Tape, Labels, Trays** → Choose "Labels, Miscellaneous" for food labels, "Cash Register Tape & Ribbons" for register tape, "Trays, Foam" for foam trays
- **Straws, Stirrers, Steak Markers, Skewers & Other** → Choose "Toothpicks" for toothpicks, "Skewers" for skewers, "Stirrers, Plastic" for stirrers
- **Aprons, Bibs, Gloves & Headware Disposable** → Choose "Gloves, Disposable Vinyl" for vinyl gloves, "Gloves, Disposable, Nitrile" for nitrile gloves, "Aprons And Bibs, Disposable" for aprons, "Hairnets" for hairnets

**IF Level I is "Chemicals & Cleaning Agents":**
- **Detergnt, Santizr, Freshnrs, Toiletries,Manual Use** → Choose "Sanitizers" for sanitizers, "Soaps & Lotions, Hand & Body And Other Toiletries" for hand soaps
- **Detergent, Soap & Additives For Machines** → Choose "Detergents, Dish Machine" for dish machine detergents
- **Cleaners, Polishes & Waxes** → Choose "Oven & Grill" for oven cleaners, "Glass & Window" for glass cleaners, "Degreasers" for degreasers

**IF Level I is "Equipment & Supplies":**
- **Smallwares, Kitchen & Bar** → Choose "Kitchen Utensils" for kitchen tools, "Food Storage Containers & Lids (W/O Wheels)" for storage containers
- **China** → Choose "Plates & Saucers" for plates, "Bowls, Dishes, Ovenware" for bowls, "Mugs" for mugs

**IF Level I is "Grocery, Dry":**
- **Pasta, Shelf Stable** → Choose "Pasta Shells, Dry" for shell pasta, "Pasta, Other, Dry" for other shapes
- **Bakery Mixes & Ingredients, Shelf Stable** → Choose "Bakery Ingredient, Specialty" for specialty ingredients

EXAMPLES OF CORRECT CATEGORIZATION:
- "9 X 9 X 2.5 KRAFT WINDOW BOX" (Disposables > Boxes) → "Paper, Bakery & Cake"
- "CAJUN DAY GLO LABEL 1000" (Disposables > Register Tape, Labels, Trays) → "Labels, Miscellaneous"
- "ALUMINUM FOIL HEAVY DUTY" (Disposables > Film, Foil, & Paper Wraps) → "Wraps & Sheets, Foil"
- "GOJO HAND SOAP" (Chemicals & Cleaning Agents > Detergnt, Santizr, Freshnrs, Toiletries,Manual Use) → "Soaps & Lotions, Hand & Body And Other Toiletries"

PRODUCT DESCRIPTION:
{description}

Return only the Level III category name from the available options above.
"""



# Enhanced Product Description Templates
ENHANCED_PRODUCT_DESCRIPTION_TEMPLATES = {
    "day_spot": """
ENHANCED PRODUCT DESCRIPTION:
{product_description}

PRODUCT TYPE: Food Safety Label
USAGE: Day-of-the-week labels used in commercial kitchens and food service establishments to mark food preparation dates
PURPOSE: Food safety compliance - helps track when food was prepared to ensure proper rotation and prevent foodborne illness
CONTEXT: Commercial food service, restaurant supplies, food safety equipment
MATERIAL: Typically adhesive labels or tags
SIZE: Various sizes (3/4 inch, 1.5 inch, etc.)
APPLICATION: Applied to food containers, storage bins, or preparation areas
REGULATORY: Required for food safety compliance in commercial kitchens
""",
    
    "face_mask": """
ENHANCED PRODUCT DESCRIPTION:
{product_description}

PRODUCT TYPE: Personal Protective Equipment (PPE)
USAGE: Disposable face masks used in food service and commercial kitchen environments
PURPOSE: Food safety and hygiene - prevents contamination of food from respiratory droplets
CONTEXT: Commercial food service, restaurant safety, food industry PPE
MATERIAL: 3-ply disposable material
FEATURES: Disposable, hygienic, food-safe
APPLICATION: Worn by kitchen staff during food preparation
REGULATORY: Required for food safety in many jurisdictions
""",
    
    "crayon_art": """
ENHANCED PRODUCT DESCRIPTION:
{product_description}

PRODUCT TYPE: Educational/Art Supplies
USAGE: Crayons used in food service training, menu planning, and educational activities
PURPOSE: Training and education for food service staff, menu design, safety training
CONTEXT: Commercial food service, restaurant training, food safety education
MATERIAL: Non-toxic, food-safe art supplies
FEATURES: Educational, training, menu planning
APPLICATION: Staff training, menu design, safety education
REGULATORY: Used for compliance training and education
""",
    
    "food_container": """
ENHANCED PRODUCT DESCRIPTION:
{product_description}

PRODUCT TYPE: Food Storage Container
USAGE: Disposable or reusable containers for food storage and service
PURPOSE: Food storage, portion control, food service, food safety
CONTEXT: Commercial food service, restaurant supplies, food storage
MATERIAL: Food-safe plastic or disposable material
FEATURES: Food-safe, portion-controlled, disposable or reusable
APPLICATION: Food storage, portion control, food service
REGULATORY: Must meet food safety standards
""",
    
    "default": """
ENHANCED PRODUCT DESCRIPTION:
{product_description}

PRODUCT TYPE: Food Service Supply
USAGE: Commercial food service and restaurant supply item
PURPOSE: Food service operations, kitchen equipment, restaurant supplies
CONTEXT: Commercial food service, restaurant industry, food safety
MATERIAL: Food-safe materials appropriate for commercial use
FEATURES: Commercial grade, food-safe, restaurant quality
APPLICATION: Food service operations, commercial kitchen use
REGULATORY: Must meet food safety and commercial standards
"""
}