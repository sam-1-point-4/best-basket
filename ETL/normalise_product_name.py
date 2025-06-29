import knime.scripting.io as knio
import pandas as pd
import re

df = knio.input_tables[0].to_pandas()

# Define the 15 canonical normalized product names.
# These are the *target* names we want all raw product names to map to.
CANONICAL_PRODUCT_NAMES = [
    "philadelphia frischkäse", "merci helle vielfalt", "wassermelone", "banane",
    "essiggurken", "pesto", "apfel schorle", "hirtenkäse", "paprika",
    "mie nudeln", "milch", "eier", "red bull energy drink",
    "eisbergsalat", "pampers windeln"
]

def normalize_product_name(raw_name):
    if pd.isna(raw_name): return None
    name = str(raw_name).lower()

    # --- Phase 1: Aggressive Pre-processing ---
    # Remove numbers and units (e.g., "250g", "0,25l", "5 stück", "2-5 kg")
    name = re.sub(r'\b\d+[,.]?\d*\s*(?:g|ml|l|kg|stk|stueck|packung|schale|flasche|box|tuepfchen|t\u00f6pfchen|liter|gramm|kilogramm|milliliter|kg|st)\b', '', name)
    name = re.sub(r'\b\d+-\d+\s*kg\b', '', name) # Specific for ranges like "2-5 kg"

    # Remove common descriptive words and redundant terms
    words_to_remove_general = [
        r'\b(?:ca|rot|gelb|grün|gr\u00fcn|natürlich|premium|beste wahl|klassik|original|bio|frisch|haltbar|pflanzlich|vegan|glutenfrei|laktosefrei|frei|aus bodenhaltung|kernarm|mini|gro\u00dfe|grosse|grösse|light|ohne zucker|zuckerfrei|hell|dunkel|fein|extra|classic|selection|pur|mild|mild-cremig|cremig|doppelrahm|naturell|unbehandelt|gekocht|geräuchert|geschnitten|ganz|portionspackung|pack|rewe|aldi|wolt|markt|market|online|bestellen|html|deu|de|finest|grosse|vielfalt|deluxe|best|choice|fresh|sweet)\b'
    ]
    for pattern in words_to_remove_general:
        name = re.sub(pattern, '', name)

    # Remove common brand names aggressively for normalization purposes
    brand_names = [
        'alnatura', 'bamboo garden', 'barilla', 'kühne', 'gut bio', "king's crown",
        'landfreude', 'milsani', 'rio d\'oro', 'storck', 'diamond', 'ja!', 'hellas',
        'landkost', 'rewe beste wahl', 'gerolsteiner', 'pampers', 'red bull',
        'philadelphia', 'merci', 'dr oetker', 'edeka', 'gut & günstig', 'k-classic',
        'maggi', 'thomy'
    ]
    for brand in brand_names:
        name = name.replace(brand, '')

    # Standardize core product types to a single, generic term
    name = name.replace('frischkäsezubereitung', 'frischkäse')
    name = name.replace('doppelrahmkäse', 'frischkäse')
    name = name.replace('natur doppelrahmstufe', 'frischkäse')
    name = name.replace('mie-nudeln', 'mie nudeln')
    name = name.replace('cornichons', 'essiggurken')
    name = name.replace('pesto alla genovese', 'pesto')
    name = name.replace('pesto,glas', 'pesto')
    name = name.replace('apfelschorle', 'apfel schorle')
    name = name.replace('apelschorle', 'apfel schorle')
    name = name.replace('energy-drink', 'energy drink')
    name = name.replace('vollmilch', 'milch')
    name = name.replace('eier', 'ei')
    name = name.replace('wassermelone', 'melone')
    name = name.replace('eisbergsalat', 'salat')
    name = name.replace('schokolade', 'schoko') 
    name = re.sub('.*essiggurken.*', 'salzdillgurken', name)
    name = re.sub('.*banane.*', 'bananen', name)
    name = re.sub(r'.*ei .*', 'ei', name) 
    name = re.sub(r'.*feta.*', "feta", name)
    name = re.sub(r'(melone).*', r'\1', name)
    name = re.sub(r'(salat).*', r'\1', name)
    name = name.replace("hirtenkäse", "feta")

    # Remove everything before and after 'milch' and 'merci' when found
    name = re.sub(r'.*(milch).*', r'\1', name)
    name = name.replace("packung", "merci helle vielfalt")


    name = name.replace('schoko', 'merci')
    name = re.sub(r'(merci).*', r'\1', name)
    name = re.sub('.*protection.*', 'pampers windeln', name)
    name = re.sub(r'.*frischkäse.*', 'frischkäse', name)
    
    # Final cleanup of special characters and excessive spaces
    name = re.sub(r'[^\w\s]', '', name) # Remove any remaining special characters like commas, periods, etc.
    name = re.sub(r'\s+', ' ', name).strip() # Replace multiple spaces with single, strip whitespace

    # --- Phase 2: Pattern Matching / Mapping to Canonical Names ---
    best_match = None
    max_score = 0
    
    for canonical_name in CANONICAL_PRODUCT_NAMES:
        canonical_words = set(canonical_name.split())
        current_words = set(name.split())
        
        # Calculate score based on common words (intersection)
        score = len(canonical_words.intersection(current_words))
        
        # Boost for direct phrase matches within the cleaned name
        for i in range(len(canonical_name.split()) - 1):
            phrase = " ".join(canonical_name.split()[i:i+2])
            if phrase in name:
                score += 0.5 # Small boost for partial phrase match

        # Consider if the canonical name is a single word and it's a direct hit
        if len(canonical_words) == 1 and list(canonical_words)[0] == name:
            score += 1 # Strong boost for exact single word match after cleaning

        if score > max_score:
            max_score = score
            best_match = canonical_name
        elif score == max_score and best_match is not None:
            # Tie-breaker: prefer the canonical name that is a closer length
            if abs(len(canonical_name) - len(name)) < abs(len(best_match) - len(name)):
                best_match = canonical_name
            # Or if names are similar length, prefer lexicographically (consistent tie-break)
            elif abs(len(canonical_name) - len(name)) == abs(len(best_match) - len(name)):
                if canonical_name < best_match:
                    best_match = canonical_name

    # If a good match is found, return it. Otherwise, return the cleaned name as a fallback.
    if best_match:
        # Special handling for 'ei' to 'eier' if 'eier' is preferred in canonical list
        if best_match == 'ei' and 'eier' in CANONICAL_PRODUCT_NAMES:
            return 'eier' # Adjust for plural consistency
        return best_match
    
    return name if name else None # Fallback if no reasonable match

def normalize_price(price_str):
    if pd.isna(price_str): return None
    price_str = str(price_str).replace(',', '.').replace('€', '').replace('*', '').strip()
    try:
        return float(price_str)
    except ValueError:
        return None

# Use 'product' as the input column for raw name and 'price' for raw price, as per your workflow.
df['normalized_product_name'] = df['product'].apply(normalize_product_name)
df['normalized_price'] = df['price'].apply(normalize_price)

knio.output_tables[0] = knio.Table.from_pandas(df)