import knime.scripting.io as knio
import pandas as pd
import json

# Input: Output of Node 1 (table with 'id', 'category', 'image', 'stores_json' columns)
df_products = knio.input_tables[0].to_pandas()

# --- Define fixed metadata for the final JSON structure ---
STORES_METADATA = [
    {"name": "Aldi", "logo": "https://images.seeklogo.com/logo-png/32/1/aldi-logo-png_seeklogo-326055.png"},
    {"name": "Rewe", "logo": "https://images.seeklogo.com/logo-png/25/1/rewe-logo-png_seeklogo-252426.png"},
    {"name": "Wolt", "logo": "https://images.seeklogo.com/logo-png/26/1/edeka-logo-png_seeklogo-269652.png"}
]

CATEGORIES_LIST = [
    "Milk Products", "Fruits & Vegetables", "Cans & Preserved Foods",
    "Pasta & Rice", "Drinks", "Sweets & Snacks", "Baby & Hygiene Products"
]

# Reconstruct the 'products' list by parsing the 'stores_json' column
products_data = []
for idx, row in df_products.iterrows():
    product_entry = {
        "id": row["id"],
        "category": row["category"],
        "image": row["image"],
        "stores": json.loads(row["stores_json"]) # Parse the JSON string back into a dictionary
    }
    products_data.append(product_entry)

# Construct the final overarching JSON object
final_json_output = {
    "stores": STORES_METADATA,
    "categories": CATEGORIES_LIST,
    "products": products_data
}

# Output the complete JSON as a single string column.
# You can connect this output to a "String to File" node in KNIME to save it as 'data.json'.
output_df = pd.DataFrame([json.dumps(final_json_output, indent=2)], columns=['json_data'])
knio.output_tables[0] = knio.Table.from_pandas(output_df)