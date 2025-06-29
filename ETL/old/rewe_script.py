import json
import re
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
import codecs

flowFile = session.get()
if flowFile is not None:
    try:
        # Read FlowFile content
        content_holder = [None]
        def read_callback(input_stream):
            reader = codecs.getreader("utf-8")(input_stream)
            content_holder[0] = reader.read()
        session.read(flowFile, read_callback)

        # Ensure we have content
        if not content_holder[0]:
            log.error("Empty FlowFile content")
            session.transfer(flowFile, REL_FAILURE)
            session.commit()
        else:
            product = json.loads(content_holder[0])

            # Debug logging to see what types we're getting
            log.info("Product data types - title: {}, price: {}, image: {}".format(
                type(product.get("title")).__name__,
                type(product.get("price")).__name__,
                type(product.get("image")).__name__
            ))

            # Safely get values - title and image as strings, price_raw can be any type
            title_raw = product.get("title", "")
            title = str(title_raw) if title_raw is not None else ""
            
            price_raw = product.get("price", "")  # Keep original type
            
            image_raw = product.get("image", "")
            image = str(image_raw) if image_raw is not None else ""

            if "cornichon" in title.lower() or "gurken" in title.lower():
                session.remove(flowFile)
            else:
                # Handle price processing - convert to string only for regex processing
                price = None
                if price_raw is not None and price_raw != "":
                    # Convert to string for regex processing
                    price_str = str(price_raw)
                    price_clean = re.sub(r"[^\d,\.]", "", price_str).replace(",", ".")
                    if price_clean:  # Only process if we have digits
                        try:
                            price = round(float(price_clean), 2)  # This will be a float/number
                        except (ValueError, TypeError):
                            price = None

                normalized_title = None

                if "philadelphia" in title.lower():
                    normalized_title = "Philadelphia"
                elif "feta" in title.lower() or "hirtenkäse" in title.lower():
                    normalized_title = "Feta"
                elif "banane" in title.lower():
                    normalized_title = "Banane"
                elif "wassermelone" in title.lower():
                    normalized_title = "Wassermelone"
                    if price is not None:
                        price = round(price / 3.5, 2)  # Average 3.5kg per melon
                elif "eier" in title.lower():
                    normalized_title = "Eier"
                elif "red bull" in title.lower():
                    normalized_title = "Red Bull"
                elif "vollmilch" in title.lower():
                    normalized_title = "Vollmilch"
                elif "pesto" in title.lower():
                    normalized_title = "Pesto"
                elif "merci" in title.lower():
                    normalized_title = "Merci"
                elif "mie" in title.lower():
                    normalized_title = "Mie Nudeln"
                elif "pampers" in title.lower():
                    normalized_title = "Pampers"
                elif "paprika" in title.lower():
                    normalized_title = "Paprika"
                elif "apfelschorle" in title.lower():
                    normalized_title = "Apfelschorle"
                    if price is not None:
                        price = round(price / 1.5, 2)
                elif "eisbergsalat" in title.lower():
                    normalized_title = "Eisbergsalat"
                else:
                    normalized_title = title.strip()

                cleaned = {
                    "title": normalized_title,
                    "market": "Rewe",
                    "price": price,
                    "image": image
                }

                def write_callback(output_stream):
                    # Make sure all values are JSON serializable
                    safe_cleaned = {
                        "title": str(cleaned["title"]) if cleaned["title"] is not None else "",
                        "market": str(cleaned["market"]),
                        "price": cleaned["price"],  # Keep as number or None
                        "image": str(cleaned["image"]) if cleaned["image"] is not None else ""
                    }
                    json_str = json.dumps(safe_cleaned, ensure_ascii=False)
                    output_stream.write(json_str.encode('utf-8'))

                flowFile = session.write(flowFile, write_callback)
                session.transfer(flowFile, REL_SUCCESS)

        session.commit()

    except Exception as e:
        # Use simple string conversion to avoid any type issues
        error_msg = "Error processing Rewe product: " + str(e)
        log.error(error_msg)
        session.transfer(flowFile, REL_FAILURE)
        session.commit()
else:
    session.commit()