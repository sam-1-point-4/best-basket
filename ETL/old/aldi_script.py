import json
import re
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from java.io import ByteArrayInputStream

flowFile = session.get()
if flowFile is not None:
    try:
        # Read FlowFile content using proper callback
        content_holder = [None]  # Use list to hold the content
        def read_callback(input_stream):
            content_holder[0] = IOUtils.toString(input_stream, StandardCharsets.UTF_8)
        
        session.read(flowFile, read_callback)
        text = content_holder[0]
        product = json.loads(text)

        # Get basic fields
        title = product.get("title", "")
        price_raw = product.get("price", "")
        amount = product.get("amount", "")
        image = product.get("image", "")

        # Skip Cornichones
        if "cornichon" in title.lower():
            session.remove(flowFile)
        else:
            # Clean price (remove *, €, tabs, etc.)
            price_clean = re.sub(r"[^\d,\.]", "", price_raw).replace(",", ".")
            try:
                price = round(float(price_clean), 2)
            except (ValueError, TypeError):
                price = None

            # Normalize product titles and apply custom logic
            normalized_title = None

            if "philadelphia" in title.lower():
                normalized_title = "Philadelphia"
            elif "feta" in title.lower():
                normalized_title = "Feta"
            elif "banane" in title.lower():
                normalized_title = "Banane"
                # Convert kg price to 1 piece (200g)
                if price:
                    price = round(price / 5.0, 2)
            elif "wassermelone" in title.lower():
                normalized_title = "Wassermelone"
                # Already kg price
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
                # Amount is 1.5 L → normalize to 1.0 L price
                if price:
                    price = round(price / 1.5, 2)
            elif "eisbergsalat" in title.lower():
                normalized_title = "Eisbergsalat"
            else:
                normalized_title = title.strip()

            # Create cleaned output
            cleaned = {
                "title": normalized_title,
                "market": "Aldi",
                "price": price,
                "image": image
            }

            # Write the result back to the FlowFile using proper callback
            def write_callback(output_stream):
                output_stream.write(json.dumps(cleaned).encode('utf-8'))
            
            flowFile = session.write(flowFile, write_callback)
            session.transfer(flowFile, REL_SUCCESS)
            
        # Commit the session
        session.commit()

    except Exception as e:
        log.error("Error processing Aldi product: " + str(e))
        session.transfer(flowFile, REL_FAILURE)
        session.commit()
else:
    # No FlowFile available
    session.commit()