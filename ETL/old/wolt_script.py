import json
import re
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets

flowFile = session.get()
if flowFile is not None:
    try:
        content_holder = [None]
        def read_callback(input_stream):
            content_holder[0] = IOUtils.toString(input_stream, StandardCharsets.UTF_8)
        session.read(flowFile, read_callback)
        product = json.loads(content_holder[0])

        title = product.get("name", "")  # Wolt uses "name" instead of "title"
        price_raw = product.get("price", "")
        image = product.get("url", "")

        if "cornichon" in title.lower():
            session.remove(flowFile)
        else:
            price_clean = re.sub(r"[^\d,\.]", "", price_raw).replace(",", ".")
            try:
                price = round(float(price_clean), 2)
            except (ValueError, TypeError):
                price = None

            normalized_title = None

            if "philadelphia" in title.lower():
                normalized_title = "Philadelphia"
            elif "feta" in title.lower():
                normalized_title = "Feta"
            elif "banane" in title.lower():
                normalized_title = "Banane"
            elif "wassermelone" in title.lower():
                normalized_title = "Wassermelone"
                # Already price per kg
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
                if price:
                    price = round(price / 1.5, 2)
            elif "eisbergsalat" in title.lower():
                normalized_title = "Eisbergsalat"
            else:
                normalized_title = title.strip()

            cleaned = {
                "title": normalized_title,
                "market": "Wolt",
                "price": price,
                "image": image
            }

            def write_callback(output_stream):
                output_stream.write(json.dumps(cleaned).encode('utf-8'))

            flowFile = session.write(flowFile, write_callback)
            session.transfer(flowFile, REL_SUCCESS)

        session.commit()

    except Exception as e:
        log.error("Error processing Wolt product: " + str(e))
        session.transfer(flowFile, REL_FAILURE)
        session.commit()
else:
    session.commit()