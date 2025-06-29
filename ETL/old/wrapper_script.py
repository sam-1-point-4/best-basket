import json
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets

flowFile = session.get()
if flowFile is not None:
    try:
        # Read the merged product array
        content_holder = [None]
        def read_callback(input_stream):
            content_holder[0] = IOUtils.toString(input_stream, StandardCharsets.UTF_8)
        session.read(flowFile, read_callback)

        products = json.loads(content_holder[0])

        # Define static stores with logos
        stores = [
            {"name": "Aldi", "logo": "https://images.seeklogo.com/logo-png/32/1/aldi-logo-png_seeklogo-326055.png"},
            {"name": "Rewe", "logo": "https://images.seeklogo.com/logo-png/25/1/rewe-logo-png_seeklogo-252426.png"},
            {"name": "Wolt", "logo": "https://www.wolt.com/_next/static/media/logo_square.20562b88.svg"}  # You can replace with PNG if you prefer
        ]

        # Define your known categories
        categories = [
            "Obst & Gemüse",
            "Käse & Milchprodukte",
            "Konserven & Eingemachtes",
            "Teigwaren & Reis",
            "Getränke",
            "Süßwaren & Snacks",
            "Baby & Hygieneartikel"
        ]

        # Assemble the final object
        output = {
            "stores": stores,
            "categories": categories,
            "products": products
        }

        def write_callback(out_stream):
            out_stream.write(json.dumps(output, indent=2).encode("utf-8"))

        flowFile = session.write(flowFile, write_callback)
        session.transfer(flowFile, REL_SUCCESS)

        session.commit()

    except Exception as e:
        log.error("Error in final wrapper script: " + str(e))
        session.transfer(flowFile, REL_FAILURE)
        session.commit()
else:
    session.commit()
