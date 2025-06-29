import json
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets

flowFile = session.get()
if flowFile is not None:
    try:
        # Read merged lines from previous MergeContent
        content_holder = [None]
        def read_callback(input_stream):
            content_holder[0] = IOUtils.toString(input_stream, StandardCharsets.UTF_8)
        session.read(flowFile, read_callback)

        lines = content_holder[0].strip().splitlines()

        prices = {}
        image = None
        title = None

        for line in lines:
            try:
                item = json.loads(line)

                # Skip if item is not a single product dict
                if not isinstance(item, dict):
                    log.warn("Skipping non-object line: " + str(type(item)))
                    continue

                market = item.get("market")
                price = item.get("price")

                if market and price is not None:
                    prices[market] = price

                if not image:
                    image = item.get("image")
                if not title:
                    title = item.get("title")

            except Exception as e:
                log.warn("Skipping invalid line: " + str(e))

        # Category mapping based on normalized title
        category = None
        if title:
            lower = title.lower()
            if lower in ["banane", "wassermelone", "paprika", "eisbergsalat"]:
                category = "Obst & Gemüse"
            elif lower in ["philadelphia", "feta", "vollmilch", "eier"]:
                category = "Käse & Milchprodukte"
            elif lower in ["pesto"]:
                category = "Konserven & Eingemachtes"
            elif lower in ["mie nudeln"]:
                category = "Teigwaren & Reis"
            elif lower in ["red bull", "apfelschorle"]:
                category = "Getränke"
            elif lower in ["merci"]:
                category = "Süßwaren & Snacks"
            elif lower in ["pampers"]:
                category = "Baby & Hygieneartikel"
            else:
                category = "Sonstige"

        # Build merged product record
        merged = {
            "name": title,
            "category": category,
            "image": image,
            "prices": prices
        }

        def write_callback(output_stream):
            output_stream.write(json.dumps(merged).encode("utf-8"))

        flowFile = session.write(flowFile, write_callback)
        session.transfer(flowFile, REL_SUCCESS)
        session.commit()

    except Exception as e:
        log.error("Error merging product records: " + str(e))
        session.transfer(flowFile, REL_FAILURE)
        session.commit()
else:
    session.commit()