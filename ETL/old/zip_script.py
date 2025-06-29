import json
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets

flowFile = session.get()
if flowFile is not None:
    try:
        # Read content using proper callback pattern
        content_holder = [None]
        def read_callback(input_stream):
            content_holder[0] = IOUtils.toString(input_stream, StandardCharsets.UTF_8)
        
        session.read(flowFile, read_callback)
        text = content_holder[0]
        data = json.loads(text)

        # List of zip codes
        zip_codes = ["10115", "10117", "10119"]

        for zip_code in zip_codes:
            new_data = dict(data)  # Copy the original data
            new_data["zip_code"] = zip_code

            # Create new FlowFile
            new_flowfile = session.create(flowFile)
            
            # Write content using proper callback
            def write_callback(output_stream):
                output_stream.write(json.dumps(new_data).encode('utf-8'))
            
            new_flowfile = session.write(new_flowfile, write_callback)
            new_flowfile = session.putAttribute(new_flowfile, "zip_code", zip_code)
            session.transfer(new_flowfile, REL_SUCCESS)

        # Remove the original FlowFile
        session.remove(flowFile)
        
        # Commit the session
        session.commit()

    except Exception as e:
        log.error("Duplication error: " + str(e))
        session.transfer(flowFile, REL_FAILURE)
        session.commit()
else:
    # No FlowFile available
    session.commit()