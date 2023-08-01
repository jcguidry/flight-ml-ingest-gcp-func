import base64
import functions_framework

from ingest import main

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def ingest(cloud_event):

    flight_ident = base64.b64decode(cloud_event.data["message"]["data"])

    # Print out the data from Pub/Sub, to prove that it worked
    main(identifier = flight_ident)
    
    
