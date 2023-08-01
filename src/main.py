import base64
import functions_framework

from ingest import main

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def ingest(cloud_event):

    print(f'cloud_event: {cloud_event}')

    print(f'cloud_event.data: {cloud_event.data}')

    flight_ident = base64.b64decode(cloud_event.data["message"]["data"])
    print(f"Flight ident: {flight_ident}")
    # Print out the data from Pub/Sub, to prove that it worked
    main(identifier = flight_ident)
    
    
