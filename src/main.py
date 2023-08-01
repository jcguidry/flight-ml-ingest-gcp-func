import json
import base64
import logging
import functions_framework

from ingest import main

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def ingest(cloud_event):

    # Receive the Pub/Sub message from the CloudEvent
    pubsub_message = cloud_event.data["message"]["data"]
    message_str = base64.b64decode(pubsub_message).decode('utf-8')
    message_json = json.loads(message_str)

    #Obtain flight ident
    flight_ident = message_json.get('flight_ident')
    logging.info(f"Flight ident: {flight_ident}", stacklevel=logging.DEBUG)

    # Print out the data from Pub/Sub, to prove that it worked
    main(identifier = flight_ident)
    
    
