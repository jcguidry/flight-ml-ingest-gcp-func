import json
import functions_framework
import logging
logging.basicConfig(level=logging.DEBUG)


from ingest import main

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def ingest(cloud_event):

    # Receive the Pub/Sub message from the CloudEvent
    event_message = cloud_event.data["message"]["data"].decode("utf-8")
    event_json = json.loads(event_message)

    #Obtain flight ident
    flight_ident = event_json.get('flight_ident')
    logging.info(f"Flight ident: {flight_ident}")

    # Print out the data from Pub/Sub, to prove that it worked
    main(identifier = flight_ident)
    
    
