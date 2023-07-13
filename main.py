import base64
import functions_framework

from src.ingest_flight_snapshots import main

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    # Print out the data from Pub/Sub, to prove that it worked
    main()
    
    print(base64.b64decode(cloud_event.data["message"]["data"]))
    
    
