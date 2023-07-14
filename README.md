# flight-ml-ingest-gcp-func

# Project Overview

This repository contains the code for a machine learning data ingestion pipeline for flight data. The pipeline uses Google Cloud Platform (GCP) functions and interacts with the FlightAware API.

The data Is Ingested as 'snapshots' of the flight statuses provided by the API, with a timestamp of the current snapshot run and the previous snapshot run. Each new snapshot Is appended to a Google BigQuery table when this cloud function Is Invoked.

## Project Structure

The repository is structured as follows:

- `src/main.py`: This is the main execution point of the Python cloud function. It is responsible for importing and calling the `src/ingest.py` script.
- `src/ingest.py`: This Python script is used to get flight data from the FlightAware API, given a flight identifier and a time range. It then stores the data in a BigQuery table.
- `src/ingest.ipynb`: This Jupyter notebook is where the ingest.py process can be developed and debugged. It is identical to `src/ingest.py`, and can be converted to a Python script using `src/convert_to_py.ipynb`. All execution must occur in the `main()` function.
- `src/convert_to_py.ipynb`: This Jupyter notebook will convert the `src/ingest.ipynb` notebook to a Python script. You can run this after making changes to the `src/ingest.ipynb` notebook.
- `src/utils.py`: This Python script has utility functions for encoding JSON objects to strings and decoding strings back to JSON objects. These functions are useful for storing JSON objects in environment variables, rather than importing them from a JSON file. Additionally, this script contains a helper class for the FlightAware API.
- `.github/workflows/deploy.yaml`: This file defines a GitHub Actions workflow for deploying the cloud function.

Additional files in the repository include:

- `src/requirements.txt`: This file lists the Python dependencies required by the project.

## Setup and Usage

To set up and run the project, you typically need to do the following:

1. Create a virtual environment for with Python 3.10.0 for dev purposes. If using Miniconda, you can do this by running `conda create -n <env_name> python=3.10.0`.
2. Install the required Python dependencies listed in `src/requirements.txt`. You can do this by running `pip install -r src/requirements.txt`.
3. Create a `.env` file in the base directory. Two environment vairble keys will be stored here:
    - `FLIGHTAWARE_API_KEY` - API key for the FlightAware API.
    - `GCP_CREDENTIALS_JSON_ENCODED` - A GCP service account key, encoded as a string. 
        - This key is used to authenticate with GCP services such as the BigQuery client and pandas_gbq. To encode the key, you can use the `JSON_EncoderDecoder` class in `src/utils.py` script. 
        - The easiest way to do this is to use the `src/ingest.ipynb` notebook, in a new cell, paste your service key as a JSON object, encode the key using `JSON_EncoderDecoder(json_object).encode().get()`, and copy the encoded key to the `.env` file. Don't forget to delete the cell after you're done.
4. Debug the `src/ingest.ipynb` notebook to ensure that the data is being ingested correctly. (Optional)
    - If your goal is to test, debug, or modify this app, you can run the `src/ingest.ipynb` notebook. It is identical to `src/ingest.ipynb`. Should you make any changes, you can run `src/convert_to_py.ipynb` to copy the changes to `src/ingest.py`. This allows the project to be run as a Python script, as opposed to a Jupyter notebook, while still allowing for easy testing and debugging.
5. Run the main Python script with `python src/main.py`.



## Deployment

- Github Actions Authentication
  - The project is set up for deployment with GitHub Actions, as defined in the `.github/workflows/deploy.yaml` file.
  - To deploy the project, add a `GCP_SA_KEY` secret to your GitHub Actions environment. This allows the deployment workflow to authenticate with GCP services. When doing so,  copy the contents of your GCP service account key to the secret as s full JSON object.
- Cloud Function Authentication
  - You will need to add the following environment variables to your GCP Cloud Function:
    - `FLIGHTAWARE_API_KEY`
    - `GCP_CREDENTIALS_JSON_ENCODED`
  - These can be stored in the GCP Secret Manager, and then referenced in the Cloud Function deployment.
    - These secrets are referenced by name in the `deploy.yaml` workflow under the `secret_environment_variables` parameter, along with a `service-account-email` which the deployed app will belong to. Be sure to provide a service account email that has access to these secrets. This service account's perfmissions can be configured in GCP's IAM & Admin section.
