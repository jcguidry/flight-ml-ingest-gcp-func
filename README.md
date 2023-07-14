# flight-ml-ingest-gcp-func

# Project Overview

This repository contains the code for a machine learning data ingestion pipeline for flight data. The pipeline uses Google Cloud Platform (GCP) functions and interacts with the FlightAware API.

## Project Structure

The repository is structured as follows:

- `src/main.py`: This is the main Python script of the project. Its specific functionality is not documented.
- `src/ingest.py`: This Python script is used to get flight data from the FlightAware API, given an identifier and a time range.
- `src/utils.py`: This Python script has utility functions for encoding JSON objects to strings and decoding strings back to JSON objects. These functions are useful for storing JSON objects in environment variables.
- `src/ingest.ipynb`: This Jupyter notebook appears to be related to the data ingestion process. It does not contain explanatory markdown cells.
- `src/convert_to_py.ipynb`: This Jupyter notebook appears to be used for converting some data to a Python format. It does not contain explanatory markdown cells.
- `.github/workflows/deploy.yaml`: This file defines a GitHub Actions workflow for deploying the project.

Additional files in the repository include:

- `.env`: This file typically contains environment-specific configurations, such as API keys and database connection strings.
- `.gitattributes`: This file defines attributes for pathnames.
- `src/requirements.txt`: This file lists the Python dependencies required by the project.

## Setup and Usage

To set up and run the project, you typically need to do the following:

1. Install the required Python dependencies listed in `src/requirements.txt`. You can do this by running `pip install -r src/requirements.txt`.
2. Set up your environment-specific configurations in the `.env` file. This might include API keys and database connection strings.
3. Run the main Python script with `python src/main.py`.

Please note that the above instructions are generally common for Python projects, and specific steps for this project might vary.

## Deployment

The project is set up for deployment with GitHub Actions, as defined in the `.github/workflows/deploy.yaml` file. To deploy the project, you might need to set up your GitHub Actions environment and configure your deployment settings.

