import os
import sys
import json
import logging
import requests
import pytz
import shutil
from datetime import datetime
from snowflake.snowpark import Session
from dotenv import load_dotenv

# 1. Load environment variables from .env file
load_dotenv()

# 2. Logging Configuration
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# 3. Time and File Setup
ist_timezone = pytz.timezone('Asia/Kolkata')
current_time_ist = datetime.now(ist_timezone)

timestamp = current_time_ist.strftime('%Y_%m_%d_%H_%M_%S')
today_string = current_time_ist.strftime('%Y_%m_%d')
file_name = f'air_quality_data_{timestamp}.json'

# Temporary folder
TEMP_DIR = "tmp_aqi_data"
os.makedirs(TEMP_DIR, exist_ok=True)
file_path = os.path.join(TEMP_DIR, file_name)


# 4. Snowflake Session Setup
def snowpark_basic_auth() -> Session:
    """Create Snowflake Snowpark session using credentials from .env"""
    try:
        connection_parameters = {
            "ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT"),
            "REGION": os.getenv("SNOWFLAKE_REGION"),
            "USER": os.getenv("SNOWFLAKE_USER"),
            "PASSWORD": os.getenv("SNOWFLAKE_PASSWORD"),
            "ROLE": os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
            "DATABASE": os.getenv("SNOWFLAKE_DATABASE", "AQI_PROJECT_DB"),
            "SCHEMA": os.getenv("SNOWFLAKE_SCHEMA", "STAGE_SCH"),
            "WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE", "LOAD_WH")
        }

        # Check all required environment variables
        missing = [k for k, v in connection_parameters.items() if v in (None, "")]
        if missing:
            raise ValueError(f"Missing environment variables: {', '.join(missing)}")

        return Session.builder.configs(connection_parameters).create()

    except Exception as e:
        logging.error(f"Failed to create Snowflake session: {e}")
        sys.exit(1)

# 5. API Data Extraction and Upload
def get_air_quality_data(limit: int):
    """Fetch data from API, save temporarily, upload to Snowflake stage, then clean up."""
    api_key = os.getenv("API_KEY")
    if not api_key:
        logging.error("API_KEY not found in environment variables.")
        sys.exit(1)

    api_url = "https://api.data.gov.in/resource/3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69"

    params = {
        "api-key": api_key,
        "format": "json",
        "limit": limit
    }

    headers = {"accept": "application/json"}

    try:
        logging.info("Sending request to API...")
        response = requests.get(api_url, params=params, headers=headers)

        if response.status_code != 200:
            logging.error(f"Error: {response.status_code} - {response.text}")
            sys.exit(1)

        json_data = response.json()

        # Save to temp file
        with open(file_path, "w") as json_file:
            json.dump(json_data, json_file, indent=2)
        logging.info(f"JSON data saved temporarily at {file_path}")

        # Upload to Snowflake stage
        stg_location = f"@aqi_project_db.stage_sch.raw_stg/india/{today_string}/"
        sf_session = snowpark_basic_auth()

        logging.info(f"Uploading {file_name} to stage: {stg_location}")
        put_result = sf_session.file.put(file_path, stg_location, auto_compress=True, overwrite=True)
        logging.info(f"PUT result: {put_result}")

        # Verify file upload
        list_query = f"LIST {stg_location}{file_name}.gz"
        result_lst = sf_session.sql(list_query).collect()
        logging.info(f"File successfully listed in stage: {result_lst}")

        logging.info("ETL job completed successfully.")
        return json_data

    except Exception as e:
        logging.error(f"An error occurred during data ingestion: {e}")
        sys.exit(1)

    finally:
        # Cleanup temp file and directory
        try:
            if os.path.exists(TEMP_DIR):
                shutil.rmtree(TEMP_DIR)
                logging.info(f"Temporary folder '{TEMP_DIR}' deleted after upload.")
        except Exception as cleanup_error:
            logging.warning(f"Failed to clean up temporary files: {cleanup_error}")


if __name__ == "__main__":
    limit_value = int(os.getenv("API_LIMIT", 4000))
    get_air_quality_data(limit_value)
