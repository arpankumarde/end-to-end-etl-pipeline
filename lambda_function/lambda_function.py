import os
import logging
import requests
import boto3
from botocore.exceptions import BotoCoreError, ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def lambda_handler(event, context):
    try:
        file_url = os.getenv(
            "FILE_URL",
            "https://data.wa.gov/api/views/f6w7-q2d2/rows.csv?accessType=DOWNLOAD",
        )
        bucket_name = os.getenv("BUCKET_NAME", "etl-s3-2")
        file_name = os.getenv("FILE_NAME", "ev_dataset.csv")
        s3_path = os.getenv("S3_PATH", f"source_folder/{file_name}")

        # Log the start of the process
        logger.info("Starting file download from %s", file_url)

        # Download the file
        req = requests.get(file_url)
        req.raise_for_status()  # Raise an error for HTTP issues

        # Log successful download
        logger.info("File downloaded successfully")

        # Upload to S3
        s3 = boto3.resource("s3")
        s3.Bucket(bucket_name).put_object(Key=s3_path, Body=req.content)

        # Log successful upload
        logger.info("File uploaded to S3 bucket '%s' at '%s'", bucket_name, s3_path)

        return {"message": "File uploaded to S3"}

    except requests.exceptions.RequestException as e:
        logger.error("Error downloading the file: %s", e)
        return {"error": "Failed to download the file"}

    except (BotoCoreError, ClientError) as e:
        logger.error("Error uploading the file to S3: %s", e)
        return {"error": "Failed to upload the file to S3"}

    except Exception as e:
        logger.error("An unexpected error occurred: %s", e)
        return {"error": "An unexpected error occurred"}
