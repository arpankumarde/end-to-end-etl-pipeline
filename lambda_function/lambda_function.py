import os
import logging
import requests
import io
import boto3
from botocore.exceptions import BotoCoreError, ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

s3 = boto3.client("s3")


def lambda_handler(event, context):
    try:
        return download_and_upload_file()
    except Exception as e:
        logger.error("An unexpected error occurred: %s", e)
        return {"error": "An unexpected error occurred"}


def download_and_upload_file():
    try:
        file_url, bucket_name, s3_path = get_environment_variables()
        logger.info("Starting file download from %s", file_url)

        with requests.get(file_url, stream=True) as r:
            r.raise_for_status()
            mpu = initiate_multipart_upload(bucket_name, s3_path)
            parts = upload_file_parts(r, bucket_name, s3_path, mpu)
            complete_multipart_upload(bucket_name, s3_path, mpu, parts)

        logger.info("File uploaded to S3 bucket '%s' at '%s'", bucket_name, s3_path)
        return {"message": "File uploaded to S3"}

    except requests.exceptions.RequestException as e:
        logger.error("Error downloading the file: %s", e)
        return {"error": "Failed to download the file"}
    except (BotoCoreError, ClientError) as e:
        return handle_s3_error(e)


def get_environment_variables():
    file_url = os.getenv(
        "FILE_URL",
        "https://data.wa.gov/api/views/f6w7-q2d2/rows.csv?accessType=DOWNLOAD",
    )
    bucket_name = os.getenv("BUCKET_NAME", "etl-s3-2")
    file_name = os.getenv("FILE_NAME", "ev_dataset.csv")
    s3_path = os.getenv("S3_PATH", f"source_folder/{file_name}")
    return file_url, bucket_name, s3_path


def initiate_multipart_upload(bucket_name, s3_path):
    return s3.create_multipart_upload(Bucket=bucket_name, Key=s3_path)


def upload_file_parts(r, bucket_name, s3_path, mpu):
    parts = []
    part_number = 1
    with io.BytesIO() as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
            if f.tell() >= 5 * 1024 * 1024:
                upload_part(f, bucket_name, s3_path, mpu, part_number, parts)
                part_number += 1
                f.seek(0)
                f.truncate()
        if f.tell() > 0:
            upload_part(f, bucket_name, s3_path, mpu, part_number, parts)
    return parts


def upload_part(f, bucket_name, s3_path, mpu, part_number, parts):
    f.seek(0)
    part = s3.upload_part(
        Body=f,
        Bucket=bucket_name,
        Key=s3_path,
        UploadId=mpu["UploadId"],
        PartNumber=part_number,
    )
    parts.append({"PartNumber": part_number, "ETag": part["ETag"]})


def complete_multipart_upload(bucket_name, s3_path, mpu, parts):
    s3.complete_multipart_upload(
        Bucket=bucket_name,
        Key=s3_path,
        UploadId=mpu["UploadId"],
        MultipartUpload={"Parts": parts},
    )


def handle_s3_error(e):
    if isinstance(e, ClientError):
        error_code = e.response["Error"]["Code"]
        if error_code == "NoSuchBucket":
            logger.error("S3 bucket does not exist: %s", e)
            return {"error": "S3 bucket does not exist"}
        elif error_code == "AccessDenied":
            logger.error("Access denied to S3 bucket: %s", e)
            return {"error": "Access denied to S3 bucket"}
        else:
            logger.error("S3 ClientError: %s", e)
            return {"error": f"S3 ClientError: {error_code}"}
    else:
        logger.error("BotoCoreError when uploading to S3: %s", e)
        return {"error": "Failed to upload the file to S3 due to a BotoCoreError"}
