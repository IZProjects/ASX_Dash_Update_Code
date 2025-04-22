import tempfile
import requests
import boto3
from dotenv import load_dotenv
import os

load_dotenv()
s3_bucket = os.getenv("AWS_S3_BUCKET_NAME")
s3_folder = os.getenv("AWS_S3_FOLDER")

# Download the file
url = 'https://cdn-api.markitdigital.com/apiman-gateway/ASX/asx-research/1.0/file/2924-02937965-6A1260783&v=7bc42bd11d853ed5e8c28f2ffcd6a069ee5cd6b4'

def upload_file(url, filename):
    # include .pdf in file name. should be a string. eg "Filename.pdf"

    response = requests.get(url)

    if response.status_code == 200:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(response.content)
            temp_path = tmp.name
            print(f"✅ Saved file to {temp_path}")

        # S3 upload
        bucket_name = s3_bucket
        s3_file_key = f'{s3_folder}/{filename}'

        s3 = boto3.client('s3')

        try:
            s3.upload_file(
                temp_path,
                bucket_name,
                s3_file_key,
                ExtraArgs={
                    'ContentType': 'application/pdf',
                    'ContentDisposition': 'inline'
                }
            )
            print(f"✅ Uploaded to s3://{bucket_name}/{s3_file_key}")
        except Exception as e:
            print(f"❌ Upload error: {e}")
    else:
        print(f"❌ Failed to download file: Status code {response.status_code}")


def delete_folder_content(folder):
    # folder input to include /, eg "ASX_Announcements/". Should be a string
    s3 = boto3.client('s3')

    # List all objects with the given prefix
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=s3_bucket, Prefix=folder)

    objects_to_delete = []

    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                objects_to_delete.append({'Key': obj['Key']})

            # Batch delete in chunks of 1000 (S3 limit)
            while objects_to_delete:
                chunk = objects_to_delete[:1000]
                s3.delete_objects(Bucket=s3_bucket, Delete={'Objects': chunk})
                objects_to_delete = objects_to_delete[1000:]

    print(f"Deleted all objects under prefix '{folder}' in bucket '{folder}'")


#upload_file(url, "test.py")
#delete_folder_content(s3_folder)