import tempfile
import requests
import boto3
from dotenv import load_dotenv

load_dotenv()

# Download the file
url = 'https://cdn-api.markitdigital.com/apiman-gateway/ASX/asx-research/1.0/file/2924-02937965-6A1260783&v=7bc42bd11d853ed5e8c28f2ffcd6a069ee5cd6b4'
response = requests.get(url)

if response.status_code == 200:
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(response.content)
        temp_path = tmp.name
        print(f"✅ Saved file to {temp_path}")

    # S3 upload
    bucket_name = 'aws-bucket-iz'
    s3_file_key = 'TodaysAnnsASX/test.pdf'

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
