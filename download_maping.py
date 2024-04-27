import boto3
import os

# Create an S3 client
s3 = boto3.client('s3')

# Specify the bucket name
bucket_name = 'primula-genetic-consulting-datalake'

# Specify the prefix
prefix = 'Resource/Build_Map/'

# Create a directory to store downloaded files
os.makedirs('Resource/Build_Map/', exist_ok=True)
os.makedirs('Resource/Build_Map/all', exist_ok=True)

# List objects within the specified prefix
paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

# Download each object
for page in pages:
    if "Contents" in page:
        for obj in page['Contents']:
            file_name = obj['Key']
            if '.parquet' in file_name:
                print(file_name)
            # local_path = os.path.join(download_dir, file_name.split('/')[-1])
            # print(f"Downloading {file_name} to {local_path}...")
                s3.download_file(bucket_name, file_name, file_name)
            # print(f"Downloaded {file_name}")

print("All files downloaded.")
