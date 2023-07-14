from concurrent import futures
from random import random
from time import sleep
import boto3
import threading
import os

# Example usage
bucket_name = 'bucket-name'
aws_region = 'us-west-2'
file_keys = []
destination_directory = 'download-directory'
batch_size = 10
num_threads = 5


def download_file_from_s3(file_keys):
    bucket = boto3.client('s3', region_name='aws-region')
    for key in file_keys:
        try:
            local_file_path = os.path.join(destination_directory, os.path.basename(key))
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            bucket.download_file(bucket_name, key, local_file_path)
            print(f"File downloaded successfully: {local_file_path}")
        except Exception as e:
            print(f"Error downloading file '{key}': {e}")
    return 2

def download_files_in_parallel(bucket_name, prefix):
    s3 = boto3.client('s3', region_name=aws_region)
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    all_files = []

    for page in pages:
        for obj in page['Contents']:
            all_files.append(obj['Key'])

    for obj in all_files:
        if obj.endswith('.extension'):
            file_keys.append(obj)

    # Split the list into smaller lists
    file_groups = [file_keys[i:i + 100] for i in range(0, len(all_files), 100)]

    # Create worker processes
    with futures.ProcessPoolExecutor(max_workers=num_threads) as executor:
        promises = {executor.submit(download_file_from_s3, n) for n in file_groups}

        for future in promises.as_completed(promises):
            print(future.result())

# If the above executor doesn't work use this.
    # with ThreadPoolExecutor(max_workers=100) as executor:
    #     executor.map(rename_file, filtered_file_keys_without_metadata, chunksize=100, timeout=1000)

if __name__ == "__main__":
    download_files_in_parallel(bucket_name, 'prefix/')
