from concurrent import futures
from random import random
from time import sleep
import boto3
import threading
import os

# Example usage
destination_bucket_name = "destination-bucket-name"

local_upload_directory = 'local-upload-directory/'
file_keys = []
num_threads = 5


def upload_files_to_s3(file_keys):
    bucket = boto3.client('s3', region_name='us-west-2')
    for local_file_path in file_keys:
        try:
            s3_folder = 'foldername'
            s3_key = os.path.join(s3_folder, os.path.relpath(local_file_path, local_upload_directory))
            bucket.upload_file(local_file_path, destination_bucket_name, s3_key)
            print(f"Uploaded: {local_file_path} -> s3://{destination_bucket_name}/{s3_key}")
        except Exception as e:
            print(f"Error downloading file '{local_file_path}': {e}")
    return 2

def upload_files_in_parallel():
    all_files = []
    for root, dirs, files in os.walk(local_upload_directory):
        for file in files:
            local_file_path = os.path.join(root, file)
            all_files.append(local_file_path)

    # Split the list into smaller lists
    file_groups = [file_keys[i:i + 100] for i in range(0, len(all_files), 100)]


    # Create worker processes
    with futures.ProcessPoolExecutor(max_workers=num_threads) as executor:
        promises = {executor.submit(upload_files_to_s3, n) for n in file_groups}

        for future in promises.as_completed(promises):
            print(future.result())

if __name__ == "__main__":
    upload_files_in_parallel()