import boto3
from concurrent.futures import ThreadPoolExecutor

# create a boto client for s3 with region name as us-west-2
s3 = boto3.client('s3', region_name='aws-region')

bucket_name = 'bucket-name'
prefix = 'prefix/'

# write me a function to rename a file in s3 bucket
def rename_file(old_file_name):
    # rename the file in s3 bucket as per the logic.
    print("Old File Name: " + old_file_name)

    parts = old_file_name.split('.')
    new_file_name = '.'.join(parts[:-1]) + '.extension'
    print("New File Name: " + new_file_name)

    copysource = bucket_name + '/' + old_file_name
    print("Copy Source: " + copysource)

    # use copy_object with bucket_name, prefix and new_file_name
    s3.copy_object(Bucket=bucket_name, CopySource=copysource, Key=new_file_name)

    # # delete old file using delete_object with bucket_name and old_file_name
    s3.delete_object(Bucket=bucket_name, Key=old_file_name)



# function to get a list of all file names in a s3 bucket
def get_file_names(bucket_name, prefix):
    # use list_objects_v2 method of s3 client to get the list of all file names in a s3 bucket and prefix as metadata/
    # return the list of file names
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    file_names = []

    for page in pages:
        for obj in page['Contents']:
            file_names.append(obj['Key'])
    return file_names


# function which takes a list of file names and renames them in parallel using multithreading in a batch of 100
def rename_files_in_parallel(bucket_name, prefix):
    # call get_file_names function to get the list of file names
    file_names = get_file_names(bucket_name, prefix)

    #print total number of files
    print("Total number of files: " + str(len(file_names)))

    filter_json_file_keys = []
    for obj in file_names:
        if obj.endswith('.json'):
            filter_json_file_keys.append(obj)

    #print total number of json files
    print("Total number of json files: " + str(len(filter_json_file_keys)))

    filtered_file_keys_without_metadata = []
    for file_name in filter_json_file_keys:
        if not file_name.endswith('.metadata.json'):
            filtered_file_keys_without_metadata.append(file_name)

    # print total number of json files without metadata
    print("Total number of json files without metadata: " + str(len(filtered_file_keys_without_metadata)))

    with ThreadPoolExecutor(max_workers=100) as executor:
        executor.map(rename_file, filtered_file_keys_without_metadata, chunksize=100, timeout=1000)


if __name__ == "__main__":
    rename_files_in_parallel(bucket_name, prefix)
