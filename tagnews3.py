import boto3
import json
import csv

def create_s3_bucket(bucket_name):
    # Create S3 client
    s3_client = boto3.client('s3')

    # Create the S3 bucket
    response = s3_client.create_bucket(Bucket=bucket_name)

    print(f"S3 bucket '{bucket_name}' has been created.")

def gather_and_store_tags(region_name='us-east-1'):
    # Create AWS client for resource tagging
    resource_tagging_client = boto3.client('resourcegroupstaggingapi', region_name=region_name)

    # Create S3 client
    s3_client = boto3.client('s3')

    # Create paginator for resource tag mapping list
    paginator = resource_tagging_client.get_paginator('get_resources')

    # Iterate over all pages
    for page in paginator.paginate():
        # Iterate over the resources
        for resource in page['ResourceTagMappingList']:
            resource_arn = resource['ResourceARN']
            tags = resource['Tags']

            # Convert tags to a list of dictionaries
            tags_list = []
            for key, value in tags.items():
                tags_list.append({'Key': key, 'Value': value})

            # Extract the resource name from the ARN
            resource_name = resource_arn.split('/')[-1]

            # Determine the bucket name based on the account and environment
            account_id = boto3.client('sts').get_caller_identity().get('Account')
            bucket_name = f"{account_id}-{region_name}-{resource['ResourceType']}"

            # Check if the bucket exists
            try:
                s3_client.head_bucket(Bucket=bucket_name)
            except:
                create_s3_bucket(bucket_name)

            # Convert tags to CSV format
            csv_data = ''
            if tags_list:
                csv_data += ','.join(tags_list[0].keys()) + '\n'
                for item in tags_list:
                    csv_data += ','.join(item.values()) + '\n'

            # Store the CSV file in the S3 bucket and folder
            s3_key = f'{resource_name}/tags.csv'
            s3_client.put_object(Body=csv_data, Bucket=bucket_name, Key=s3_key)

            print(f"Tags for resource {resource_arn} have been converted to CSV and stored in S3 bucket '{bucket_name}', folder '{resource_name}'.")

    print("All AWS tags have been gathered, converted to CSV, and stored successfully.")

# Call the function with default parameter values
gather_and_store_tags()
