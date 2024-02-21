import os
import io
from datetime import datetime, timedelta

import requests
import pandas as pd
import boto3
from dotenv import load_dotenv

from bgg_webscrape import random_sleep

#AWS parameters
load_dotenv()
access_key = os.getenv("AWS_ACCESS_KEY")
secret_key = os.getenv("AWS_SECRET_KEY")
aws_region = os.getenv("AWS_REGION")
role_arn = os.getenv("AWS_ROLE_ARN")
bucket_name = os.getenv("S3_BUCKET_NAME")
bucket_key = os.getenv("S3_BUCKET_KEY")

#BGG dataframe
df_bgg = pd.read_csv('data/boardgamegeek.csv')
df_bgg['bgg_id'] = df_bgg['bgg_id'].astype(str)


def request_connection(url, status_code=500):
    """Sends a request to the target url until a response code of 200 is returned.

    Parameters:
        url {string} -- the endpoint and query parameters of the target API.
        status_code {int} -- HTTP response status codes that is initialized with a server error.

    Returns:
        {Response} -- contains the API response including the status code, headers, and content.
    """

    while status_code != 200:

        random_sleep(min_sec=3, max_sec=5)

        try:
            response = requests.get(url)
            status_code = response.status_code

            if status_code != 200:
                print(f"Status code: {status_code}.")
                print("Error establishing connection. Retrying to connect.")

            if status_code == 414:
                print("URI too long.")
                return response

        except Exception as e:
            print(f"Exception occured: {e}")
            raise

    return response

def initialize_aws_role_and_s3(access_key, secret_key, aws_region, role_arn):
    """Initializes AWS user, role, and S3 client.

    Parameters:
        access_key {string} -- an ID of unique identifier to authenticate requests to AWS.
        secret_key {string} -- a digital signature for API requests made to AWS. 
        aws_region {string} -- location of AWS resources.
        role_arn {string} -- unique identifier for an IAM role to grant permission to AWS resources.

    Returns:
        {s3 client} -- an instance of boto3 S3 client intialized with the credentials \
            provided.
    """

    try:
        sts_client = boto3.client(
            'sts',
            aws_access_key_id = access_key,
            aws_secret_access_key= secret_key,
            region_name = aws_region,
        )

        assumed_role = sts_client.assume_role(
            RoleArn=role_arn,
            RoleSessionName='BGGRoleSession',
            DurationSeconds=18000, #role is only valid for 5 hours
        )

        aws_user = sts_client.get_caller_identity()['Arn'].split("/")[-1]
        aws_role = assumed_role['AssumedRoleUser']['Arn'].split("/")[-2]
        token_expiration = assumed_role['Credentials']['Expiration'] \
            + timedelta(hours=8) #Philippine Time

        print(f"AWS user '{aws_user}' assigned with role '{aws_role}'.")
        print(f"Token expiration time: {token_expiration.strftime('%I:%M %p %d-%b-%Y')}")

    except Exception as e:
        print(f"Error initializing AWS: {e}")
        raise

    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=assumed_role['Credentials']['AccessKeyId'],
            aws_secret_access_key=assumed_role['Credentials']['SecretAccessKey'],
            aws_session_token=assumed_role['Credentials']['SessionToken']
        )
        print("S3 successfully initilized.")

        return s3

    except Exception as e:

        print(f"Error initializing S3: {e}")
        raise


def upload_xml_to_s3(response, s3, bucket_name, bucket_key, file_name):
    """Uploads binary data to AWS S3.

    Parameters:
        response {Response class} -- contains the API response including the status code, \
            headers, and content.
        s3 {s3 client} -- an instance of the boto3 S3 client intialized with the \
            credentials provided.
        bucket_name {string} -- name of the output s3 bucket where the files will be stored.
        bucket_key {string} -- name of s3 bucket key where the files will be stored.
        file_name {string} -- desired name of the file that will be stored in the given \
            s3 bucket and key.

    Returns:
        {None} -- function returns no value but prints an update for files that were successfully \
            uploaded to sS3 bucket.
    """

    today = datetime.today().strftime("%Y-%d-%m")

    try:
        s3.upload_fileobj(
            Fileobj = io.BytesIO(response.content),
            Bucket = bucket_name,
            Key = f"{bucket_key}date={today}/{file_name}.xml"
        )

        print(f"Index {file_name} of BGG dataframe uploaded successfully to S3.")

    except Exception as e:
        print(f"Error: {e}")
        raise


def main():
    "Main driver function."

    try:

        s3_client = initialize_aws_role_and_s3(access_key, secret_key, aws_region, role_arn)
        n, no_of_bg = 0, 1200 #More than 1200 games per API call will result to 414 status code

        while n < df_bgg.shape[0]:

            url = 'https://boardgamegeek.com/xmlapi2/thing?id=' + \
            ','.join(df_bgg['bgg_id'][n:n + no_of_bg]) + '&stats=1'

            response = request_connection(url)

            if response.status_code == 414:
                no_of_bg -= 100 #reduce board games by 100
                continue

            file_name = f"{n}-{n + no_of_bg}"
            upload_xml_to_s3(response, s3_client, bucket_name, bucket_key, file_name)

            n += no_of_bg

    except Exception as e:
        print("Error occured. Stopping script.")

if __name__ == "__main__":
    main()
