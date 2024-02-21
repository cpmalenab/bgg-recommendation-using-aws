import os
import json
import urllib.parse

from xml.etree import ElementTree as ET
from datetime import date
import pandas as pd
import awswrangler as wr
import boto3


s3 = boto3.client('s3')

s3_output_path = os.environ['s3_output_path']
glue_database = os.environ['glue_database']
glue_table = os.environ['glue_table']
mode_operation = os.environ['mode_operation']
partitions= ['date', 'type']

def lambda_handler(event, context):

    s3_event = json.loads(event['Records'][0]['Sns']['Message'])
    bucket = s3_event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(s3_event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    s3_response = s3.get_object(Bucket=bucket, Key=key)
    xml_data = s3_response['Body'].read().decode(encoding='utf-8')

    try:
        df_bgg = parse_bgg(xml_data)

        wr_response = wr.s3.to_parquet(df=df_bgg,
                                       path=s3_output_path,
                                       dataset=True,
                                       database=glue_database,
                                       table=glue_table,
                                       mode=mode_operation,
                                       partition_cols=partitions)

        return wr_response

    except Exception as e:
        print(f"Error occured: {e}")
        raise

def parse_bgg(xml_data):
    "Parses and flattens the XML file into a pandas dataframe."

    bgg_list = []

    root = ET.fromstring(xml_data)

    for item in root.findall('item'):

        bgg_id = item.get('id')

        for classifications in item.findall('link'):

            classification = classifications.get('type')
            value = classifications.get('value')

            bgg_list.append({
                "bgg_id":bgg_id,
                "classification":classification,
                "value":value,
                "date":date.today().strftime("%Y-%m-%d"),
                "type":"classification",
            })

    df_bgg = pd.DataFrame(bgg_list)

    return df_bgg
