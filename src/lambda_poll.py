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


def poll_vote(results):
    "Obtains the poll result with the maximum number of votes."

    results_dict = {}

    for result in results.findall('result'):
        value = result.get('value')
        numvotes = int(result.get('numvotes'))
        results_dict[value] = numvotes

    poll_winner = max(results_dict, key=results_dict.get)

    return poll_winner


def parse_bgg(xml_data):
    "Parses and flattens the XML file into a pandas dataframe."

    root = ET.fromstring(xml_data)

    bgg_list = []

    for item in root.findall('item'):

        bgg_id = item.get('id')

        for poll in item.findall('poll'):
            poll_name = poll.get('name')
            total_votes = poll.get('totalvotes')

            if poll_name == "suggested_numplayers" and total_votes != "0":

                for results in poll.findall('results'):
                    poll_title = f"{poll_name} - {results.get('numplayers')}"
                    poll_answer = poll_vote(results)

                    bgg_list.append({
                        "bgg_id": bgg_id,
                        "poll_title":poll_title,
                        "poll_answer": poll_answer,
                        "date":date.today().strftime("%Y-%m-%d"),
                        "type":"poll",
                    })
            else:

                poll_answer = poll_vote(poll.find('results')) if total_votes != "0" else "N/A"

                bgg_list.append({
                    "bgg_id": bgg_id,
                    "poll_title":poll_name,
                    "poll_answer": poll_answer,
                    "date":date.today().strftime("%Y-%m-%d"),
                    "type":"poll",
                })


    df_bgg = pd.DataFrame(bgg_list)

    return df_bgg
    