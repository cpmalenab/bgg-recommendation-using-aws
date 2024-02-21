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

def get_attribute(item, tag, attribute):
    "Goes to the first instance of child tag of the current tag and extracts a specific attribute."

    child_tag = item.find(tag)
    return child_tag.get(attribute) if child_tag is not None else None

def get_text(item,tag):
    "Goes to the first instance of child tag of the current tag and extracts the text of the tag."

    child_tag = item.find(tag)
    return child_tag.text if child_tag is not None else None

def parse_bgg(xml_data):
    "Parses and flattens the XML file into a pandas dataframe."

    bgg_list = []

    root = ET.fromstring(xml_data)

    for item in root.findall('item'):

        bgg_id = item.get('id')
        bgg_type = item.get('type')

        name = get_attribute(item, 'name', 'value')
        image_src=get_text(item, 'image') if not None else "N/A"
        description = get_text(item, 'description')
        year_published = get_attribute(item, 'yearpublished', 'value')
        min_players = get_attribute(item, 'minplayers', 'value')
        max_players = get_attribute(item, 'maxplayers', 'value')
        playing_time = get_attribute(item, 'playingtime', 'value')
        min_playtime = get_attribute(item, 'minplaytime', 'value')
        max_playtime = get_attribute(item, 'maxplaytime', 'value')
        min_age = get_attribute(item, 'minage', 'value')

        for ratings in item.find('statistics'):
            users_rated = get_attribute(ratings, 'usersrated','value')
            average = get_attribute(ratings, 'average','value')
            bayes_average = get_attribute(ratings, 'bayesaverage','value')
            num_owners = get_attribute(ratings, 'owned', 'value')
            num_weights = get_attribute(ratings,'numweights' ,'value')
            average_weight = get_attribute(ratings, 'averageweight', 'value')
            rank = get_attribute(ratings.find('ranks'), 'rank','value')

            rank_tags = ratings.findall('ranks')[0].findall('rank')
            bgg_rank = int(rank) if rank != "Not Ranked" else 0

            subdomain_1, subdomain_1_rank = "N/A", 0
            subdomain_2, subdomain_2_rank = "N/A", 0

            if len(rank_tags) > 1:
                subdomain_rank = rank_tags[1].get('value')
                subdomain_1 = rank_tags[1].get('friendlyname')
                subdomain_1_rank = int(subdomain_rank) if subdomain_rank != "Not Ranked" else 0
            if len(rank_tags) > 2:
                subdomain_rank = rank_tags[1].get('value')
                subdomain_2 = rank_tags[2].get('friendlyname')
                subdomain_2_rank = int(subdomain_rank) if subdomain_rank != "Not Ranked" else 0

        bgg_list.append({
            'bgg_id':bgg_id, 
            'bgg_type':bgg_type, 
            'name':name,
            'img_src':image_src,
            'description':description,         
            'year_published':int(year_published), 
            'min_players':int(min_players), 
            'max_players':int(max_players),
            'playing_time':int(playing_time),
            'min_playtime':int(min_playtime),
            'max_playtime':int(max_playtime), 
            'min_age':int(min_age), 
            'users_rated':int(users_rated), 
            'average':float(average), 
            'bayes_average':float(bayes_average), 
            'num_weights':int(num_weights), 
            'average_weight':float(average_weight),
            'bgg_rank':bgg_rank, 
            'num_owners':int(num_owners), 
            'subdomain_1':subdomain_1, 
            'subdomain_1_rank':subdomain_1_rank,
            'subdomain_2':subdomain_2, 
            'subdomain_2_rank':subdomain_2_rank, 
            'date':date.today().strftime("%Y-%m-%d"), 
            'year':date.today().year, 
            'month':date.today().month, 
            'day':date.today().day,
            'type':'details',
        })

    df_bgg = pd.DataFrame(bgg_list)

    return df_bgg
