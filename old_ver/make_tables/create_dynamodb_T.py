#! /home/heondokim/envs/reddit_pl/bin/python3
# create dynamodb table from example text files

import boto3
import praw
import json
import pandas as pd
import datetime as dt
from dateutil import parser
import pprint
# import ast
from ast import literal_eval
import re
import os
import amazondax
import botocore.session

# create table in dynamo db for each one, after that....fix oop practice to
# do a thing to insert or update into one ofthose dynamodbs


if __name__ == "__main__":
    session = botocore.session.get_session()
    dynamodb = session.create_client('dynamodb', region_name="us-west-2")
    tablename = 'comments_table'
    # author,body,created_utc,date_uploaded,distinguished,edited,id,
    # is_submitter,link_id,parent_id,permalink,score,stickied,subreddit,subreddit_id
    params = {
    'TableName' : tablename,
    'KeySchema': [       
        { 'AttributeName': "id", 'KeyType': "HASH"}    # Partition key
        # { 'AttributeName': "parent_id", 'KeyType': "RANGE" }   # Sort key
    ],
    'AttributeDefinitions': [       
        { 'AttributeName': "id", 'AttributeType': "S" }
        # { 'AttributeName': "parent_id", 'AttributeType': "S" }
        ],
    'ProvisionedThroughput': {       
        'ReadCapacityUnits': 10, 
        'WriteCapacityUnits': 10
        }
    }
    dynamodb.create_table(**params)