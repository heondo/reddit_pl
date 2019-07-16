import boto3
import praw
import json
import pandas as pd
import numpy as np
import datetime as dt
from dateutil import parser
import pprint
from ast import literal_eval
import re
from decimal import Decimal
from functools import reduce
import os
# import amazondax
import botocore.session
import personal_funcs as mf

class reddit_posts:
    def __init__(self, configs, subredd, time_period, limit_num, curr_time):
        self.reddit = praw.Reddit(client_id = configs['client_id'],
                    client_secret=configs['client_secret'],
                    user_agent = configs['user_agent'],
                    username = configs['username'],
                    password=configs['password'])
        self.sub = self.reddit.subreddit(subredd)
        self.curr_time = curr_time
        self.top_subs = self.sub.top(time_period, limit=limit_num)
        self.sub_json = mf.subs_to_json(self.top_subs, curr_time, subredd)
        self.comm_json = mf.subs_to_comms(self.sub_json, curr_time, subredd)
    def pop_comments(self):
        self.sub_json = list(map(mf.remove_comments, self.sub_json))
    # now with sub/comm jsons, i want to upload each on to....
    # function needs to take in data (self), which bucket, curr time
    def up_to_s3(self):
        mf.upload_to_s3(self.sub_json, "submits", self.curr_time)
        mf.upload_to_s3(self.comm_json, "comments", self.curr_time)
    # next thing...creating Dataframes..for each and cleaning the submits or comments
    # into DFs -> 
    # jfc, everything after this just got so stupid. So, after I upload to s3 forunately I have the txt files
    # and things that worked prior.

    # read in text stuff and create DF
    def make_df(self):
        self.subDF = mf.create_df("submits")
        self.commDF = mf.create_df("comments")
        self.subDF = mf.fix_subs(self.subDF)
        self.commDF = mf.fix_comms(self.commDF)

    def into_dynamo(self):
        # update/put into dynamodb
        mf.df_json_dynamo(self.subDF, "submits")
        mf.df_json_dynamo(self.commDF, "comments")

def lambda_handler(event, context): 

    reddit_creds = mf.txt_json('config/reddit_creds.txt')
    time_rn = parser.parse(event['time'])
    time_rn = re.sub(r' ', '_', str(time_rn))

    # create class for weedstocks, of the day, no limit
    r_object = reddit_posts(configs=reddit_creds, subredd='weedstocks', 
    time_period= 'day', limit_num = None, curr_time=time_rn)
    # remove comment forest after comments are extracted, cleaned.
    r_object.pop_comments()
    # upload those text files to s3
    r_object.up_to_s3()
    r_object.make_df()
    r_object.into_dynamo()

    # so everything up to this point works, files uploaded to s3 and items uploaded into dynamo
    # adjust into dynamo so that it updates for ID, 
    # now....next func should clean/insert into dynamodb...due to its restraints with
    # data types and nulls, bools, etc.



if __name__ == "__main__":
    # to make it work in lambdas just put reddit_creds in the lambda handler and thats it.
    # reddit_creds = mf.txt_json('config/reddit_creds.txt')
    event = {
        "id": "cdc73f9d-aea9-11e3-9d5a-835b769c0d9c",
        "detail-type": "Scheduled Event",
        "source": "aws.events",
        "account": "{{account-id}}",
        "time": str(dt.datetime.now()),
        "region": "us-west-2",
        "resources": [
            "arn:aws:events:us-west-2:123456789012:rule/ExampleRule"
        ],
        "detail": {}
        }
    context = None
    lambda_handler(event, context)