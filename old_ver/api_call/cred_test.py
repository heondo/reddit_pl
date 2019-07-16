import boto3
import praw
import pandas as pandas
import json
import datetime as dt
from dateutil import parser
import pprint
# import ast
from ast import literal_eval
import re

redd_creds = open('config/reddit_creds.txt', 'r').read()

print(json.loads(redd_creds))

redd_creds = json.loads(redd_creds))
reddit = praw.Reddit(client_id = redd_creds.client_id,
client_secret=redd_creds.client_secret,
user_agent = redd_creds.user_agent,
username = redd_creds.username,
password=redd_creds.password)
r_wstocks = reddit.subreddit(subred)