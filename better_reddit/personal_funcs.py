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


def subs_to_json(top_posts, curr_time, subredd):
    subs_json = []
    for sub in top_posts:
        sub.comments.replace_more(limit=None)
        temp = {"author" : sub.author.name,
        "comments" : sub.comments,
        "date_uploaded": curr_time,
        "created_utc" : sub.created_utc,
        "distinguished" : sub.distinguished,
        "edited" : sub.edited,
        "id" : sub.id,
        "is_self" : sub.is_self,
        # "link_flair_template_id" : sub.link_flair_template_id,
        "link_flair_text" : sub.link_flair_text,
        "locked" : sub.locked,
        "name" : sub.name,
        "num_comments" : sub.num_comments,
        "over_18" : sub.over_18,
        "permalink" : sub.permalink,
        "score" : sub.score,
        "selftext" : sub.selftext,
        "spoiler" : sub.spoiler,
        "stickied" : sub.stickied,
        "subreddit" : subredd,
        "title" : sub.title,
        "upvote_ratio" : sub.upvote_ratio,
        "url" : sub.url
        }
        subs_json.append(temp)
    return subs_json

def commtree_json(comment_tree, curr_time, subredd):
    comm_tree_list = []
    for sub in comment_tree.list():
        try:
            temp = {"author" : sub.author.name,
            "body": sub.body,
            # "comments" : sub.comments,
            "date_uploaded": curr_time,
            "created_utc" : sub.created_utc,
            "distinguished" : sub.distinguished,
            "edited" : sub.edited,
            "id" : sub.id,
            "is_submitter" : sub.is_submitter,
            "link_id" : sub.link_id,
            "parent_id" : sub.parent_id,
            # "locked" : sub.locked,
            # "name" : sub.name,
            # "num_comments" : sub.num_comments,
            # "over_18" : sub.over_18,
            "permalink" : sub.permalink,
            "score" : sub.score,
            # "selftext" : sub.selftext,
            # "spoiler" : sub.spoiler,
            "stickied" : sub.stickied,
            "subreddit_id" : sub.subreddit_id,
            "subreddit" : "weedstocks"
            # "title" : sub.title,
            # "upvote_ratio" : sub.upvote_ratio,
            # "url" : sub.url
            }
            # print(temp)
            comm_tree_list.append(temp)
        except:
            temp = {"author" : None,
            "body": sub.body,
            # "comments" : sub.comments,
            "date_uploaded": curr_time,
            "created_utc" : sub.created_utc,
            "distinguished" : sub.distinguished,
            "edited" : sub.edited,
            "id" : sub.id,
            "is_submitter" : sub.is_submitter,
            "link_id" : sub.link_id,
            "parent_id" : sub.parent_id,
            # "locked" : sub.locked,
            # "name" : sub.name,
            # "num_comments" : sub.num_comments,
            # "over_18" : sub.over_18,
            "permalink" : sub.permalink,
            "score" : sub.score,
            # "selftext" : sub.selftext,
            # "spoiler" : sub.spoiler,
            "stickied" : sub.stickied,
            "subreddit_id" : sub.subreddit_id,
            "subreddit" : "weedstocks"
            # "title" : sub.title,
            # "upvote_ratio" : sub.upvote_ratio,
            # "url" : sub.url
            }
            comm_tree_list.append(temp)
            print(sub.id, "comment deleted")
    return comm_tree_list



def subs_to_comms(jsonofsubs, curr_time, subredd):
    comm_json = []
    for values in jsonofsubs:
        # print(values.items())
        # print(values["comments"])
        temp = commtree_json(values["comments"], curr_time, subredd)
        comm_json.append(temp)
    comm_json1 = [x for x in comm_json if x != []]
    return comm_json1

def remove_comments(subsofjson):
    # what = literal_eval(subs_to_json)
    subsofjson.pop("comments")
    return subsofjson


# build class for praw object...I think
def txt_json(text):
    thing = open(text, 'r').read()
    thing = json.loads(thing, parse_float=Decimal)
    return thing

# i just need...to write a function that will take in the list of submissions
# and create the list of comments. So basically, map comment_tree to sub_json
# what i was doing before is fine i just need to remember to pop the comment forest....
# i believe. but yeah back....again
def upload_to_s3(data, types, time_stamp):
    with open("/tmp/{}_data.txt".format(types), "w") as outfile:
        json.dump(data, outfile)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('cannabis-stocks')
    bucket.upload_file("/tmp/{}_data.txt".format(types),"{}/{}.txt".format(types, time_stamp))


# functions to take list of json posts, list of list of comments
# convert to dataframes and do cleaning...I think
# def type_conf(list_of_stuff):
#     try:
#         if isinstance(list_of_stuff[0], list):
#             return "comments"
#         else:
#             return "submissions"
#     except:
#         print("neither sub or comment")


def comm_to_df(lists):
    temp = list(map(pd.DataFrame.from_records, lists))
    temp = reduce(lambda x, y: pd.concat([x, y]), temp)
    return temp


def create_df(text_file):
    lists = txt_json('/tmp/{}_data.txt'.format(text_file))
    if text_file == 'comments':
        thing = comm_to_df(lists)
    elif text_file == 'submits':
        thing = pd.DataFrame.from_records(lists)
        thing['selftext'] = np.where(thing['selftext'] == "", None, thing['selftext'])
    return thing

def fix_subs(subDF):
    subDF['distinguished'] = subDF['distinguished'].fillna("false")
    subDF['selftext'] = subDF['selftext'].fillna("false")
    # turn it into a json, reread with parse decimal?
    return subDF

def fix_comms(commDF):
    commDF['author'] = commDF['author'].fillna("false")
    commDF['distinguished'] = commDF['distinguished'].fillna("false")
    return commDF

def df_json_dynamo(data, dynadb):
    resource = boto3.resource('dynamodb', region_name='us-west-2')
    table = resource.Table(dynadb)
    item = data.T.to_dict().values()
    for i in item:
        try:
            table.put_item(Item=i)
        except:
        #     # so automoderator posts dont work, i think its because the files are different
            print('failed with {}'.format(i['id']))