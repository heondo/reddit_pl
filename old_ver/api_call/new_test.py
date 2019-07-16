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


def subs_to_json(subred, time_frame, limit_num):
    redd_creds = open('config/reddit_creds.txt', 'r').read()
    redd_creds = json.loads(redd_creds)
    reddit = praw.Reddit(client_id = redd_creds['client_id'],
    client_secret=redd_creds['client_secret'],
    user_agent = redd_creds['user_agent'],
    username = redd_creds['username'],
    password=redd_creds['password'])
    r_wstocks = reddit.subreddit(subred)
    subs_top = r_wstocks.top(time_frame, limit=limit_num)
    # print(vars(subs_top))
    subs_json = []
    for sub in subs_top:
        try:
            sub.comments.replace_more(limit=None)
            temp = {"author" : sub.author.name,
            "comments" : sub.comments,
            "date_uploaded": str(dt.datetime.now()),
            "created_utc" : sub.created_utc,
            "distinguished" : sub.distinguished,
            "edited" : sub.edited,
            "id" : sub.id,
            "is_self" : sub.is_self,
            "link_flair_template_id" : sub.link_flair_template_id,
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
            "subreddit" : subred,
            "title" : sub.title,
            "upvote_ratio" : sub.upvote_ratio,
            "url" : sub.url
            }
            subs_json.append(temp)
        except:
            temp = {"author" : sub.author.name,
            "comments" : sub.comments,
            "date_uploaded": str(dt.datetime.now()),
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
            "subreddit" : subred,
            "title" : sub.title,
            "upvote_ratio" : sub.upvote_ratio,
            "url" : sub.url
            }
            subs_json.append(temp)
            print(sub.id, 'no link_flair_template id')
    return subs_json


def commtree_json(comment_tree):
    comm_tree_list = []
    for sub in comment_tree.list():
        try:
            temp = {"author" : sub.author.name,
            "body": sub.body,
            # "comments" : sub.comments,
            "date_uploaded": str(dt.datetime.now()),
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
            "date_uploaded": str(dt.datetime.now()),
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



def subs_to_comms(jsonofsubs):
    comm_json = []
    for values in jsonofsubs:
        # print(values.items())
        # print(values["comments"])
        temp = commtree_json(values["comments"])
        comm_json.append(temp)
    return comm_json

def remove_comments(subsofjson):
    # what = literal_eval(subs_to_json)
    subsofjson.pop("comments")
    return subsofjson




def lambda_handler(event, context):
    time_stamp = parser.parse(event['time'])
    time_stamp = re.sub(r' ', '_', str(time_stamp))
    submit_json_list = subs_to_json("weedstocks", "day", None)
    # print(submit_json_list)
    # print(subs_json)
    # all right so the issue is that the comments are an instance so I need
    # to clean that and sort that out...but i already got the stuff so.
    comments_json_list = subs_to_comms(submit_json_list)
    comments_json_list_2 = [x for x in comments_json_list if x != []]
    submit_json_list_2 = list(map(remove_comments, submit_json_list)) # pop("comments")
    # print(comments_json_list)
    # print(submit_json_list_2)
    # for i in comments_json_list:
    #     for x in i:
    #         try:
    #             print(x['parent_id'])
    #         except:
    #             print('dunno')
    # print(what)
    with open("submission_data.txt", "w") as outfile:
        json.dump(submit_json_list_2, outfile)
    with open("comment_data.txt", "w") as outfile:
        json.dump(comments_json_list_2, outfile)
    # so i have my two files here, need to upload to s3
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('heondo-reddit-wstocks')
    bucket.upload_file("comment_data.txt","comments_day_hourly/{}.txt".format(time_stamp))
    bucket.upload_file("submission_data.txt","submits_day_hourly/{}.txt".format(time_stamp))


if __name__ == "__main__":
    event = {
  "id": "cdc73f9d-aea9-11e3-9d5a-835b769c0d9c",
  "detail-type": "Scheduled Event",
  "source": "aws.events",
  "account": "{{account-id}}",
  "time": "2019-06-11T00:00:00Z",
  "region": "us-west-2",
  "resources": [
    "arn:aws:events:us-west-2:123456789012:rule/ExampleRule"
  ],
  "detail": {}
}
    context = None
    lambda_handler(event, context)