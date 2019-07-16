import boto3
import praw
import json
import pandas as pd
import numpy as np
import datetime as dt
from dateutil import parser
import pprint
# import ast
from ast import literal_eval
import re
from decimal import Decimal
from functools import reduce

def type_conf(list_of_stuff):
    try:
        if isinstance(list_of_stuff[0], list):
            return "comments"
        else:
            return "submissions"
    except:
        print("neither sub or comment")


def comm_to_df(lists):
    temp = list(map(pd.DataFrame.from_records, lists))
    temp = reduce(lambda x, y: pd.concat([x, y]), temp)
    return temp

def read_txt(text):
    thing = open(text, 'r').read()
    thing = json.loads(thing, parse_float=Decimal)
    return thing

def create_df(lists, typer):
    if typer == 'comments':
        thing = comm_to_df(lists)
    elif typer == 'submissions':
        thing = pd.DataFrame.from_records(lists)
        thing['selftext'] = np.where(thing['selftext'] == "", None, thing['selftext'])
        thing.drop(columns=["link_flair_template_id"], inplace=True)
    return thing

class json_txt:
    def __init__(self, filename):
        self.sub_comms = read_txt(filename)
        self.type = type_conf(self.sub_comms)
        self.DF = create_df(self.sub_comms, self.type)
    # def to_dynamo(self):
    def into_dynamo(self):
        # update/put into dynamodb
        self.l_json = self.DF.T.to_dict().values()
        print(len(self.l_json))


if __name__ == "__main__":
    json_t = json_txt("2019-07-11_00_00_00+00_00.txt")
    json_t.into_dynamo()
    resource = boto3.resource('dynamodb', region_name='us-west-2')
    table = resource.Table('submits_table')
    for i in json_t.l_json:
        # print(i['id'])
        try:
            table.put_item(Item=i)
        except:
            # so automoderator posts dont work, i think its because the files are different
            print('failed with {}'.format(i['id']))
    # print(len(json_t['id']))
    # json_t.DF.to_csv('{}.csv'.format(json_t.type), index=False)
    # so things look good right now, with types + dataframe for comment or submission
    # with these things, the thing to do is insert....update into two dynamodbs
    # one for comments and submissions, both being the most recent version of a comment or post, unless deleted


    

