# -*- coding: utf-8 -*-
"""
Created on Mon Jan 26 19:28:59 2015

@author: Chris
"""

import json
import tweepy
import pymongo

# twitter set up
file_directory='C:\\Users\\Chris\\Desktop\\twitter_keys.json'
json_data=open(file_directory).read()
data = json.load(json_data)

auth = tweepy.OAuthHandler(data["consumer_key"],data["consumer_secret"])
auth.set_access_token(data["access_token"],data["access_token_secret"])

api = tweepy.API(auth)

# mongo set up
from pymongo import MongoClient
client = MongoClient()

db = client.mydb
tweets = db.tweets
users = db.users

# twitter accounts
list_members = api.list_members('tweetminster','ukmps',-1)

for user in tweepy.Cursor(api.list_members,'tweetminster','ukmps').items():
    users.insert(json.loads((json.dumps(user._json))))

#lists = api.list_members(list_id='tweetminster')


####
#### Initial Download
####

# list to keep track of users whose tweets have been downloaded
downloaded_users = []

for user in users.find({"screen_name": { "$nin": downloaded_users }}):
    print user["screen_name"]
    status = api.user_timeline(screen_name=user['screen_name'],count=30)
    for tweet in status:
        if len(list(tweets.find({"id": tweet.id})))<1:
            store_tweet(tweet,tweets)
    downloaded_users.append(user["screen_name"])


####
#### Production Download
####

for user in users.find():
    #print(user['id'])
    tweets.find({'source_user_id': user['id']}).sort({'':1}) 
    
    api.user_timeline(screen_name=user['screen_name'],count=1)
    
def store_tweet(tweet,collection):
    # Empty dictionary for storing tweet related data
    data ={}
    data['created_at'] = tweet.created_at
    data['favorite_count'] = tweet.favorite_count
    data['favorited'] = tweet.favorited
    data['id'] = tweet.id
    data['lang'] = tweet.lang
    data['retweet_count'] = tweet.retweet_count
    data['retweeted'] = tweet.retweeted
    data['truncated'] = tweet.truncated
    #data['user'] = tweet.user
    #data['user_mentions'] = tweet.user_mentions
    data['geo'] = tweet.geo
    data['id'] = tweet.id
    data['source'] = tweet.source
    data['text'] = tweet.text
    # Insert process
    collection.insert(data)

