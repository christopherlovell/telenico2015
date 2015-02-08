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
data = json.loads(json_data)

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

####
#### Download
####


#for user in users.find({},{"screen_name"}):
#    print "user: " + user["screen_name"] 
#    
#    status = tweets.find({"user.screen_name": user["screen_name"] }).limit(1).sort("created_at",pymongo.DESCENDING)
#    for tweet in status:
#        print "tweet: " + tweet.author.screen_name
#    

def store_tweets(tweets,collection):    
    for tweet in tweets:
        collection.insert(tweet._json)


for tweet in status:
    print(tweet.created_at)
    print(tweet.id)
    print(tweet.user.screen_name)


# list to keep track of users whose tweets have been downloaded
downloaded_users = []

for user in users.find({"screen_name": { "$nin": downloaded_users }}):
    print user["screen_name"]
    initial = tweets.find({"user.screen_name": user['screen_name'] }).sort([['_id',pymongo.ASCENDING]]).limit(1)
    if(initial.count()>0):
        status = api.user_timeline(screen_name=initial[0]['user']['screen_name'],since_id=initial[0]['_id'])#retry_count=10,retry_delay=100,)
    else:
        print "no tweets"
        try:
            status = api.user_timeline(screen_name=user['screen_name'],count=100)#retry_count=10,retry_delay=100,)
        except tweepy.TweepError, e:
            if e.message == 'Not authorized.':
                print "Error: tweets protected"
                continue
            if e.message[0]['code'] == 34:
                print "Error: user not found"
                continue
            elif e.message[0]['code'] == 88:
                print "Error: exceeded limit"
                continue
            else:
                print e
                continue
        store_tweets(status,tweets)
        downloaded_users.append(user["screen_name"])


len(users.find().distinct("screen_name"))
len(tweets.find().distinct("user.screen_name"))


