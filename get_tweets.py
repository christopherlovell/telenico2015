# -*- coding: utf-8 -*-
"""
Created on Mon Jan 26 19:28:59 2015

@author: Chris
"""

import json
import tweepy
import pymongo

import time

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



#for tweet in status:
#    print(tweet.created_at)
#    print(tweet.id)
#    print(tweet.user.screen_name)


# loop through users in collection and check that they exist
# update access_status meta data accordingly
#for user in users.find({}):
#    print user["screen_name"]
#    result = None
#    while result is None:    
#        try:
#            api.get_user(screen_name = user["screen_name"])
#        except tweepy.TweepError, e:
#            print(e.message)
#            if e.message[0]['code'] == 88:
#                print "Error: exceeded limit"
#                time.sleep(60)
#            elif e.message == 'Not authorized.':
#                print "Error: tweets protected"
#                users.update({"screen_name": user["screen_name"]},{"$set": {"access_status": "protected" }})
#                result=True
#            elif e.message[0]['code'] == 34:
#                print "Error: user not found"
#                users.update({"screen_name": user["screen_name"]},{"$set": {"access_status": "not_found" }})
#                result=True
#            else:
#                print "Error: unidentified error occured"
#                users.update({"screen_name": user["screen_name"]},{"$set": {"access_status": e.message }})
#                result=True


# list to keep track of users whose tweets have been downloaded
downloaded_users = []

api.rate_limit_status()


def tweepy_error_handler(error,user,collection):
    if error.message[0]['code'] == 88:
        print "Error: exceeded limit"
        return(False)
    elif error.message == 'Not authorized.':
        print "Error: tweets protected"
        collection.update({"screen_name": user["screen_name"]},{"$set": {"access_status": "protected" }})
        return(True)
    elif error.message[0]['code'] == 34:
        print "Error: user not found"
        collection.update({"screen_name": user["screen_name"]},{"$set": {"access_status": "not_found" }})
        return(True)
    else:
        print "Error: unidentified error occured"
        collection.update({"screen_name": user["screen_name"]},{"$set": {"access_status": e.message }})
        return(True)


#for user in users.find({"screen_name": { "$nin": downloaded_users }}):
for user in users.find({"access_status": { "$nin": ["protected","not_found"]}}):
    print user["screen_name"]
    last_tweet = tweets.find({"user.screen_name": user['screen_name'] }).sort([['_id',pymongo.ASCENDING]]).limit(1)
    # if there are tweets for this user in the collection, get the latest    
    if(last_tweet.count()>0):
        print "update tweets"
        result = None
        while result is None: 
            try:
                status = api.user_timeline(screen_name=last_tweet[0]['user']['screen_name'],since_id=last_tweet[0]['id'])#retry_count=10,retry_delay=100,)
            except tweepy.TweepError, e:
                if tweepy_error_handler(e,user,users) is False:
                    time.sleep(10)
                    continue
                else:
                    result = False 
            result = True
    
    # else get 100 of the last tweets
    else:
        print "no tweets"
        result = None
        while result is None: 
            try:
                status = api.user_timeline(screen_name=user['screen_name'],count=100)#retry_count=10,retry_delay=100,)
            except tweepy.TweepError, e:
                if tweepy_error_handler(e,user,users) is False:
                    time.sleep(10)
                    continue
                else:
                    result = False
            result = True
    
    if result is True:
        store_tweets(status,tweets)
        downloaded_users.append(user["screen_name"])
    time.sleep(5)

len(users.find().distinct("screen_name"))
len(tweets.find().distinct("user.screen_name"))


