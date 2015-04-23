# -*- coding: utf-8 -*-
"""
Created on Tue Apr 21 20:54:49 2015

@author: Chris
"""

downloaded_users = []
while True:
    # refresh mongo connection details after each run through to prevent time out
    client = MongoClient()
    db = client.mydb
    tweets = db.tweets
    users = db.users

    #for user in users.find({"screen_name": { "$nin": downloaded_users }}):
    users_mongo = users.find({"twitter_username": { "$exists": "true", "$nin": ["null"]}})

    for user in users_mongo:
        print user["twitter_username"] + " " + str(datetime.datetime.now().time())
        status=[]
        last_tweet = []
        oldest_tweet=[]
        last_tweet = tweets.find({"user.screen_name": user['twitter_username'] }).sort([['_id',pymongo.ASCENDING]]).limit(1)
        # if there are tweets for this user in the collection, get the latest  since the last tweet   
        if(last_tweet.count()>0):
            print "update tweets"+ " " + str(datetime.datetime.now().time())
            while True: 
                try:
                    status = api.user_timeline(screen_name=last_tweet[0]['user']['screen_name'],since_id=last_tweet[0]['id'])#retry_count=10,retry_delay=100,)
                    if(status):
                        break
                    else:
                        oldest_tweet = tweets.find({"user.screen_name": user['twitter_username'] }).sort([['_id',pymongo.DESCENDING]]).limit(1)
                        status = api.user_timeline(screen_name=oldest_tweet[0]['user']['screen_name'],max_id=oldest_tweet[0]['id'],count=200)#retry_count=10,retry_delay=100,)
                        if(status):
                            break
                        else:
                            print("No tweets to collect")+str(datetime.datetime.now().time())
                            users.update({"screen_name": user["twitter_username"]},{"$set": {"access_status": "No tweets to collect" }})
                            break
                except tweepy.TweepError, e:
                    if(tweepy_error_handler(e,user,users)):
                        break
        
        # else get 100 of the last tweets
        else:
            print "no tweets"+ " " + str(datetime.datetime.now().time())
            while True: 
                try:
                    status = api.user_timeline(screen_name=user['twitter_username'],count=200)#retry_count=10,retry_delay=100,)
                    if(status):
                        break
                    else:
                        print("No tweets to collect")+str(datetime.datetime.now().time())
                        users.update({"screen_name": user["twitter_username"]},{"$set": {"access_status": "No tweets to collect" }})
                        break
                except tweepy.TweepError, e:
                    if(tweepy_error_handler(e,user,users)):
                        break
                    
        if(status):
            store_tweets(status,tweets)
        downloaded_users.append(user["twitter_username"])    
    continue
