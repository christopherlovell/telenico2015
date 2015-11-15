
import json
import tweepy
import pymongo
import pandas as pd

import time
import datetime

from pymongo import MongoClient

# twitter set up
file_directory='C:\\Users\\Chris\\Desktop\\twitter_keys.json'
json_data=open(file_directory).read()
data = json.loads(json_data)

auth = tweepy.OAuthHandler(data["consumer_key"],data["consumer_secret"])

try:
    redirect_url = auth.get_authorization_url()
except tweepy.TweepError:
    print 'Error! Failed to get request token.'
    
auth.set_access_token(data["access_token"],data["access_token_secret"])

api = tweepy.API(auth)


# mongo set up
client = MongoClient()
db = client.mydb
tweets = db.tweets
users = db.users


# ----  twitter accounts ----
# old list method
#list_members = api.list_members('tweetminster','ukmps',-1)

#for user in tweepy.Cursor(api.list_members,'tweetminster','ukmps').items():
#    users.insert(json.loads((json.dumps(user._json))))

# new csv method, from https://yournextmp.com/help/api/
file_directory='C:\\Users\\Chris\\Documents\\Python Scripts\\telenico2015\\candidates.csv'
candidates=pd.read_csv(file_directory,sep=",")

users.insert(json.loads(candidates.to_json(orient="records")))


# ----  access status  ----
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


#len(users.find().distinct("screen_name"))
#len(tweets.find().distinct("user.screen_name"))


def store_tweets(tweets,collection):
    for tweet in tweets:
        collection.insert(tweet._json)
      
    
def tweepy_error_handler(error,user,collection):
    if type(error.message) is str:
        if type(error.message) == 'Not authorized.':
            print "Error: tweets protected. "+ str(datetime.datetime.now().time())
            collection.update({"screen_name": user["twitter_username"]},{"$set": {"access_status": "protected" }})
            return(True)
    elif type(error.message) is str:
        if error.message[0]['code'] == 88:
            print "Error: exceeded limit. " + str(datetime.datetime.now().time())
            apilimit = api.rate_limit_status()
            timediff = apilimit['resources']['application']['/application/rate_limit_status']['reset']-time.time()
            time.sleep(timediff+1)
            return(False)
        elif error.message[0]['code'] == 34:
            print "Error: user not found. "+ str(datetime.datetime.now().time())
            collection.update({"screen_name": user["twitter_username"]},{"$set": {"access_status": "not_found" }})
            return(True)
    else:
        print "Error: unidentified error occured. "+ str(datetime.datetime.now().time())
        collection.update({"screen_name": user["twitter_username"]},{"$set": {"access_status": e.message }})
        return(True)


downloaded_users = []
while True:
    # refresh mongo connection details after each run through to prevent time out
    client = MongoClient()
    db = client.mydb
    tweets = db.tweets
    users = db.users

    #for user in users.find({"screen_name": { "$nin": downloaded_users }}):
    users_mongo = users.find({"twitter_username": { "$exists": "true", "$nin": ['null']}})

    for user in users_mongo:
        # test for empty username field (can't filter out at mongo call)
        if user["twitter_username"] is None:
            continue
        print user["twitter_username"] + " " + str(datetime.datetime.now().time())
        status=[]
        last_tweet = []
        oldest_tweet=[]
        last_tweet = tweets.find({"user.screen_name": user['twitter_username'] }).sort([['_id',pymongo.ASCENDING]]).limit(1)
        # if there are tweets for this user in the collection, get the latest  since the last tweet   
        if(last_tweet.count()>0):
            if(last_tweet[0]['created_at']<datetime.datetime(2015, 5, 9, 1, 1, 1)):
                print "update tweets"+ " " + str(datetime.datetime.now().time())
                while True:
                    try:
                        status = api.user_timeline(screen_name=last_tweet[0]['user']['screen_name'],since_id=last_tweet[0]['id'])#retry_count=10,retry_delay=100,)
                        if(status):
                            print "new"
                            break
    #                    else:
    #                        oldest_tweet = tweets.find({"user.screen_name": user['twitter_username'] }).sort([['_id',pymongo.DESCENDING]]).limit(1)
    #                        status = api.user_timeline(screen_name=oldest_tweet[0]['user']['screen_name'],max_id=oldest_tweet[0]['id'],count=200)#retry_count=10,retry_delay=100,)
    #                        if(status):
    #                            print "old"
    #                            break
    #                        else:
    #                            print("No tweets to collect")+str(datetime.datetime.now().time())
    #                            users.update({"screen_name": user["twitter_username"]},{"$set": {"access_status": "No tweets to collect" }})
    #                            break
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
            print "storing tweets!"
            store_tweets(status,tweets)
        downloaded_users.append(user["twitter_username"])
    continue
