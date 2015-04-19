
import json
import tweepy
import pymongo

import time
import datetime

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
from pymongo import MongoClient

# twitter accounts
#list_members = api.list_members('tweetminster','ukmps',-1)
#
#for user in tweepy.Cursor(api.list_members,'tweetminster','ukmps').items():
#    users.insert(json.loads((json.dumps(user._json))))


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

#def pymongo_error_handler (error,user,collection):
        
    
def tweepy_error_handler(error,user,collection):
    if error.message == 'Not authorized.':
        print "Error: tweets protected. "+ str(datetime.datetime.now().time())
        collection.update({"screen_name": user["screen_name"]},{"$set": {"access_status": "protected" }})
        return(True)
    elif error.message[0]['code'] == 88:
        print "Error: exceeded limit. " + str(datetime.datetime.now().time())
        apilimit = api.rate_limit_status()
        timediff = apilimit['resources']['application']['/application/rate_limit_status']['reset']-time.time()
        time.sleep(timediff+1)
        return(False)
    elif error.message[0]['code'] == 34:
        print "Error: user not found. "+ str(datetime.datetime.now().time())
        collection.update({"screen_name": user["screen_name"]},{"$set": {"access_status": "not_found" }})
        return(True)
    else:
        print "Error: unidentified error occured. "+ str(datetime.datetime.now().time())
        collection.update({"screen_name": user["screen_name"]},{"$set": {"access_status": e.message }})
        return(True)


downloaded_users = []
while True:
    # refresh mongo connection details after each run through to prevent time out
    client = MongoClient()
    db = client.mydb
    tweets = db.tweets
    users = db.users

    #for user in users.find({"screen_name": { "$nin": downloaded_users }}):
    users_mongo = users.find({"access_status": { "$nin": ["protected","not_found"]}})

    for user in users_mongo:
        print user["screen_name"] + " " + str(datetime.datetime.now().time())
        status=[]
        last_tweet = []
        oldest_tweet=[]
        last_tweet = tweets.find({"user.screen_name": user['screen_name'] }).sort([['_id',pymongo.ASCENDING]]).limit(1)
        # if there are tweets for this user in the collection, get the latest  since the last tweet   
        if(last_tweet.count()>0):
            print "update tweets"+ " " + str(datetime.datetime.now().time())
            while True: 
                try:
                    status = api.user_timeline(screen_name=last_tweet[0]['user']['screen_name'],since_id=last_tweet[0]['id'])#retry_count=10,retry_delay=100,)
                    if(status):
                        break
                    else:
                        oldest_tweet = tweets.find({"user.screen_name": user['screen_name'] }).sort([['_id',pymongo.DESCENDING]]).limit(1)
                        status = api.user_timeline(screen_name=oldest_tweet[0]['user']['screen_name'],max_id=oldest_tweet[0]['id'],count=200)#retry_count=10,retry_delay=100,)
                        if(status):
                            break
                        else:
                            print("No tweets to collect")+str(datetime.datetime.now().time())
                            users.update({"screen_name": user["screen_name"]},{"$set": {"access_status": "No tweets to collect" }})
                            break
                except tweepy.TweepError, e:
                    if(tweepy_error_handler(e,user,users)):
                        break
        
        # else get 100 of the last tweets
        else:
            print "no tweets"+ " " + str(datetime.datetime.now().time())
            while True: 
                try:
                    status = api.user_timeline(screen_name=user['screen_name'],count=200)#retry_count=10,retry_delay=100,)
                    if(status):
                        break
                    else:
                        print("No tweets to collect")+str(datetime.datetime.now().time())
                        users.update({"screen_name": user["screen_name"]},{"$set": {"access_status": "No tweets to collect" }})
                        break
                except tweepy.TweepError, e:
                    if(tweepy_error_handler(e,user,users)):
                        break
                    
        if(status):
            store_tweets(status,tweets)
        downloaded_users.append(user["screen_name"])    
    continue
