
wd<-"C:/Users/Chris/Documents/Data Science Files/telenico2015/"
setwd(wd)

library(tm)
library(Narrative)
library(RWeka)
library(rjson)
library(data.table)
library(dplyr)

library(zoo)
library(ggplot2)

# read candidates data
candidates<-read.csv(file = "candidates.csv",sep = ",")

library(rmongodb)
mongo<-rmongodb::mongo.create()  # connect to mongo
coll <- "mydb.tweets"

buf <- mongo.bson.buffer.create()  # create query string
mongo.bson.buffer.start.object(buf, "created_at")
mongo.bson.buffer.append.time(buf, "$gt", strptime("2015-03-01","%Y-%m-%d"))
mongo.bson.buffer.finish.object(buf)
query <- mongo.bson.from.buffer(buf)  # create query object (bson)
# execute query, return cursor
cursor <- mongo.find(mongo, coll, query, fields = '{"text":1,"favorite_count":1,"retweet_count":1,"created_at":1,"user.verified":1,"user.followers_count":1,"user.listed_count":1,"user.statuses_count":1,"user.description":1,"user.friends_count":1,"user.location":1,"user.name":1,"user.favourites_count":1,"user.screen_name":1,"user.protected":1}')  

tweets.list<-mongo.cursor.to.list(cursor)  # convert to list
tweets.dt<-data.table::rbindlist(lapply(lapply(tweets.list,unlist),as.list))  # unlist and turn in to data frame

# convert data types
tweets.dt[,"created_at":=as.data.frame(as.POSIXct(as.numeric(tweets.dt[["created_at"]]),origin="1970-01-01",tz="GMT"))]
tweets.dt[,favorite_count:=as.numeric(favorite_count)]
tweets.dt[,retweet_count:=as.numeric(retweet_count)]
tweets.dt[,user.verified:=as.logical(user.verified)]
tweets.dt[,user.protected:=as.logical(user.protected)]
tweets.dt[,user.followers_count:=as.numeric(user.followers_count)]
tweets.dt[,user.listed_count:=as.numeric(user.listed_count)]
tweets.dt[,user.statuses_count:=as.numeric(user.statuses_count)]
tweets.dt[,user.friends_count:=as.numeric(user.friends_count)]
tweets.dt[,user.favourites_count:=as.numeric(user.favourites_count)]

rm(tweets.list,mongo,coll,cursor,buf,query)

# match screen name with candidates data. return ordered data from candidates
candidate_fields<-c("party","constituency","name","gender","birth_date","honorific_prefix","party_id")
tweet.data<-lapply(tweets.dt[["user.screen_name"]],function(x) candidates[match(x,candidates[,"twitter_username"]),candidate_fields])

# convert to data frame
tweet.data.df<-dplyr::rbind_all(lapply(tweet.data, data.frame, stringsAsFactors = FALSE))

# convert data types
tweet.data.df[,"birth_date"]<-lapply(tweet.data.df[,"birth_date"],as.Date)
#tweet.data.df[,"gender"]<-lapply(tweet.data.df[,"gender"],function(x) as.String(tolower(x)))
rm(tweet.data,candidates,candidate_fields)


corp<-tm::VCorpus(tm::VectorSource(tweets.dt[["text"]]))

clean_non_ascii <- function(x){iconv(x, "latin1", "ASCII", sub="")}
corp.clean<-tm::tm_map(corp,content_transformer(clean_non_ascii))
corp.clean<-tm::tm_map(corp.clean,content_transformer(removePunctuation),preserve_intra_word_dashes=T)
corp.clean<-tm::tm_map(corp.clean,content_transformer(removeNumbers))
corp.clean<-tm::tm_map(corp.clean,content_transformer(stripWhitespace))
corp.clean<-tm::tm_map(corp.clean,content_transformer(tolower))
corp.clean<-tm::tm_map(corp.clean,content_transformer(removeWords),stopwords("english"))

tdm<-tm::TermDocumentMatrix(corp.clean)

#wc<-Narrative::wordCount(t(tdm))

tdm.2<-Narrative::tdmGenerator(seq(1,2,by=1),corp.clean)

#save.image(file=paste(wd,"matched_materials_small.RData",sep=""))
load(file = "matched_materials.RData")
