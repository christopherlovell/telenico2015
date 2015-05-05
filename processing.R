
wd<-"C:/Users/Chris/Documents/Data Science Files/telenico2015/"
setwd(wd)

library(tm)
library(Narrative)
library(rjson)
library(data.table)
library(dplyr)

library(zoo)
library(ggplot2)

# RMongo attempt
# library(RMongo)
# mg1 <- RMongo::mongoDbConnect('mydb')
# print(RMongo::dbShowCollections(mg1))
# 
# query <- RMongo::dbGetQuery(rmongo.object = mg1,collection =  'tweets', "{created_at: {$gt: { $date: '2015-04-20T00:00:00Z'}}},{text: 1, favorite_count: 1,}")
# 
# # unpack user json
# query.user<-lapply(query[,"user"],function(x) rjson::fromJSON(x))

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

rm(mongo,coll,cursor,buf)

# match screen name with candidates data. return ordered data from candidates
candidate_fields<-c("party","constituency","name","gender","birth_date","honorific_prefix","party_id")
tweet.data<-lapply(tweets.dt[["user.screen_name"]],function(x) candidates[match(x,candidates[,"twitter_username"]),candidate_fields])

# convert to data frame
tweet.data.df<-dplyr::rbind_all(lapply(tweet.data, data.frame, stringsAsFactors = FALSE))

# convert data types
tweet.data.df[,"birth_date"]<-lapply(tweet.data.df[,"birth_date"],as.Date)
#tweet.data.df[,"gender"]<-lapply(tweet.data.df[,"gender"],function(x) as.String(tolower(x)))
rm(tweet.data)

tweet.meta.data<-data.table()
tweet.data.df

#save.image(file=paste(wd,"matched_materials.RData",sep=""))

corp<-tm::VCorpus(tm::VectorSource(tweets.dt[["text"]]))

# get all meta fields, ignoring content
meta_fields<-names(tweets.dt)[!(names(tweets.dt) %in% c("text"))]

clean_non_ascii <- function(x){iconv(x, "latin1", "ASCII", sub="")}
corp.clean<-tm::tm_map(corp,content_transformer(clean_non_ascii))
corp.clean<-tm::tm_map(corp.clean,content_transformer(removePunctuation),preserve_intra_word_dashes=T)
corp.clean<-tm::tm_map(corp.clean,content_transformer(removeNumbers))
corp.clean<-tm::tm_map(corp.clean,content_transformer(stripWhitespace))
corp.clean<-tm::tm_map(corp.clean,content_transformer(tolower))
corp.clean<-tm::tm_map(corp.clean,content_transformer(removeWords),stopwords("english"))

tdm<-tm::TermDocumentMatrix(corp.clean)

tm::findFreqTerms(tdm,lowfreq = 30)

terms<-c("nhs")
normalise<-T
search.result<-as.matrix(t(tdm[terms,]))

logic.labour<-as.logical(tweet.data.df$party=="Labour Party")
logic.labour[is.na(logic.labour)]<-F
xts.search<-Narrative::xtsGenerate(tweets.dt[logic.labour]$created_at,search.result[logic.labour])
xts.search.aggregate<-Narrative::xtsAggregate(xts.search,time_aggregate="daily",normalisation = normalise)
names(xts.search.aggregate)<-"labour"

logic.ukip<-as.logical(tweet.data.df$party==c("UK Independence Party (UK I P)","UK Independence Party (UKIP)"))
logic.ukip[is.na(logic.ukip)]<-F
xts.search<-Narrative::xtsGenerate(tweets.dt[logic.ukip]$created_at,search.result[logic.ukip])
xts.search.aggregate.ukip<-Narrative::xtsAggregate(xts.search,time_aggregate="daily",normalisation = normalise)
names(xts.search.aggregate.ukip)<-"UKIP"

logic.conservative<-as.logical(tweet.data.df$party=="Conservative Party")
logic.conservative[is.na(logic.conservative)]<-F
xts.search<-Narrative::xtsGenerate(tweets.dt[logic.conservative]$created_at,search.result[logic.conservative])
xts.search.aggregate.conservative<-Narrative::xtsAggregate(xts.search,time_aggregate="daily",normalisation = normalise)
names(xts.search.aggregate.conservative)<-"conservative"

logic.liberals<-as.logical(tweet.data.df$party=="Liberal Democrats")
logic.liberals[is.na(logic.liberals)]<-F
xts.search<-Narrative::xtsGenerate(tweets.dt[logic.liberals]$created_at,search.result[logic.liberals])
xts.search.aggregate.liberals<-Narrative::xtsAggregate(xts.search,time_aggregate="daily",normalisation = normalise)
names(xts.search.aggregate.liberals)<-"liberals"

logic.green<-as.logical(tweet.data.df$party=="Green Party")
logic.green[is.na(logic.green)]<-F
xts.search<-Narrative::xtsGenerate(tweets.dt[logic.green]$created_at,search.result[logic.green])
xts.search.aggregate.green<-Narrative::xtsAggregate(xts.search,time_aggregate="daily",normalisation = normalise)
names(xts.search.aggregate.green)<-"green"

#sent.zoo<-zoo::as.zoo(xts.search.aggregate.ukip)
sent.zoo<-zoo::as.zoo(cbind(xts.search.aggregate,
                            xts.search.aggregate.ukip,
                            xts.search.aggregate.conservative,
                            xts.search.aggregate.liberals,
                            xts.search.aggregate.green))

p<-ggplot2::autoplot(na.approx(sent.zoo),facet=NULL)
p+xlab("Year")+ylab("Absolute Count")

rm(xts.search.aggregate,xts.search.aggregate,search.result,xts.search)
