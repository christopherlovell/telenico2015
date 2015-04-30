
wd<-"C:/Users/Chris/Documents/Data Science Files/telenico2015/"
setwd(wd)

library(RMongo)
library(tm)
library(Narrative)
library(rjson)

candidates<-read.csv(file = "candidates.csv",sep = ",")

mg1 <- RMongo::mongoDbConnect('mydb')
print(RMongo::dbShowCollections(mg1))

query <- RMongo::dbGetQuery(rmongo.object = mg1,collection =  'tweets', "{created_at: {$gt: { $date: '2015-04-20T00:00:00Z'}}}")

# unpack user json
query.user<-lapply(query[,"user"],function(x) rjson::fromJSON(x))

# unfold user data
query.user<-data.frame()
i<-1
while(i<=nrow(query)){
  print(i)
  if(nchar(query[i,"user"])<1){
    query.user<-rbind(query.user,c(NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA))
  }else{
    query.user<-rbind(query.user,as.data.frame(rjson::fromJSON(query[i,"user"])[c("followers_count","verified","created_at","screen_name","favourites_count","lang","name","geo_enabled","location","friends_count","description","statuses_count","listed_count")]))  
  }
  i<-i+1
}

# append to query data frame
query<-cbind(query,query.user)
rm(query.user)

match(candidates[1,"twitter_username"],query[,"screen_name"])


corp<-tm::VCorpus(tm::VectorSource(query$text))

# get all meta fields, ignoring content
meta_fields<-names(query)[!(names(query) %in% c("text"))]

i<-1
while(i<=nrow(query)){
  for(j in meta_fields){
    if(is.factor(query[i,j])){
      meta(corp[[i]],tag=j,type="indexed")<-as.character(query[i,j])
    }else{
      meta(corp[[i]],tag=j,type="indexed")<-query[i,j]
    }
  }
  i<<-i+1
}
rm(i,j,meta_fields)
gc()

as.Date(unlist(meta(corp[[1]],tag="created_at")),format="%a %b %d %H:%M:%S %z %Y")

clean_non_ascii <- function(x){iconv(x, "latin1", "ASCII", sub="")}
corp.clean<-tm::tm_map(corp,content_transformer(clean_non_ascii))
corp.clean<-tm::tm_map(corp.clean,content_transformer(removePunctuation),preserve_intra_word_dashes=T)
corp.clean<-tm::tm_map(corp.clean,content_transformer(removeNumbers))
corp.clean<-tm::tm_map(corp.clean,content_transformer(stripWhitespace))
corp.clean<-tm::tm_map(corp.clean,content_transformer(tolower))
corp.clean<-tm::tm_map(corp.clean,content_transformer(removeWords),stopwords("english"))

tdm<-tm::TermDocumentMatrix(corp.clean)

tm::findFreqTerms(tdm,lowfreq = 30)

terms<-c("immigration")
search.result<-as.matrix(t(tdm[terms,]))
xts.search<-Narrative::xtsGenerate(do.call(c,meta(corp.clean,"created_at")),search.result)
xts.search.aggregate<-Narrative::xtsAggregate(xts.search,time_aggregate="daily",normalisation = T)
names(xts.search.aggregate)<-terms

sent.zoo<-as.zoo(xts.search.aggregate)

p<-autoplot(na.approx(sent.zoo),facet=NULL)
p+xlab("Year")+ylab("Normalised Count")

rm(xts.search.aggregate,xts.search.aggregate,search.result,xts.search)
