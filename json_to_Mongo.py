from datetime import datetime
import pickle as pkl
import pandas as pd
import re
import ast
import mmap
from os import listdir
from os.path import isfile, join
import csv
import pymongo
import json

def mapcount(filename):
    f = open(filename, "r+")
    buf = mmap.mmap(f.fileno(), 0)
    lines = 0
    readline = buf.readline
    while readline():
      lines += 1
    return lines

# This function was for the test format that Deb had sent
def convert_list_to_dict(l):
    d = {}
    # Tweet Record
    # UID,Datetime,User Name,Screen Name,Description,URLs,Tweet Text,Tweet Lang,Trigrams,Profile Picture URL,Original Text,No User Mentions,No URLs,No Tweets,No Replies,No Likes,No Hashtags,No Friends,No Followers,Mentions,Hashtags,Clean Tweet,Bigrams,TweetID,TweetIDStr,truncated,Location,TweetReplyNo,TweetReTweetNo,TweetFavoriteNo,RetweetedStatus
    d_name = ["UID", "Datetime", "User Name", "Screen Name", "Description", "URLs", "Tweet Text", "Tweet Lang", "Trigrams", "Profile Picture URL", "Original Text", "No User Mentions", "No URLs", "No Tweets", "No Replies", "No Likes", "No Hashtags", "No Friends", "No Followers", "Mentions", "Hashtags", "Clean Tweet", "Bigrams", "TweetID", "TweetIDStr", "truncated", "Location", "TweetReplyNo", "TweetReTweetNo", "TweetFavoriteNo", "RetweetedStatus"]
    for (i, e) in enumerate(l):
        d[d_name[i]] = e
        
    if (len(d["Tweet Text"]) >= 2 and d["Tweet Text"].lstrip()[:2] == "RT"):
        d["RetweetedStatus"] = "True"
     
    return d
    
def convert_list_to_dict_tweet_format(l):
    d = json.loads(l)
    d["created_at"] = datetime.strptime(d["created_at"], '%a %b %d %H:%M:%S +0000 %Y')
    d["user"]["created_at"] = datetime.strptime(d["user"]["created_at"], '%a %b %d %H:%M:%S +0000 %Y')
    d["UID"] = d["user"]["id"]
    
    if (len(d["text"]) >= 2 and d["text"].lstrip()[:2] == "RT"):
        d["retweeted"] = True
        
    if (d["truncated"] and "extended_tweet" in d and "full_text" in d["extended_tweet"]):
        d["text"] = d["extended_tweet"]["full_text"]
        
    if ("retweeted_status" in d and "extended_tweet" in d["retweeted_status"] and "full_text" in d["retweeted_status"]["extended_tweet"]):
        d["text"] = d["retweeted_status"]["extended_tweet"]["full_text"]

    return d
        
#     d = json.loads(re.escape(l[0]))
# #     d = (ast.literal_eval(re.escape(l[0]).lstrip().rstrip()))
#     if (d["truncated"]):
#         print("gets here")
#         print(d)
#         exit()
#     return d

model = None
def load_model():
    global model
    with open('./adaboost_model', 'rb') as clf:
        model = pkl.load(clf)

def isBot(user):
    x_in = [user["statuses_count"], user["followers_count"], user["friends_count"], user["favourites_count"], user["listed_count"], \
          int(user["verified"] == True), int(user["verified"] == False), int(user["protected"] == True), int(user["protected"] == False)]
  
  # predict
    prediction = model.predict([x_in])[0]

    if (prediction != 1):
        return True
    return False

def insert_records():
    load_model()
       
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    
    mydb = myclient["VP_Debate_Test_v6"]
    
#     db.RetweetTweets.find( {"text": {$regex: /RT/}, "retweeted": true}).limit(1)[0]["retweeted_status"]
    
    non_bots = mydb["NonBotAccounts"]
    non_bots.create_index("id", unique=True)
    
    bots = mydb["BotAccounts"]
    bots.create_index("id", unique=True)

    source_tweets = mydb["SourceTweets"]
    source_tweets.create_index("id", unique=True)

    quoted_tweets = mydb["QuoteTweets"]
    quoted_tweets.create_index("id", unique=True)

    retweet_tweets = mydb["RetweetTweets"]
    retweet_tweets.create_index("id", unique=True)
        
    bot_tweets = mydb["BotTweets"]
    bot_tweets.create_index("id", unique=True)

    print(myclient.list_database_names())
    
    data_path = "./test/" #"vp_debate/"
    files_in_dir = [data_path + "/" + f for f in listdir(data_path) if isfile(data_path + "/" + f)]
    
    retweet_count = 0
    source_count = 0
    quoted_count = 0
    duplicate = 0
    for file in files_in_dir:
        local_count = 0
        
        tweet_full_collection = []
        lines = [line.rstrip('\n') for line in open(file)]
        print (file)
        print (len(lines))
        tweet_full_collection.extend(lines)
            
        data_python = json.loads(json.dumps(tweet_full_collection))
        
#         with open (file, "r") as inptr:
#             print("Started processing ", file)
#             reader = csv.reader(inptr)
#             next(reader, None)  # skip the headers

        for row in data_python:
            record = convert_list_to_dict_tweet_format(row)          
            
            if (not isBot(record["user"])):
                try:
                    non_bots.insert_one(record["user"])
                except pymongo.errors.DuplicateKeyError as e:
                    pass
                
                try:               
                    if (record["retweeted"]): 
                        retweet_tweets.insert_one(record)
                        retweet_count += 1
                        local_count += 1    
                    elif ("quoted_status" in record):
                        quoted_tweets.insert_one(record)
                        quoted_count += 1
                        local_count += 1   
                    else:
                        source_tweets.insert_one(record)
                        source_count += 1
                        local_count += 1
                except pymongo.errors.DuplicateKeyError as e:
                    try:
                        print("Duplicate ", record["id"])
                        duplicate += 1
                    except:
                        print("Duplicate (full record) ", record)
            
            else:
                try:
                    bot_tweets.insert_one(record)
                    bots.insert_one(record["user"])
                    
                except pymongo.errors.DuplicateKeyError as e:
                    try:
                        print("Bot Tweet Duplicate ", record["id"])
                        duplicate += 1
                    except:
                        print("Duplicate (full record) ", record)

            if (local_count != 0 and local_count % 10000 == 0):
                print("DuplicateCount = ", duplicate, " LocalCount = ", local_count, " RetweetCount = ", retweet_count, " SourceCount = ", source_count, " QuotedCount = ", quoted_count)
                
        print("GlobalCount = ", retweet_count)

if __name__ == "__main__":
#     setup_db()
#     file = "vp_debate_sample.csv"
#     file = "small_sample.csv"
    insert_records()

    #     RT @DrDenaGrayson: NO WAY should @KamalaHarris or @JoeBiden go anywhere the Branch Covidians.\u1F9A0\u1F637\n\n#BranchCovidians #TrumpVirus #GOPSuperSpre\u2026   
    # all retweets containing "NO WAY should ... "
#     tweets = (retweet_tweets.find({
#         "text": {"$regex" : "NO WAY should @KamalaHarris or @JoeBiden go anywhere the Branch Covidians"}
#     }))
    
#     # all UIDS of these tweets
#     uids = [tweet["UID"] for tweet in tweets]
    
#     # how many of these were made by bots
#     tmp_count = 0
#     for uid in uids:
#         if (non_bots.count_documents({ "id": uid, "created_at": {"$gt": datetime(2015, 1, 1)} }) != 0):
#             tmp_count += 1
        
            
#     print("Total tweets = ", len(uids), " out of which ", tmp_count, " were by non-bots that were created after 2015")
#     exit()