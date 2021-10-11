import ast
import os
from datetime import datetime
import time
import tweepy 
import json
import csv
import sys

import time
import mmap
import random
from collections import defaultdict

def mapcount(filename):
    lines = 0
    for line in open(filename):
        lines += 1
    return lines

# Usage: api_version.py [api_keys_file] [input] [key_no]

api = None
def read_api_keys():
  all_keys = []
  with open(sys.argv[1], "r") as inptr:
    for i in range(75):
      inptr.readline()
      consumer_key=inptr.readline().lstrip("consumer_key=\"").rstrip("\"\n")
      consumer_secret=inptr.readline().lstrip("consumer_secret=\"").rstrip("\"\n")
      access_key=inptr.readline().lstrip("access_key=\"").rstrip("\"\n")
      access_secret=inptr.readline().lstrip("access_secret=\"").rstrip("\"\n")
      inptr.readline()

      all_keys.append([consumer_key, consumer_secret, access_key, access_secret])
  
  return all_keys

def setup_tweepy():
  global api

  api_keys = read_api_keys()
  key_num = int(sys.argv[3]) - 1

  # assign the values accordingly 
  consumer_key=api_keys[key_num][0].lstrip().rstrip()
  consumer_secret=api_keys[key_num][1].lstrip().rstrip()
  access_key=api_keys[key_num][2].lstrip().rstrip()
  access_secret=api_keys[key_num][3].lstrip().rstrip()
  
  # authorization of consumer key and consumer secret 
  auth = tweepy.OAuthHandler(consumer_key, consumer_secret) 
    
  # set access to user's access key and access secret  
  auth.set_access_token(access_key, access_secret) 
    
  # calling the api  
  api = tweepy.API(auth, wait_on_rate_limit = True, wait_on_rate_limit_notify = True)

def reformat(data):
  data = data.split("},")
  ans = []
  for i in range(len(data)-1):
    tweet = data[i]
    try:
      tweet = tweet + "}"
      ans.append(json.loads(tweet))
    except:
      print("Error in reformat ", tweet, i, len(data), data[i-1])
      exit()
  return ans

def convert_line_to_dict(line): 
  return ast.literal_eval(line)

# with open("en_geo_2020-03-01.json_categorized.csv", "r") as inptr:
#   for line in inptr:
#     d = convert_line_to_dict(line)

# def classify(tweet_id, username):
#   global api
#   try:
#     status = api.get_status(tweet_id) 
#   except Exception as e:
#     print(e)
#     return ("deleted", "")

#   if (status.in_reply_to_status_id != None):
#     return ("reply", status)
#   elif (username.lower() != status.user.screen_name.lower() or (hasattr(status, 'retweeted_status') and status.retweeted_status == True)):
#     return ("retweet", status)
#   return ("source", status)

def classify(tweet_id, userid):
  userid = str(userid)
  status = None
  global api
  try:
    status = api.get_status(tweet_id) 
  except tweepy.RateLimitError as e:
    print("Rate limit error exceed waiting for 15 mins")
    print(datetime.now())
    time.sleep(60 * 15)
    return ("Rate limit", "")
  except Exception as e:
    if(str(e).find("Could not authenticate you") != -1 or str(e).find("Authentication") != -1 or str(e).find("token") != -1):
      print(str(e))
      exit()
    return (str(e), "")

  text = status.text.strip('\n').strip('\r')
  # print(text)
  if (status.in_reply_to_status_id != None):
    return ("reply", text)
  elif (text.find("RT @") != -1 or userid != status.user.id_str or (hasattr(status, 'retweeted_status') and status.retweeted_status == True)):
    return ("retweet", text)
  return ("source", text)

def has_US_location(tweet):
  # change it to consider only user_location
  return str(tweet).find("\'us\', \'state\'") != -1

def main():

  all_files = ['en_geo_2020-04-03-029.json', 'en_geo_2020-02-20.json', 'en_geo_2020-03-31-027.json', 'en_geo_2020-04-19-043.json', 'en_geo_2020-03-02.json', 'en_geo_2020-02-17.json', 'en_geo_2020-03-18.json', 'en_geo_2020-03-17-034.json', 'en_geo_2020-02-07.json', 'en_geo_2020-04-25.json', 'en_geo_2020-03-26-049.json', 'en_geo_2020-04-16-039.json', 'en_geo_2020-02-10.json', 'en_geo_2020-04-04.json', 'en_geo_2020-02-18.json', 'en_geo_2020-02-02.json', 'en_geo_2020-02-25.json', 'en_geo_2020-04-24-053.json', 'en_geo_2020-03-28-025.json', 'en_geo_2020-03-24.json', 'en_geo_2020-04-06-036.json', 'en_geo_2020-03-23-023.json', 'en_geo_2020-03-15.json', 'en_geo_2020-02-04.json', 'en_geo_2020-03-08.json', 'en_geo_2020-03-22.json', 'en_geo_2020-02-26.json', 'sample.json', 'en_geo_2020-04-21-050.json', 'en_geo_2020-03-20.json', 'en_geo_2020-03-05.json', 'en_geo_2020-03-13-024.json', 'en_geo_2020-02-13.json', 'en_geo_2020-03-19.json', 'en_geo_2020-02-27.json', 'en_geo_2020-04-15-055.json', 'en_geo_2020-04-20-057.json', 'en_geo_2020-04-29.json', 'en_geo_2020-03-10.json', 'en_geo_2020-04-05-030.json', 'en_geo_2020-02-21.json', 'en_geo_2020-03-01.json', 'en_geo_2020-02-12.json', 'en_geo_2020-02-23.json', 'en_geo_2020-03-12-022.json', 'en_geo_2020-04-28.json', 'en_geo_2020-04-17-042.json', 'en_geo_2020-04-11-031.json', 'en_geo_2020-03-03.json', 'en_geo_2020-04-27.json', 'en_geo_2020-03-27-020.json', 'en_geo_2020-02-28.json', 'en_geo_2020-04-14-035.json', 'en_geo_2020-02-06.json', 'en_geo_2020-04-01-046.json', 'en_geo_2020-03-04.json', 'en_geo_2020-02-16.json', 'en_geo_2020-04-07-044.json', 'en_geo_2020-04-02-054.json', 'en_geo_2020-04-09-033.json', 'en_geo_2020-03-07.json', 'en_geo_2020-03-21.json', 'en_geo_2020-02-05.json', 'en_geo_2020-04-13-041.json', 'en_geo_2020-04-26-052.json', 'en_geo_2020-02-11.json', 'en_geo_2020-02-29.json', 'en_geo_2020-03-11.json', 'en_geo_2020-04-10-058.json', 'en_geo_2020-02-01.json', 'en_geo_2020-02-08.json', 'en_geo_2020-04-12-028.json', 'en_geo_2020-03-29-032.json', 'en_geo_2020-03-25.json', 'en_geo_2020-03-16.json', 'en_geo_2020-02-03.json', 'en_geo_2020-04-30.json', 'en_geo_2020-04-08-040.json', 'en_geo_2020-02-09.json', 'en_geo_2020-04-18-038.json', 'en_geo_2020-02-22.json', 'en_geo_2020-04-22-056.json', 'en_geo_2020-02-15.json', 'en_geo_2020-03-09.json', 'en_geo_2020-04-23-048.json', 'en_geo_2020-03-30-045.json', 'en_geo_2020-02-14.json', 'en_geo_2020-02-19.json', 'en_geo_2020-03-14.json', 'en_geo_2020-02-24.json']
  all_files_without_feb = [f for f in all_files if f.find("en_geo_2020-02") == -1]
  all_files_without_feb.sort()

  # remove this line later
  # in order to bring <2% to 3%
  all_files_without_feb = ["en_geo_2020-04-14-035.json", "en_geo_2020-02-26.json", "en_geo_2020-02-27.json","en_geo_2020-02-21.json","en_geo_2020-02-12.json","en_geo_2020-02-23.json","en_geo_2020-02-28.json","en_geo_2020-02-06.json","en_geo_2020-02-16.json","en_geo_2020-02-05.json","en_geo_2020-02-11.json","en_geo_2020-02-29.json","en_geo_2020-02-01.json","en_geo_2020-02-08.json","en_geo_2020-02-03.json","en_geo_2020-02-09.json","en_geo_2020-02-22.json","en_geo_2020-02-15.json","en_geo_2020-02-14.json","en_geo_2020-02-19.json"]
  all_files_without_feb = ["en_geo_2020-02-26.json",  "en_geo_2020-02-27.json","en_geo_2020-02-21.json","en_geo_2020-02-12.json","en_geo_2020-02-23.json","en_geo_2020-02-28.json","en_geo_2020-02-06.json","en_geo_2020-02-16.json","en_geo_2020-02-05.json","en_geo_2020-02-11.json","en_geo_2020-02-29.json"]

  json_file = all_files_without_feb[int(sys.argv[2])-1]


  # print('\n\n\n\n\nTest' + json_file + (sys.argv[2]))
  # with open("./Testing/" + json_file, "w") as outptr:
  #   outptr.write("Test")
  # exit()
  
  print("Started")
  print(json_file)
  # data = None
  # with open("./Data/" + json_file, "r") as inptr:
  #   data = inptr.read()

  # all_tweets = reformat(data)
  
  setup_tweepy()

  filtered_num = 0
  processed_num = 0
  with open("./Data (Randomized)/" + json_file + "_randomized.csv", "r") as inptr:
    if (os.path.exists("./Output/" + json_file + "_categorized.csv")):
      lines = mapcount("./Output/" + json_file + "_categorized.csv")
      for i in range(lines):
        inptr.readline()
      print("Skipped ", lines, " lines")
      processed_num += lines

      mode = "a"
    else: 
      mode = "w"
    
    with open("./Output/" + json_file + "_categorized.csv", mode) as outptr:
      writer = csv.writer(outptr) 
      try:
        for line in inptr:
          tweet = json.loads(line)
          processed_num += 1
          id = tweet["tweet_id"]  

          if (has_US_location(tweet)):
            filtered_num += 1             
            tweet_label = "source"   
            tweet_label, all_fields = classify(id, tweet["user_id"])

            # You can remove all_fields from below to avoid excess data storage
            writer.writerow([{"ID": id, "label": tweet_label, "text":all_fields}])
          else:
            writer.writerow([{"ID": id, "label": "Not in US", "text": ""}])
          
          if(processed_num % 1000 == 0):
            print("Processed: ", processed_num, " Filtered: ", filtered_num)
      except Exception as e:
        print ("Error = ", e)
        exit()

if __name__ == '__main__':
  main()

  # # Examples of all three
  # setup_tweepy()

  # # print(classify(1215411257303539712, "__COVID19____"))    # Source
  # print(classify(1215411257303539712, 2699020789))    # Source
  

  # # print(classify(1215060830204637187, "cod_coronavirus"))  # Reply
  # print(classify(1215060830204637187, 950805648131985408))  # Reply
  
  # # Retweet - https://twitter.com/gppvt/status/474162466776449024, I know this is a retweet because when I go to 
  # # the link I am redirected to the original tweet - https://twitter.com/kpdepauw/status/474162466776449024
  # # print(classify(474162466776449024, "ggpvt"))             
  # print(classify(474162466776449024, 592656953))             
  