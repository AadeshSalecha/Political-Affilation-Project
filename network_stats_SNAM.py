from numpy import random
import igraph
import csv
from os import listdir
from os.path import isfile, join
import sys
import os
import time
import numpy as np
import pickle as pkl
import json
import pickle

model = None

# model = load_model('./Models/warproxxx.h5')
x = np.array ([[205, 75, 635, 4487, 0, 0, 1, 0, 1], [22, 1, 45, 11, 0, 0, 1, 0, 1], [1105, 1745, 386, 1245, 3, 0, 1, 0, 1], [399, 673, 423, 571, 0, 0, 1, 0, 1], [29, 27, 158, 1105, 0, 0, 1, 0, 1], [49693, 13591, 214, 7373, 206, 0, 1, 0, 1], [52, 7, 146, 374, 0, 0, 1, 0, 1], [3591, 328, 1011, 5012, 0, 0, 1, 0, 1], [62, 12, 88, 507, 0, 0, 1, 0, 1], [2984, 223, 607, 2778, 1, 0, 1, 0, 1], [43, 10, 344, 598, 0, 0, 1, 0, 1], [29, 27, 158, 1105, 0, 0, 1, 0, 1]])

def load_model():
  global model
  with open('./adaboost_model', 'rb') as clf:
    model = pkl.load(clf)

def format_convert(d):
  m = [d["statuses_count"], d["followers_count"], d["friends_count"], d["favourites_count"], d["listed_count"]]

  # bg_image_false bg_image_true 
  # m.extend([0, 1])

  # verified_false verified_true 
  m.extend([int(d["verified"] == True), int(d["verified"] == False)])

  # protected_false protected_true
  m.extend([int(d["protected"] == True), int(d["protected"] == False)])

  return m

def make_folder(name):
  if not os.path.exists(name):
    os.makedirs(name)

def isBot(row):
  # x_in = [user.statuses_count, user.followers_count, user.friends_count, user.favourites_count, user.listed_count, \
  #         int(user.verified == True), int(user.verified == False), int(user.protected == True), int(user.protected == False)]
  x_in = [int(row[14]), int(row[7]), int(row[8]), int(row[11]), int(row[9]), \
                      int(row[13] == "True"), int(row[13] == "False"), \
                      int(row[6] == "True"), int(row[6] == "False"), ]
  # predict
  prediction = model.predict([x_in])[0]

  return (prediction != 1)
    

def anaylze_complete_initial_sample():
  # in_file = "vp_debate_merged.csv"

  # with open(in_file, "r") as inptr:
  #   for i in range(100):
  #     print(inptr.readline().rstrip('\n'))
  # exit()

  global model
  load_model()

  # Setup stats folder
  folder_name = "Bot_Stats/" + time.strftime("%Y%m%d%H%M%S")
  make_folder(folder_name)

  # open each network one at a time
  with open(in_file, "r") as inptr:
    with open(out_file, "w") as outptr:
      while True:
        line = inptr.readline()
        if (not line):
            break

        total_users += 1
        # Print progress
        if (total_users % 1000 == 0):
          print(network, " ", total_users)
        if (total_users % 10000 == 0):
          pickle.dump( [total_users, bots_in_network] , open("./" + network + ".pk", "wb" ) )

        # process dictionary + # convert to array
        x_in = format_convert(json.loads(raw_str))
        
        # predict
        prediction = model.predict([x_in])[0]

        if (prediction != 1):
          outptr.write()
          total_bots += 1
          
    # output network info file
    print("Total bots = " + str(total_bots) + "\n")
    print("Total users = " + str(total_users) + "\n")

node_to_id = {}
counter = 1
# Setup global graph
g = igraph.Graph(directed = True)

def getNodeID(s):
  # return node_to_id[s]
  return s

def storeNode(s):
  global counter
  if (s not in node_to_id):
    node_to_id[s] = counter
    counter += 1

import matplotlib.pyplot as plt
import numpy as np

def plot_degree_distribution(degrees):
  plt.hist(degrees, bins = 10)
  plt.yscale('log')
  plt.show()

  # plt.rcParams.update({'font.size': 25})

  # x = [x for x in range(max(degrees)+1)]
  # degree_counts = [0 for x in range(100)]

  # for i in degrees:
  #   degree_counts[i] += 1

  # print("Degree having the maximum number of vertices:", degree_counts.index(max(degree_counts)))
  # print("Number of vertices having the most abundant degree:", max(degree_counts))

  # plt.figure(figsize=(40,10))
  # plt.plot(x, degree_counts, linewidth=3.0)
  # plt.ylabel('Number of vertices having the given degree')
  # plt.xlabel('Degree')
  # plt.title('Degree Distribution of Vertices in the CiteSeer Graph')

  # plt.xlim(0,100)
  # plt.xticks(np.arange(min(x), max(x)+1, 2.0))
  # plt.grid(True)
  # plt.savefig('degree_distribution.png', bbox_inches='tight')
  # plt.show()
  # plt.draw()
# def first_level_degree():
  degrees = []
  cur_files = ["./All_Followers/" + f for f in listdir("./All_Followers/") if isfile("./All_Followers/" + "/" + f)]
  for (i, f) in enumerate(cur_files):
    print(i, f)

    with open(f, mode='r') as inptr:
      reader = csv.reader(inptr)
      for row in reader:
        try:
          if (row[0] == "Error"):
            i += 1
          
          elif (row[0] == "Account belongs to bot"):
            i += 1

          else:
            degrees.append(int(row[7]) + int(row[8]))
        except Exception as e:
          print(e)
          exit()

  plot_degree_distribution(degrees)

                  #  writer.writerow([uid, user.id, user.name, user.screen_name, user.location, \
                  #           user.description, \
                  #           user.protected, user.followers_count, user.friends_count, \
                  #           user.listed_count, user.created_at, user.favourites_count, \
                  #           user.geo_enabled, user.verified, \
                  #           user.statuses_count])

def basic_stats():
  global g
  global node_to_id

  # Pickle load
  if isfile("./SavedData/stats") and isfile("./SavedData/g") and isfile("./SavedData/all_nodes"): 
    print("Using saved data")
    with open("./SavedData/stats", "rb") as inptr:
      (num_follower_scraped, num_friends_scraped, first_level_bots, second_level_bots, all_edges, error_dict) = pkl.load(inptr)
  
    with open("./SavedData/g", "rb") as inptr:
      g = pkl.load(inptr)

    with open("./SavedData/all_nodes", "rb") as inptr:
      all_nodes = pkl.load(inptr)
  
  else: 
    small_graph = ['1317891095624359936', '1060210407816736768', '1056186431981412352', '4637042115', '1251766229993431040', '732903753070809088', '831837628941086721', '1390729892644466690', '577767726', '35287235', '1320424215309250560', '1383414666907377667', '1019752780754399233', '1359193453368909825', '947683015634731009', '150369838', '51548332', '805232373247971329', '1240566579806470144', '1341868492635721731', '593258460', '2382750942', '855428178298511360', '3282759936', '1176968840795676672', '1225144211936120832', '801873524482863104', '565846780', '172602592', '2314208179', '1203696542214811648', '860807042', '223705296', '976014320478969856', '16022444', '773736691', '3321496300', '3245126146', '582932632', '137851125', '22875347', '1881402330', '23982121', '3109303102', '2895136126', '16368643', '475224726', '1187799573533736960', '271027174', '198770199', '247334270', '567911598', '19576894', '713157058871300097', '223118993', '1109015748', '919458265', '3510138854', '28064611', '225096034', '632327307', '740590839177617408', '17606706', '2604441055', '1279416968559292417', '1167476245694599168', '22778877', '822498529302212608', '241089047', '932104233469562880', '1150368282828005376', '836643193', '1259440570956685312', '56929231', '1209131834623320064', '861054567517229057', '1972641', '2557301882', '240111318', '1151174115467939840', '1059779460914864131', '3230920029', '2808757106', '1256585539161120769', '45241761', '2413913117', '100949009', '1143376314', '74913466', '2870986967', '34391224', '723255965475172352', '785795854183440385', '2203172486', '65279376', '1114631166', '2393578705', '1398317413', '117612202', '1216436361529454592']

    print("Getting latest data")
    # Bot detection model
    load_model()

    # Setup counters
    all_nodes = {}
    all_edges = 0

    cur_files = ["./All_Followers/" + f for f in listdir("./All_Followers/") if isfile("./All_Followers/" + "/" + f)]
    num_follower_scraped = len(cur_files)

    cur_files += ["./All_Friends/" + f for f in listdir("./All_Friends/") if isfile("./All_Friends/" + "/" + f)]
    num_friends_scraped = len(cur_files) - num_follower_scraped

    first_level_bots = 0
    second_level_bots = {}

    error_dict = {}
    print("Total files to process = ", len(cur_files))

    # Process file by file
    for (i, f) in enumerate(cur_files):
      print(i, f)

      with open(f, mode='r') as inptr:
        reader = csv.reader(inptr)
        for row in reader:
          try:
            if (row[0] == "Error"):
              error_dict[row[1]] = error_dict.get(row[1], 0) + 1
            
            elif (row[0] == "Account belongs to bot"):
              first_level_bots += 1

            else:
              # Change depending on small_graph or not
              if(True) #(row[0] in small_graph):

                if(row[0] not in all_nodes):
                  g.add_vertex(name = row[0])

                all_nodes[row[0]] = all_nodes.get(row[0], 0) + 1
                
                # if follower / friend is being added first time
                if (row[1] not in all_nodes):
                  g.add_vertex(name = row[1])

                  # check if bot
                  if (isBot(row)):
                    second_level_bots[row[1]] = 0
                
                # add edge to graph
                g.add_edge((row[0]), (row[1]))

                # update counters
                all_nodes[row[1]] = all_nodes.get(row[1], 0) + 1
                all_edges += 1

          except Exception as e:
            first_level_bots += 1
            print(row, e)

    # Pickle dump
    with open("./SavedData/stats", "wb") as outptr:
      pkl.dump((num_follower_scraped, num_friends_scraped, first_level_bots, second_level_bots, all_edges, error_dict), outptr)
    with open("./SavedData/g", "wb") as outptr:
      pkl.dump(g, outptr)
    with open("./SavedData/all_nodes", "wb") as outptr:
      pkl.dump(all_nodes, outptr)
    
  # Stat printout
  print("Users for which followers are scraped = ", num_follower_scraped)
  print("Users for which followers are scraped = ", num_friends_scraped)
  print()

  print("First level bots = ", first_level_bots)
  print("Second level bots = ", len(second_level_bots))
  print()

  
  print("Total edges = ", all_edges)
  print("Total nodes = ", len(all_nodes))
  print("Density of the graph:", 2*g.ecount()/(g.vcount()*(g.vcount()-1)))

  degrees = []
  bins = []
  total_degree = 0

  for node in all_nodes:
    neighbours = g.neighbors(node, mode='ALL')
    total_degree += len(neighbours)
    degrees.append(len(neighbours))

  # Small graph degree plotting for first level
  # small_graph = ['1317891095624359936', '1060210407816736768', '1056186431981412352', '4637042115', '1251766229993431040', '732903753070809088', '831837628941086721', '1390729892644466690', '577767726', '35287235', '1320424215309250560', '1383414666907377667', '1019752780754399233', '1359193453368909825', '947683015634731009', '150369838', '51548332', '805232373247971329', '1240566579806470144', '1341868492635721731', '593258460', '2382750942', '855428178298511360', '3282759936', '1176968840795676672', '1225144211936120832', '801873524482863104', '565846780', '172602592', '2314208179', '1203696542214811648', '860807042', '223705296', '976014320478969856', '16022444', '773736691', '3321496300', '3245126146', '582932632', '137851125', '22875347', '1881402330', '23982121', '3109303102', '2895136126', '16368643', '475224726', '1187799573533736960', '271027174', '198770199', '247334270', '567911598', '19576894', '713157058871300097', '223118993', '1109015748', '919458265', '3510138854', '28064611', '225096034', '632327307', '740590839177617408', '17606706', '2604441055', '1279416968559292417', '1167476245694599168', '22778877', '822498529302212608', '241089047', '932104233469562880', '1150368282828005376', '836643193', '1259440570956685312', '56929231', '1209131834623320064', '861054567517229057', '1972641', '2557301882', '240111318', '1151174115467939840', '1059779460914864131', '3230920029', '2808757106', '1256585539161120769', '45241761', '2413913117', '100949009', '1143376314', '74913466', '2870986967', '34391224', '723255965475172352', '785795854183440385', '2203172486', '65279376', '1114631166', '2393578705', '1398317413', '117612202', '1216436361529454592']
  # for user in small_graph:
  #   degrees.append(len(g.neighbors(user, mode = 'ALL')))
  # total_degree = sum(degrees)
  
  # bins of size 3342
  # the following code was to test out Debarthi's sampling approach
  # by making a list of users that we would need to use
  # low = []
  # med = []
  # high = []
  # for node in all_nodes:
  #   neighbours = g.neighbors(node, mode = 'ALL')
  #   if (len(neighbours) < 3342 and len(low) < 20):
  #     print(len(neighbours), end = ', ')
  #     low.append(node)
  #   elif (len(neighbours) > 3342 and len(neighbours) < 3342 * 2 and len(med) < 60):
  #     print(len(neighbours), end = ', ')
  #     med.append(node)
  #   elif (len(neighbours) > 3342 * 2 and len(high) < 20):
  #     print(len(neighbours), end = ', ')
  #     high.append(node)
  # print()
  # print(low+med+high)
  
  # print(len(low), len(med), len(high))


  print("Average degree:", total_degree/len(all_nodes))
  print("Maximum degree:", max(degrees))
  
  print()

  print ("Error dict = ", error_dict)
  print()

  print(g.summary())

  plot_degree_distribution(degrees)

  second_level_bots_edges = 0
  for bot in second_level_bots:
    second_level_bots[bot] = all_nodes[bot]
    second_level_bots_edges += all_nodes[bot]

  print("Total edges by first-level bots = ", 0)
  print("Total edges by second-level bots = ", second_level_bots_edges)

  print("\n\n\n10 Most common users by edge number\n\nScreen_name : Indegree + Outdegree")
  count = 0
  for key, value in sorted(all_nodes.items(), key=lambda p:p[1], reverse=True):
    print(key + ": " + str(value))
    count += 1
    if(count > 10):
      break

if __name__ == "__main__":
  basic_stats()
  # first_level_degree()