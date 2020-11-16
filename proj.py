import sys
#create/run a mongodb server on local to use
#sudo systemctl start mongod
#sudo systemctl stop mongod
import pymongo
from pymongo import MongoClient
import json


#pass in param port number from command line  
portNumber = sys.argv[1]
#conect to local host mongodb on a given port number
client = MongoClient(host = 'localhost', port = int(portNumber))

#if database 291 dne then create it (check if in dblist)
dblist = client.list_database_names()
if "291db" not in dblist:
  db = client["291db"]
  print("test")

#read in files posts.json, tags.json, votes.json
with open("Posts.json") as posts_json:
  #json.load returns a dict obj 
  #(use insert_one later bc this is a single document)
  postsData = json.load(posts_json)

with open("Votes.json") as votes_json:
  #json.load returns a dict obj 
  #(use insert_one later bc this is a single document)
  votesData = json.load(votes_json)

with open("Tags.json") as tags_json:
  #json.load returns a dict obj 
  #(use insert_one later bc this is a single document)
  tagsData = json.load(tags_json)

#check if posts collections exist 
#insert into posts collection
if "posts" in db.list_collection_names():
  #if it exists we drop it and create a populate new a collection 
  posts.drop()
 

if "tags" in db.list_collection_names():
  #if it exists we drop it and create a populate new a collection 
  tags.drop()


if "votes" in db.list_collection_names():
  #if it exists we drop it and create a populate new a collection 
  votes.drop()

#if the collections dont exist 
#create the posts collection
posts = db["posts"]
#it is list so insert many 
#break posts up into single rows and insert (smaller size < doc limit)
#convert into a row of data
for postsKey, postValue in postsData.items():
  for rowKey, rowValue in postValue.items():
    #rowValue is the [] with {}obj in for each post
    #print("type rowValue: ", type(rowValue))
    for row in rowValue:
      posts.insert_one(row)

print(posts.find_one())

# #create the votes collection
# tags = db["tags"]
# #it is list so insert many 
# tags.insert_one(tagsData)

# #create the votes collection
# votes = db["votes"]
# #it is list so insert many 
# votes.insert_one(votesData)
