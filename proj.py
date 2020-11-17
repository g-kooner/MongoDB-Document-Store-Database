import sys
#create/run a mongodb server on local to use
#sudo systemctl start mongod
#sudo systemctl stop mongod
import pymongo
from pymongo import MongoClient
import json
import re


#pass in param port number from command line  
portNumber = sys.argv[1]
#conect to local host mongodb on a given port number
client = MongoClient(host = 'localhost', port = int(portNumber))

#if database 291 dne then create it (check if in dblist)
dblist = client.list_database_names()
if "291db" not in dblist:
  db = client["291db"]
  print("test")
else:
  db = client["291db"]

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
  db.posts.drop()
 

if "tags" in db.list_collection_names():
  #if it exists we drop it and create a populate new a collection 
  db.tags.drop()


if "votes" in db.list_collection_names():
  #if it exists we drop it and create a populate new a collection 
  db.votes.drop()

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


#create the tags collection
tags = db["tags"]
#it is list so insert many 
#break posts up into single rows and insert (smaller size < doc limit)
#convert into a row of data
for tagsKey, tagsValue in tagsData.items():
  for rowKey, rowValue in tagsValue.items():
    #rowValue is the [] with {}obj in for each post
    for row in rowValue:
      tags.insert_one(row)



#create the votes collection
votes = db["votes"]
for votesKey, votesValue in votesData.items():
  for votesKey, votesValue in votesValue.items():
    #rowValue is the [] with {}obj in for each post
    for row in rowValue:
      votes.insert_one(row)



#extract all terms of len 3 char or more in posts title and body
#create index on this array of terms in posts collections


postsCursor = db.posts.find({}, {"Title":1}, {"Body":1})
for documents in postsCursor:
  titleTerms = []
  #print(title)
  #gives {'_id':'...' , 'Title': '...'}
  #extract
  for titleKeys ,titleValues in documents.items():
    #print(titleValues) 
    #print(documents[titleKeys])
    #title extraction
    if(titleKeys == "Title"):
      #print(documents["Title"])
      #gives string titles for each post that has a title 
      terms = re.split(r'\W+', documents["Title"])
      #print(terms)
      for word in terms:
        if len(word) >= 3:
          #removes word if length is less than 3
          titleTerms.append(word)
    #title extraction

  print(titleTerms)


      
















