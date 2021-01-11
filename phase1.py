import sys
#create/run a mongodb server on local to use
#sudo systemctl start mongod
#sudo systemctl stop mongod
import pymongo
from pymongo import MongoClient
import json
import re
def main():
  global db
  #pass in param port number from command line  
  portNumber = sys.argv[1]
  #conect to local host mongodb on a given port number
  client = MongoClient(host = 'localhost', port = int(portNumber))

  #if database 291 dne then create it (check if in dblist)
  dblist = client.list_database_names()
  if "291db" not in dblist:
    db = client["291db"]
  else:
    db = client["291db"]
    print("made db")


  #read in files posts.json, tags.json, votes.json
  #with open("PostsTest.json") as posts_json:
  with open("Posts.json") as posts_json:
  #   #json.load returns a dict obj 
  #   #(use insert_one later bc this is a single document)
    postsData = json.load(posts_json)

  #with open("VotesTest.json") as votes_json:
  with open("Votes.json") as votes_json:
    #json.load returns a dict obj 
    #(use insert_one later bc this is a single document)
    votesData = json.load(votes_json)

  #with open("TagsTest.json") as tags_json:
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

 
  termsOfBodyStr = []
  finalTerms = []
  termsOfTitleStr = []
  print("starting updating terms")
  for postValue in postsData.values():
    for rowValue in postValue.values():
      for x in rowValue:
    
        x["Terms"] = []
        for key in x.keys():
          #print(t)

          #if the key title exists in the post:
          #split it by white space and punctuation
          #append all terms with len >= 3 to terms 
          if key == "Title":
            termsOfTitleStr = re.split(r'\W+', x["Title"])
            for term in termsOfTitleStr:
              #print(term, len(term))
              if len(term) >= 3:
                #finalTerms.append(term)
                x["Terms"].append(term.lower())

          #if the key Body exists in the post:
          #split it by white space and punctuation
          #append all terms with len >= 3 to terms 
          if key == "Body":
            termsOfBodyStr = re.split(r'\W+', x["Body"])
            for term in termsOfBodyStr:
              #print(term, len(term))
              if len(term) >= 3:
                #finalTerms.append(term)
                x["Terms"].append(term.lower())
          
          if key == "Tags":
            #tagsTerms = re.split('\W+',x["Tags"])
            tagsTerms = re.split(r'<|>',x["Tags"])
            for term in tagsTerms:
              #print("term tag:", term)
              if len(term) >= 3:
                #print(term)
                #finalTerms.append(term)
                x["Terms"].append(term.lower())

        finalTerms.clear()
        termsOfBodyStr.clear()
        termsOfTitleStr.clear()
       

  print("starting creating database")

  posts = db["posts"]
  #create posts collection
  for postValue in postsData.values():
    for rowValue in postValue.values():
      posts.insert_many(rowValue)

  #create the tags collection
  tags = db["tags"]
  #it is list so insert many 
  for tagsValue in tagsData.values():
    for rowValue in tagsValue.values():
      tags.insert_many(rowValue)
      

  #create the votes collection
  votes = db["votes"]
  for votesValue in votesData.values():
    for rowValue in votesValue.values():
      votes.insert_many(rowValue)

  db.posts.create_index("Terms")
  

  print("DONE")
main()