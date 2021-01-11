import sys
#create/run a mongodb server on local to use
#sudo systemctl start mongod
#sudo systemctl stop mongod
import pymongo
from pymongo import MongoClient
import json
import re
import uuid 
from datetime import datetime
def mainMenu(posts, votes, tags):
  global db

  #ask user to provide useerid or continue without one(no report)
  flag = True
  while flag:
    option = input("Would you like to provide a userID? (y/n): ")
    if option == 'y':
      userID = input("Please enter a numeric userID: ")
      try:
        intUserID = int(userID)
        flag = False
      except:
        print("Enter a valid userID")
    elif option == 'n':
      #continue without provide user id (no report)
      userID = None
      flag = False
    else:
      print("Please enter valid option")

  #if userID provided --> show report
  #(1) the number of questions owned and the average score for those questions
  #(2) the number of answers owned and the average score for those answers
  #(3) the number of votes registered for the user.
  if userID != None:
    # (1)
    questionResult = db.posts.find({"OwnerUserId": userID, "PostTypeId": "1"})
    qCount = 0
    for row in questionResult:
      #print(row)
      qCount = qCount + 1
    
    #first obj is query obj 
    #second is projection operation of which fields to keep (maybe always need to include _id)
    questionScores = db.posts.find({"OwnerUserId": userID, "PostTypeId": "1",} , {"_id": 0, "Score": 1})
    qScores = 0
    for row in questionScores:
      #print(row)
      qScores = qScores + int(row["Score"])
    if(qCount != 0):
      avgQuestionScores = (qScores / qCount)
    else:
      avgQuestionScores = 0

    print("Number of owned questions: ", qCount)
    print("Average score of owned questions: ", avgQuestionScores)

    #(2)
    answersResult = db.posts.find({"OwnerUserId": userID, "PostTypeId": "2",})
    aCount = 0
    for row in answersResult:
      aCount = aCount + 1
    
    answersScores = db.posts.find({"OwnerUserId": userID, "PostTypeId": "2",} , {"_id": 0, "Score": 1})
    aScores = 0
    for row in answersScores:
      aScores = aScores + int(row["Score"])
    if(aCount != 0):
      avgAnswerScores = (aScores / aCount)
    else:
      avgAnswerScores = 0
    
    print("Number of owned answers: ", aCount)
    print("Average score of owned answers: ", avgAnswerScores)
    
    #(3)
    #link from votes table to posts table (votes-->postID matches the id from posts)
    voteIDs = db.votes.find({"UserId": userID},{"_id": 0, "UserId": 1})
    #userPosts = db.posts.find({"OwnerUserId": userID} , {"_id": 0, "Id": 1})
    vcount = 0
    for vote in voteIDs:
      vcount = vcount + 1
    # for vid in voteIDs:
    #   for post in userPosts:
    #     if vid["PostId"] == post["Id"]:
    #       vcount = vcount + 1
    print("Number of votes registered to user: ", vcount)
  
  #query action menu
  flag = True
  while flag:
    print("\n1)Post a Question")
    print("2)Search for Questions")
    print("3)Exit the program")
    option = input("Please select an option: ")
    if option == '1':
      postQuestion(userID)
    elif option == '2':
      searchQuestion(userID)
    elif option == '3':
      exit()
    else:
      print("Please pick a valid option")

  return
#################################################################################################
#POST A QUESTION
def postQuestion(OwnerUserId):
  global db
  id = str(uuid.uuid1())

  #current date and time
  now = str(datetime.now())

  title = input("Title: ")
  body = input("Body: ")
  tags = input("Tags (seperate by space or if none press enter): ")

  if OwnerUserId == None:

    if tags == "":
        questionPost = {"Id": id, 
                    "PostTypeId": "1",
                    "Title": title,
                    "Body": body, 
                    "CreationDate": now,
                    "Score":0,
                    "ViewCount": 0,
                    "AnswerCount": 0,
                    "CommentCount": 0,
                    "FavoriteCount": 0,
                    "ContentLicense": "CC BY-SA 2.5"
                    }
        db.posts.insert_one(questionPost)
    else: 
      #puts the tags in <tag> format 
      tagsString = ""
      tagsArray = tags.split()
      for tag in tagsArray:
        tagsString = tagsString + "<"+tag+">"

      questionPost = {"Id": id, 
                      "PostTypeId": "1",
                      "Title": title,
                      "Body": body, 
                      "CreationDate": now,
                      "Tags": tagsString,
                      "ViewCount": 0,
                      "AnswerCount": 0,
                      "CommentCount": 0,
                      "FavoriteCount": 0,
                      "ContentLicense": "CC BY-SA 2.5"
                      }
      db.posts.insert_one(questionPost)

      #inserting/updating tags in tag collection
      for tag in tagsArray:
        results = db.tags.find({"TagName": tag})
        count = db.tags.count_documents({"TagName": tag})
        if (count == 0):
          #if tag doesn't exist insert tag
          tagId = str(uuid.uuid1())
          db.tags.insert_one({"Id":tagId, 
                          "TagName":tag, 
                          "Count":1})
        if (count == 1):
          #if tag exists increment Count by 1
          for result in results:
            tagCount = result["Count"] + 1
            db.tags.update_one({"TagName":tag}, {'$set' : {'Count' : tagCount }})

  else:
        if tags == "":
          questionPost = {"Id": id, 
                      "PostTypeId": "1",
                      "Title": title,
                      "Body": body, 
                      "CreationDate": now,
                      "OwnerUserId": OwnerUserId,
                      "Score":0,
                      "ViewCount": 0,
                      "AnswerCount": 0,
                      "CommentCount": 0,
                      "FavoriteCount": 0,
                      "ContentLicense": "CC BY-SA 2.5"
                      }
          db.posts.insert_one(questionPost)
        else: 
          #puts the tags in <tag> format 
          tagsString = ""
          tagsArray = tags.split()
          for tag in tagsArray:
            tagsString = tagsString + "<"+tag+">"

          questionPost = {"Id": id, 
                          "PostTypeId": "1",
                          "Title": title,
                          "Body": body, 
                          "CreationDate": now,
                          "OwnerUserId": OwnerUserId,
                          "Tags": tagsString,
                          "ViewCount": 0,
                          "AnswerCount": 0,
                          "CommentCount": 0,
                          "FavoriteCount": 0,
                          "ContentLicense": "CC BY-SA 2.5"
                          }
          db.posts.insert_one(questionPost)

          #inserting/updating tags in tag collection
          for tag in tagsArray:
            results = db.tags.find({"TagName": tag})
            count = db.tags.count_documents({"TagName": tag})
            if (count == 0):
              #if tag doesn't exist insert tag
              tagId = str(uuid.uuid1())
              db.tags.insert_one({"Id":tagId, 
                              "TagName":tag, 
                              "Count":1})
            if (count == 1):
              #if tag exists increment Count by 1
              for result in results:
                tagCount = result["Count"] + 1
                db.tags.update_one({"TagName":tag}, {'$set' : {'Count' : tagCount }})


  print("Your question was posted!")

  return

#################################################################################################
#Search Question 
def searchQuestion(userID):
  global db
  #use terms to search title and body 
  matchPosts = []
  matchPosts2 = []
  found = False
  #take in user input for keywords (split on space)
  keyWords = input("Please enter a space seperated list of keywords to search: ").split(' ')
  

  #IF TERMS ARE 3 CHAR OR GREATER CAN USE THE INDEXED TERMS ARRAY TO QUERY 
  lenCheck = True
  print("{:9}{:<81} {:<30} {:<10} {:<10}".format("Id", "Title", "Creation Date", "Score", "AnswerCount"))
  for word in keyWords:
    if len(word) < 3:
      lenCheck = False
  if lenCheck:
    for word in keyWords:
      #print(word)
      results = db.posts.find({"PostTypeId": "1", "Terms":word.lower()},{"_id":0, "Id": 1, "Title":1, "Body":1, "CreationDate":1, "Score":1, "AnswerCount":1})
      for post in results:
        #print(post)
        #print("\n")
        if post["Id"] not in matchPosts2:
          matchPosts2.append(post["Id"])
          if len(post["Title"])>70:
            post["Title"] = post["Title"][:69]+'...'
          print("{:<9}{:<81} {:<30} {:<10} {:<10}".format( post["Id"], post["Title"], post["CreationDate"], post["Score"], post["AnswerCount"]))

  else:    
    #questions only
    #get matches of title and body using terms for each posts
    termsCursor = db.posts.find({"PostTypeId": "1","Tags": {'$exists': True}},{"_id":1, "Id": 1, "Title":1, "Body":1, "Tags":1, "CreationDate":1, "Score":1, "AnswerCount":1})
    #iterate overs termsCursor to find matches in fields
    for fields in termsCursor:
      #get body terms
      bodyTerms = fields["Body"].split(' ')
      for index in range(len(bodyTerms)):
        bodyTerms[index] = bodyTerms[index].lower()

      for word in keyWords:
        if word.lower() in bodyTerms:
          found = True
          #store the matched posts id to display later
          if fields["_id"] not in matchPosts:
            matchPosts.append(fields["_id"])
      
      #get tile terms
      titleTerms = fields["Title"].split(" ")
      for index in range(len(titleTerms)):
        titleTerms[index] = titleTerms[index].lower()
      
      for word in keyWords:
        if word.lower() in titleTerms:
          found = True
          #store the matched posts id to display later
          if fields["_id"] not in matchPosts:
            matchPosts.append(fields["_id"])
      
      #split tags terms on < > (re split --> splits on multiple chars) to get into list
      tagsTerms = re.split(r'<|>',fields["Tags"]) 
      #tagsTerms = re.split('>| < | ><',fields["Tags"])
      if word in tagsTerms:
        found = True
        #append post id unless already in list matchposts 
        if fields["_id"] not in matchPosts:
          matchPosts.append(fields["_id"])
    
    #print the output of matched posts
    if found == False:
      print("\nNo matching posts containing the keywords were found\n")
    elif found == True:
      #print("\nId\t\t\tTitle\t\t\tCreationDate\t\t\tScore\t\t\tAnswerCount")
      #print("{:<9}{:<81} {:^50} {:^10} {:^10}".format("Id", "Title", "Creation Date", "Score", "AnswerCount"))
      #print("---\t\t\t-----\t\t\t---------\t\t\t-----\t\t\t----------\n")
      #query matching posts using the id 
      for idPosts in matchPosts:
        resultPost = db.posts.find({"_id": idPosts},{"_id":0, "Id": 1, "Title":1, "CreationDate":1, "Score":1, "AnswerCount":1})
        for post in resultPost:
          if len(post["Title"])>70:
            post["Title"] = post["Title"][:69]+'...'
          #print(post)
          #print("\n",post["Id"],"\t\t\t",post["Title"],"\t\t\t",post["CreationDate"],"\t\t\t",post["Score"],"\t\t\t",post["AnswerCount"])
          print("{:<9}{:<81} {:<30} {:<10} {:<10}".format( post["Id"], post["Title"], post["CreationDate"], post["Score"], post["AnswerCount"]))

  #select a post to view
  flag = True
  while flag:
    print("\nOptions: ")
    print("1)Go back to main menu")
    print("2)View full post")
    option = input("Select an option: ")
    if option == '1':
      return
    elif option == '2':
      viewPostId = input("Enter id of the post to view the full fields: ")
      viewPost = db.posts.find({"Id": viewPostId},{"_id":0})
      print("\n")
      for post in viewPost:
        for keys,values in post.items():
          if "ViewCount" == keys:
            db.posts.update_one({"Id": viewPostId},{ "$inc": {"ViewCount": 1 }}, upsert = False)
          print(keys,": ",values)
      flag = False

  #selection menu
  print("\n1)Question Action Answer")
  print("2)Question Action List Answers")
  print("3)Question/Answers Action Vote")
  print("4)Go back to main menu\n")
  flag = True
  while flag:
    option = input("what would you like to do: ")
    if option == '1':
      actionAnswer(userID, viewPostId)
      break
    elif option == '2':
      ListAllAnswers(userID, viewPostId)
      break
    elif option == '3':
      actionVote(userID, viewPostId)
      break
    elif option == '4':
      return

  return
###################################################################################################
#Question action-Answer
def actionAnswer(OwnerUserId, questionId):
  global db
  id = str(uuid.uuid1())
  now = str(datetime.now())

  body = input("Answer: ")

  if OwnerUserId == None:
    answerPost = {  "Id": id, 
                  "PostTypeId": "2",
                  "ParentId": questionId,
                  "Body": body, 
                  "CreationDate": now,
                  "CommentCount": 0,
                  "Score": 0,
                  "ContentLicense": "CC BY-SA 2.5"
                  }
  
  else:
    answerPost = {  "Id": id, 
                    "PostTypeId": "2",
                    "ParentId": questionId,
                    "Body": body, 
                    "CreationDate": now,
                    "OwnerUserId": OwnerUserId,
                    "CommentCount": 0,
                    "Score": 0,
                    "ContentLicense": "CC BY-SA 2.5"
                    }
  db.posts.insert_one(answerPost)
  print("You answered the question!")
  return
###################################################################################################
#Question action-List answers. 
def ListAllAnswers(OwnerUserId, selectedQuestionId):
  global db

  print("\nlisting all answers from this question\n")

  #finds the Id of accepted answer
  acceptedAnswer = db.posts.find_one({"Id": selectedQuestionId}, {"_id": 0, "AcceptedAnswerId": 1})

  #finds all answers for the selected question
  allAnswers = db.posts.find({"ParentId": selectedQuestionId, "PostTypeId": "2"}, {"_id": 0, "Id": 1, "Body": 1, "CreationDate": 1, "Score": 1})
  
  #loop through answers placing the accepted answer in PostsAcceptedAnswer and the rest in questionsAnswers
  PostsAcceptedAnswer = []
  questionsAnswers = []
  for answer in allAnswers:
    answer["Body"] = answer["Body"].replace("\n", "")
    if len(acceptedAnswer) != 0 and answer["Id"] == acceptedAnswer["AcceptedAnswerId"]:
      print("found")
      #answer["Body"] = "*" + answer["Body"]
      #answer["Id"] = answer["Id"] + "*"
      PostsAcceptedAnswer = answer
    else: 
      questionsAnswers.append(answer) 

  #check if question has accepted answer and insert the accpeted answer at the start of the list if it does
  if len(PostsAcceptedAnswer) != 0:
    questionsAnswers.insert(0, PostsAcceptedAnswer)
  else:
    print("This question has no accepted answer")

  #check if question has any answers
  if len(questionsAnswers) == 0:
    print("This question has no answers\n")
    return

  #create table to display all answers for selected question
  titleLines = "-" * 124
  print("\n {:^100} ".format("ANSWERS TABLE"))
  print(titleLines)
  #print("{:<5} {:^81} {:^26} {:^10}".format("Id", "Body", "Creation Date", "Score"))
  print("{:<5} {:^81} {:^26} {:^10}".format("Index", "Body", "Creation Date", "Score"))
  print(titleLines)
 
  #print all answers

  count = 0
  for answer in questionsAnswers:
    if count == 0 and len(PostsAcceptedAnswer) != 0:
      star = "*"
    else:
      star = ""
    #print("{:<5} {:^81} {:^26} {:^10}".format(answer["Id"], answer["Body"][:80],answer["CreationDate"],answer["Score"]))
    print("{}{:>5} {:^81} {:^26} {:^10}".format(count,star, answer["Body"][:80],answer["CreationDate"],answer["Score"]))
    count += 1

  #user can choose to see full post or go back to Main Menu
  flag = True
  while flag:
    userOption = input("\n1) see full post\n2) Main Menu\n-> ")
    if userOption == "1":

      UserAnswerOption = True
      while UserAnswerOption:
        answerId = int(input("\nselect answer index: \n"))
        
        if answerId >= 0 and answerId <= len(questionsAnswers)-1:
          ans = questionsAnswers[answerId]
       
          selectedAnswers = db.posts.find({"Id": ans["Id"], "PostTypeId": "2"}, {"_id": 0})

          print("\n")
          for answer in selectedAnswers:
            for keys,values in answer.items():
              print(keys,": ",values)
              if keys == "Id":
                selectedQuestionId = values
            
              UserAnswerOption = False
              flag = False
        
        if UserAnswerOption == True:
          print("pick a valid answer index: ")

    elif userOption == "2":
      return

    else:
      print("pick a valid option")

  #user can vote on selected answer or go back to main menu
  flag = True
  while flag:
    userInput = input("\n1)Vote on answer\n2)Main Menu\n-> ")
    if userInput == "1":
      actionVote(OwnerUserId,selectedQuestionId)
    elif userInput == "2":
      flag = False
    else:
      print("pick a valid option")

  return
###################################################################################################
#Question action-Vote
def incrementScore(postId):
  #increments score field in post when user votes on it
  posts = db.posts.find({"Id": postId})
  for post in posts:
      voteScore = post["Score"]
      db.posts.update_one({"Id": postId}, {'$set' : {'Score' : voteScore + 1}})
  return

def actionVote(userId, postId):
  global db
  id = str(uuid.uuid1())
  now = str(datetime.now())

  #check if there already exists a vote on a post with the current userId
  count = db.votes.count_documents({"PostId": postId,"UserId": userId})

  #user doesn't provide userId
  if userId == None:
    vote = {
              "Id": id,
              "PostId": postId,
              "VoteTypeId": "2",
              "CreationDate": now
            }
    db.votes.insert_one(vote)
    incrementScore(postId)
    print("Voted!")
  else:
    #user provides userId
    #count is 0 if they didn't vote on post already
    if count == 0:
      vote = {
                "Id": id,
                "PostId": postId,
                "VoteTypeId": "2",
                "UserId": userId,
                "CreationDate": now
              }
      db.votes.insert_one(vote)
      incrementScore(postId)
      print("Voted!")
    else:
      #count is more than 0 which means they already voted on this post
      print("You already voted on this post")
  return
#################################################################################################
def main():
  global db
  connection= MongoClient()
  db = connection["291db"]
  print("made db")

  posts = db["posts"]
  tags = db["tags"]
  votes = db["votes"]


  mainMenu(posts,votes,tags)

main()