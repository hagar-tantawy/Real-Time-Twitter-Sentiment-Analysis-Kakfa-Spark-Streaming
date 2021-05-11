#Required libraries
import tweepy 
# Tweepy is open-sourced, hosted on GitHub and enables Python to communicate with Twitter platform and use its API
import time
from kafka import KafkaConsumer, KafkaProducer  
from datetime import datetime, timedelta

# twitter setup 
#Twitter API Credentials  

consumer_key = '################'
consumer_secret = '################'
access_token = '################'
access_token_secret = '################' 

# Creating the authentication object
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
# Setting your access token and secret
auth.set_access_token(access_token, access_token_secret)
# Creating the API object by passing in auth information
api = tweepy.API(auth) 


# A helper function to normalize the time a tweet was created with the time of our system
def normalize_timestamp(time):
    mytime = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
    #mytime += timedelta(hours=1)   # the tweets are timestamped in GMT timezone, while I am in +1 timezone
    return (mytime.strftime("%Y-%m-%d %H:%M:%S")) 
	

#Defining the Kafka producer
	#specify the Kafka Broker
	#specify the topic name
producer = KafkaProducer(bootstrap_servers='172.18.0.2:6667') 
topic_name = 'tweets'	 



#Producing and sending records to the Kafka Broker
#querying the Twitter API Object
#extracting relevant information from the response
#formatting and sending the data to proper topic on the Kafka Broker
#resulting tweets have following attributes:	
def get_twitter_data():
    res = api.search("iPhone") #Hashtag/Keyword we are investigating/searching for
    for i in res:
        record =''
        record +=str(i.user.id_str) #user ID 
        record += ';'
        record += str(normalize_timestamp(str(i.created_at))) #Tweet timestamp
        record += ';'
        record +=str(i.user.followers_count) #Followers Count 
        record += ';'
        record +=str(i.user.location) # Tweet Location
        record += ';'
        record +=str(i.text) #Tweet Content
        record += ';'
        record +=str(i.retweet_count) #Retweet Count
        record += ';'
        record +=str(i.id) #Tweet ID
        record += ';'
        record +=str(i.user.screen_name) #User handle (@user_name) 
        record += ';'
        producer.send(topic_name, str.encode(record))
        print(i)
        print("##############################################################")	
        print ("\n")		
		
		   
		
get_twitter_data() 
#perform the task every couple of minutes and wait in between
def periodic_work(interval):
    while True:
        get_twitter_data()
		#interval should be an integer, the number of seconds to wait
        time.sleep(interval)
        
periodic_work(60* 0.1) # get data every couple of minutes		
		
		
	