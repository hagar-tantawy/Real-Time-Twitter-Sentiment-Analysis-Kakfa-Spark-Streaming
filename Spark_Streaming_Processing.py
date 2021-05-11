from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as f
from textblob import TextBlob 
import tweepy 

consumer_key= 'MY9pxTIaAn2WJAspReR2TrIjv'
consumer_secret= '7rnNpY4ObSNNNAZzfO6lM7sYYDt3BRrGUKj73TuZizAHcirmDK'
access_token= '1385692233962246146-YgMXaKunXAf6jw76G9KbEwaCHWyNbG'
access_token_secret= 'LHKDI3oNZvxVQ56FELu3OjDZZHO8rtl03MX3GnCXrqGW1' 

# authorization of consumer key and consumer secret
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
  
# set access to user's access key and access secret 
auth.set_access_token(access_token, access_token_secret)
  
# calling the api 
api = tweepy.API(auth)

def preprocessing(lines):
    words = lines.select(explode(split(lines.value, "t_end")).alias("word"))
    words = words.na.replace('', None)
    words = words.na.drop()
    words.printSchema()
    
    words= words.withColumn('user_id',f.split('word',';').getItem(0)).withColumn('tweet_timestamp', f.split('word', ';').getItem(1)).withColumn('followers_count', f.split('word', ';').getItem(2)).withColumn('location', f.split('word', ';').getItem(3)).withColumn('text', f.split('word', ';').getItem(4)).withColumn('retweet_count', f.split('word', ';').getItem(5)).withColumn('tweet_id', f.split('word', ';').getItem(6)).withColumn('user_name', f.split('word', ';').getItem(7))
    
    words.printSchema()
    return words

# text classification
def polarity_detection(text):
    return TextBlob(text).sentiment.polarity
def subjectivity_detection(text):
    return TextBlob(text).sentiment.subjectivity
def text_classification(words):
    # polarity detection
    polarity_detection_udf = udf(polarity_detection, StringType())
    words = words.withColumn("polarity", polarity_detection_udf("word"))
    # subjectivity detection
    subjectivity_detection_udf = udf(subjectivity_detection, StringType())
    words = words.withColumn("subjectivity", subjectivity_detection_udf("word"))
    return words 
def reply_to_tweet(tweet): 
    user = tweet.user_name
    if tweet.Sentiment=="positive":
      msg = "@%s Positive Tweet!" %user
	  print("################Reply Posted##########################################") 
    else:
      msg = "@%s Negative Tweet! :(" %user 
	  print("################Reply Posted##########################################")
    msg_sent = api.update_status(msg, tweet.tweet_id)
     


  
	
if __name__ == "__main__":
    # create Spark session
    spark = SparkSession.builder.appName("TwitterSentimentAnalysis").getOrCreate()

    
    #### Subscribe to 1 topic
    lines = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "172.18.0.2:6667").option("subscribe", "tweets").load()
    lines.printSchema()
    # Preprocess the data
    words = preprocessing(lines)
    #words.show()
    # text classification to define polarity and subjectivity
    words = text_classification(words)
    words = words.withColumn('Sentiment',when( words.polarity > 0, "positive").otherwise("negative")) 
    words.writeStream.foreach(reply_to_tweet).start() #words = words.select(col("word"),col("polarity"),col("subjectivity")) 
    #reply_query = words.writeStream.foreach(reply_to_tweet).start()
    words = words.repartition(1) 
	
    query = words.writeStream.queryName("all_tweets")\
         .outputMode("append").format("parquet")\
         .option("path", "hdfs://sandbox-hdp.hortonworks.com:8020/casestudy")\
         .option("checkpointLocation","hdfs://sandbox-hdp.hortonworks.com:8020/checkpoint")\
         .trigger(processingTime='60 seconds').start()
    query.awaitTermination() 
