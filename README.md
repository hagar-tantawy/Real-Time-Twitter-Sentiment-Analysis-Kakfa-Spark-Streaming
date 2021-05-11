# Real-Time-Twitter-Sentiment-Analysis-Kakfa-Spark-Streaming 

# Introduction
The project aims at building a data platform for real time moderation and analytics of twitter data. The implementation will utilize different big data technologies as Spark, Kafka and Hive, in addition to PowerBI visualization tool for data discovery and delivering insights. 

# Steps: 

1) Create a Twitter Developer Account in order to access Twitter API (GET, POST) and get your credentials. (Link > https://developer.twitter.com/en ) 
2) I was workong on Hortonworks HDP 2.6.5 virtual machine and set up the required envronmet # Please check Environment Setup.txt 
3) Using Mobaxterm SSH Session: 
  1) Deal with Kafka: 
  # Create a Kafka topic 
  >> cd /usr/hdp/current/kafka-broker 
  >> bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1  --partitions 1  --topic <name>   
  
  # To list topics in Kafka 
  >> bin/kafka-topics.sh --list --zookeeper localhost:2181 
  
  # To test Kafka is working fine 
  # Create a Producer  
  >> bin/kafka-console-producer.sh --broker-list 172.18.0.2:6667 --topic <name>  
 
  # Create a Consumer 
  >> bin/kafka-console-consumer.sh --bootstrap-server 172.18.0.2:6667 --topic <name> --from-beginning 
 
  # To avoid Error 401  
  >> ntpdate -u time.google.com 
  
  # Run Producer Script aka Ingesting_Real_Time_Tweets_Using_Tweepy_Kafka_Python.py 
  >> python Ingesting_Real_Time_Tweets_Using_Tweepy_Kafka_Python.py  
  
  **Untill now We are producing tweets from Twitter, Save it to a Kafka topic, waiting to be consumed by our next componet >> Spark and Spark Streaming 
  
  3) Deal with Spark  
  # Run our processing component to do the following >> Sentiment Analysis Using TextBlob lib in python, Reply to tweets based on sentiment and finally store the       output on HDFS parquet format  

 >> spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.4.6,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.6 --master local[*] /root/Spark_Streaming_Processing.py
 
  4) Deal with Parquet  
>> Spark is responsible of dealing with this part, check the previous script.  

  5) Deal with Hive  
  # Create a Hive table 
  >> hive 
  >> create database bigdata; 
  >> use bigdata; 
  >> create external table bigdata.tweets(user_id string,tweet_timestamp string,followers_count string,location string, text string, retweet_count string,tweet_id string,user_name string, polarity string, subjectivity string,sentiment string) stored as parquet location 'hdfs://sandbox-hdp.hortonworks.com:8020/casestudy'; 
  
  # To show data on hive 
  >> select * from tweettable;

  7) Deal with ODBC and PowerBI
  # Download and Setup ClouderaHiveODBC64 https://drive.google.com/file/d/16FJcIBEM-lw_XmlZpU3KpQcfw88JIBWI/view?usp=sharing 
  >> After Installing
  >>  1- open ODBC data sources
      2- choose system DSN
      3- choose button Add
      4- Data Source Name: set any_name
      5- Host: localhost
      6- Database: bigdata "same database name in hive"
      7- Hive Server Type: choose Hive Server 2
      8- Test and Ok
      9- Connection Successfuly 
      
  # Download PowerBI Desktop https://www.microsoft.com/en-us/download/details.aspx?id=58494 
 >> After Installing
 >>  1- open PowerBI Desktop
      2- choose Get Data
      3- choose ODBC ,then connect
      4- choose Data Source Name "same name you set it in system DSN" ,then OK
      5- choose Hive --> bigdata ---> tweets ---> Load  
      6- Start building your dashboard  
      
  # Ta DA ! :)

