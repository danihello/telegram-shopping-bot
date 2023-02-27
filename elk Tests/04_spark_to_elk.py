## code from topic to elastic

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from textblob import TextBlob
from pyspark.sql.types import StringType, StructType, IntegerType, FloatType
from aa_elasticsearch_client import elasticsearch_client as es
import datetime


# connection between  spark and kafka
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

bootstrapServers = "cnt7-naya-cdh63:9092"
topics = "TweeterData"

spark = SparkSession \
    .builder \
    .appName("ReadTweets") \
    .getOrCreate()

# ReadStream from kafka
df_kafka = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrapServers) \
    .option("subscribe", topics) \
    .load()

# Create schema for create df from json
schema = StructType() \
    .add("tweet_created_at", StringType()) \
    .add("location", StringType()) \
    .add("url", StringType()) \
    .add("tweet_id", StringType()) \
    .add("text", StringType()) \
    .add("user_acount_created_at", StringType()) \
    .add("user_id", StringType()) \
    .add("name", StringType()) \
    .add("followers_count", IntegerType()) \
    .add("friends_count", IntegerType()) \
    .add("listed_count", IntegerType())

# change json to dataframe with schema
df_tweets = df_kafka.select(col("value").cast("string")) \
    .select(from_json(col("value"), schema).alias("value")) \
    .select("value.*")

# Add current time in timestamp in column "current_ts"
df_tweets = df_tweets.withColumn("current_ts", current_timestamp())

# Add hour current time  in column "hour"
df_tweets = df_tweets.withColumn("hour", hour("current_ts").cast('integer'))

# Add minute current time in column "minute"
df_tweets = df_tweets.withColumn("minute", minute("current_ts").cast('integer'))

# Add wordcount in column "wordCount"
df_tweets = df_tweets.withColumn('wordCount', size(split(col('text'), ' ')))


# Add sentiment analysis in column "wordCount"
def get_sentiment(string1):
    return TextBlob(string1).sentiment.polarity


get_sentiment_udf = udf(get_sentiment, FloatType())
df_tweets = df_tweets.withColumn('sentiment', get_sentiment_udf(col('text')))


# From sss save Elasticsearch
# Save other topic and save Elasticsearch

elasticsearch_handler = es()

def store_in_es_batch(df, epoch_id):
    print("---------------------Epoch Id is {} -----------------------------".format(epoch_id))
    try:
        df_collect = df.collect()
        for row in df_collect:
            row_as_dict = row.asDict(True)
            row_as_dict["current_ts"] = row_as_dict["current_ts"].strftime('%Y-%m-%dT%H:%M:%S')
            elasticsearch_handler.store_record(row["tweet_id"], record=row_as_dict)
            print("---------------------my twitter row {} -----------------------------".format(row))
    except Exception as e:
        print(e)


df_tweets \
    .writeStream \
    .foreachBatch(store_in_es_batch) \
    .start() \
    .awaitTermination()
