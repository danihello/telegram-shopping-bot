import pymongo
import os
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType
from pyspark.sql import SparkSession, SQLContext
import Configuration as c
from elasticsearch import Elasticsearch


# Creating a Connection Between Spark And Kafka
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'
# create es connection
es = Elasticsearch(hosts="http://localhost:9200")

# topics = "shopping_bot"
# Creating Spark Session
spark = SparkSession\
        .builder\
        .appName("bot")\
        .getOrCreate()


# ReadStream From Kafka telegram Topic
df_kafka = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "cnt7-naya-cdh63:9092")\
    .option("subscribe", c.topics)\
    .load()


# Creating a Schema for Spark Structured Streaming
schema = StructType() \
    .add("product_list", StringType())\
    .add("user_id", StringType())\
    .add("chat_id", StringType())\
    .add("user_first_name", StringType())\
    .add("user_last_name", StringType())\
    .add("user_user_name", StringType())\
    .add("min_aff", StringType())\
    .add("min_price", StringType())

# df_kafka.printSchema()
#

# Change Json To Dataframe With Schema
df_kafka = df_kafka.select(col("value").cast("string"))\
    .select(from_json(col("value"), schema).alias("value"))\
    .select("value.*")
df_kafka.printSchema()

# Adding Calculated Columns To Spark Data Frame
df_kafka = df_kafka.withColumn("current_ts", current_timestamp().cast('string'))


# Defining A Function To Send Dataframe To MongoDB
def write_df_mongo(target_df):
    mongodb_client = pymongo.MongoClient(c.mongo_host)
    my_db = mongodb_client[c.db_name]
    my_col = my_db[c.col_name]
    post = {
        "product_list": target_df.product_list,
        "user_id": target_df.user_id,
        "chat_id": target_df.chat_id,
        "user_first_name": target_df.user_first_name,
        "user_last_name": target_df.user_last_name,
        "user_user_name": target_df.user_user_name,
        "current_ts": target_df.current_ts,
        "min_aff": target_df.min_aff,
        "min_price": target_df.min_price,
    }
    my_col.insert_one(post)
    print('item inserted')
    print(post)
    print(target_df.product_list)


# Spark Action
df_kafka \
    .writeStream \
    .foreach(write_df_mongo)\
    .outputMode("append") \
    .start() \
    .awaitTermination()



