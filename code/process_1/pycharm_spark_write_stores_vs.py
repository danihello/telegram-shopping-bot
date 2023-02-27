from pyspark.sql import SparkSession
import os
import sys
sys.path.append('..')
import json as js
import pyspark.sql.functions as f
from pyspark.sql import types as t
from pyspark.sql.functions import *
from IPython.core.display import display
from dateutil.parser import parse
from pyspark.sql import DataFrameWriter
import pandas as pd
from elasticsearch import Elasticsearch
import Configuration as c



#================== integrate wth kafka======================================================#
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

#================== connection between  spark and kafka=======================================#
#==============================================================================================
# spark = SparkSession.builder.config("spark.jars", "/home/naya/elasticsearch-hadoop-7.17.7.jar")\
#                             .config("fs.s3a.access.key", c.aws_access_key) \
#                             .config("fs.s3a.secret.key", c.aws_secret_key) \
#                             .config("fs.s3a.endpoint", c.aws_bucket) \
#         .appName("Kafka_to_spark_2").getOrCreate()

spark = SparkSession.builder.config("spark.jars", "/home/naya/elasticsearch-hadoop-7.17.7.jar") \
                            .appName("Kafka_to_spark_2").getOrCreate()
#==============================================================================================
#=========================================== ReadStream from kafka===========================#

bootstrapServers = c.bootstrapservers

socketDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrapServers) \
    .option("Subscribe", c.stores_topic)\
    .load()\
    .selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")
#==============================================================================================
#==============================Create schema for create df from json=========================#
schema = t.StructType() \
    .add("StoreName", t.StringType())                    .add("Address", t.StringType()) \
    .add("City", t.StringType())                         .add("store_id", t.StringType()) \
    .add("ChainId", t.StringType())                         .add("ChainName", t.StringType()) \
    .add("pulling_date", t.StringType())                 .add("Latitude", t.FloatType()) \
    .add("Longitude", t.FloatType())
#==============================================================================================
#==========================change json to dataframe with schema==============================#
df_products = socketDF.select(f.col("value").cast("string")).select(f.from_json(f.col("value"), schema).alias("value")).select("value.*")

df_products.printSchema()

df_products \
   .writeStream \
   .outputMode("append") \
   .queryName("stores_1")\
   .format('org.elasticsearch.spark.sql') \
   .option('es.nodes', 'localhost') \
   .option('es.port', 9200) \
   .option('es.resource', '%s/%s' % (c.stores_idx, '_doc')) \
   .option("checkpointLocation", "/home/naya/temp_4")\
   .option("failOnDataLoss", 'false')\
   .start() \
   .awaitTermination()

# df_products \
#    .writeStream \
#    .format("console") \
#    .option("truncate", "False")\
#    .start()\
#    .awaitTermination()

# df_products \
#     .writeStream \
#     .format('json') \
#     .outputMode("append") \
#     .option("path","s3a://de-fp-stores/stores/spark/") \
#     .option("checkpointLocation", "s3a://de-fp-stores/stores/checkpoint")\
#     .start() \
#     .awaitTermination()


