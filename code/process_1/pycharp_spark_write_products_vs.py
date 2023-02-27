from pyspark.sql import SparkSession
import os
import json as js
import pyspark.sql.functions as f
import sys
sys.path.append('..')
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
spark = SparkSession.builder.config("spark.jars", "/home/naya/elasticsearch-hadoop-7.17.7.jar").appName("Kafka_to_spark_2").getOrCreate()


#==============================================================================================
#=========================================== ReadStream from kafka===========================#

socketDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", c.bootstrapservers) \
    .option("Subscribe", c.products_topic)\
    .load()\
    .selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")
#==============================================================================================
#==============================Create schema for create df from json=========================#
schema = t.StructType() \
    .add("id", t.StringType())                    .add("name", t.StringType()) \
    .add("chains", t.MapType(t.StringType(),t.MapType(t.StringType(), t.StringType()))) \
    .add("chains_count", t.IntegerType())         .add("stores_count", t.IntegerType())
#==============================================================================================
#==========================change json to dataframe with schema==============================#
df_products = socketDF.select(f.col("value").cast("string")).select(f.from_json(f.col("value"), schema).alias("value")).select("value.*")

# Print to check the data values
#taxiTripsDF.show(1, False)
#display(taxiTripsDF.dtypes)
df_products.printSchema()
# connector to mysql

#es = Elasticsearch(hosts="http://localhost:9200")


#df_products.write.format(
#    'org.elasticsearch.spark.sql'
#).option(
#    'es.nodes', 'localhost'
#).option(
#    'es.port', 9200
#).option(
#    'es.resource', '%s/%s' % ('index_name_2', 'doc_type_name'),
#).save()


df_products \
    .writeStream \
    .outputMode("append") \
    .queryName("writing_to_es_2")\
    .format('org.elasticsearch.spark.sql') \
    .option('es.nodes', 'localhost') \
    .option('es.port', 9200) \
    .option('es.resource', '%s/%s' % (c.products_idx, '_doc')) \
    .option("checkpointLocation", "/home/naya/kafka_products_fp")\
    .option("failOnDataLoss", 'false')\
    .start() \
    .awaitTermination()

#df_products \
#    .write \
#    .format("console") \
#    .start()\
#    .awaitTermination()

