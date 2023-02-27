from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import from_json, col, explode, count
from pyspark.sql.types import StringType, ArrayType, MapType


def calc_top_5():
    spark = SparkSession\
        .builder\
        .config("spark.mongodb.input.uri", "mongodb://localhost:27017/shopping_carts_db.Shopping_Carts_Requests")\
        .config("spark.mongodb.output.uri", "mongodb://localhost:27017/shopping_carts_db.Shopping_Carts_Requests")\
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.4.0")\
        .getOrCreate()

    df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

    return (df.select(from_json("product_list", ArrayType(StringType())).alias("col"))
        .select(explode("col"))
        .select(from_json("col", MapType(StringType(), StringType())).alias("product_list"))
        .select("product_list.name", "product_list.product_quantity",)
        .groupBy("name").count()
        .sort(col('count').desc())
    ).limit(5).toPandas()

# top5 = calc_top_5()
# print(top5)