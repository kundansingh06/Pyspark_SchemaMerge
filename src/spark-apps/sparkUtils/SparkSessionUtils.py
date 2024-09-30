import sys
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


def getSparkSession():
    spark = (SparkSession
             .builder.master("local[*]")
             .appName("merge-schema-driver")
             .getOrCreate())
    return spark