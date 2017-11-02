from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext,HiveContext,Row,SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import *
import json
import logging
import sys
import datetime
import os

def main(sc, SQLContext):

    sqlContext = SQLContext(sc)

	output_dir="s3n://output"
	
	df = sc.textFile("s3n://file_feed/input.txt")
	
	df1 = df.map(lambda l: l.split("\x1F"))
	
	df2 = df1.map(lambda p: Row(A=p[0], B=p[1], C = p[2], D = p[3], E = p[4], F = p[5], H = p[6]))
	
	df2 = sqlContext.createDataFrame(df2)
	
	df3 = df2.select("A","B","C","D","E","F","H")
	
	df3.printSchema()
	
	df3.write.parquet(output_dir, mode="overwrite")

if __name__ == "__main__":
    conf = SparkConf().setMaster("yarn-client")
    conf = conf.setAppName("PySPARK")
    sc   = SparkContext(conf=conf)

main(sc, SQLContext)
