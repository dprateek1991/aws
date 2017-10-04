[hadoop@ip-172-31-30-4 ~]$ cat sample.py
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext,HiveContext
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import *
import json
import logging
import sys
import datetime
import os

def main(sc, SQLContext):

        SQLContext=HiveContext(sc)
        sc.setLogLevel("ERROR")
        query1="use sampledb"
        df1=SQLContext.sql(query1)
        query2="select count(*) from airline"
        df2=SQLContext.sql(query2)
        df2.show()

if __name__ == "__main__":
    conf = SparkConf().setMaster("yarn-client")
    conf = conf.setAppName("First PySpark Code")
    sc   = SparkContext(conf=conf)

main(sc, SQLContext)



[hadoop@ip-172-31-30-4 ~]$ spark-submit --master yarn sample.py
