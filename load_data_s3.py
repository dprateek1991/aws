import sys 
from awsglue.transforms import * 
from awsglue.utils import getResolvedOptions 
from pyspark.sql import SQLContext, Row
from pyspark.context import SparkContext 
from awsglue.context import GlueContext 
from awsglue.dynamicframe import DynamicFrame 
from awsglue.job import Job 
from pyspark.sql import SparkSession 
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

output_dir="s3://prateek/detail/ACCOUNT"

df = sc.textFile("s3a://prateek/ACCOUNT.txt")

df1 = df.map(lambda l: l.split("\x1F"))

df2 = df1.map(lambda p: Row(A = p[0], B = p[1], C = p[2], D = p[3], E = p[4], F = p[5], G = p[6], H = p[7], I = p[8], J = p[9]))

df3 = df2.toDF().select("A","B","C","D","E","F","G","H","I","J")

df3.write.parquet(output_dir, mode="overwrite", partitionBy=['I'])

job.commit()
