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
import boto3

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

glue = boto3.client(service_name='glue', region_name='us-east-1',endpoint_url='https://glue.us-east-1.amazonaws.com')

table_create = glue.create_table(DatabaseName='awsglue',
                TableInput={
  "Name": "ACCOUNT",
  "Description": "ACCOUNT",
  "Owner": "AWS Glue",
  "Retention": 10,
  "StorageDescriptor": {
    "Columns": [
      { "Name": "A", "Type": "varchar"},
{ "Name": "B", "Type": "varchar"},
{ "Name": "C", "Type": "varchar"},
{ "Name": "D", "Type": "varchar"}
],
    "Location": "s3://prateek/ACCOUNT/",
    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
    "SerdeInfo": {
      "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
      "Parameters": {"serialization.format": "1"
          }
    },
	"Parameters": {
		"compressionType": "none",
		"classification": "CSV",
		"typeOfData": "file"
	},
  },
  "Parameters": {
		"compressionType": "none",
		"classification": "CSV",
		"typeOfData": "file"
  }
})

job.commit()
