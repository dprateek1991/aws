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

table_delete = glue.delete_table( DatabaseName='awsglue',Name='ddb_features_new')

table_create = glue.create_table(DatabaseName='awsglue',
                TableInput={
  "Name": "ddb_features_new",
  "Description": "ddb_features_new",
  "Owner": "AWS Glue",
  "Retention": 10,
  "StorageDescriptor": {
    "Columns": [
      {
        "Name": "feature_id",
        "Type": "BIGINT"
      },
          {
        "Name": "feature_name",
        "Type": "string"
      },
          {
        "Name": "feature_class",
        "Type": "string"
      },
          {
        "Name": "state_alpha",
        "Type": "string"
      },
          {
        "Name": "prim_lat_dec",
        "Type": "DOUBLE"
      },
          {
        "Name": "prim_long_dec",
        "Type": "DOUBLE"
      },
          {
        "Name": "elev_in_ft",
        "Type": "BIGINT"
      }
    ],
"InputFormat": "org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler",
"OutputFormat": "org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler",
"SerdeInfo": {
			"name": "",
			"serializationLib": "org.apache.hadoop.hive.dynamodb.DynamoDBSerDe",
			"parameters": {
				"serialization.format": "1"
			}
		},
"Parameters": {
			"dynamodb.table.name" : "Features"
	}
}
})
job.commit()



