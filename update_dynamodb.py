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
import json
import decimal

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('Movies')

response = table.get_item(
   Key={
        'year': 1933,
        'title': 'King Kong'
    }
)

print("getItem succeeded:")
item = response ['Item']
print(item)

print ("Next part of update")
table1 = dynamodb.Table('Forum')
table1.update_item(
	Key={
		'Name': 'Amazon S3'
	},
	UpdateExpression='SET Category = :val1',
	ExpressionAttributeValues={
		':val1': 'Update Amazon Web Services'
	}
)
print ("Next part of retrive")

response = table1.get_item(
    Key={
        'Name': 'Amazon S3'
    }
)
print ("Updated value is")
item1 = response ['Item']
print(item1)
