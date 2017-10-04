import sys 
from awsglue.transforms import * 
from awsglue.utils import getResolvedOptions 
from pyspark.context import SparkContext 
from awsglue.context import GlueContext 
from awsglue.dynamicframe import DynamicFrame 
from awsglue.job import Job 
from pyspark.sql import SparkSession 
from pyspark.sql.functions import udf,col
from pyspark.sql.types import StringType 
from pyspark.sql import functions as F
import datetime

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

today = str(datetime.date.today())
curr_year = today[:4]
curr_month = today[5:7]
year_month = int(curr_year+curr_month)

db_name = "aws-poc" 
tbl_airline = "airline" 

output_dir="s3://prateek/output/airline"

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = db_name, table_name = tbl_airline)

airline = datasource0.toDF() 
airline.createOrReplaceTempView("airline")

query="""select reporting_period,type_of_operation,count(no_flights) as count,sum(no_flights) as total
        from airline 
        where reporting_period={0} 
        group by reporting_period,type_of_operation
      """
      
query = query.format(year_month)

airline_op = spark.sql(query)

airline_output = DynamicFrame.fromDF(airline_op, glueContext, "airline_op")

airline_output.toDF().write.parquet(output_dir, mode="append", partitionBy=['reporting_period'])

##glueContext.write_dynamic_frame.from_options(frame = airline_output, connection_type = "s3", connection_options = {"path": output_dir}, format = "parquet")

job.commit()
