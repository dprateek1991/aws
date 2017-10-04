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

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

db_name = "aws-poc" 
tbl_driver = "driver" 
tbl_timesheet = "timesheet" 

output_dir_1="s3://prateek/output/driver_timesheet_1"
output_dir_2="s3://prateek/output/driver_timesheet_2"
output_dir_3="s3://prateek/output/driver_timesheet_3"
output_dir_4="s3://prateek/output/driver_timesheet_4"

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = db_name, table_name = tbl_driver)
datasource1 = glueContext.create_dynamic_frame.from_catalog(database = db_name, table_name = tbl_timesheet)

driver = datasource0.toDF() 
timesheet = datasource1.toDF() 

timesheet_query_temp = timesheet.groupBy('driverid').agg(F.sum('hours_logged').alias('total_hours'), F.sum('miles_logged').alias('total_miles'))

timesheet_query = timesheet_query_temp.select(col("driverid"), col("total_hours"), col("total_miles"))

driver_timesheet_inner_join = driver.join(timesheet_query, driver.driverid==timesheet_query.driverid, 'inner').select(driver.driverid, driver.name, timesheet_query.total_hours, timesheet_query.total_miles)
driver_timesheet_left_join = driver.join(timesheet_query, driver.driverid==timesheet_query.driverid, 'leftouter').select(driver.driverid, driver.name, timesheet_query.total_hours, timesheet_query.total_miles)
driver_timesheet_right_join = driver.join(timesheet_query, driver.driverid==timesheet_query.driverid, 'rightouter').select(driver.driverid, driver.name, timesheet_query.total_hours, timesheet_query.total_miles)
driver_timesheet_full_join = driver.join(timesheet_query, driver.driverid==timesheet_query.driverid, 'fullouter').select(driver.driverid, driver.name, timesheet_query.total_hours, timesheet_query.total_miles)

driver_timesheet1 = DynamicFrame.fromDF(driver_timesheet_inner_join, glueContext, "driver_timesheet_inner_join")
driver_timesheet2 = DynamicFrame.fromDF(driver_timesheet_left_join, glueContext, "driver_timesheet_left_join")
driver_timesheet3 = DynamicFrame.fromDF(driver_timesheet_right_join, glueContext, "driver_timesheet_right_join")
driver_timesheet4 = DynamicFrame.fromDF(driver_timesheet_full_join, glueContext, "driver_timesheet_full_join")

glueContext.write_dynamic_frame.from_options(frame = driver_timesheet1, connection_type = "s3", connection_options = {"path": output_dir_1}, format = "parquet")
glueContext.write_dynamic_frame.from_options(frame = driver_timesheet2, connection_type = "s3", connection_options = {"path": output_dir_2}, format = "parquet")
glueContext.write_dynamic_frame.from_options(frame = driver_timesheet3, connection_type = "s3", connection_options = {"path": output_dir_3}, format = "parquet")
glueContext.write_dynamic_frame.from_options(frame = driver_timesheet4, connection_type = "s3", connection_options = {"path": output_dir_4}, format = "parquet")

job.commit()
