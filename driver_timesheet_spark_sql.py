import sys 
from awsglue.transforms import * 
from awsglue.utils import getResolvedOptions 
from pyspark.context import SparkContext 
from awsglue.context import GlueContext 
from awsglue.dynamicframe import DynamicFrame 
from awsglue.job import Job 
from pyspark.sql import SparkSession 
from pyspark.sql.functions import udf 
from pyspark.sql.types import StringType 


args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

db_name = "aws-poc" 
tbl_driver = "driver" 
tbl_timesheet = "timesheet" 

output_dir="s3://prateek/output/driver_timesheet"

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = db_name, table_name = tbl_driver)
datasource1 = glueContext.create_dynamic_frame.from_catalog(database = db_name, table_name = tbl_timesheet)

driver = datasource0.toDF() 
driver.createOrReplaceTempView("driver") 

timesheet = datasource1.toDF() 
timesheet.createOrReplaceTempView("timesheet") 

query="""SELECT d.driverid, d.name, t.total_hours, t.total_miles from driver d  
          JOIN (SELECT driverid, sum(hours_logged)total_hours, sum(miles_logged)total_miles FROM timesheet GROUP BY driverid) t  
          ON d.driverid = t.driverid
      """
      
driver_timesheet = spark.sql(query)

driver_timesheet1 = DynamicFrame.fromDF(driver_timesheet, glueContext, "driver_timesheet")

glueContext.write_dynamic_frame.from_options(frame = driver_timesheet1, connection_type = "s3", connection_options = {"path": output_dir}, format = "parquet")

job.commit()
