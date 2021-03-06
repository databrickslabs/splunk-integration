# Databricks notebook source
# MAGIC %md
# MAGIC ## Input parameters from user
# MAGIC ** All Input parameters are mandatory . **
# MAGIC <br>The VPC Logs Path, Region Name and Headers can be obtained from AWS account admin.
# MAGIC 1. **VPC Logs Path** : The folder in the S3 bucket from which to collect data. It should be of the form `s3://<bucket-name>/AWSLogs/<aws-account-id>/vpcflowlogs/<bucket-region>/*`.Specify the specific account id and bucket region in case there are multiple such directories in the bucket or you may use \* in place for selecting all.
# MAGIC <br>**Example:** VPC Logs Path : `s3://mybucket/AWSLogs/1234567890/vpcflowlogs/us-east-1/*`
# MAGIC 2. **Delta Output Path** : The DBFS or S3 path where the parsed data files should be stored. Ensure that this path is either empty(contains no data files) or is not a pre-existing path or does not contain any data that does not follow VPC Logs schema (schema as specified in cmd 5).
# MAGIC <br>**Example:** Delta Output Path : `/VpcLogData/`
# MAGIC 3. **Checkpoint Path** : The path for checkpoint files. The checkpoint files store information regarding the last processed record written to the table. Ensure that only one VPC Logs Path is associated with a given checkpoint Path, that is, the same checkpoint Path should not be used for any other VPC Logs Path.
# MAGIC <br>**Example:** Checkpoint Path : `/VpcLogData.checkpoint`
# MAGIC 4. **Table Name** : The table name to create. A table name can contain only lowercase alphanumeric characters and underscores and must start with a lowercase letter or underscore. Ensure a table with provided name does not pre-exist, else it will not be created.
# MAGIC 5. **Region Name** : The region name in which S3 bucket and the AWS SNS and SQS services are created.
# MAGIC <br>**Example:** Region Name : `us-east-1`
# MAGIC 6. **Headers** : Comma seperated list of headers for the vpc logs in the order as they are written to S3 bucket.
# MAGIC <br>**Example:** Headers : `version,account-id,interface-id,srcaddr,dstaddr,srcport,dstport,protocol,packets,bytes,start,end,action,logstatus`
# MAGIC 
# MAGIC ##Troubleshooting
# MAGIC ###Issue
# MAGIC <p> cmd 11 throws "AnalysisException : You are trying to create an external table default.`<table>` from `<Delta Output Path>` using Databricks Delta, but the schema is not specified when the input path is empty". After a few seconds, the write stream command in cmd 8 will also stop with "stream stopped" message. This issue occurs when the write stream command in cmd 8 has not written output to `Delta Output Path>` and not completed initialization (indicated by "stream initializing" message displayed)</p>
# MAGIC 
# MAGIC ###Solution
# MAGIC <p>In case of above issue run the cmd 8 cell individually  using the `Run > Run cell` option on the top right corner of the cell. Once the stream initialization is completed, and some output is written to the `Delta Output Path>`, run the command in cmd 11 cell individually  using the `Run > Run cell` option on the top right corner of the cell.</p>

# COMMAND ----------

# Defining the user input widgets
dbutils.widgets.removeAll()
dbutils.widgets.text("VPC Logs Path","","1. VPC Logs Path")
dbutils.widgets.text("Delta Output Path","","2. Delta Output Path")
dbutils.widgets.text("Checkpoint Path","","3. Checkpoint Path")
dbutils.widgets.text("Table Name","","4. Table Name")
dbutils.widgets.text("Region Name","","5. Region Name")
dbutils.widgets.text("Headers","","6. Headers")

# COMMAND ----------

# Reading the values of user input
vpcLogsPath=dbutils.widgets.get("VPC Logs Path")
deltaOutputPath=dbutils.widgets.get("Delta Output Path")
checkpointPath=dbutils.widgets.get("Checkpoint Path")
tableName=dbutils.widgets.get("Table Name")
regionName=dbutils.widgets.get("Region Name")
headers=dbutils.widgets.get("Headers")
listColumns=headers.split(",")
if ((vpcLogsPath==None or vpcLogsPath=="")or(deltaOutputPath==None or deltaOutputPath=="")or(checkpointPath==None or checkpointPath=="")or(tableName==None or tableName=="")or(regionName==None or regionName=="")or(headers==None or headers=="")):
  dbutils.notebook.exit("All parameters are mandatory. Ensure correct values of all parameters are specified.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Defining schema for VPC Flow Logs
# MAGIC Reference : [VPC Flow Logs Format](https://docs.aws.amazon.com/vpc/latest/userguide/flow-logs.html#flow-logs-default)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

stringTypeFields=["account-id","interface-id","srcaddr","dstaddr","action","log-status","vpc-id","subnet-id","instance-id","type","pkt-srcaddr","pkt-dstaddr","region","az-id","sublocation-type","sublocation-id"]
intTypeFields=["version","srcport","dstport","protocol","packets","bytes","tcp-flags"]
longIntTypeFields=["start","end"]

vpcLogSchema = StructType() 
for each in listColumns :
        if(each.lower() in stringTypeFields) :
          vpcLogSchema.add(each, StringType()) 
        elif (each.lower() in intTypeFields) :
          vpcLogSchema.add(each, IntegerType()) 
        elif (each.lower() in longIntTypeFields):
          vpcLogSchema.add(each, LongType())
        else :
          vpcLogSchema.add(each, StringType()) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading from the stream , parsing it and writing to delta files 

# COMMAND ----------

rawRecords = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option("cloudFiles.includeExistingFiles", "true") \
  .option("cloudFiles.useNotifications", "true") \
  .option("cloudFiles.region", regionName) \
  .option("cloudFiles.validateOptions", "true") \
  .option("delimiter"," ") \
  .option("header","true") \
  .schema(vpcLogSchema) \
  .load(vpcLogsPath)

# COMMAND ----------

streamingETLQuery = rawRecords \
  .writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", checkpointPath) \
  .start(deltaOutputPath)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating table from the parsed data

# COMMAND ----------

# The sleep statement is added here to wait for 5 mins till the write stream command in cmd 8 is initialized and parsed data starts to be written into the specified  deltaOutputPath so that the table can be created using command in cmd-11
#However even after waiting for 5 mins, the write stream command in cmd 8 has not written output to <Delta Output Path> and not completed initialization (indicated by "stream initializing" message displayed), the command in cmd 11 throws  `AnalysisException : You are trying to create an external table default.<table> from <Delta Output Path> using Databricks Delta, but the schema is not specified when the input path is empty`.After a few seconds, the write stream command in cmd 8 will also stop with `stream stopped` message. 
#In case of the above issue run the cmd 8 cell individually using the `Run > Run cell` option on the top right corner of the cell. Once the stream initialization is completed, and some output is written to the <Delta Output Path> , run the command in cmd 11 cell individually using the `Run > Run cell` option on the top right corner of the cell.
import time
time.sleep(300)

# COMMAND ----------

create_table_query="CREATE TABLE IF NOT EXISTS "+tableName+" USING DELTA LOCATION '"+ deltaOutputPath +"'"
spark.sql(create_table_query)
