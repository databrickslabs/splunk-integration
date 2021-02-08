# Databricks notebook source
# MAGIC %md
# MAGIC ## Input parameters from user
# MAGIC ** All Input parameters are mandatory . **
# MAGIC <br>This notebook is used where the syslogs format is according to RFC-5424
# MAGIC <br>The Syslog Logs Path and Region Name can be obtained from AWS account admin.
# MAGIC 1. **Syslogs Logs Path** : The folder in the S3 bucket from which to collect data.. It should be of the form `s3://<bucket-name>/<path_to_syslog_folder>/*`.
# MAGIC <br>**Example:** Syslogs Path : `s3://mybucket/my_syslog_folder/*`
# MAGIC 2. **Delta Output Path** : The DBFS or S3 path where the parsed data files should be stored. Ensure that this path is either empty(contains no data files) or is not a pre-existing path or does not contain any data that does not follow Syslog schema (schema as specified in cmd 5).
# MAGIC <br>**Example:** Delta Output Path : `/SyslogData/`
# MAGIC 3. **Checkpoint Path** : The path for checkpoint files. The checkpoint files store information regarding the last processed record written to the table. Ensure that only one Syslog Logs Path is associated with a given checkpoint Path, that is, the same checkpoint Path should not be used for any other Syslog Logs Path.
# MAGIC <br>**Example:** Checkpoint Path : `/SyslogData.checkpoint`
# MAGIC 4. **Table Name** : The table name to create. A table name can contain only lowercase alphanumeric characters and underscores and must start with a lowercase letter or underscore. Ensure a table with provided name does not pre-exist, else it will not be created.
# MAGIC 5. **Region Name** : The region name in which S3 bucket and the AWS SNS and SQS services are created.
# MAGIC <br>**Example:** Region Name : `us-east-1`
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
dbutils.widgets.text("Syslogs Path","","1. Syslog Logs Path")
dbutils.widgets.text("Delta Output Path","","2. Delta Output Path")
dbutils.widgets.text("Checkpoint Path","","3. Checkpoint Path")
dbutils.widgets.text("Table Name","","4. Table Name")
dbutils.widgets.text("Region Name","","5. Region Name")


# COMMAND ----------

# Reading the values of user input
sysLogsPath=dbutils.widgets.get("Syslogs Path")
deltaOutputPath=dbutils.widgets.get("Delta Output Path")
checkpointPath=dbutils.widgets.get("Checkpoint Path")
tableName=dbutils.widgets.get("Table Name")
regionName=dbutils.widgets.get("Region Name")
if ((sysLogsPath==None or sysLogsPath=="")or(deltaOutputPath==None or deltaOutputPath=="")or(checkpointPath==None or checkpointPath=="")or(tableName==None or tableName=="")or(regionName==None or regionName=="")):
  dbutils.notebook.exit("All parameters are mandatory. Ensure correct values of all parameters are specified.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Defining schema for Syslogs (RFC-5424)
# MAGIC Reference : [Syslog Format as logged by Fluentd](https://docs.fluentd.org/parser/syslog#rfc-5424-pattern) and [Example](https://docs.fluentd.org/parser/syslog#rfc-5424-log)

# COMMAND ----------

import  pyspark.sql.functions as F
from pyspark.sql.types import *
from datetime import datetime
syslogSchema = StructType()\
  .add("time", StringType()) \
  .add("host", StringType()) \
  .add("ident", StringType()) \
  .add("pid", StringType()) \
  .add("msgid", StringType()) \
  .add("extradata", StringType()) \
  .add("message", StringType()) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading from the stream , parsing it and writing to delta files 

# COMMAND ----------

rawRecords = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "json") \
  .option("cloudFiles.includeExistingFiles", "true") \
  .option("cloudFiles.useNotifications", "true") \
  .option("cloudFiles.region", regionName) \
  .option("cloudFiles.validateOptions", "true") \
  .schema(syslogSchema) \
  .load(sysLogsPath)

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
