# Databricks notebook source
# MAGIC %md
# MAGIC # 5. Near Realtime Streaming Analytics
# MAGIC Enrich data with threat intel and Detect malicious activity in real-time using the analytics and enrichments 

# COMMAND ----------

# MAGIC %run ./Shared_Include

# COMMAND ----------

# Defining the schema for pDNS.
# You can use either the python style syntax or the SQL DDL syntax to define your schema.

# from pyspark.sql.types import StructType, StructField, StringType, LongType, StringType, ArrayType
# pdns_schema = (StructType()
#     .add("rrname", StringType(), True)
#     .add("rrtype", StringType(), True)
#     .add("time_first", LongType(), True)
#     .add("time_last", LongType(), True)
#     .add("count", LongType(), True)
#     .add("bailiwick", StringType(), True)
#     .add("rdata", ArrayType(StringType(), True), True)
# )

pdns_schema = """
  rrname     string,
  rrtype     string,
  time_first long,
  time_last  long,
  count      long,
  bailiwick  string,
  rdata      array<string>
"""

# COMMAND ----------

# Load the DGA model from before and make available as a UDF so we can apply it to our dataframe.
import mlflow
import mlflow.pyfunc

model_path = f'dbfs:{get_default_path()}/model'
loaded_model = mlflow.pyfunc.load_model(model_path)
ioc_detect_udf = spark.udf.register("ioc_detect", loaded_model.predict)

# COMMAND ----------

# Load test data set
# Setting maxFilesPerTrigger to 1 to simulate streaming from a static set of files.  You wouldn't normally add this option in production.
df=(spark.readStream
    .option("maxFilesPerTrigger", 1)
    .json(f"{get_default_path()}/datasets/latest/", schema=pdns_schema)
    .withColumn("isioc", ioc_detect_udf(domain_extract_udf("rrname")))
    .withColumn("domain", domain_extract_udf("rrname"))
)
df.createOrReplaceTempView("dns_latest_stream")

# COMMAND ----------

# MAGIC %md
# MAGIC ##6.1 Find threats in DNS Event Stream

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM dns_latest_stream  WHERE isioc = 'ioc'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Phishing or Typosquating?
# MAGIC -- This is where we do typosquatting detection
# MAGIC -- By using dnstwist, we find the suspicious domain, googlee
# MAGIC SELECT silver_twisted_domain_brand.*  FROM dns_latest_stream, silver_twisted_domain_brand 
# MAGIC WHERE silver_twisted_domain_brand.dnstwisted_domain = dns_latest_stream.domain

# COMMAND ----------

# The next few lines we will be applying our models:
#  - To detect the bad domains
#  - Create an alerts table
dns_stream_iocs = spark.sql("Select * from dns_latest_stream  where isioc = 'ioc'")
# dbutils.fs.rm('dbfs:/tmp/datasets/gold/delta/DNS_IOC_Latest', True)
# spark.sql("drop table if exists DNS_IOC_Latest")
(dns_stream_iocs.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", f"{get_default_path()}/_checkpoints/DNS_IOC_Latest")
  .table("DNS_IOC_Latest")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Agent Tesla
# MAGIC Success!!! 
# MAGIC - We used the DGA detection model on streaming DNS events, 
# MAGIC - Identified a supsicious domain (ioc) in our DNS logs, 
# MAGIC - Enriched the ioc with URLHaus
# MAGIC - We can we can see that it this DGA domain is serving up agent tesla

# COMMAND ----------

# MAGIC %sql
# MAGIC -- We found the bad domain - lets see if our enriched threat feeds have intel on this domain? 
# MAGIC select * from silver_threat_feeds 
# MAGIC where silver_threat_feeds.domain = domain_extract('ns1.asdklgb.cf.')

# COMMAND ----------

# Uncomment this line to remove database & all files
# cleanup_files_and_database()

# COMMAND ----------

# Please stop all your streams before you go.  This will ensure clusters can timeout and shutdown after class.
for s in spark.streams.active:
  s.stop()
