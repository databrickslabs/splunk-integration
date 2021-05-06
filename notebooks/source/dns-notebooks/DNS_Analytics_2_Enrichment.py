# Databricks notebook source
# MAGIC %md
# MAGIC # 2. Loading the data
# MAGIC We admit, that felt like a lot of work to prep URLHaus and dnstwist. But we are now ready for typosquatting detection and threat intel enrichment. 
# MAGIC 
# MAGIC Now, we can entrich the pDNS data with tldextract, GeoIP lookups, a DGA Classifier, URLHaus, threat intel lookups.
# MAGIC We will do this using Spark SQL.

# COMMAND ----------

# MAGIC %run ./Shared_Include

# COMMAND ----------

# Create user defined functions (UDF) for loading and manipulating the Geo data 
# The code here will perform Geo-IP lookups using the ip address available in the rdata field in our bronze table
# We use a free geo database from Maxmind: https://dev.maxmind.com/geoip/geoip2/geolite2/ 
import geoip2.errors
from geoip2 import database

import pandas as pd

from pyspark.sql.functions import pandas_udf
from pyspark import SparkFiles

# You can download this database from: https://dev.maxmind.com/geoip/geoip2/geolite2/ 
# You can upload the GeoLite2_City database file by using the databricks UI. 
# Databricks Navigator (lefthand bar) -> Data -> Upload File -> Select 
# Note if you receive an error here, you need to check exact location and adjust it
city_db = f'{get_default_path()}/datasets/GeoLite2_City.mmdb'

if not dbfs_file_exists(city_db):
  raise Exception(f'Please download GeoLite2_City database and put into {city_db}')

def extract_geoip_data(ip: str, geocity):
  print(ip)
  if ip:
    try:
      record = geocity.city(ip)
      return {'city': record.city.name, 'country': record.country.name, 'country_code': record.country.iso_code}
    except geoip2.errors.AddressNotFoundError:
      pass
  
  return {'city': None, 'country': None, 'country_code': None}

@pandas_udf("city string, country string, country_code string")
def get_geoip_data(ips: pd.Series) -> pd.DataFrame:
  # TODO: re-think that into more portable, as this may not work on CE
  geocity = database.Reader(f'/dbfs{city_db}')
  extracted = ips.apply(lambda ip: extract_geoip_data(ip, geocity))
  
  return pd.DataFrame(extracted.values.tolist())

spark.udf.register("get_geoip_data", get_geoip_data)

# COMMAND ----------

# Load the DGA model. This is a pre-trained model that we will use to enrich our incoming DNS events. You will see how to train this model in a later step.
import mlflow
import mlflow.pyfunc

model_path = f'dbfs:{get_default_path()}/model'
loaded_model = mlflow.pyfunc.load_model(model_path)
ioc_detect_udf = spark.udf.register("ioc_detect", loaded_model.predict)

# COMMAND ----------

# Filtering on the rrtype of A 
dns_table = (spark.table("bronze_dns")
              .selectExpr("*", "case when rrtype = 'A' then element_at(rdata, 1) else null end as ip_address ")
            )

# COMMAND ----------

#Enrich the data with city, country, country codes, ioc and domain name
dns_table_enriched = dns_table.withColumn("geoip_data", get_geoip_data(dns_table.ip_address))\
  .selectExpr("*", "geoip_data.*", 
              "case when char_length(domain_extract(rrname)) > 5 then ioc_detect(string(domain_extract(rrname))) else null end as ioc",
              "domain_extract(rrname) as domain_name").drop("geoip_data")

# COMMAND ----------

# Persist the enriched DNS data
(dns_table_enriched.write
  .format("delta")
  .mode('overwrite')
  .option("mergeSchema", True)
  .saveAsTable('silver_dns')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC /* We check to see how many records we have loaded */
# MAGIC select count(*) from silver_dns
