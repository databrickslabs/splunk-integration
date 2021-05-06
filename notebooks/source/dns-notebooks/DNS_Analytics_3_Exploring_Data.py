# Databricks notebook source
# MAGIC %md
# MAGIC # 3. Ad-Hoc Analytics: Exploring the data
# MAGIC FINALLY!!!! We have data. And we can start poking around. This is an optional section for you to familiarize yourself with the data. And pick up some spark SQL tricks. You can use these tactics to explore and expand on the analytics.
# MAGIC 
# MAGIC **Add your own queries!**

# COMMAND ----------

# MAGIC %run ./Shared_Include

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Lets take a look at the number of unique domains in our dataset 
# MAGIC select count(distinct(domain_name)) from silver_dns

# COMMAND ----------

# MAGIC %sql select count(*) from silver_dns

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ioc is a field we've created as a result of running the DGA model. If the ioc field has a value of ioc, it means that the DGA model has determeined the domain to be an ioc (indicator of compromise)
# MAGIC -- The query below is for a total count of rows where the DGA algorithm has detected an ioc. But excludes an domains that have the string 'ip' in it and has a domain name length of more than 10 characters
# MAGIC select count(*), domain_name, country 
# MAGIC   from silver_dns 
# MAGIC   where ioc = 'ioc' and domain_name not like '%ip%' and char_length(domain_name) > 8 
# MAGIC   group by domain_name, country 
# MAGIC   order by count(*) desc

# COMMAND ----------

# MAGIC %md
# MAGIC Let us check against the known threat feeds

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query for domains in the silver.dns, silver.EnrichedThreatFeeds tables where there is an ioc match.
# MAGIC -- You may have experienced: many to many match/join is compute cost prohibitive in most SIEM/log aggregation systems. Spark SQL is a lot more efficient. 
# MAGIC select count(distinct(domain_name))
# MAGIC   from silver_dns, silver_threat_feeds 
# MAGIC   where silver_dns.domain_name == silver_threat_feeds.domain

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Query for ioc matches across multiple tables. Similar to previous example but with additional columns in the results table
# MAGIC select  domain_name, rrname, country, time_first, time_last, ioc,rrtype,rdata,bailiwick, silver_threat_feeds.* 
# MAGIC   from silver_dns, silver_threat_feeds 
# MAGIC   where silver_dns.domain_name == silver_threat_feeds.domain and ioc='ioc'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Looking for specific rrnames in multiple tables.
# MAGIC select  domain_name, rrname, country, time_first, time_last, ioc,rrtype,rdata,bailiwick, silver_threat_feeds.* 
# MAGIC   from silver_dns, silver_threat_feeds 
# MAGIC   where silver_dns.domain_name == silver_threat_feeds.domain  and (silver_dns.rrname = "ns1.asdklgb.cf." OR silver_dns.rrname LIKE "%cn.")

# COMMAND ----------

# MAGIC %sql describe table silver_threat_feeds

# COMMAND ----------

# MAGIC %sql describe table silver_dns
