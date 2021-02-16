# Databricks notebook source
# MAGIC %md
# MAGIC ## Input parameters from the user
# MAGIC This gives a brief description of each parameter.
# MAGIC <br>Before using the notebook, please go through the user documentation of this notebook to use the notebook effectively.
# MAGIC 1. **Protocol** ***(Mandatory Field)*** : The protocol on which Splunk HTTP Event Collector(HEC) runs. Splunk HEC runs on `https` if Enable SSL checkbox is selected while configuring Splunk HEC Token in Splunk, else it runs on `http` protocol. If you do not have access to the Splunk HEC Configuration page, you can ask your Splunk Admin if the `Enable SSL checkbox` is selected or not.
# MAGIC 2. **Verify Certificate** ***(Mandatory Field)*** : Specify if SSL server certificate verification is required for communication with Splunk. If Splunk HEC runs on http, SSL certificate verification doesn't work. If you set Verify Certificate as True, you may have to import a custom certificate from the Splunk server into Databricks. For this refer ***section : Import custom certificate to Databricks*** from the user documentation for this notebook.  You can get the custom certificate from Splunk Admin.
# MAGIC 3. **Splunk Ip/Hostname** ***(Mandatory Field)*** : The Splunk Ip/Hostname to push data to.
# MAGIC <br>**Example:** Splunk Ip/Hostname : `123.123. 123.123` or `mysplunkhost-1`
# MAGIC 4. **Splunk HEC Port** ***(Mandatory Field)*** : The Splunk HEC port. Default Splunk HEC Port is `8088`.You can get this value from Splunk Admin or Splunk HEC Configuration page.
# MAGIC 5. **Databricks Secret Scope** ***(Either Databricks Secret Scope and Secret Key or Splunk HEC Token are Mandatory Fields)*** : The Databricks Secret Scope created using Databricks CLI to store the Secret Key containing the Splunk HEC Token value. 
# MAGIC <br>Refer : [Databricks-CLI Installation and setup](https://docs.databricks.com/dev-tools/cli/index.html#install-the-cli) to install and setup Databricks-CLI on a local system.
# MAGIC <br>Refer : [Scope creation](https://docs.databricks.com/security/secrets/secret-scopes.html#create-a-databricks-backed-secret-scope) to create a Databricks scope.
# MAGIC <br>**Example:** Databricks Secret Scope : `scope1`
# MAGIC 6. **Secret Key** ***(Either Databricks Secret Scope and Secret Key or Splunk HEC Token are Mandatory Fields.)*** : The secret key associated with specified Databricks Secret Scope which securely stores the Splunk HEC Token value.
# MAGIC <br>Refer : [Setting Splunk HEC](https://docs.splunk.com/Documentation/Splunk/latest/Data/UsetheHTTPEventCollector) to create Splunk HEC token on the Splunk side. If you do not have access to the Splunk HEC page, ask your Splunk Admin to provide the value.
# MAGIC <br>Refer : [Storing secret](https://docs.databricks.com/security/secrets/secrets.html#create-a-secret) to store the Splunk HEC token value in the Databricks scope and key.
# MAGIC <br>**Example:** Secret Key : `key1`
# MAGIC 7. **Splunk HEC Token** ***(Either Databricks Secret Scope and Secret Key or Splunk HEC Token are Mandatory Fields.)*** : The Splunk HEC token value configured in Splunk. It is used when the Databricks Secret Scope and Secret Key are not specified.
# MAGIC <br>Refer : [Setting Splunk HEC](https://docs.splunk.com/Documentation/Splunk/latest/Data/UsetheHTTPEventCollector) to create the Splunk HEC token on the Splunk side. If you do not have access to the Splunk HEC page, ask your Splunk Admin to provide Splunk HEC Token value.
# MAGIC <br>**Example:** Splunk HEC Token : `12345678-1234-1234-1234-1234567890AB`
# MAGIC 8. **Index** ***(Mandatory Field)*** : The Splunk index to push data into. Ensure that the Splunk index specified here is one of the allowed indexes associated with the Splunk HEC Token you are using here. You can get the list of such indexes from the Splunk HEC Configuration page or from your Splunk Admin.
# MAGIC <br>**Example:** Index : `main`
# MAGIC 9. **Source** ***(Optional Field)*** : It indicates the source of an event(in Splunk), that is, where the event originated.
# MAGIC <br>**Example:** Source : `http:<tokenname>`
# MAGIC 10. **Sourcetype** ***(Optional Field)*** : The sourcetype for an event is used to specify the data structure of an event. Ensure that this sourcetype is configured on the Splunk side to parse the events properly. If you do not specify the sourcetype here, ensure that the default sourcetype associated with the Splunk HEC Token being used here is also configured on the Splunk side for proper parsing of events. If you cannot make this configuration, ask your Splunk Admin to make the configuration for the sourcetype.Refer ***section: Configure Sourcetype for the events*** from the user documentation for this notebook.
# MAGIC <br>**Example:** Sourcetype : `databricks_syslog`
# MAGIC 11. **Host** ***(Optional Field)*** : The hostname or IP address of the network device that generated an event.
# MAGIC <br>**Example:** Host : `localhost`
# MAGIC 12. **Database** ***(Either Database and Table Name or Advanced Query are Mandatory Fields.)*** : The Databricks Database whose table needs to be used.
# MAGIC 13. **Table** ***(Either Database and Table Name or Advanced Query are Mandatory Fields.)*** : Table from which the data to be pushed is obtained.The data is obtained by the query : `select * from <database>.<table>`.If you want a different form of query, use the `Advanced query` parameter.
# MAGIC 14. **Filter** ***(Optional Field)*** : Any filter or queries to run on top of the table specified.
# MAGIC <br>**Example:** Filter : `WHERE <column-1>="<value-1>"`
# MAGIC 14. **Advanced Query** ***(Either Database and Table Name or Advanced Query are Mandatory Fields.)*** : Complete query whose results you want to push to Splunk. If Advanced Query, Database, Table and Filter all are specified, the Advanced Query takes precedence and the other 3 parameters are ignored. The events to be pushed to Splunk are obtained from the Advanced Query in that case. However, if the Advanced query parameter is empty, the rows from the specified table based on the specified Filter are pushed to Splunk.
# MAGIC <br>**Example:** Advanced Query : `SELECT <table-1>.<column-1>,<table-2>.<column-2> FROM <database>.<table-1> JOIN <database>.<table-2> ON <table-1>.<column-1>=<table-2>.<column-1>`

# COMMAND ----------

# Defining the user input widgets
dbutils.widgets.removeAll()
dbutils.widgets.dropdown("Protocol","https",["https","http"],"01. Splunk HEC Protocol")
dbutils.widgets.dropdown("Verify Certificate","False",["True","False"],"02. Verify Certificate")
dbutils.widgets.text("Splunk Address","","03. Splunk IP/Hostname")
dbutils.widgets.text("Splunk HEC Port","8088","04. Splunk HEC Port")
dbutils.widgets.text("Databricks Secret Scope","","05. Databricks Secret Scope")
dbutils.widgets.text("Secret Key","","06. Secret Key")
dbutils.widgets.text("Splunk HEC Token","","07. Splunk HEC Token")
dbutils.widgets.text("Index","","08. Index")
dbutils.widgets.text("Source","","09. Source")
dbutils.widgets.text("Sourcetype","","10. Sourcetype")
dbutils.widgets.text("Host","","11. Host")
dbutils.widgets.dropdown("Database",[each.name for each in spark.catalog.listDatabases()][0],[each.name for each in spark.catalog.listDatabases()],"12. Database")
dbutils.widgets.text("Table","","13. Table Name")
dbutils.widgets.text("Filter","","14. Filter")
dbutils.widgets.text("Advanced Query","","15. Advanced Query")

# COMMAND ----------

# Reading the values of user input
protocol=dbutils.widgets.get("Protocol")
sslVerify=dbutils.widgets.get("Verify Certificate")
splunkAddress=dbutils.widgets.get("Splunk Address")
splunkPort=dbutils.widgets.get("Splunk HEC Port")
secretScope=dbutils.widgets.get("Databricks Secret Scope")
secretKey=dbutils.widgets.get("Secret Key")
index=dbutils.widgets.get("Index")
source=dbutils.widgets.get("Source")
sourcetype=dbutils.widgets.get("Sourcetype")
host=dbutils.widgets.get("Host")
advancedQuery=dbutils.widgets.get("Advanced Query")
if (sslVerify == "True" and protocol=="http"):
  sslVerify=="False"
  print("SSL Certificate Verification doesn't work with http protocol. Use https protocol if you need to use ssl server certificate validation")
if (splunkAddress=="" or splunkAddress==None):
  dbutils.notebook.exit("Splunk Ip/Hostname is a required field.Provide a valid Splunk IP or Hostname.")
try:
  if not (int(splunkPort) > 0 and int(splunkPort) <= 65353 ):
    dbutils.notebook.exit("Splunk HEC Port Number should bein range 0-65363.Provide a valid Splunk HEC Port Number.")
except ValueError:
  dbutils.notebook.exit("Splunk HEC Port Number should be of type Integer.Provide a valid Splunk HEC Port Number.")
if(secretScope or secretKey):
  splunkHecToken=None
  if not secretScope :
    dbutils.notebook.exit("Secret Scope is required when Secret Key has been specified.Provide a Secret Scope value.")
  elif not secretKey :
    dbutils.notebook.exit("Secret Key is required when Secret Scope has been specified.Provide a Secret Key value.")
  else :
    try:
      splunkHecToken=dbutils.secrets.get(secretScope,secretKey)
    except Exception as e :
      dbutils.notebook.exit("Some Error occured when fetching the Splunk HEC Token value from the given Secret Scope and Secret Key. Error: {}".format(str(e)))
else:
  splunkHecToken=dbutils.widgets.get("Splunk HEC Token")
if (splunkHecToken=="" or splunkHecToken==None):
  dbutils.notebook.exit("Splunk HEC Token value is empty.Provide valid Splunk HEC Token via Secret Scope and Secret Key or directly in Splunk Hec Token field.")
if (index=="" or index==None):
  dbutils.notebook.exit("Index is a required field.Provide the Index value.")
if (advancedQuery=="" or advancedQuery==None):
  table=dbutils.widgets.get("Table")
  database=dbutils.widgets.get("Database")
  filterQuery=dbutils.widgets.get("Filter")
  if (database=="" or database==None):
    dbutils.notebook.exit("If Advanced Query is not provided, Database is a required field.Provide the Database value.")
  if (table=="" or table==None):
    dbutils.notebook.exit("If Advanced Query is not provided, Table Name is a required field.Provide the Table name value.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Modularized code for pushing data to splunk using HttpEventCollector class

# COMMAND ----------

import requests 
from requests.adapters import HTTPAdapter
import json
import time
import traceback
import os
import uuid
requests.packages.urllib3.disable_warnings()
  
class HttpEventCollector:
  
  maxByteLength = 1000000
  
  def __init__(self,protocol,splunk_address,splunk_port,splunk_hec_token,index,source,sourcetype,host,ssl_verify="false"):
    
    self.protocol = protocol
    if (splunk_address=="" or splunk_address==None):
      raise ValueError("Invalid Splunk IP/Hostname")
    else:
      self.splunk_address = splunk_address
    if (int(splunk_port) and (int(splunk_port) >= 0 and int(splunk_port) <= 65353 )):
      self.splunk_port = splunk_port
    else:
      raise ValueError("Invalid Port Number")
    if (splunk_hec_token=="" or splunk_hec_token==None):
      raise ValueError("Empty Hec token value")
    else:
      self.token = splunk_hec_token
    if(ssl_verify.lower()=="false"):
      self.ssl_verify = False
    else :
      self.ssl_verify = True
    if (index=="" or index==None):
      raise ValueError("Index value is empty")
    else:
      self.index = index
    self.source = source
    self.sourcetype = sourcetype
    self.host = host
    self.batch_events = []
    self.current_byte_length = 0
      
    
  def requests_retry_session(self,retries=3):
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session
  
  @property
  def server_uri(self):
    # splunk HEC url used to push data
    endpoint="/raw?channel="+str(uuid.uuid1())
    server_uri = '%s://%s:%s/services/collector%s' % (self.protocol, self.splunk_address, self.splunk_port, endpoint)
    return (server_uri)
  
  @property
  def parameters(self):
    params={}
    if not( self.sourcetype == None or self.sourcetype == ""):
      params.update({"sourcetype":self.sourcetype})
    if not( self.source == None or self.source == ""):
      params.update({"source":self.source})
    if not( self.index == None or self.index == ""):
      params.update({"index":self.index})
    if not( self.host == None or self.host == ""):
      params.update({"host":self.host})
    return (params)    
  
  def batch_and_push_event(self,event):
    # divide the resut payload into batches and push to splunk HEC
    payload_string = str(event)
    if not payload_string.endswith("\n"):
      payload_string=payload_string+"\n"
    payload_length = len(payload_string)

    if ((self.current_byte_length+payload_length) > self.maxByteLength ):
      self.push_event()
      self.batch_events = []
      self.current_byte_length = 0

    self.batch_events.append(payload_string)
    self.current_byte_length += payload_length
  
  def push_event(self):
    # Function to push data to splunk
    payload = " ".join(self.batch_events)
    headers = {'Authorization':'Splunk '+self.token}
    response = self.requests_retry_session().post(self.server_uri, data=payload, headers=headers,params=self.parameters, verify=self.ssl_verify)
    if not (response.status_code==200 or response.status_code==201) :
      raise Exception("Response status : {} .Response message : {}".format(str(response.status_code),response.text))



# COMMAND ----------

from pyspark.sql.functions import *

if(advancedQuery):
  full_query=advancedQuery
elif (table and database):
  basic_query="select * from "+database+"."+table+" "
  if (filterQuery == None or filterQuery == "" ) :
    full_query=basic_query
  else :
    full_query = basic_query+filterQuery
else:
  dbutils.notebook.exit("Advanced Query or Table name and Database name are required.Please check input values.")
try :
  read_data=spark.sql(full_query)
  events_list=read_data.toJSON().collect()
except Exception as e:
  print ("Some error occurred while running query. The filter may be incorrect  : ".format(e))
  traceback.print_exc()
  exit()
  
try :
  http_event_collector_instance=HttpEventCollector(protocol,splunkAddress,splunkPort,splunkHecToken,index,source,sourcetype,host,ssl_verify=sslVerify)
  
  for each in events_list:
    http_event_collector_instance.batch_and_push_event(each)
  if(len(http_event_collector_instance.batch_events)>0):
    http_event_collector_instance.push_event()
    http_event_collector_instance.batch_events = []
    http_event_collector_instance.current_byte_length = 0
  

except Exception as ex:
  print ("Some error occurred.")
  traceback.print_exc()
  exit()
