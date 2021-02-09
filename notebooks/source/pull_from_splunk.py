# Databricks notebook source
# MAGIC %md
# MAGIC ## Input parameters from the user
# MAGIC This gives a brief description of each parameter.
# MAGIC <br>Before using the notebook, please go through the user documentation of this notebook to use the notebook effectively.
# MAGIC 1. **Splunk Ip/Hostname** ***(Mandatory Field)*** : The Splunk Ip/Hostname to pull data from.
# MAGIC <br>**Example:** Splunk Ip/Hostname : `123.123. 123.123` or `mysplunkhost-1`
# MAGIC 2. **Splunk Management Port** ***(Mandatory Field)*** : The Splunk Management port. Default Splunk Management port is `8089`. You can get this value from Splunk Admin.
# MAGIC 3. **Verify Certificate** ***(Mandatory Field)*** : Specify if SSL server certificate verification is required for communication with Splunk. If you set Verify Certificate as True, you may have to import a custom certificate from the Splunk server into Databricks. For this refer ***section : Import custom certificate to Databricks*** from the user documentation for this notebook. You can get the custom certificate from Splunk Admin.
# MAGIC 4. **Splunk Username** ***(Mandatory Field)*** : The Splunk username used to authenticate an user with Splunk. This is the username used by a user to log into Splunk.
# MAGIC 5. **Databricks Secret Scope** ***(Mandatory Field)*** : The Databricks Secret Scope created using Databricks CLI to store the Splunk password corresponding to splunk username in a Databricks Secret Key.
# MAGIC <br>Refer : [Databricks-CLI Installation and setup](https://docs.databricks.com/dev-tools/cli/index.html#install-the-cli) to install and setup Databricks-CLI on a local system.
# MAGIC <br>Refer : [Scope creation](https://docs.databricks.com/security/secrets/secret-scopes.html#create-a-databricks-backed-secret-scope) to create a Databricks scope.
# MAGIC <br>**Example:** Databricks Secret Scope : `scope1`
# MAGIC 6. **Secret Key** ***(Mandatory Field)*** : The secret key associated with specified Databricks Secret Scope which securely stores the Splunk password value corresponding to splunk username.
# MAGIC <br>Refer : [Storing secret](https://docs.databricks.com/security/secrets/secrets.html#create-a-secret) to store the Splunk user password value in the Databricks scope and key.
# MAGIC <br>**Example:** Secret Key : `key1`
# MAGIC 7. **Splunk Query** ***(Mandatory Field)*** : Splunk search query whose results you want to pull from Splunk. Specify the timerange for search using `earliest` and `latest` time modifiers. If the time range is not specified, the default timerange will be used.
# MAGIC <br>Refer : [Time modifiers](https://docs.splunk.com/Documentation/Splunk/8.1.1/Search/Specifytimemodifiersinyoursearch) to understand how to use time modifiers.
# MAGIC <br>**Example:** `index="<my-index>" earliest=-24h@h latest=now() | table *`
# MAGIC 8. **Search Mode** ***(Mandatory Field)*** : Search mode for Splunk search query execution. They include verbose, fast and smart modes.
# MAGIC <br>Refer : [Search Modes](https://docs.splunk.com/Documentation/Splunk/8.1.2/Search/Changethesearchmode) to understand the three Splunk search modes.
# MAGIC 9. **Splunk App Namespace** ***(Optional Field)*** : The splunk application namespace in which to restrict searches, that is, the app context. You can obtain this from Splunk Admin or Splunk user. If not specified the default application namespace is used.
# MAGIC 10. **Table Name** ***(Optional Field)*** : A Table name to create table based on the splunk search results. A table name can contain only lowercase alphanumeric characters and underscores and must start with a lowercase letter or underscore

# COMMAND ----------

# Defining the user input widgets
dbutils.widgets.removeAll()
dbutils.widgets.text("Splunk Address","","01. Splunk IP/Hostname")
dbutils.widgets.text("Splunk Management Port","8089","02. Splunk Management Port")
dbutils.widgets.dropdown("Verify Certificate","False",["True","False"],"03. Verify Certificate")
dbutils.widgets.text("Splunk Username","","04.Splunk Username")
dbutils.widgets.text("Databricks Secret Scope","","05. Databricks Secret Scope")
dbutils.widgets.text("Secret Key","","06. Secret Key")
dbutils.widgets.text("Splunk Query","","07. Splunk Query")
dbutils.widgets.dropdown("Splunk Search Mode","smart",["verbose","fast","smart"],"08. Splunk Search Mode")
dbutils.widgets.text("Splunk App Namespace","","09. Splunk App Namespace")
dbutils.widgets.text("Table Name","","10. Table Name")

# COMMAND ----------

# Reading the values of user input
sslVerify=dbutils.widgets.get("Verify Certificate")
splunkAddress=dbutils.widgets.get("Splunk Address")
splunkPort=dbutils.widgets.get("Splunk Management Port")
secretScope=dbutils.widgets.get("Databricks Secret Scope")
secretKey=dbutils.widgets.get("Secret Key")
splunkUsername=dbutils.widgets.get("Splunk Username")
splunkQuery=dbutils.widgets.get("Splunk Query")
searchLevel=dbutils.widgets.get("Splunk Search Mode")
namespace=dbutils.widgets.get("Splunk App Namespace")
tableName=dbutils.widgets.get("Table Name")
if (splunkAddress=="" or splunkAddress==None):
  dbutils.notebook.exit("Splunk Ip/Hostname is a required field.Provide a valid Splunk IP or Hostname.")
try:
  if not (int(splunkPort) > 0 and int(splunkPort) <= 65353 ):
    dbutils.notebook.exit("Splunk Management Port Number should bein range 0-65363.Provide a valid Splunk Management Port Number.")
except ValueError:
  dbutils.notebook.exit("Splunk Management Port Number should be of type Integer.Provide a valid Splunk Management Port Number.")
if not splunkUsername :
  dbutils.notebook.exit("Splunk Username is required.Provide a valid Splunk username.")
if not secretScope :
  dbutils.notebook.exit("Secret Scope is required.Provide a Secret Scope value.")
if not secretKey :
  dbutils.notebook.exit("Secret Key is required.Provide a Secret Key value.")
splunkPassword=dbutils.secrets.get(secretScope,secretKey)
if not splunkQuery :
  dbutils.notebook.exit("Splunk Query is required.Provide a Search Query value.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Modularized code for pushing data to splunk using HttpEventCollector class

# COMMAND ----------

from pyspark.sql import *
import requests 
import json
import time
import traceback
import os
import re
import pandas as pd  
from xml.etree.ElementTree import XML
from urllib.error import HTTPError
requests.packages.urllib3.disable_warnings()

AUTH_ENDPOINT="/auth/login"
JOBS_ENDPOINT="/search/jobs"
SID_JOBS_ENDPOINT="/search/jobs/{}"
RESULTS_ENDPOINT="/search/jobs/{}/results"

class Client:
    def __init__(self,splunk_address,splunk_port,splunk_username,splunk_namespace,ssl_verify="false"):
        if (splunk_address=="" or splunk_address==None):
            raise ValueError("Invalid Splunk IP/Hostname")
        else:
            self.splunk_address = splunk_address
        if (int(splunk_port) and (int(splunk_port) >= 0 and int(splunk_port) <= 65353 )):
            self.splunk_port = splunk_port
        else:
            raise ValueError("Invalid Port Number")
        if(ssl_verify.lower()=="false"):
            self.ssl_verify = False
        else :
            self.ssl_verify = True
        if (splunk_username=="" or splunk_username==None):
            raise ValueError("Invalid Splunk Username")
        else:
            self.splunk_username = splunk_username
        if (splunk_namespace=="" or splunk_namespace==None):
            self.splunk_namespace = "-"
        else:
            self.splunk_namespace = splunk_namespace
    @property
    def auth_url(self):
        auth_url = "https://{}:{}/services/auth/login".format(self.splunk_address,self.splunk_port)
        return (auth_url)
     
    @property
    def mgmt_segment(self):
        mgmt_segment_part = "https://{}:{}/servicesNS/{}/{}/".format(self.splunk_address,self.splunk_port,self.splunk_username,self.splunk_namespace)
        return (mgmt_segment_part)

    def connect(self,splunk_password):
        try:
            response = requests.post(
                self.auth_url,
                data={"username":self.splunk_username,
                "password":splunk_password},verify=self.ssl_verify) 
            session = XML(response.text).findtext("./sessionKey")
            if (session=="" or session==None):
              dbutils.notebook.exit("Issue in Authentication : Type - "+XML(response.text).find("./messages/msg").attrib["type"]+"\n Message - "+XML(response.text).findtext("./messages/msg"))
            else :
              self.token = "Splunk {}".format(session)
        except HTTPError as e:
            if e.status == 401:
                raise AuthenticationError("Login failed.", e)
            else:
                raise e
              
    def create_search(self,query,search_level,splunk_namespace):
        pattern=r"^search\s+"
        query=query.strip()
        search_query= query if query.startswith("|") else query if re.search(pattern,query) else "search "+query
        try:
            response = requests.post(
                self.mgmt_segment +JOBS_ENDPOINT,
                headers={'Authorization': self.token},
                data={"search":search_query,
                "adhoc_search_level":search_level,
                "namespace":self.splunk_namespace},
                verify=self.ssl_verify) 
            sid = XML(response.text).findtext("./sid")
            if (sid==None or sid==""):
              dbutils.notebook.exit("Issue in search : Type - "+XML(response.text).find("./messages/msg").attrib["type"]+" \n Message - "+XML(response.text).findtext("./messages/msg")) 
            else :
              print("Search Query submitted to Splunk Instance.")
            return sid
        except HTTPError as e:
            raise e
        return None

    def is_search_done(self,sid):
        print("Polling to check search status.")
        isnotdone = True
        while isnotdone:
            try:
                response = requests.get(
                    self.mgmt_segment +SID_JOBS_ENDPOINT.format(sid),
                    headers={'Authorization': self.token},
                    verify=self.ssl_verify)
                isdonestatus = re.compile('isDone">(0|1)')
                isdonestatus = isdonestatus.search(response.text).groups()[0]
                if (isdonestatus == '1'):
                    isnotdone = False
                    print("Search execution completed.")
                    return True
                time.sleep(5)
            except HTTPError as e:
                raise e
        return False

    def store_search_results(self,sid,table_name):
        print("Fetching results in chunks from Splunk Instance ....")
        OFFSET=0
        COUNT=0
        more_results=True
        while more_results:
            try:
                response = requests.get(
                    self.mgmt_segment +RESULTS_ENDPOINT.format(sid),
                    headers={'Authorization': self.token},
                    params={"output_mode":"json","offset":OFFSET,"count":0},
                    verify=False) 
                
                if ("results" not in response.json() and "messages" in response.json()):
                    raise(Exception(response.json()["messages"][0]["type"]+" : "+response.json()["messages"][0]["text"]))
                
                json_response_results=response.json()["results"]
                
                if(len(json_response_results)==0 and OFFSET==0):
                    print("Zero results returned.Check time range and query provided.")
                    break
                elif (len(json_response_results)==0):
                    print("Completed fetching from Splunk Instance .")
                    break
                else :
                  df=pd.DataFrame(json_response_results) 
                  df_sp=spark.createDataFrame(df)
                  df_sp.write.format('delta').mode("append").option("mergeSchema", "true").saveAsTable(table_name,overwrite=False)
                    
                COUNT=len(json_response_results)
                OFFSET=OFFSET+COUNT

            except HTTPError as e:
                    raise e
                
    def process_search_results(self,sid):
        print("Fetching results in chunks from Splunk Instance ....")
        OFFSET=0
        COUNT=0
        more_results=True
        while more_results:
            try:
                response = requests.get(
                    self.mgmt_segment +RESULTS_ENDPOINT.format(sid),
                    headers={'Authorization': self.token},
                    params={"output_mode":"json","offset":OFFSET,"count":0},
                    verify=False) 
                
                if ("results" not in response.json() and "messages" in response.json()):
                    raise(Exception(response.json()["messages"][0]["type"]+" : "+response.json()["messages"][0]["text"]))
                
                json_response_results=response.json()["results"]
                
                if(len(json_response_results)==0 and OFFSET==0):
                    print("Zero results returned.Check time range and query provided.")
                    break
                elif (len(json_response_results)==0):
                    print("Completed fetching from Splunk Instance .")
                    break
                else :
                  df=pd.DataFrame(json_response_results) 
                  df_sp=spark.createDataFrame(df)
                  # Add logic to process the data frame obtained in chunks here.
                    
                COUNT=len(json_response_results)
                OFFSET=OFFSET+COUNT

            except HTTPError as e:
                    raise e  

# COMMAND ----------

try :
  client=Client(splunkAddress,splunkPort,splunkUsername,namespace,sslVerify)
  client.connect(splunkPassword)
  sid=client.create_search(splunkQuery,searchLevel,namespace)
  search_done_status=client.is_search_done(sid)
  if (search_done_status)==True:
      client.process_search_results(sid)
      # client.store_search_results(sid,tableName)
except Exception as e:
  print("Exception occured : {}".format(e))
  traceback.print_exc()
  
