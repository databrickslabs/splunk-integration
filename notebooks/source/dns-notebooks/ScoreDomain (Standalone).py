# Databricks notebook source
# MAGIC %md Read Parameterized inputs

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("DomainName","","01. Domain to be scored")

# COMMAND ----------

domain=dbutils.widgets.get("DomainName")

# COMMAND ----------

# MAGIC %md Download Databricks Trained DGA Detection model file for scoring

# COMMAND ----------

# MAGIC %sh 
# MAGIC if [ ! -d /tmp/dga_model ]; then
# MAGIC   mkdir -p /tmp/dga_model
# MAGIC   curl -o /tmp/dga_model/python_model.pkl https://raw.githubusercontent.com/zaferbil/dns-notebook-datasets/master/model/python_model.pkl
# MAGIC   curl -o /tmp/dga_model/MLmodel https://raw.githubusercontent.com/zaferbil/dns-notebook-datasets/master/model/MLmodel
# MAGIC   curl -o /tmp/dga_model/conda.yaml https://raw.githubusercontent.com/zaferbil/dns-notebook-datasets/master/model/conda.yaml
# MAGIC fi

# COMMAND ----------

# MAGIC %md Load the model using mlflow

# COMMAND ----------

# Load the DGA model. 

# this is an optimization to not to reload model on evey invocation!
import json
ctx = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
if spark.conf.get(f"dga_model_is_loaded_{ctx['extraContext']['notebook_path']}", "false") == "false":
  
  import mlflow
  import mlflow.pyfunc

  # you can change to your own path copied from the output of 4th notebook
  model_path = 'dbfs:/FileStore/tables/dga_model'
  dbutils.fs.cp("file:/tmp/dga_model/", model_path, True)
  print(f"loading model from {model_path}")
  loaded_model = mlflow.pyfunc.load_model(model_path)
  spark.conf.set(f"dga_model_is_loaded_{ctx['extraContext']['notebook_path']}", "true")

# COMMAND ----------

# MAGIC %md Score the domain name with the function.

# COMMAND ----------

print(f'Score for Domain {domain} is : {loaded_model.predict(domain)}')


# COMMAND ----------

#print(f'Test Execution for google.com: {loaded_model.predict("google.com")}')
