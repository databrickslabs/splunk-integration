# Databricks notebook source
# MAGIC %md
# MAGIC # 4. ML Training and Analytics
# MAGIC In this section we will build the DGA model and the typosquatting model. Slides below have some high level discussion on DGA.  
# MAGIC - A detailed discussion on DGA is here: http://www.covert.io/getting-started-with-dga-research/
# MAGIC - A more detailed discussion on typosquatting is here: https://www.mcafee.com/blogs/consumer/what-is-typosquatting/
# MAGIC 
# MAGIC At a high level we will:
# MAGIC - Extract the domain names from the data removing gTLD (e.g. .com, .org) and ccTLD (e.g. .ru, cn, .uk, .ca)
# MAGIC - Build the models 

# COMMAND ----------

# MAGIC %scala
# MAGIC displayHTML("""<iframe src="https://docs.google.com/presentation/d/e/2PACX-1vRqDKRAKkXWhcRavKMvJE1BKzpoI4UvofIFQdIpoTV1d7Z3b4XdIsRt6O0iAFV8waBPvrMLVUdHFcND/embed?start=false&loop=false&delayms=3000" frameborder="0" width="960" height="569" allowfullscreen="true" mozallowfullscreen="true" webkitallowfullscreen="true"></iframe>
# MAGIC """)

# COMMAND ----------

# MAGIC %run ./Shared_Include

# COMMAND ----------

# Read the Alexa list of domains
# Alexa is a list of the most popular domains on the internet ranked by popularity
# Alexa is not intended as a whitelist. 
import pandas as pd
import mlflow
import mlflow.sklearn
import mlflow.pyfunc

dbutils.fs.cp(f'{get_default_path()}/datasets/alexa_100k.txt', f'file://{get_default_path()}/datasets/alexa_100k.txt')
alexa_dataframe = pd.read_csv(f'{get_default_path()}/datasets/alexa_100k.txt')
display(alexa_dataframe)

# COMMAND ----------

# Extract the domains names without gTLD or ccTLD (generic or country code top-level domain) from the registered domain and subdomains of a URL.
# We only need the domain names for training.
# Example fields in a tldextract result: ExtractResult(subdomain='forums.news', domain='cnn', suffix='com')
import tldextract
import numpy as np

alexa_dataframe['domain'] = [ domain_extract(uri) for uri in alexa_dataframe['uri']]
del alexa_dataframe['uri']
del alexa_dataframe['rank']
display(alexa_dataframe)

# COMMAND ----------

# Add legitimate domains from Alexa to the training data
# It's possible we have NaNs from blanklines or whatever
alexa_dataframe = alexa_dataframe.dropna()
alexa_dataframe = alexa_dataframe.drop_duplicates()

# Set the class
alexa_dataframe['class'] = 'legit'

# Shuffle the data (important for training/testing)
alexa_dataframe = alexa_dataframe.reindex(np.random.permutation(alexa_dataframe.index))
alexa_total = alexa_dataframe.shape[0]
print('Total Alexa domains %d' % alexa_total)
display(alexa_dataframe)

# COMMAND ----------

file_location = f'{get_default_path()}/datasets/dga_domains_header.txt'
dbutils.fs.cp(f'{get_default_path()}/datasets/dga_domains_header.txt', f'file:{file_location}')


# For big datasets we'll use Koalas!
dga_dataframe = pd.read_csv(file_location, header=0);
# We noticed that the blacklist values just differ by captilization or .com/.org/.info
dga_dataframe['domain'] = dga_dataframe.applymap(lambda x: x.split('.')[0].strip().lower())

# It's possible we have NaNs from blanklines or whatever
dga_dataframe = dga_dataframe.dropna()
dga_dataframe = dga_dataframe.drop_duplicates()
dga_total = dga_dataframe.shape[0]
print('Total DGA domains %d' % dga_total)

# Set the class
dga_dataframe['class'] = 'ioc'

print('Number of DGA domains: %d' % dga_dataframe.shape[0])
all_domains = pd.concat([alexa_dataframe, dga_dataframe], ignore_index=True)

# COMMAND ----------

# Output of DGA detections from our dataset
display(dga_dataframe)

# COMMAND ----------

# Lets do some feature engineering and add calculations for entropy and length to our dataset.
# We calculate entropy by comparing the number of unique characters in our string to its length.
all_domains['length'] = [len(x) for x in all_domains['domain']]
all_domains = all_domains[all_domains['length'] > 6]

import math
from collections import Counter
 
def entropy(s):
    p, lns = Counter(s), float(len(s))
    return -sum( count/lns * math.log(count/lns, 2) for count in p.values())
  
all_domains['entropy'] = [entropy(x) for x in all_domains['domain']]

# COMMAND ----------

# Print the results. The higher the entropy the higher the potential for DGA. But we aren't done quite yet.
display(all_domains)

# COMMAND ----------

# Here we do additional feature engineering to do n-gram frequency analysis our valid domains

y = np.array(all_domains['class'].tolist()) # Yes, this is weird but it needs 

import sklearn.ensemble
from sklearn import feature_extraction

alexa_vc = sklearn.feature_extraction.text.CountVectorizer(analyzer='char', ngram_range=(3,5), min_df=1e-4, max_df=1.0)
counts_matrix = alexa_vc.fit_transform(alexa_dataframe['domain'])
alexa_counts = np.log10(counts_matrix.sum(axis=0).getA1())
ngrams_list = alexa_vc.get_feature_names()

# COMMAND ----------

# Load dictionary words into a dataframe
dbutils.fs.cp(f'{get_default_path()}/datasets/words.txt', f'file://{get_default_path()}/datasets/words.txt')
file_location = f'{get_default_path()}/datasets/words.txt'
word_dataframe = pd.read_csv(file_location, header=0, sep=';');
word_dataframe = word_dataframe[word_dataframe['words'].map(lambda x: str(x).isalpha())]
word_dataframe = word_dataframe.applymap(lambda x: str(x).strip().lower())
word_dataframe = word_dataframe.dropna()
word_dataframe = word_dataframe.drop_duplicates()

# COMMAND ----------

# Create a dictionary from the word list
dict_vc = sklearn.feature_extraction.text.CountVectorizer(analyzer='char', ngram_range=(3,5), min_df=1e-5, max_df=1.0)
counts_matrix = dict_vc.fit_transform(word_dataframe['words'])
dict_counts = np.log10(counts_matrix.sum(axis=0).getA1())
ngrams_list = dict_vc.get_feature_names()

def ngram_count(domain):
    alexa_match = alexa_counts * alexa_vc.transform([domain]).T  # Woot vector multiply and transpose Woo Hoo!
    dict_match = dict_counts * dict_vc.transform([domain]).T
    print(f'Domain: {domain} Alexa match: {alexa_match} Dict match: {dict_match}')

# Examples:
ngram_count('beyonce')
ngram_count('dominos')
ngram_count('1cb8a5f36f')
ngram_count('zfjknuh38231')
ngram_count('bey6o4ce')
ngram_count('washington')

# COMMAND ----------

# Create n-grams from the dictionary and Alex 100k list. And build a matching function. And run test examples.
# More on ngrams here: https://blog.xrds.acm.org/2017/10/introduction-n-grams-need/ 

all_domains['alexa_grams']= alexa_counts * alexa_vc.transform(all_domains['domain']).T 
all_domains['word_grams']= dict_counts * dict_vc.transform(all_domains['domain']).T 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build a vectorized model of the n-grams
# MAGIC 
# MAGIC We need vectors for building the model

# COMMAND ----------

weird_cond = (all_domains['class']=='legit') & (all_domains['word_grams']<3) & (all_domains['alexa_grams']<2)
weird = all_domains[weird_cond]
print(weird.shape[0])
all_domains.loc[weird_cond, 'class'] = 'weird'
print(all_domains['class'].value_counts())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Let's train our model

# COMMAND ----------

#Labelling the domains based on weirdness 
# Using ML runtime, my packages come pre-installed
# Using ML flow, we can track our expirements as we iterate

from sklearn.model_selection import train_test_split
clf = sklearn.ensemble.RandomForestClassifier(n_estimators=20) # Trees in the forest

not_weird = all_domains[all_domains['class'] != 'weird']
X = not_weird[['length', 'entropy', 'alexa_grams', 'word_grams']].values

# Labels (scikit learn uses 'y' for classification labels)
y = np.array(not_weird['class'].tolist())

with mlflow.start_run() as run:
  mlflow.sklearn.autolog() # automatically log model parameters
  # We can also add the call to mlflow.spark.autolog() to track the data source, but it requires an additional Jar: https://mlflow.org/docs/latest/tracking.html#spark-experimental 
  # Train on a 80/20 split
  X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
  clf.fit(X_train, y_train)
  y_pred = clf.predict(X_test)
  # First train on the whole thing before looking at prediction performance
  clf.fit(X, y)

# Locate the model in our content library using MLFlow
run_id = run.info.run_id
print(f'MLflow run_id: {run_id}, model_uri: runs:/{run_id}/model')

# COMMAND ----------

# Build a predict function to be used laterto do DGA predictions
# Add in pre and post processing for our predict function

import mlflow.pyfunc

class vc_transform(mlflow.pyfunc.PythonModel):
    def __init__(self, alexa_vc, dict_vc, ctx):
        self.alexa_vc = alexa_vc
        self.dict_vc = dict_vc
        self.ctx = ctx

    def predict(self, context, model_input):
        _alexa_match = alexa_counts * self.alexa_vc.transform([model_input]).T  
        _dict_match = dict_counts * self.dict_vc.transform([model_input]).T
        _X = [len(model_input), entropy(model_input), _alexa_match, _dict_match]
        return str(self.ctx.predict([_X])[0])

# COMMAND ----------

# Save our model
from mlflow.exceptions import MlflowException
model_path = f'{get_default_path()}/new_model/dga_model'

dbutils.fs.rm(f'file://{model_path}', True)

vc_model = vc_transform(alexa_vc, dict_vc, clf)
mlflow.pyfunc.save_model(model_path, python_model=vc_model)
dbutils.fs.cp(f'file://{model_path}', model_path, True)
print(f'new DGA model is copied to DBFS to dbfs:{model_path}')

# COMMAND ----------

from mlflow.tracking.client import MlflowClient
client = MlflowClient()

model_name = f"{get_user_prefix()}_dns_dga"

# Usually it's enough to call the mlflow.register_model(model_uri=f"runs:/{run_id}/model", name=model_name) 
# but because we have custom model we need to 
try:
  client.get_registered_model(model_name)
except:
  client.create_registered_model(model_name)

model_version = client.create_model_version(model_name, f"dbfs:{model_path}", run.info.run_id, description="DGA detection model")
print(f"Model is registerd with name: {model_name}, version: {model_version.version}")
# Uncomment if you want to promote this version into Staging
# client.transition_model_version_stage(name=model_name, version=model_version.version, stage="Staging")

# COMMAND ----------

# do predict 
vc_model.predict(mlflow.pyfunc.PythonModel, '7ydbdehaaz')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## What problems can you spot with that model? How we can improve this model?
# MAGIC 
# MAGIC **How would you approach to this problem?**
# MAGIC 
# MAGIC For example:
# MAGIC * domain registration/update/expiration date ?
# MAGIC * information about DNS registrant ?
# MAGIC * information about autonomous system ?
# MAGIC * references from other domains?
