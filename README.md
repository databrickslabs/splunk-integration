The Bidirection Splunk Integration project consists of 3 main components as dipicted in the below digagram (Ref : Architecture):
<br/>
1) The [Databricks add-on for Splunk](https://splunkbase.splunk.com/app/5416/), an app, that allows Splunk Enterprise and Splunk Cloud users to run queries and execute actions, such as running notebooks and jobs, in Databricks. 
2) Splunk SQL database extension (Splunk DB Connect) configratuion for Databricks connectivity
3) Notebooks for Push and Pull events and alerts from Splunk Databricks.
<br/>
We also provided extensive documentation for Log Collection to ingest, store, and process logs on economical and performant Delta lake.

### Features

Architecture:<br/>
<img src="/docs/markdown/images/functional_architecture.png" height="70%" width="70%">


- Run Databricks SQL queries right from the Splunk search bar and see the results in Splunk UI  (Ref: Fig 1)
- Execute actions in Databricks, such as notebook runs and jobs, from Splunk (Ref: Fig 2 & Fig 3)
- Use Splunk SQL database extension to integrate Databrics information with Splunk queries and reports (Ref: Fig 4 & Fig 5)
- Push events, summary, alerts to Splunk from Databricks (Ref: Fig 6 and Fig 7)
- Pull events, alerts data from Splunk into Databricks (Ref: Fig 8)

Fig 1: Run Databricks SQL queries right from the Splunk search bar and see the results in Splunk UI
<br/><img src="/docs/markdown/images/databricksquery.png" height="70%" width="70%"><br/>
Fig 2: Execute actions in Databricks, such as notebook runs and jobs, from Splunk
<br/><img src="/docs/markdown/images/databricksrun.png" height="70%" width="70%"><br/>
Fig 3: <br/><img src="/docs/markdown/images/databricksjob.png" height="70%" width="70%"><br/>
<br/><br/>
Fig 4: Use Splunk SQL database extension to integrate Databrics information with Splunk queries and reports <br/><img src="/docs/markdown/images/dbconnect1.png" height="70%" width="70%"><br/>
Fig 5: <br/><img src="/docs/markdown/images/dbconnect2.png" height="70%" width="70%"><br/>
<br/><br/>
Fig 6: Push events, summary, alerts to Splunk from Databricks 
<br/> <img src="/docs/markdown/images/pushtosplunk1.png" height="70%" width="70%"><br/>
Fig 7: Pull events, alerts data from Splunk into Databricks
<br/> <img src="/docs/markdown/images/pushtosplunk2.png" height="70%" width="70%"><br/>
<br/><br/>
Fig 8:<br/> <img src="/docs/markdown/images/pullfromsplunk.png" height="70%" width="70%"><br/>

#### Bidirection Splunk Connector "how to" documentation:

* Databricks Add-on for Splunk Integration Installation And Usage Guide:
   * Documenation:  [[markdown](/docs/markdown/Splunk%20Integration%20Installation%20And%20Usage%20Guide.md), [pdf](/docs/pdf/Splunk%20Integration%20Installation%20And%20Usage%20Guide%20-%201.0.0.pdf), [word](/docs/word/Splunk%20Integration%20Installation%20And%20Usage%20Guide%20-%201.0.0.docx)]
   * [Link to Databricks add-on for Splunk on Splunkbase](https://splunkbase.splunk.com/app/5416)
* Splunk DB Connect Guide for Databricks:
  * Documenation:  [[markdown](/docs/markdown/Splunk%20DB%20Connect%20guide%20for%20Databricks.md), [pdf](/docs/pdf/Splunk%20DB%20Connect%20Guide%20for%20Databricks.pdf), [word](/docs/word/Splunk%20DB%20Connect%20Guide%20for%20Databricks.docx)]
* Push Data to Splunk from Databricks.docx: 
  * Documenation: [[markdown](/docs/markdown/Databricks%20-%20Push%20to%20Splunk.md), [pdf](/docs/pdf/Push%20Data%20to%20Splunk%20from%20Databricks.pdf), [word](/docs/word/Push%20Data%20to%20Splunk%20from%20Databricks.docx)]
  * Notebook - push_to_splunk: [source](/notebooks/source/push_to_splunk.py), [html](/notebooks/html/push_to_splunk.html), [dbc](/notebooks/dbc/push_to_splunk.dbc)
* Pull Data from Splunk into Databricks.docx:
  * Documenation:  [[markdown](/docs/markdown/Databricks%20-%20Pull%20from%20Splunk.md), [pdf](/docs/pdf/Push%20Data%20to%20Splunk%20from%20Databricks.pdf), [word](/docs/word/Push%20Data%20to%20Splunk%20from%20Databricks.docx)]
  * Notebook - pull_from_splunk: [source](/notebooks/source/pull_from_splunk.py), [html](/notebooks/html/pull_from_splunk.html), [dbc](/notebooks/dbc/pull_from_splunk.dbc)

### Data collection sources with Notebooks and documentation are included for the following sources: 

This project also provides documentation and notebooks to show case specifics on how to use Databricks for collecting varous logs (a comprehensive list is provided below) via stream ingest and batch-ingest using Databricks autoloader and Spark streaming into cloud Data lakes for durable storage on S3. The included documentation and notebooks also provide methods and code detials for each log type: parsing, schematizing, ETL/Aggregation, and storing in Delta format to make them available for analytics. 

#### Log Collection documentation:

* Cloudtrail logs
  * Documenation: [[markdown](/docs/markdown/Databricks%20%20-%20AWS%20CloudTrail.md), [pdf](/docs/pdf/Databricks%20%20-%20AWS%20CloudTrail.pdf), [word](/docs/word/Databricks%20%20-%20AWS%20CloudTrail.docx)]
  * Notebook 1 - cloudtrail_ingest: [source](/notebooks/source/cloudtrail_ingest.py), [html](/notebooks/html/cloudtrail_ingest.html), [dbc](/notebooks/dbc/cloudtrail_ingest.dbc)
  * Notebook 2 - cloudtrail_insights_ingest: [source](/notebooks/source/cloudtrail_insights_ingest.py), [html](/notebooks/html/cloudtrail_insights_ingest.html), [dbc](/notebooks/dbc/cloudtrail_insights_ingest.dbc)
* VPC flow logs
  * Documenation: [[markdown](/docs/markdown/Databricks%20-%20AWS%20VPC%20Logs.md), [pdf](/docs/pdf/Databricks%20-%20AWS%20VPC%20Logs.pdf), [word](/docs/word/Databricks%20-%20AWS%20VPC%20Logs.docx)]
  * Notebook - vpc_flowlogs_ingest: [source](/notebooks/source/vpc_flowlogs_ingest.py), [html](/notebooks/html/vpc_flowlogs_ingest.html), [dbc](/notebooks/dbc/vpc_flowlogs_ingest.dbc)
* syslogs
  * Documenation: [[markdown](/docs/markdown/Databricks%20-%20Syslog.md), [pdf](/docs/pdf/Databricks%20-%20Syslog.pdf), [word](/docs/word/Databricks%20-%20Syslog.docx)]
  * Notebook 1 - syslog_rfc3164: [source](/notebooks/source/syslog_rfc3164.py), [html](/notebooks/html/syslog_rfc3164.html), [dbc](/notebooks/dbc/syslog_rfc3164.dbc) 
  * Notebook 2 - syslog_rfc5424: [source](/notebooks/source/syslog_rfc5424.dbc), [html](/notebooks/html/syslog_rfc5424.html), [dbc](/notebooks/dbc/syslog_rfc5424.dbc) 

This connector is not officially supported by Databricks. Please send an email to cybersecurity@databricks.com for help.
