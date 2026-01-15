# Databricks Splunk Integration 

[Features](#Features) |
[Architecture](#Architecture) |
[Documentation References](#Documentation) |
[Compatibility](#Compatibility)|
[Log Ingestion Examples](#log-ingestion) |
[Feedback](#feedback) |
[Legal Information](#legal-information) 



The Splunk Integration project is a non-supported bidirectional connector consisting of three main components as depicted in the [architecture](#Architecture) diagram:

1. The [Databricks add-on for Splunk](https://splunkbase.splunk.com/app/5416/), an app, that allows Splunk Enterprise and Splunk Cloud users to run queries and execute actions, such as running notebooks and jobs, in Databricks
1. Splunk SQL database extension (Splunk DB Connect) configuration for Databricks connectivity
1. Notebooks for Push and Pull events and alerts from Splunk Databricks.

We also provided extensive documentation for Log Collection to ingest, store, and process logs on economical and performant Delta lake.

## Features

- Run Databricks SQL queries right from the Splunk search bar and see the results in Splunk UI  ([Fig 1](#fig-1) )
- Execute actions in Databricks, such as notebook runs and jobs, from Splunk ([Fig 2 & Fig 3](#fig-2))
- Use Splunk SQL database extension to integrate Databricks information with Splunk queries and reports ([Fig 4 & Fig 5](#fig-4))
- Push events, summary, alerts to Splunk from Databricks ([Fig 6 and Fig 7](#fig-6))
- Pull events, alerts data from Splunk into Databricks ([Fig 8](#fig-8))
---
###### Fig 1: 
Run Databricks SQL queries right from the Splunk search bar and see the results in Splunk UI
<br/><img src="/docs/markdown/images/databricksquery.png" height="70%" width="70%"><br/>
###### Fig 2: 
Execute actions in Databricks, such as notebook runs and jobs, from Splunk
<br/><img src="/docs/markdown/images/databricksrun.png" height="70%" width="70%"><br/>
###### Fig 3:
<img src="/docs/markdown/images/databricksjob.png" height="70%" width="70%"><br/>
###### Fig 4: 
Use Splunk SQL database extension to integrate Databricks information with Splunk queries and reports <br/>
<img src="/docs/markdown/images/dbconnect1.png" height="70%" width="70%"><br/>
###### Fig 5: 
<img src="/docs/markdown/images/dbconnect2.png" height="70%" width="70%"><br/>
###### Fig 6: 
Push events, summary, alerts to Splunk from Databricks 
<br/> <img src="/docs/markdown/images/pushtosplunk1.png" height="70%" width="70%"><br/>
###### Fig 7: 
<img src="/docs/markdown/images/pushtosplunk2.png" height="70%" width="70%"><br/>
###### Fig 8:
Pull events, alerts data from Splunk into Databricks
<br/> <img src="/docs/markdown/images/pullfromsplunk.png" height="70%" width="70%"><br/>
---

## Architecture

<img src="/docs/markdown/images/functional_architecture.png" height="70%" width="70%">


## Documentation

* Databricks Add-on for Splunk Integration Installation And Usage Guide:
   * Documentation:  [[markdown](/docs/markdown/Splunk%20Integration%20Installation%20And%20Usage%20Guide%20-%201.1.0.md), [pdf](/docs/pdf/Splunk%20Integration%20Installation%20And%20Usage%20Guide%20-%201.1.0.pdf), [word](/docs/word/Splunk%20Integration%20Installation%20And%20Usage%20Guide%20-%201.1.0.docx)]
   * [Link to Databricks add-on for Splunk on Splunkbase](https://splunkbase.splunk.com/app/5416)
* Splunk DB Connect Guide for Databricks:
  * Documentation:  [[markdown](/docs/markdown/Splunk%20DB%20Connect%20guide%20for%20Databricks.md), [pdf](/docs/pdf/Splunk%20DB%20Connect%20Guide%20for%20Databricks.pdf), [word](/docs/word/Splunk%20DB%20Connect%20Guide%20for%20Databricks.docx)]
* Push Data to Splunk from Databricks.docx: 
  * Documentation: [[markdown](/docs/markdown/Databricks%20-%20Push%20to%20Splunk.md), [pdf](/docs/pdf/Push%20Data%20to%20Splunk%20from%20Databricks.pdf), [word](/docs/word/Push%20Data%20to%20Splunk%20from%20Databricks.docx)]
  * Notebook - `push_to_splunk`: [source](/notebooks/source/push_to_splunk.py)
* Pull Data from Splunk into Databricks.docx:
  * Documentation:  [[markdown](/docs/markdown/Databricks%20-%20Pull%20from%20Splunk.md), [pdf](/docs/pdf/Push%20Data%20to%20Splunk%20from%20Databricks.pdf), [word](/docs/word/Push%20Data%20to%20Splunk%20from%20Databricks.docx)]
  * Notebook - `pull_from_splunk`: [source](/notebooks/source/pull_from_splunk.py)
  
## Compatibility

Databricks Add-on for Splunk, notebooks and documentation provided in this project are compatible with:
  * Splunk Enterprise version: 9.3.x, 9.2.x and 9.1.x
  * Databricks REST API: 1.2 and 2.0:
    * Azure Databricks
    * AWS SaaS, E2 and PVC deployments
    * GCP
  * OS: Platform independent
  * Browser: Safari, Chrome and Firefox

## Log ingestion

This project also provides documentation and notebooks to showcase specifics on how to use Databricks for collecting various logs (a comprehensive list is provided below) via stream ingest and batch-ingest using Databricks autoloader and Spark streaming into cloud Data lakes for durable storage on S3. The included documentation and notebooks also provide methods and code details for each log type: parsing, schematizing, ETL/Aggregation, and storing in Delta format to make them available for analytics. 

Data collection sources with notebooks and documentation are included for the following sources: 

* Cloudtrail logs:
  * Documentation: [[markdown](/docs/markdown/Databricks%20%20-%20AWS%20CloudTrail.md), [pdf](/docs/pdf/Databricks%20%20-%20AWS%20CloudTrail.pdf), [word](/docs/word/Databricks%20%20-%20AWS%20CloudTrail.docx)]
  * Notebook 1 - `cloudtrail_ingest`: [source](/notebooks/source/cloudtrail_ingest.py)
  * Notebook 2 - `cloudtrail_insights_ingest`: [source](/notebooks/source/cloudtrail_insights_ingest.py)
* VPC flow logs:
  * Documentation: [[markdown](/docs/markdown/Databricks%20-%20AWS%20VPC%20Logs.md), [pdf](/docs/pdf/Databricks%20-%20AWS%20VPC%20Logs.pdf), [word](/docs/word/Databricks%20-%20AWS%20VPC%20Logs.docx)]
  * Notebook - `vpc_flowlogs_ingest`: [source](/notebooks/source/vpc_flowlogs_ingest.py)
* Syslog:
  * Documentation: [[markdown](/docs/markdown/Databricks%20-%20Syslog.md), [pdf](/docs/pdf/Databricks%20-%20Syslog.pdf), [word](/docs/word/Databricks%20-%20Syslog.docx)]
  * Notebook 1 - `syslog_rfc3164`: [source](/notebooks/source/syslog_rfc3164.py)
  * Notebook 2 - `syslog_rfc5424`: [source](/notebooks/source/syslog_rfc5424.py)

## Feedback

Issues with the application?  Found a bug?  Have a great idea for an addition?
Feel free to [file an issue](https://github.com/databrickslabs/splunk-integration/issues) or submit a pull request.

## Legal Information

This software is provided as-is and is not officially supported by Databricks through customer technical support channels.
Support, questions, help, and feature requests can be communicated via email -> [cybersecurity@databricks.com](mailto:cybersecurity@databricks.com) or through the Issues page of this repo.





