# Pull Data from Splunk into Databricks

**User Guide 1.0.0**

Note:

This document is produced by Databricks as a reference. Databricks makes no warranties or guarantees. Information contained within may not be the most up-to-date available. Information in this document is subject to change without notice. Databricks shall not be liable for any damages resulting from technical errors or omissions that may be present in this document, or from use of this document.

Databricks and the Databricks logo are trademarks or service marks of Databricks, Inc. in the US and other countries. Microsoft Azure and Azure are trademarks of Microsoft Inc. Azure Databricks is a product provided by Microsoft, please see their website for additional information. All other trademarks within this document are property of their respective companies. Copyright 2020 Databricks, Inc. For more information, visit [http://www.databricks.com](http://www.databricks.com/).

Technical documentation and support materials include details based on the full set of capabilities and features of a specific release. Access to some functionality requires specific license types (tiers). 

# Contents
- [Overview](#Overview)
- [Pull Data From Splunk](#Pull%20Data%20From%20Splunk)
    - [Using Databricks secret scope to store the Splunk Password securely](#Using%20Databricks%20secret%20scope%20to%20store%20the%20Splunk%20Password%20securely)
    - [Notebook Parameters](#Notebook%20Parameters)
    - [Modular code in the notebook](#Modular%20code%20in%20the%20notebook)
    - [Import Splunk Instance's Certificate in Databricks](#Import%20Splunk%20Instance's%20Certificate%20in%20Databricks)
- [Limitations](#Limitations)
- [References](#References)

# Overview

This document provides information on how to get data from Splunk into Databricks using a Databricks notebook.The notebook is used to execute search queries on a Splunk Instance, fetch the results obtained into Databricks and convert them into data frames for users to use as per their use case.

# Pull Data From Splunk

**Follow the steps below to pull data from Splunk to Databricks.**

## Using Databricks secret scope to store the Splunk Password securely

In the notebook to pull data from Splunk, the Splunk Password is required. Since the Splunk Password is very sensitive information, the user needs to configure Databricks secrets to store the password securely. This enables the users to store the password securely and not expose it in the notebook in plain text form.

To securely store the Splunk password, follow the following steps:

1. Install Databricks CLI and setup authentication using the below commands

    **Install:** `pip install databricks-cli`
    
    **Set-up:** `databricks configure --token`

    Running the setup command opens an interactive CLI. Specify the Databricks host address and the token value prompted and Enter. The token here refers to a personal access token created in Databricks. Follow the steps [here](https://docs.databricks.com/dev-tools/api/latest/authentication.html) to configure the token.

    Refer [https://docs.databricks.com/dev-tools/cli/index.html#set-up-the-cli](https://docs.databricks.com/dev-tools/cli/index.html#set-up-the-cli) for more details regarding Databricks CLI.

2. Create a Databricks backed Secret scope using following commands:

    `databricks secrets create-scope --scope <scope-name>`

    **Or**

    `databricks secrets create-scope --scope <scope-name> --initial-manage-principal users`

    Refer [https://docs.databricks.com/security/secrets/secret-scopes.html#create-a-databricks-backed-secret-scope](https://docs.databricks.com/security/secrets/secret-scopes.html#create-a-databricks-backed-secret-scope) for more details regarding databricks scope creation.

3. Create a key and store the Splunk password using following command:

    `databricks secrets put --scope <scope-name> --key <key-name>`

    The command opens an editor. Enter or paste the Splunk password here and save it.

    Refer [https://docs.databricks.com/security/secrets/secrets.html#create-a-secret](https://docs.databricks.com/security/secrets/secrets.html#create-a-secret) for more details regarding key creation in the Databricks scope.


## Notebook Parameters

The notebook pull_from_splunk is used to execute queries on a Splunk instance, fetch the results obtained into Databricks and convert them into data frames for users to use as per their use case.

The notebook contains the following parameters ( **\*** denotes mandatory field). Fill in these parameters based on your Splunk deployment to run the notebook.

- **Splunk Ip/Hostname \***: The Splunk Ip/Hostname to pull data from.
- **Splunk Management Port \***: The Splunk Management port. The management port is a request-response model communication path, implemented as REST over HTTP for communication with splunkd. Default Splunk Management Port is `8089`.You can get this value from Splunk Admin.
- **Verify Certificate \***: Specify if SSL server certificate verification is required for communication with Splunk. If you set this as True, you may have to import a custom certificate from the Splunk server into Databricks. For this refer section: [Import Splunk Instance's Certificate in Databricks](#Import%20Splunk%20Instance's%20Certificate%20in%20Databricks). You can get the custom certificate from Splunk Admin.
- **Databricks Secret Scope \***: The Databricks Secret Scope created using Databricks CLI to store the Splunk password in a Databricks Secret Key corresponding to a Splunk username created in step 2 here: [Databricks secret scope](#Using%20Databricks%20secret%20scope%20to%20store%20the%20Splunk%20Password%20securely)
- **Secret Key \***: The secret key associated with specified Databricks Secret Scope which securely stores the Splunk password. Created in step 3 here: [Databricks secret scope](#Using%20Databricks%20secret%20scope%20to%20store%20the%20Splunk%20Password%20securely)
- **Splunk Query \***: The Splunk search query whose results you want to pull from Splunk. Specify the time range for search using the earliest and latest time modifiers. If the time range is not specified, the default time range (last 24 hrs) would be used.  
Example: `index="<my-index>" earliest=-24h@h latest=now() | table *`
- **Splunk Search Mode \***: The search mode for Splunk search query execution. They include verbose, fast, and smart modes.
 Refer: [Search Modes](https://docs.splunk.com/Documentation/Splunk/8.1.2/Search/Changethesearchmode) to understand the three Splunk search modes.
- **Splunk App Namespace (Optional)**: The Splunk application namespace in which to restrict the Splunk search query to be executed, that is, the app context. You can obtain this from Splunk Admin or Splunk user. If not specified the Splunk default application namespace is used.
- **TableName (Optional)**: A Table name to create a table based on the Splunk search results. This is used if the user wants to create a Databricks table from search results. In this case, the user will have to modify the notebook cmd 6. In cmd 6 of the notebook, comment out the line calling function: process_search_results and uncomment the line calling function store_search_results.

To run the notebook, **attach it to a cluster**, fill in all the required parameters and select the **Run All** option. In case of any error, the error is displayed at the bottom of the notebook cells where it occurred.  


## Modular code in the notebook

The cmd 5 in the notebook contains a class Client that is used to authenticate the user to Splunk, submit the search query to Splunk, poll the Splunk search status and fetch and convert the result obtained into data frames.

- The function `connect` is used for authenticating.
- The function `create_search` submits the search query to Splunk.
- The function `is_search_done` polls the submitted search query submitted for completion.
- The function `process_search_results` fetches the search results in chunks (as dictated by the value of maxresultrows parameter in `searchresults` stanza in Splunk's `limits.conf`: default value of 50000) over multiple REST API calls for the search identified by the search id for the submitted search query. It then converts each chunk as obtained into data frames. The user can now add their logic to use these data frames as required.
- Also, the function `store_search_results` provided is similar to `process_search_results` except, it uses the data frames created to generate a Databricks table. This function is not called in the existing workflow of the notebook. However, the user can uncomment function call to `store_search_results` function in cmd 6 of the notebook to use it for creating the tables

## Import Splunk Instance's Certificate in Databricks

This step is needed only if your Splunk instance has been configured to use a certificate and that certificate is not trusted by your browser/system. Follow the below steps to import the certificate.

- Create an init script that adds the entire CA chain and sets the `REQUESTS_CA_BUNDLE` property as follows in Databricks. In this example, PEM format CA certificates are added to the file `myca.crt` which is located at `/usr/local/share/ca-certificates/`.
    `<YOUR CERTIFICATE CONTENT>` can be obtained from Splunk Admin.
    The `myca.crt` and `myca.pem` file names used here are just examples. Please use filenames that don't exist already in the locations as it may cause issues.


    ```sh
    #!/bin/bash

    cat << 'EOF' > /usr/local/share/ca-certificates/myca.crt
    -----BEGIN CERTIFICATE-----
    <YOUR CERTIFICATE CONTENT>
    -----END CERTIFICATE-----
    EOF

    update-ca-certificates

    PEM_FILE="/etc/ssl/certs/myca.pem"
    PASSWORD="<password>"
    KEYSTORE="/usr/lib/jvm/java-8-openjdk-amd64/jre/lib/security/cacerts"

    CERTS=$(grep 'END CERTIFICATE' $PEM_FILE| wc -l)

    # To process multiple certs with keytool, you need to extract
    # each one from the PEM file and import it into the Java KeyStore.

    for N in $(seq 0 $(($CERTS - 1))); do
    ALIAS="$(basename $PEM_FILE)-$N"
    echo "Adding to keystore with alias:$ALIAS"
    cat $PEM_FILE |
        awk "n==$N { print }; /END CERTIFICATE/ { n++ }" |
        keytool -noprompt -import -trustcacerts             -alias $ALIAS -keystore $KEYSTORE -storepass $PASSWORD
    done

    echo "export REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt" >> /databricks/spark/conf/spark-env.sh
    ```


- Attach the init script to the cluster as a [cluster-scoped init script](https://docs.databricks.com/clusters/init-scripts.html#cluster-scoped-init-scripts).
- Restart the cluster.

Refer [Importing custom certificates to Databricks](https://kb.databricks.com/python/import-custom-ca-cert.html) for more details on importing custom certificates in databricks.

**Now you can use this cluster to run the notebook when you want to use SSL server certificate verification for communication with Splunk.**

# Limitations

- The number of results returned by this notebook depends on the Splunk REST API limits. User needs to configure the limits on the Splunk Instance to ensure they get all the results.

    **Example 1**: By default Splunk allows only 500000 results for queries that return events (query containing streaming commands only). This is controlled by the `max_count` parameter (default value 500000) in the `search` stanza in Splunk's `limits.conf`.

    In case you have a query that returns in total more events than the default `max_count` value, you can modify the `max_count` accordingly. 

    **Example  2**: Whenever  queries that return events (query containing streaming commands only) are executed in verbose mode, it returns 1000 events only. 

    In this case, append `| table <required fields list>` **or** `| table *` to query.


# References

- Self-signed certificates Splunk: [https://docs.splunk.com/Documentation/Splunk/8.1.1/Security/Howtoself-signcertificates](https://docs.splunk.com/Documentation/Splunk/8.1.1/Security/Howtoself-signcertificates)
[https://docs.splunk.com/Documentation/Splunk/8.1.1/Security/HowtoprepareyoursignedcertificatesforSplu](https://docs.splunk.com/Documentation/Splunk/8.1.1/Security/HowtoprepareyoursignedcertificatesforSplunk)
- Importing Splunk custom certificates to Databricks: [https://kb.databricks.com/python/import-custom-ca-cert.html](https://kb.databricks.com/python/import-custom-ca-cert.html)
- Databricks CLI setup: [https://docs.databricks.com/dev-tools/cli/index.html#](https://docs.databricks.com/dev-tools/cli/index.html#)
- Databricks Secret scope: [https://docs.databricks.com/security/secrets/secret-scopes.html](https://docs.databricks.com/security/secrets/secret-scopes.html)
[https://docs.databricks.com/security/secrets/secrets.html](https://docs.databricks.com/security/secrets/secrets.html)
- Splunk search endpoints: 
[https://docs.splunk.com/Documentation/Splunk/8.1.1/RESTREF/RESTsearch#search.2Fjob](https://docs.splunk.com/Documentation/Splunk/8.1.1/RESTREF/RESTsearch#search.2Fjobs)
[https://docs.splunk.com/Documentation/Splunk/8.1.1/RESTREF/RESTsearch#search.2Fjobs.2F.7Bsearch\_id.7D.2Fresults](https://docs.splunk.com/Documentation/Splunk/8.1.1/RESTREF/RESTsearch#search.2Fjobs.2F.7Bsearch_id.7D.2Fresults)
- Splunk Authentication endpoint: 
[https://docs.splunk.com/Documentation/Splunk/8.1.1/RESTREF/RESTaccess#auth.2Flogin](https://docs.splunk.com/Documentation/Splunk/8.1.1/RESTREF/RESTaccess#auth.2Flogin)
