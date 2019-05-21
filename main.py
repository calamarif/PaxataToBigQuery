__author__ = 'Callum'
# -*- encoding: utf-8 -*-
# Python 3.7.3
#
# This script takes a Paxata library item (or items), and will export to Google BigQuery
# Version: 0.1
# Edit Date: 19 May 2019

from google.cloud import bigquery
from google.cloud.bigquery import Dataset
from google.cloud.bigquery import LoadJobConfig
from google.cloud.bigquery import SchemaField
import os, time, requests, json, string, csv
import pandas as pd
import numpy as np
from io import StringIO
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
from requests.auth import HTTPBasicAuth

def _millis():
    return int(time.time() * 1000)

def load_data_from_dataframe(client,dataset_name,table_name,SCHEMA,df):
    table_ref = client.dataset(dataset_name).table(table_name)
    load_config = LoadJobConfig()
    load_config.skip_leading_rows = 1
    load_config.schema = SCHEMA
    job = client.load_table_from_dataframe(df, table_ref, location='US')
    job.result()  # Waits for table load to complete.
    assert job.state == 'DONE'

def create_a_dataset(client,dataset_name,dataset_description):
    dataset_ref = client.dataset(dataset_name)
    dataset = Dataset(dataset_ref)
    dataset.description = dataset_description
    dataset.location = 'US'
    dataset = client.create_dataset(dataset)  # API request
    return (dataset_ref)

def test_create_table(client,dataset_ref,table_name,SCHEMA):
    table_ref = dataset_ref.table(table_name)
    table = bigquery.Table(table_ref, schema=SCHEMA)
    table = client.create_table(table)  # API request

    assert table.table_id == table_name
    # [END bigquery_create_table]

# Get all of the datasources from Paxata that are tagged with "tag"
def get_tagged_library_items(auth_token,paxata_url,paxata_tag):
    tagged_datasets_ids = []
    tagged_datasets_versions = []
    get_tags_request = (paxata_url + "/rest/library/tags")
    get_tags_response = requests.get(get_tags_request, auth=auth_token, verify=False)
    if (get_tags_response.ok):
        AllTagsDatasetsJson = json.loads(get_tags_response.content)
        i=0
        number_of_datasets = len(AllTagsDatasetsJson)
        while i < number_of_datasets:
            if (AllTagsDatasetsJson[i].get('name') == paxata_tag):
                tagged_datasets_ids.append(AllTagsDatasetsJson[i].get('dataFileId'))
                tagged_datasets_versions.append(AllTagsDatasetsJson[i].get('version'))
            i += 1
    else:
        print ("bad request> " + get_tags_response.status_code)
    return tagged_datasets_ids, tagged_datasets_versions

def get_name_and_schema_of_datasource(auth_token,paxata_url,libraryId,library_version):
    url_request = (paxata_url + "/rest/library/data/"+str(libraryId)+"/"+str(library_version))
    my_response = requests.get(url_request, auth=auth_token, verify=False)
    if(my_response.ok):
        jdata_datasources = json.loads(my_response.content)
        library_name = jdata_datasources.get('name')
        library_version = jdata_datasources.get('version')
        library_schema_dict = jdata_datasources.get('schema')
    return library_name,library_version,library_schema_dict

#function to format illegal chars out of a filename
def format_table_or_column(s):
    # Take a string and return a valid filename constructed from the string."
    valid_chars = "-_ %s%s" % (string.ascii_letters, string.digits)
    filename = ''.join(c for c in s if c in valid_chars)
    filename = filename.replace(' ','_') # Remove spaces in filenames.
    return filename

def test_list_datasets(client):
    """List datasets for a project."""
    # [START bigquery_list_datasets]
    # from google.cloud import bigquery
    # client = bigquery.Client()

    datasets = list(client.list_datasets())
    project = client.project

    if datasets:
        print('Datasets in project {}:'.format(project))
        for dataset in datasets:  # API request(s)
            print('\t{}'.format(dataset.dataset_id))
    else:
        print('{} project does not contain any datasets.'.format(project))
    # [END bigquery_list_datasets]

# [START bigquery_dataset_exists]
def dataset_exists(client, dataset_reference):
    """Return if a dataset exists.

    Args:
        client (google.cloud.bigquery.client.Client):
            A client to connect to the BigQuery API.
        dataset_reference (google.cloud.bigquery.dataset.DatasetReference):
            A reference to the dataset to look for.

    Returns:
        bool: ``True`` if the dataset exists, ``False`` otherwise.
    """
    from google.cloud.exceptions import NotFound

    try:
        client.get_dataset(dataset_reference)
        return True
    except NotFound:
        return False
# [END bigquery_dataset_exists]

# [START bigquery_table_exists]
def table_exists(client, table_reference):
    """Return if a table exists.

    Args:
        client (google.cloud.bigquery.client.Client):
            A client to connect to the BigQuery API.
        table_reference (google.cloud.bigquery.table.TableReference):
            A reference to the table to look for.

    Returns:
        bool: ``True`` if the table exists, ``False`` otherwise.
    """
    from google.cloud.exceptions import NotFound

    try:
        client.get_table(table_reference)
        return True
    except NotFound:
        return False
# [END bigquery_table_exists]

def paxata_to_bigquery(request):
    start = time.time()
    client = bigquery.Client()
    # No spaces in Dataset name or Table Name
    dataset_name = os.environ.get('dataset_name')
    dataset_description = os.environ.get('dataset_description')
    paxata_url = os.environ.get('paxata_url')
    paxata_restapi_token = os.environ.get('paxata_restapi_token')
    #paxata_tag = os.environ.get('tag')

    # Google Cloud Account Variables
    #project_id = request.get_json().get('project_id')
    #private_key_id = request.get_json().get('private_key_id')
    #client_email = request.get_json().get('client_email')
    
    # Google Cloud Data Location Variables
    #dataset_name = request.get_json().get('dataset_name')
    #dataset_description = request.get_json().get('dataset_description')

    #Paxata Variables
    #paxata_url = request.get_json().get('paxata_url')
    #paxata_restapi_token = request.get_json().get('paxata_restapi_token')
    paxata_tag = request.get_json().get('paxata_tag')

    dataset_ref = client.dataset(dataset_name)
    if not dataset_exists(client, dataset_ref):
        # Dataset doesn't exist, so create it
        dataset_ref = create_a_dataset(client,dataset_name,dataset_description)

    # set the authorization based on the username and password provided in the user variables section
    auth_token = HTTPBasicAuth("", paxata_restapi_token)
    SCHEMA = []
    dataset_ref = client.dataset(dataset_name)

    # Configures the query to append the results to a destination table, allowing field addition
    job_config = bigquery.QueryJobConfig()
    job_config.schema_update_options = [
        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
    ]
    #job_config.destination = table_ref
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    tagged_datasets_ids,tagged_datasets_versions = get_tagged_library_items(auth_token, paxata_url, paxata_tag)
    dataset_counter = 0 
    my_logging_message = []
    for id in tagged_datasets_ids:
        library_name,library_version,library_schema_dict = get_name_and_schema_of_datasource(auth_token, paxata_url, id,tagged_datasets_versions[dataset_counter])
        #convert library name to a valid table name (ie with no spaces)
        library_name = format_table_or_column(library_name)
        df_column_names = []

        #convert paxata schema format into google schema
        for field in library_schema_dict:
            column_name = format_table_or_column(field.get("name"))
            df_column_names.append(column_name)

        library_name_with_version = str(library_name) + "_" + str(library_version)
        table_ref = client.dataset(dataset_name).table(library_name_with_version)
        if  table_exists(client, table_ref):
            print ("Table already exists, not doing anything for the table :" + library_name_with_version)
        else:
            # Create schemaless table in Big Query
            test_create_table(client, dataset_ref, library_name_with_version, SCHEMA)
            # Get the data from Paxata locally
            url_request_dataload = paxata_url + "/rest/library/exports?dataFileId=" + id + "&destination=local&format=separator&quoteValues=true&valueSeparator=,&includeHeader=false&version=" + str(library_version)
            datasetToLoad = requests.post(url_request_dataload, auth=auth_token, verify=False)
            if (datasetToLoad.ok):
                paxata_data_in_bytes = (datasetToLoad.content)
                #convert it into a dataframe
                s = str(paxata_data_in_bytes, 'utf-8')
                data = StringIO(s)
                df = pd.read_csv(data, names=df_column_names)
            else:
                datasetToLoad.raise_for_status()

            # Load Data into Big Query
            load_data_from_dataframe(client, dataset_name, library_name_with_version, SCHEMA, df)
            print ("Data Taken from Paxata and loaded into Big Query Table for "+ library_name_with_version)
            end = time.time()
            print ("Time taken to export " + str(len(df)) + " rows to Google BigQuery: " + str(round((end - start),2))+ " seconds")
            my_logging_message.append("Data Taken from Paxata and loaded into Big Query Table for "+ library_name_with_version + "\n" + "Time taken to export " + str(len(df)) + " rows to Google BigQuery: " + str(round((end - start),2))+ " seconds")
        dataset_counter += 1
    return (''.join(my_logging_message))

if __name__ == "__main__":
    paxata_to_bigquery()