#!/usr/bin/env python
# coding: utf-8

import os
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import json
from google.cloud import storage,bigquery
from datetime import datetime

def get_todays_date():
    today = datetime.now().date()
    return today

def create_df(game_list, region):
    for idx, game in enumerate(game_list):
        if idx == 0:
            main_df = get_data(game, region)
            continue
        # after index 0, append into temp_df
        temp_df = get_data(game, region)
        # append main_df with temp_df
        main_df = pd.concat([main_df, temp_df])

    # reset main_df index
    main_df = main_df.reset_index()

    return main_df


def get_data(game_id, region):
    api_key = os.getenv('PLAT_PRICE_KEY')
    response = requests.get(f"https://platprices.com/api.php?key={api_key}&ppid={game_id}&region={region}")
    json_data = json.loads(response.text)
    #     print(f"json text is type {type(json_data)}")
    #     print(json_data.keys())
    df = pd.DataFrame.from_dict(json_data, orient='index')
    transpose_df = df.transpose()
    return transpose_df


def extract_multi(url, region, last_page):
    page_list = list(range(1, last_page + 1))
    ids = []
    for page in page_list:
        print(f"scraping page {page}")
        response = requests.get(f"{url}?sort=alpha&page={page}&userregion={region}")
        # get soup
        soup = BeautifulSoup(response.content, 'html.parser')
        # Find the div tag with class "game-container-lo"
        game_container_lo = soup.find_all('div', {'class': 'game-container-lo'})
        # regex pattern starts with / followed by string of digits and ends with -
        pattern = r'/(\d+)-'
        # use list comprehension and pattern to extract the ids I want
        temp = [re.search(pattern, game.find('a')['href']).group(1) for game in game_container_lo]
        print(f"found {len(temp)} items")
        ids.append(temp)

    # flatten the lists of lists into a single list
    flatten_ids = [item for sublist in ids for item in sublist]

    print(f"total ids scraped are {len(flatten_ids)} items")

    return flatten_ids


def extract_single(soup):
    # Find the div tag with class "game-container-lo"
    game_container_lo = soup.find_all('div', {'class': 'game-container-lo'})

    # regex pattern starts with / followed by string of digits and ends with -
    pattern = r'/(\d+)-'

    # use list comprehension and pattern to extract the ids I want
    ids = [re.search(pattern, game.find('a')['href']).group(1) for game in game_container_lo]

    return ids


def get_last_page(resultset):
    page_tags = resultset.find_all('a')
    last_tag = page_tags[-1]
    last_page = last_tag.text
    return last_page


def get_ids(url, region):
    """ This function iterates through the plat_price pages for PS Essential/Extra and returns a list of gameids.
    This execution only scrapes Canadian games for now..."""
    response = requests.get(f"{url}?userregion={region}")

    # get soup
    soup = BeautifulSoup(response.content, 'html.parser')

    # checks if a div class='center-xs pagin' exists, if so return the largest page number"
    pagination_yes = soup.find('div', {'class': 'center-xs pagin'})

    # Check if the div tag exists
    if pagination_yes:

        # return the largest page number
        last_page = int(get_last_page(pagination_yes))
        ids = extract_multi(url, region, last_page)
        return ids

    else:
        # extract_single_page
        ids = extract_single(soup)
        return ids

def upload_dataframe_to_gcs(storage_client,df, bucket_name, destination_blob_name):
    """Converts dataframe to csv and uploads the file to the bucket."""

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    # Create a blob in the bucket
    blob = bucket.blob(destination_blob_name)

    # Convert DataFrame to CSV
    df.to_csv(destination_blob_name, index=False)

    # Upload the local file to the blob
    blob.upload_from_filename(destination_blob_name)


def upload_csv_to_bigquery(storage_client,bigquery_client,bucket_name, file_name, dataset_id):

    # Get the Cloud Storage bucket and blob objects
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Extract the file name without the extension
    table_id = os.path.splitext(os.path.basename(file_name))[0]

    # Specify the BigQuery dataset and table where you want to upload the data
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    # Load the CSV file into BigQuery
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1  # Skip the CSV header row
    job_config.autodetect = True  # Automatically detect the schema

    load_job = bigquery_client.load_table_from_uri(
        blob.public_url,
        table_ref,
        job_config=job_config
    )

    load_job.result()  # Wait for the job to complete

    if load_job.state == "DONE":
        print(f"CSV file {file_name} uploaded to BigQuery table {table_id} successfully.")
    else:
        print(f"Error uploading CSV file {file_name} to BigQuery table {table_id}.")


if __name__ == '__main__':

    # Pass Json as a string
    service_account_json = os.getenv('GOOGLE_SERVICE_ACCOUNT')

    #Parse string as json
    credentials = json.loads(service_account_json)

    # Initialize the Cloud Storage and BigQuery clients
    storage_client = storage.Client.from_service_account_info(credentials)
    bigquery_client = bigquery.Client.from_service_account_info(credentials)

    today = get_todays_date()
    essential = get_ids("https://platprices.com/psplus/essential/", 'CA')
    essential_df = create_df(essential, 'CA')
    essential_object = f"essential_df_{today}"
    upload_dataframe_to_gcs(storage_client,essential_df,"plat-prices",f"{essential_object}.csv")
    upload_csv_to_bigquery(storage_client,bigquery_client,"plat-prices", f"{essential_object}.csv", "raw_plat_price")

    extra = get_ids("https://platprices.com/psplus/extra/", 'CA')
    extra_df = create_df(extra, 'CA')
    extra_object = f"extra_df_{today}"
    upload_dataframe_to_gcs(storage_client,extra_df,"plat-prices",f"{extra_object}.csv")
    upload_csv_to_bigquery(storage_client,bigquery_client,"plat-prices", f"{extra_object}.csv", "raw_plat_price")



