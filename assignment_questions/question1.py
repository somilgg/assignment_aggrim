

### this script fetches lowest priced SKU at input time and lowest prices for each SKU at any given point

### scheduling this script with a cron job like  "*/25 * * * *" will publish this data at every 25th minute

import logging
import datetime
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import sys
import os


def write_data_to_bq(data: list, project_id: str, dataset_name: str, table_name: str, write_disposition: str = 'replace'):
    """
    writes data to BigQuery table passed in arguements
    :param data:
    :param project_id:
    :param dataset_name:
    :param table_name:
    :param write_disposition:
    :return:
    """
    df = pd.DataFrame(data)
    credentials_json = "credential_json_here"
    credentials = service_account.Credentials.from_service_account_info(credentials_json)
    df.to_gbq('{}.{}'.format(dataset_name, table_name), project_id=project_id, if_exists=write_disposition, credentials=credentials)
    logging.info("Data Landed Successfully in BigQuery")


def run_bigquery_query(query: str) -> list:
    """
    takes a query as string and returns the result of the query as list of dicts
    :param query:
    :return:
    """
    client = bigquery.Client.from_service_account_json(
        json_credentials_path='path_to_Credential_file_here',
        project='project_id',
    )
    query_job = client.query(query)
    return [dict(row) for row in query_job.result()]


lowest_priced_sku_at_input_time_query = """

select Region, Crop_name, Crop_SKU, and selling_price 
from product_table 
where last_modified_time = {} 
order by selling_price ASC 
LIMIT 1 

"""

lowest_priced_sku = None

try:

    query_time = datetime.datetime.strptime(str(input()), '%Y-%m-%d %H:%M:%S')
    lowest_priced_sku = run_bigquery_query(lowest_priced_sku_at_input_time_query.format(query_time))[0]['Crop_SKU']

    ### publishing the lowest priced SKU at input time
    logging.info('Lowest Priced SKU at input time is : {}'.format(lowest_priced_sku))
except ValueError as e:
    logging.info('input by user for lowest priced SKU is not in correct format, expected format is YYYY-MM-DD HH:MM:SS')




lowest_price_for_each_SKU_query = """

select Crop_SKU, min(selling_price) as lowest_price_of_sku
from product_table 
group by Crop_SKU

"""

lowest_price_for_each_SKU_data = run_bigquery_query(lowest_price_for_each_SKU_query)
write_data_to_bq(lowest_price_for_each_SKU_data, 'project_id_here', 'dataset_name_here', 'table_name_here')

### publishing lowest price data for each SKU in a csv file
pd.DataFrame(lowest_price_for_each_SKU_data).to_csv('path_to_csv_here')
logging.info('CSV for lowest price of each SKU {}'.format('path to csv here'))




