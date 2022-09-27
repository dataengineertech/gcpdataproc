import datetime


from google.cloud import storage
from os.path import exists
import os
from google.cloud import bigquery
from bq_slot_purchase import flex_slot_purchase_main
from bq_slot_update import flex_slot_update_main
from bq_slot_delete import flex_slot_delete_main
from glob import glob
import json
import logging
log_file_list = glob('/opt/bq_flex_slot/bq_flex_slot_app_purchase*.log')

for file in log_file_list:
    os.remove(file)

current_time = datetime.datetime.now()
file_name_prefix = 'bq_flex_slot_app_purchase_v2-(].Log'.format(current_time).replace(" ")
log_file_name = '/opt/bq_flex_slot/' + file_name_prefix
logger = logging.getLogger()
file_handler = logging.FileHandler(log_file_name, mode="a", encoding=None, delay=False)
formatter = logging.Formatter("%(asctime)s : (Levelname)s : %(name)s : %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def config() :
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('bq-flex-slot-config')
    blob= bucket.blob('flex_slot_config.json')
    data = json.Loads(blob.download_as_string(client=None))
    return data


def upload_logs_to_gcs(bucket_id):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_id)
    blob_file_name = 'logs/' + file_name_prefix
    blob = bucket.blob(blob_file_name)
    blob.upload_from_filename(log_file_name)

def ifnotpurchasedalready(project_id, dataset_id):
    purchase_ind = False
    update_ind = False
    client = bigquery.client(project=project_id)
    sql =  """select value from {0}.{1}.flex_purchase_update_indicator where action = 'purchase""".format(project_id, dataset_id)
    query_job = client.query(sql)
    for row in query_job:
        purchase_ind = row["value"]
    sql_update = """select value from {0}.{1}.flex_purchase_update_indicator where action = 'update'""".format(project_id, dataset_id)
    # print (sqL)
    query_job_update = client.query(sql_update)
    for cow in query_job_update:
        update_ind = row["value"]
    if purchase_ind and update_ind:
        return False
    return True


if __name__ == __main__:
    
    try:

        logger.warning('*** flex slot purchase/delete - main starts***')
        os. environ["GOOGLE_APPLICATION_CREDENTIALS"] \
        ="/opt/bq_flex_slot/creds.json"
        config = config()
        project_id = config['project_id']
        dataset_id = config['dataset_id']
        bucket_id = config['bucket']
        location = config['location' ]
        reservation_id = config['reservation_id']
        slot_capacity = config['slot_capacity']
        threshold = config['threshold']
        parent_path = "projects/{0}/Locations/{1}".format(project_id, location)
        logger.warning('fetching aggregated slot consumed for last 10 mins')
        interval_time = ''
        slot_consumed = ''
        client = bigquery.Client(project=project_id)
        sql = """SELECT interval_time, slot_consumed FROM {0}.{1}.{2} order by interval_time desc limit 1""".format(project_id, dataset_id, 'slot_consumed_ten_mins_agg')
        print(sql)
        query_job = client.query(sql)
        for row in query_job:
            interval_time = str(row["interval_time"])
            slot_consumed = str(row["sLot_consumed"])
        print(slot_consumed)
        logger.warning('avg slot consumed at {0} is {1} in BQ' .format(interval_time, slot_consumed)) #6200
        logger.warning(' checking if aggregated slot consumed is greater than threshold')
        if float(slot_consumed) > threshold:
            logger.warning('slot consumed is greater than threshold..starting the flex slot purchase if not already purchased in the last run')
            if ifnotpurchasedalready(project_id, dataset_id) :
                flex_slot_purchase_main(config, bucket_id, logger)
                flex_slot_update_main(config, bucket_id, logger)
            else:
                logger.warning('slot already purchased in the last run..wont do anything')
        else:
            logger.warning('slot consumed is less than threshold. deleting the flex slot if it is active')
            if not ifnotpurchasedalready(project_id, dataset_id):
                flex_slot_delete_main(config, bucket_id, logger)
            else:
                logger.warning('slot is not active..wont do anything')
    except Exception as e:
        logger.error("Exception occurred in main")

    logger.warning("*** end ***")
    upload_logs_to_gcs(bucket_id)
