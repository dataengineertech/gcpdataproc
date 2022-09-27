from google. cloud import bigguery_reservation_v1
from google.cloud.bigquery_reservation_v1 import *
import json
import time
import logging
import os
import io
import datetime
from google.cloud import storage
from os.path import exists
from google.cloud import bigquery

from glob import glob
import bq_slot_purchase
current_time = datetime.datetime.now()
tempfileprefix="/opt/bq_flex_slot/"
capacity_commitment_temp_file = 'capacitycommitmentids.txt'
reservation_temp_file = 'resrvationfileids.txt'
assignment_temp_file ="assignmentids.txt"
capacitycommitmentfilepath = tempfileprefix + capacity_commitment_temp_file
resrvationfilepath = tempfileprefix + reservation_temp_file
assignmentfilepath = tempfileprefix + assignment_temp_file
def upload_flex_temp_files_to_gcs(temp_file, bucket_id)
    storage_client = storage.CLient()
    bucket = storage_client.get_bucket(bucket_id)
    blob_file_name = 'temp/' + temp_file
    blob = bucket.blob(blob_file_name)
    blob.upload_from_filename(tempfileprefix + temp_file)

def filestatus(PATH) :
    append_write = 'a'
    if not os.path.exists (PATH) :
        append_write = 'w'
    return append_write

def assignment_exists(project_id, logger):
    assignmentid = ''
    try:
        logger.warning("bigquery client")
        client = bigquery.Client(project=project_id)
        sql = """SELECT assignment id FROM "region-{0}-INFORMATION- SCHEMA.ASSIGNMENT"""
        logger.warning(sql)
        query_job = client.query (sql)
        for row in query_job:
            assignmentid = row["assignment_id"]
        logger.warning(assignmentid)
    except Exception as e:
        logger.error("Exception Occurred while fetching assignments details")
        logger.error(e)
    return assignmentid


def purchaseToggleoff(project_id, dataset_id) :
    purchase_ind = False
    update_ind = False
    client = bigquery.Client(project=project_id)
    sql = """select value from {0}.{1}.flex_purchase_update_indicator where action = 'purchase'""".format(project_id,dataset_id)
    query_job = client.query(sql)
    for row in query_job:
        purchase_ind = row["value"]
    sql_update = """select value from {0}.{1}.flex_purchase_update_indicator where action = 'update'""".format(project_id,dataset_id)
    query_job_update = client.query(sql_update)
    for row in query_job_update:
        update_ind = row["value"]
    if not purchase_ind and not update_ind:
        return False
    return True

def updateslotpurchaseswitch(project_id, dataset_id, indicator, logger):
    client = bigquery.Client(project=project_id)
    sql = """update {0}.{1}.flex_purchase_update_indicator set value = {2} where action = 'purchase'""". format(project_id,dataset_id,indicator)

def slot_purchase(project_id, dataset_id, location, reservation_id, slot_capacity, parent_path, logger):
    try:
        init_api = bigquery_reservation_v1.ReservationServiceClient()
        # Slot commitment
        logger.warning('slot commitment with FLEX plan')
        commit_config = CapacityCommitment (plan='FLEX', slot_count=slot_capacity)
        commit = init_api.create_capacity_commitment (parent=parent_path, capacity_commitment=commit_config)
        logger.warning (commit)
        if 'ACTIVE' in str(commit.state):
            logger.warning('slot purchased successfully')

            with io.open(capacitycommitmentfilepath, filestatus(capacitycommitmentfilepath)) as capacitycommitment_file:
                capacitycommitment_file.write(commit.name)
                capacitycommitment_file.close()
                updateslotpurchaseswitch(project_id, dataset_id,'true', logger)
            logger.warning('sleeps for 20s')
            time.sleep(20)
            logger.warning ('check assignments')
            # slot reservation
            reservation_name = init_api.reservation_path(project_id, location, reservation_id)
            # if reservation_requested != reservation_name:
            if not assignment_exists(project_id, logger):
                logger.warning(' slot reservation starts')
                reservation_config = Reservation(slot_capacity=slot_capacity, ignore_idle_slots=False)
                reserv = init_api.create_reservation(parent=parent_path, reservation_id=reservation_id, reservation=reservation_config)
                logger.warning(reserv)
                logger.warning('slot reservation successful')
                with io. open(resrvationfilepath, filestatus (resrvationfilepath)) as resrvation_file:
                    resrvation_file.write(reserv.name+',')
                resrvation_file.close()

                # slot-assignment
                logger.warning('slot assignment starts')
                reservation_name = reserv.name
                assign_config = Assignment(job_type='QUERY', assignee='projects/{0}'.format(project_id))
                assign = init_api.create_assignment(parent=reservation_name, assignment=assign_config)
                logger.warning(assign)
                logger.warning('slot assignment successful')
                with open(assignmentfilepath, filestatus(assignmentfilepath)) as assignment_file:
                    assignment_file.write(assign.name)
                assignment_file.close()
            else:
                logger.warning('reservation with name (0} already exists..it will be updated in the update script'.format(reservation_name))

    except Exception as e:
        logger.error ("Exception occurred while slot purchase/")
        logger.error(e)
        logger.error ("setting the purchase indicator to False in BQ")
        updateslotpurchaseswitch(project_id, dataset_id, 'false', logger)


def flex_slot_purchase_main(config_data, bucket_id, logger):
    logger.warning('***flex slot purchase- main starts***')
    config = config_data
    project_id = config['project_id']
    dataset_id = 'flex_slot_metrics'
    location = config['location']
    reservation_id = config['reservation_id']
    slot_capacity = config['slot_capacity']
    parent_path = "projects/(}/locations/(]".format(project_id, location)
    logger.warning(' checking if the the purchase indicator is false')
    if not purchaseToggleoff(project_id, dataset_id):
        slot_purchase(project_id, dataset_id, location, reservation_id, slot_capacity, parent_path, logger)
    else:
        logger.warning(' purchase indicator is not false or delete did not succeed..so wont buy any slots')
    # Logger.warning("***main ends***')
    logger.warning('moving temp files and Logs to GCS')
    if exists(capacitycommitmentfilepath):
        upload_flex_temp_files_to_gcs(capacity_commitment_temp_file, bucket_id)
    if exists(resrvationfilepath):
        upload_flex_temp_files_to_gcs(reservation_temp_file, bucket_id)
    if exists(assignmentfilepath):
        upload_flex_temp_files_to_gcs(assignment_temp_file, bucket_id)
# upload_logs_to_gcs()

