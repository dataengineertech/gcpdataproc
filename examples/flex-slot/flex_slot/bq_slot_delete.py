from google.cloud import bigquery_reservation_v1
from google.cloud.bigquery_reservation_v1 import *
from google.cloud.bigquery_reservation_v1.services import reservation service
from google.cloud.bigquery_reservation_v1.types import (reservation as reservation_types, )
from google.protobuf import field_mask_pb2
import json
import time
import logging
import os
import io
import datetime
from google.cloud import storage
from google.cloud import bigquery
from glob import glob


current_time = datetime.datetime.now()


def read_from_gcs(bucket, file):
    blob = bucket.blob(file)
    data = bLob.download_as_string(client=None)
    tempdata = str(data)[2:len(str(data)) - 1]
    print(tempdata)
    return tempdata

def getExistingSlotcapacity(project_id):
    assignmentid = '' 
    reservation_NAME = ''
    slot_capacity = '0'
    client = bigquery.Client (project=project_id)
    sql = """SELECT assignment_id, reservation_name FROM region- {O}.INFORMATION_SCHEMA.ASSIGNMENTS BY PROJECT where project_id = '{1}' and assignee_id ='{}' AND job_type ='QUERY'""".format('EU', project_id, project_id)
    query_job = client.query(sql)
    for row in query_job:
        assignmentid = row["assignment_id"]
        reservation_NAME = row["reservation_name"]
    if assignmentid and reservation NAME:
    sql_slot =  """SELECT slot capacity FROM `region-{0}.INFORMATION SCHEMA.RESERVATIONS_BY_PROJECT`  WHERE project_id = '{1} and reservation_name = '{2}'""".format("EU",project_id, reservation_NAME)
    query_job = client.query(sql_slot)
    for row in query_job:
        slot_capacity = row["slot_capacity"]
    return int(slot_capacity)

def slotpurchased(project_id, dataset_id):
    slotpurchased = False
    slotupdated = False
    client = bigquery.Client(project=project_id)
    sql_purchase = """select value from {0}.{1}.flex_purchase_update_indicator where action = 'purchase'""".format(project_id, dataset_id)
    query_job = client.query(sql_purchase)
    for row in query_job:
        slotpurchased = row["value"]
        sql_update = """select value from {}.{}.flex_purchase_update_indicator where action = 'update'""".format(project_id, dataset_id)
    query_job_update = client.query(sql_update)
    for row in query_job_update:
        slotupdated = row[ "value"]
    return slotpurchased and slotupdated

def updatepurchasedeleteswitch(project_id, dataset_id, indicator, logger):
    client = bigquery.Client(project=project_id)
    sql_purchase = """update {0}.{0}.flex_purchase_update_indicator set value = (} where action = 'purchase'""".format(project_id, dataset_id, indicator)
    query_job = client.query(sql_purchase)
    logger.warning('updated slot purchase indicator to {0} in BQ'.format(indicator))
    sql_update = "'"update {0}.{1}.flex_purchase_update_indicator set value = {2} where action = 'update'"'". format(project_id, dataset_id, indicator)
    query_job1 = client.query(sql_update)
    logger.warning('updated stot update indicator to (} in BQ' .format(indicator))


def flex_slot_delete_main(config_data, bucket_id, logger):
    logger.warning('***flex slot delete- main starts***')
    # config = config()
    # read_flex_purchase_details()
    project_id = config['project_id']
    dataset_id = 'flex_slot_metrics'
    location = config['location' ]
    parent_path = "projects/(}/locations/(}". format(project_id, location)
    parent_path_reserv = "projects/(}/locations/(}/reservations/-".format(project_id, location)
    capacitycommitmentids = []

    logger.warning ('checking if the purchase was done successfully')
    if slotpurchased(project_id, dataset_id) :
        logger.warning('slot was purchased and updated from update script. now, deletion started')
        storage_client = storage.Client()
        files = []
        bucket = storage_client.get_bucket(bucket_id)
        blobs = storage_client.list_blobs(bucket_id, prefix='temp/')
        for blob in blobs:
            print (blob.name)
            files.append(blob.name)
        for file in files:
            if 'capacitycommitmentids.txt' in str(file):
                data = read_from_gcs(bucket, file)
            if data:
                capacitycommitmentids = data.split(', ')
        logger.warning("flex commitments {}". format(capacitycommitmentids))
        init_api=bigquery_reservation_v1.ReservationServiceClient()
        try:
            # reduce the flex slot from the reservation
            slot_capacity = config['updated_slot_capacity']
            reservation_id = config['reservation_id']
            slot_capacity = getExistingSlotcapacity(project_id) - slot_capacity
            reservation_client = bigquery_reservation_v1.Reservationserviceclient()
            reservation_name = reservation_client.reservation_path(project_id, Location, reservation_id)
            reservation = reservation_types.Reservation(name=reservation_name, slot_capacity=slot_capacity, )
            field_mask = field_mask_pb2.FieldMask(paths=["slot_capacity"])
            reservation = reservation_client.update_reservation(reservation=reservation, update_mask=field_mask)
            # Now release the purchased slots
            req_cap = bigquery_reservation_v1.ListCapacityCommitmentsRequest(parent=parent_path)
            for commitment in init_api.List_capacity_commitments(request=req_cap):
            # logger.warning ("commitments = (]". format (commitment))
            if commitment.name in capacitycommitmentids:
            logger.warning("deleting flex capacity commitment (}".format(commitment.name))
            for commitment in init_api.List_capacity_commitments(request=req_cap):
            # Logger.warning ("commitments = ()". format (commitment))
            if commitment.name in capacitycommitmentids:
            logger.warning ("deleting flex capacity commitment (}".format(commitment.name))
            init_api.delete_capacity_commitment(name=commitment.name)
            os.remove('/opt/bq_flex_slot/capacitycommitmentids.txt*)
            Logger.warning(
            "deletion successful, setting purchase update indicator to False. so that next purchase can run')
            updatepurchasedeleteswitch(project_id, dataset_id,
            'false', Logger)
        except Exception as e:
            logger.error ("Exception occurred while deleting Flex slot ")
            Logger.error(e)
            logger.error ("setting purchase/delet indicator to True as deletion did not happen")
            updatepurchasedeleteswitch(project_id, dataset_id,
            'true', Logger)
    else:
        logger.warning('slot purchase was not successful..so wont delete anything')
    logger.warning('***flex slot delete- main ends***')
# upload_logs_to_gcs()