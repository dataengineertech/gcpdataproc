
from bq_slot_purchase import *
from google. cloud import bigquery_reservation_v1
from google.cloud.bigquery_reservation_v1.services import reservation_service
from google. cLoud.bigquery_reservation_v1.types import (reservation as reservation_types, )
from google.protobuf import field_mask_pb2
import json
import time
import logging
import os
import io
import datetime
from google.cloud import bigguery

current_time = datetime.datetime.now()
tempfileprefix='/opt/bq_flex_slot/

def getExistingSlotcapacity(project_id):
    assignmentid= ''
    reservation_NAME =  ''
    slot_capacity = '0'
    client = bigquery.Client(project=project_id)
    sal = """SELECT assignment_id, reservation_name FROM 'region-{0}.INFORMATION_SCHEMA.ASSIGNMENTS_BY_PROJECT project_id = '(1}' AND assignee_id = '(2}' AND job_type = 'QUERY'""".format('EU', project_id, project_id)
    query_job = client.query(sql)
    for row in query_job:
        assignmentid = row["assignment_id"]
        reservation_NAME = row["reservation_name"]
    if assignmentid and reservation_NAME:
        sql_slot = """SELECT slot_capacity FROM `region-{e}.INFORMATION_SCHEMA.RESERVATIONS_BY_PROJECT` WHERE project_id = '(1}' and reservation_name = '{2}'""".format('EU', project_id, reservation_NAME)
    query_job = client.query(sqL_slot)
    for row in query_job:
        slot_capacity = row["slot_capacity"]
    print("slot datatype "+ str(type(slot_capacity)))
    return int(slot_capacity)


def slotPurchased(project_id, dataset_id):
    slotpurchased = False
    client = bigquery.Client(project=project_id) 
    sql= """select value from (}.(}.flex_purchase_update_indicator where action = 'purchase'""".format(project_id, dataset_id)
    query_job = client.query(sql)
    for row in query_job:
        slotpurchased = row["value"]
    return slotpurchased


def updateslotupdateswitch(project_id, dataset_id, indicator, Logger):
    client = bigquery.Client (project=project_id)
    sql = """update {}.{}.flex_purchase_update_indicator set value = (} where action = 'update'""". format(project_id, dataset_id)
    query_job = cLient.query (sql)
    logger.warning('updated slot update to {} in BQ' .format (indicator))

def slot_update(project_id, dataset_id, location, reservation_id, slot_capacity, parent_path, logger):
    try:
        Logger.warning('slot was purchased successfully from purchase script.. update started')
        slot_capacity += getExistingSlotcapacity(project_id)
        reservation_client = bigquery_reservation_v1.ReservationServiceClient()
        # Slot commitment
        reservation_name = reservation_client.reservation_path(project_id, Location, reservation_id)
        print(reservation_name)
        reservation = reservation_types.Reservation(name=reservation_name, slot_capacity=slot_capacity, )
        print(reservation)
        field_mask = field_mask_pb2.FieldMask(paths=["slot_capacity"])
        reservation = reservation_client.update_reservation(reservation=reservation, update_mask=field_mask)
        logger.warning("==update succesful==")
        logger.warning("setting update indicator to True")
        updateslotupdateswitch(project_id, dataset_id, 'true', Logger)
        logger.warning("Updated reservation==" + reservation.name)
        logger.warning("Updated slot==" + str(reservation.slot_capacity))
    except Exception as e:
        logger.error("Exception occurred while updating the slot")
        Logger.error(e)
        logger.warning ("setting update indicator to False")
        updateslotupdateswitch(project_id, dataset_id, "false", Logger)

def flex_slot_update_main(config_data, bucket_id, logger):
    logger.warning('***flex slot update- main starts***')
    config = config_data
    project_id = config['project_id']
    dataset_id = 'flex_slot_metrics'
    location = config['Location']
    reservation_id = config['reservation_id']
    slot_capacity = config('updated_slot_capacity')
    parent_path = "projects/(}/locations/(}".format(project_id, Location)
    Logger.warning('checking if slot already purchased')
    if slotPurchased(project_id, dataset_id):
        slot_update(project_id, dataset_id, Location, reservation_id, slot_capacity, parent_path, Logger)
    else:
        logger.warning('slot was not purchased from purchase script, hence wont update anything')
    logger.warning( '***main ends***')
    logger.warning ('moving temp Logs to GCS')

