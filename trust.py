#!/usr/bin/env python3

import logging
from time import sleep
import json
import datetime
from collections import Counter, OrderedDict

import stomp

from common import database, config

def f_timestamp(timestamp):
    if not timestamp:
        return (0, None)
    timestamp = int(int(timestamp)/1000)
    return (
        timestamp,
        datetime.datetime.utcfromtimestamp(timestamp).strftime("%Y-%m-%dT%H:%M:%S")
        )

def convert_ts(timestamp):
    if timestamp:
        return int(int(timestamp)/1000)
    else:
        return None

def date_today():
    return datetime.datetime.today().date()

# I present to you... logging in Python! (TOO MANY LINES)
fh = logging.FileHandler('logs/trust.log')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
fh.setLevel(logging.DEBUG)

log = logging.getLogger("TRUST")
log.setLevel(logging.DEBUG)
format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s", '%Y-%m-%dT%H:%M:%S')
ch.setFormatter(format)
fh.setFormatter(format)
log.addHandler(fh)
log.addHandler(ch)

MESSAGES = {
    "0001" : "activation",
    "0002" : "cancellation",
    "0003" : "movement",
    "0004" : "_unidentified",
    "0005" : "reinstatement",
    "0006" : "origin change",
    "0007" : "identity change",
    "0008" : "location change",
    }

MOVEMENT_TYPES = {
    "DEPARTURE": "D",
    "ARRIVAL": "A",
    "DESTINATION": "A",
    }

VARIATION_TYPES = {
    "ON TIME":  "O",
    "EARLY":    "E",
    "LATE":     "L",
    "OFF ROUTE":"-",
    }

def connect_and_subscribe(mq):
    for n in range(1,32):
        try:
            log.info("Connecting... (attempt %s)" % n)
            mq.start()
            mq.connect(**{
                "username": config.get("nr-username"),
                "passcode": config.get("nr-password"),
                "wait": True,
                "client-id": config.get("nr-username"),
                })
            mq.subscribe(**{
                "destination": config.get("trust-subscribe"),
                "id": 1,
                "ack": "client-individual",
                "activemq.subscriptionName": config.get("trust-identifier"),
                })
            log.info("Connected!")
            return
        except Exception as e:
            log.exception("Failed to connect. Next attempt in {}s".format(n**2))
            sleep(n**2)
    log.error("Connection attempts exhausted")

class Listener(stomp.ConnectionListener):
    def __init__(self, mq, cursor):
        self._mq = mq
        self.cursor = cursor

    def on_message(self, headers, message):
        c = self.cursor

        self._mq.ack(id=headers['message-id'], subscription=headers['subscription'])
        parsed = json.loads(message)

        c.execute("BEGIN;")
        for train in parsed:
            try:
                head = train["header"]
                body = train["body"]
                type = head["msg_type"]

                trust_id = body.get("current_train_id") or body.get("train_id")
                toc_id = body.get("toc_id")

                headcode, tspeed = trust_id[2:6], trust_id[6]

                if type=="0001": # Activation
                    c.execute("UPDATE flat_schedules SET (trust_id, actual_signalling_id, actual_service_code, activation_datetime, train_call_type)=(%s, %s, %s, %s, %s) WHERE uid=%s AND start_date=%s;", (trust_id, headcode, body["train_service_code"], convert_ts(body["creation_timestamp"]), body["train_call_type"][0], body["train_uid"], body["tp_origin_timestamp"]))
                elif type=="0002": # Cancellation
                    pass
                elif type=="0003": # Movement
                    relative_variation = int(body["timetable_variation"])
                    if body["variation_status"][0]=="E":
                        relative_variation = 1 - relative_variation

                    direction_ind = body["direction_ind"]
                    if direction_ind:
                        direction_ind = direction_ind[0]

                    c.execute("""INSERT INTO flat_schedules
                        (start_date, trust_id, actual_signalling_id, actual_service_code, current_location, current_variation)
                        VALUES (%s, %s, %s, %s, (SELECT iid FROM locations WHERE stanox=%s LIMIT 1), %s)
                        ON CONFLICT (start_date, trust_id) DO UPDATE SET (actual_service_code, current_location, current_variation)=
                        (%s, (SELECT iid FROM locations WHERE stanox=%s ORDER BY crs LIMIT 1), %s)
                        RETURNING iid;""", (date_today(), trust_id, headcode, body['train_service_code'], body['loc_stanox'], relative_variation,
                            body['train_service_code'], body['loc_stanox'], relative_variation))

                    c.execute("""INSERT INTO trust_movements
                        (flat_schedule_iid, stanox, datetime_scheduled, datetime_actual, movement_type,
                        actual_platform, actual_route, actual_line, actual_variation_status, actual_variation,
                        actual_direction, actual_source) VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""", (
                        c.fetchone()[0], body["loc_stanox"], convert_ts(body["planned_timestamp"]) or None, convert_ts(body["actual_timestamp"]),
                        MOVEMENT_TYPES[body["planned_event_type"]], body["platform"], body["route"], body["line_ind"],
                        VARIATION_TYPES[body["variation_status"]], body["timetable_variation"], direction_ind,
                        body["event_source"][0]))
                elif type=="0005": # Reinstatement
                    pass
                elif type=="0006": # Origin change
                    pass
                elif type=="0007": # Identity change
                    c.execute("UPDATE flat_schedules SET (trust_id,actual_signalling_id)=(%s,%s) WHERE trust_id=%s;",
                        (body["revised_train_id"], body["revised_train_id"][2:6], trust_id))
                elif type=="0008":
                    pass
                else:
                    log.warning("Unknown message type: " + type)
                    continue
            except Exception as e:
                log.exception("Failed to insert individual record")
        c.execute("COMMIT;")

    def on_error(self, headers, message):
        print('received an error "%s"' % message)

    def on_heartbeat_timeout(self):
        log.error("Heartbeat timeout")
        self._mq.set_listener("swallow", self)
        connect_and_subscribe(self._mq)

    def on_disconnected(self):
        log.error("Disconnected")


mq = stomp.Connection([('datafeeds.networkrail.co.uk', 61618)],
    keepalive=True, heartbeats=(10000, 10000))

with database.DatabaseConnection() as db_connection, db_connection.new_cursor() as cursor:
    mq.set_listener('swallow', Listener(mq, cursor))
    connect_and_subscribe(mq)

    while True:
        sleep(5)
