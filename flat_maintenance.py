#!/usr/bin/env python3

import json, os, sys, datetime, multiprocessing, time, queue
from collections import Counter, OrderedDict

import psycopg2, psycopg2.extras

from common import database

def flat_worker(q, return_queue):
    with database.DatabaseConnection() as db_connection, db_connection.new_cursor() as c:
        c.execute("BEGIN;")
        c.execute("PREPARE insert_flat_schedule (INTEGER, CHAR(7), DATE) AS INSERT INTO flat_schedules VALUES (DEFAULT, $1, $2, $3) RETURNING iid;")
        c.execute("PREPARE insert_flat_timing(BIGINT, BIGINT, INT, BIGINT, BIGINT, BIGINT) AS INSERT INTO flat_timing VALUES ($1, $2, $3, $4, $5, $6);")
        c.execute("set LOCAL application_name = 'fs_maintain';")
        count = 0
        insertion_batch = []
        while True:
            count += 1
            try:
                queue_entry = q.get(True, 5)

                if not queue_entry:
                    raise ValueError()

                uid, flatten_from, duration_days, reconstitution = queue_entry
            except (queue.Empty, ValueError) as e:
                if insertion_batch:
                    psycopg2.extras.execute_batch(c,"EXECUTE insert_flat_timing(%s, %s, %s, %s, %s, %s);", insertion_batch, page_size=100)
                    insertion_batch.clear()
                    c.execute("COMMIT; BEGIN;")
                if type(e) is ValueError:
                    # Signal that everything has been committed
                    return_queue.put(1)
                continue
            date_range = [flatten_from + datetime.timedelta(days=a) for a in range(duration_days+1)]
            end_date = date_range[-1]

            # The trigger for flat schedule deletion can be activated even if it already exists
            # We don't want to trash schedules which exist already, that would be bad
            if reconstitution:
                c.execute("SELECT uid FROM flat_schedules WHERE uid=%s AND start_date=%s", (uid, date))
                if c.fetchall():
                    c.execute("DELETE FROM flat_reconstitution WHERE uid=%s AND start_date=%s", (uid, date))
                    continue

            # This means most important STP status (C) will be taken *last*
            c.execute("SELECT iid, uid, stp, weekdays, valid_from, valid_to, flattened_to FROM schedule_validities WHERE uid=%s AND valid_to >= %s AND valid_from <= %s ORDER BY stp DESC;", (uid, flatten_from, end_date))
            schedules = c.fetchall()
            for date in date_range:
                already_processed = False
                schedule_iid = None
                schedule_matches = 0
                for col_iid, uid, stp, weekdays, valid_from, valid_to, flattened_to in schedules:
                    # If the schedule is valid on the given day
                    if valid_from <= date and valid_to >= date and weekdays[date.weekday()]=="1":
                        # In this instance, a flat schedule is highly likely to already exist
                        if flattened_to and flattened_to >= date:
                            already_processed = True
                        schedule_matches += 1
                        # Exclude a cancelled service
                        schedule_iid = None if stp=="C" else col_iid

                # Probably best to not go around deleting random schedules
                if not schedule_matches:
                    continue

                # The schedule already exists, and we're not meant to replace it
                if flattened_to and flattened_to >= date and not reconstitution:
                    continue

                # If the last stp is C, and schedule_iid is None, the last schedule is valid but does not run on this day
                # If schedule_iid is not null, but this date has already been flattened, this is probably a replacement
                if (stp=="C" and schedule_iid==None and already_processed) or (already_processed and schedule_iid):

                    # There's no particularly neat way to stop this triggering, so for now...
                    c.execute("DELETE FROM flat_schedules WHERE uid=%s and start_date=%s;", (uid, date))
                    c.execute("DELETE FROM flat_reconstitution WHERE uid=%s and start_date=%s;", (uid, date))

                if schedule_iid:
                    dt_offset = int(datetime.datetime.combine(date, datetime.time(0,0)).timestamp())
                    c.execute("EXECUTE insert_flat_schedule (%s, %s, %s);", (col_iid, uid, date))
                    flat_schedule_iid = c.fetchone()[0]
                    c.execute("SELECT iid, location_iid, arrival_time, departure_time, pass_time FROM schedule_locations WHERE schedule_iid=%s ORDER BY iid;", [schedule_iid])
                    for sched_location_iid, location_iid, arrival_time, departure_time, pass_time in c.fetchall():
                        insertion_batch.append((
                            flat_schedule_iid, sched_location_iid, location_iid,
                            dt_offset+arrival_time*30 if arrival_time else None,
                            dt_offset+departure_time*30 if departure_time else None,
                            dt_offset+pass_time*30 if pass_time else None
                            ))

            if not reconstitution:
                c.execute("UPDATE schedule_validities SET flattened_to=%s WHERE uid=%s;", (end_date, uid))

            if count%100==0:
                psycopg2.extras.execute_batch(c, "EXECUTE insert_flat_timing(%s, %s, %s, %s, %s, %s);", insertion_batch, page_size=100)
                insertion_batch.clear()
                c.execute("COMMIT; BEGIN;")

with database.DatabaseConnection() as db_connection, db_connection.new_cursor() as c:
    worker_count = 4
    uid_queues = []
    occupation_queue = multiprocessing.Queue()

    for i in range(worker_count):
        uid_queues.append(multiprocessing.Queue())
        p = multiprocessing.Process(target=flat_worker, args=(uid_queues[-1], occupation_queue))
        p.start()

    duration_days = 14
    start_date = datetime.datetime.now().date()
    end_date = start_date + datetime.timedelta(duration_days)

    workers_occupied = 0

    while True:
        if not workers_occupied:
            c.execute("SELECT DISTINCT uid FROM schedule_validities WHERE valid_to >= %s AND valid_from <= %s AND (flattened_to < %s OR flattened_to IS NULL);", (start_date, end_date, end_date))
            uids = [a[0] for a in c.fetchall()]

            for i, uid in enumerate(uids):
                uid_queues[i%worker_count].put((uid, start_date, duration_days, False))

            if c.rowcount:
                workers_occupied = worker_count

            c.execute("SELECT uid,start_date FROM flat_reconstitution;")
            for i, (uid, date) in enumerate(c.fetchall()):
                uid_queues[i%worker_count].put((uid, date, 1, True))

            if c.rowcount:
                workers_occupied = worker_count

                # Heavily suggest the workers should commit the remaining insert batch
                for i in range(worker_count):
                    uid_queues[i].put(None)

        sys.stdout.write("\r" + ", ".join(["{:<7}".format(a.qsize()) for a in uid_queues]))
        sys.stdout.flush()
        try:
            workers_occupied -= occupation_queue.get(True, 2)
            print()
            print("Worker complete")
        except queue.Empty as e:
            pass
