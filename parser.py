#!/usr/bin/env python3

import json, os, sys, argparse, datetime
from collections import Counter, OrderedDict

import psycopg2, psycopg2.extras

from common import database

def c_str(string):
    return string.rstrip()

def c_str_n(string):
    return string.rstrip() or None

def c_time(string):
    if string=="     ": return
    return int(string[:2])*120 + int(string[2:4])*2 + (string[4]=="H")

def c_date(string):
    return "20" + string[0:2] + "-" + string[2:4] + "-" + string[4:6]

def c_date_dmy(string):
    return "20" + string[4:6] + "-" + string[2:4] + "-" + string[0:2]

def c_num(string):
    if string.strip():
        return int(string.strip())
    else:
        return None

# CORPUS has fewer duplicate STANOX codes, for no particularly apparent reason
def incorporate_corpus(include_nalco_only):
    with open("datasets/corpus.json", encoding="iso-8859-1") as f:
        corpus = json.load(f)["TIPLOCDATA"]
    with database.DatabaseConnection() as db_connection, db_connection.new_cursor() as c:
        c.execute("BEGIN;")
        for entry in corpus:
            tiploc, stanox, crs = c_str_n(entry["TIPLOC"]), c_str_n(entry["STANOX"]), c_str_n(entry["3ALPHA"])
            if tiploc or stanox or crs or include_nalco_only:
                c.execute("INSERT INTO locations(tiploc, nalco, name, stanox, crs) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;", [
                    tiploc, entry["NLC"], c_str_n(entry["NLCDESC"]), stanox, crs])
        c.execute("COMMIT;")

def parse_cif(f):
    start_timestamp = datetime.datetime.now().timestamp()
    with database.DatabaseConnection() as db_connection, db_connection.new_cursor() as c:
        count = 0
        location_batch = []
        location_delete_batch = []

        # Inline selects for tiploc/iid mappings presents a major bottleneck, and it should all fit in memory easily enough
        tl_map = {}
        c.execute("SELECT tiploc,iid FROM locations;")
        for tiploc,iid in c:
            tl_map[tiploc] = iid

        c.execute("PREPARE location_plan (INTEGER, INTEGER, VARCHAR(1), SMALLINT, SMALLINT, SMALLINT, VARCHAR(4), VARCHAR(4), VARCHAR(3), VARCHAR(3), VARCHAR(3), VARCHAR(12), VARCHAR(2), VARCHAR(2), VARCHAR(2)) AS INSERT INTO schedule_locations VALUES (DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15);")
        c.execute("PREPARE location_delete_plan (INTEGER) AS DELETE FROM schedule_locations WHERE schedule_iid=$1;")

        c.execute("BEGIN;")
        while True:
            # All records are padded to 80cols
            record = f.read(80)
            # And have a following \n which isn't
            f.read(1)
            record_type, line = record[:2], record[2:]
            count +=1
            if count%100==0:
                sys.stdout.write("\r%8s %s" % (count, record_type))
                sys.stdout.flush()
                # These are are the largest part of the schedule, the less time wasted the better
                psycopg2.extras.execute_batch(c,"EXECUTE location_delete_plan (%s);", location_delete_batch)
                location_delete_batch.clear()
                psycopg2.extras.execute_batch(c,"EXECUTE location_plan (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);", location_batch, page_size=100)
                location_batch.clear()

            if record_type == "HD":
                identity, extract_date, extract_time, current_ref, last_ref = (
                    line[0:20], c_date_dmy(line[20:26]), line[26:30], line[30:37], line[37:44])
                update_indicator, version, user_start_date, user_end_date = (
                    line[44], line[45], c_date_dmy(line[46:52]), c_date_dmy(line[52:58]))
                c.execute("INSERT INTO headers VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);",
                    (identity, extract_date, extract_time, current_ref, last_ref, update_indicator,
                    version, user_start_date, user_end_date))
                print("{}:  {} {} for {}..{}".format(identity, extract_date, update_indicator, user_start_date, user_end_date))

            # AA line[1:7]->uid_main, line[7:13]->uid_assoc, c_date(line[13:19])->valid_from, c_date(line[19:25])->valid_to, line[25:32]->assoc_days, c_str_n(line[32:34])->category, line[34]->date_indicator, c_str(line[35:42])->tiploc, c_num(line[42])->suffix, c_num(line[43])->suffix_assoc, line[45]->assoc_type, line[77]->stp
            if record_type == "AA":
                transaction_type=line[0]
                if transaction_type in "NR":
                    c.execute("""INSERT INTO associations VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (uid, uid_assoc, valid_from, stp)
                        DO UPDATE SET (valid_to, assoc_days, category, date_indicator, tiploc, suffix, suffix_assoc, type)=
                        (EXCLUDED.valid_to, EXCLUDED.assoc_days, EXCLUDED.category, EXCLUDED.date_indicator, EXCLUDED.tiploc,
                        EXCLUDED.suffix, EXCLUDED.suffix_assoc, EXCLUDED.type);""",
                        [line[1:7], line[7:13], c_date(line[13:19]), c_date(line[19:25]), line[25:32],
                        c_str_n(line[32:34]), line[34], c_str(line[35:42]), c_num(line[42]), c_num(line[43]),
                        line[45], line[77]])
                else:
                    c.execute("DELETE FROM associations WHERE uid=%s AND uid_assoc=%s AND valid_from=%s AND stp=%s;", (line[1:7], line[7:13], c_date(line[13:19]), line[77]))

            # TI c_str(l[0:7])->tiploc, c_num(l[7:9])->caps_ident, c_num(l[9:15])->nlc, l[15]->nlc_check, c_str(l[16:42])->description_tps, l[42:47]->stanox, c_num(l[47:51])->pomcp, c_str_n(l[51:54])->crs, c_str(l[54:70])->description_nlc
            elif record_type == "TI" or record_type == "TA":
                tiploc, caps_ident, nlc, nlc_check, description_tps, stanox, pomcp, crs, description_nlc = c_str(line[0:7]), c_num(line[7:9]), c_str(line[9:15]), line[15], c_str(line[16:42]), c_num(line[42:47]), c_num(line[47:51]), c_str_n(line[51:54]), c_str(line[54:70])
                if record_type=="TI":
                    c.execute("INSERT INTO locations(tiploc, nalco, name, stanox, crs) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING RETURNING tiploc,iid;", [tiploc, nlc, description_tps, stanox, crs])
                    row = c.fetchone()
                    if row:
                        tl_map[row[0]] = row[1]
                elif record_type=="TA":
                    replacement_tiploc = c_str(line[70:77])
                    if replacement_tiploc:
                        c.execute("UPDATE locations SET (tiploc, nalco, name, stanox, crs) = (%s, %s, %s, %s, %s) WHERE tiploc = %s RETURNING iid;", (
                            replacement_tiploc, nlc, description_tps, stanox, crs, tiploc))
                    else:
                        c.execute("UPDATE locations SET (nalco, name, stanox, crs) = (%s, %s, %s, %s) WHERE tiploc = %s RETURNING iid;", (
                            nlc, description_tps, stanox, crs, tiploc))

            elif record_type == "TD":
                tiploc = c_str(line[0:7])
                print(record_type + line)
                c.execute("DELETE FROM locations WHERE tiploc=%s;", (tiploc,))

            # BS l[1:7]->uid, c_date(l[7:13])->valid_from, c_date(l[13:19])->valid_to, l[19:26]->days_running, l[26]->bankholiday_running, l[27]->status, l[28:30]->category, c_str_n(l[30:34])->signalling_id, c_str_n(l[34:38])->headcode, l[47]->business_sector, c_str_n(l[48:51])->power, c_str_n(l[51:55])->timing_load, c_str_n(l[55:58])->speed, l[58:64]->operating_characteristics, c_str_n(l[64])->seating_class, c_str_n(l[65])->sleepers, c_str_n(l[66])->reservations, l[68:72]->catering, l[72:76]->branding, l[77]->stp
            elif record_type == "BS":
                transaction_type = line[0]
                if transaction_type in "NR":
                    # Used to ensure that BS/CR are properly replaced
                    segment_id = 0
                    c.execute("""INSERT INTO schedule_validities
                        VALUES (DEFAULT, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (uid, valid_from, stp) DO
                            UPDATE SET (uid, valid_from, valid_to, weekdays, bank_holiday_running, stp)=
                            (EXCLUDED.uid, EXCLUDED.valid_from, EXCLUDED.valid_to, EXCLUDED.weekdays, EXCLUDED.bank_holiday_running, EXCLUDED.stp)
                        RETURNING iid;""",
                        [line[1:7], c_date(line[7:13]), c_date(line[13:19]), line[19:26], line[26], line[77]])
                    sv_id = c.fetchone()[0]

                    c.execute("""INSERT INTO schedules VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (validity_iid, segment_instance) DO UPDATE SET (status, category, signalling_id,
                            headcode, business_sector, power_type, timing_load, speed, operating_characteristics, seating_class, sleepers,
                            reservations, catering, branding, traction_class, uic_code, atoc_code, applicable_timetable)=(
                            EXCLUDED.status, EXCLUDED.category,
                            EXCLUDED.signalling_id, EXCLUDED.headcode, EXCLUDED.business_sector, EXCLUDED.power_type, EXCLUDED.timing_load,
                            EXCLUDED.speed, EXCLUDED.operating_characteristics, EXCLUDED.seating_class, EXCLUDED.sleepers,
                            EXCLUDED.reservations, EXCLUDED.catering, EXCLUDED.branding, EXCLUDED.traction_class, EXCLUDED.uic_code,
                            EXCLUDED.atoc_code, EXCLUDED.applicable_timetable) RETURNING iid;""",
                        [sv_id, segment_id, line[27],
                        line[28:30], c_str_n(line[30:34]), c_str_n(line[34:38]), line[47], c_str_n(line[48:51]),
                        c_str_n(line[51:55]), c_str_n(line[55:58]), line[58:64], c_str_n(line[64]), c_str_n(line[65]),
                        c_str_n(line[66]), line[68:72], line[72:76], None, None, "ZZ", None])
                    bs_id = c.fetchone()[0]
                else:
                    c.execute("DELETE FROM schedule_validities WHERE uid=%s AND valid_from=%s AND stp=%s;", (line[1:7], c_date(line[7:13]), line[77]))

            # BX l[0:4]->traction_class, c_str(l[4:9])->uic_code, l[9:11]->atoc_code, l[11]->applicable_timetable
            elif record_type == "BX":
                c.execute("UPDATE schedules SET traction_class=%s, uic_code=%s, atoc_code=%s, applicable_timetable=%s WHERE iid=%s;",
                    [line[0:4], c_str(line[4:9]), line[9:11], line[11], bs_id])

            # LO c_str(l[0:7])->tiploc, c_str_n(l[7])->tiploc_instance, None->arrival, c_str_n(l[8:13])->departure, None->pass, None->public_arrival, c_str_n(l[13:17])->public_departure, c_str_n(l[17:20])->platform, c_str_n(l[20:23])->line, None->path, c_str_n(l[23:25])->engineering_allowance, c_str_n(l[25:27])->pathing_allowance, l[27:39]->activity, c_str_n(l[39:41])->performance_allowance
            # LI c_str(l[0:7])->tiploc, c_str_n(l[7])->tiploc_instance, c_str_n(l[8:13])->arrival, c_str_n(l[13:18])->departure, c_str_n(l[18:23])->pass, c_str_n(l[23:27])->public_arrival, c_str_n(l[27:31])->public_departure, c_str_n(l[31:34])->platform, c_str_n(l[34:37])->line, c_str_n(l[37:40])->path, l[40:52]->activity, c_str_n(l[52:54])->engineering_allowance, c_str_n(l[54:56])->pathing_allowance, c_str_n(l[56:58])->performance_allowance
            # LT c_str(l[0:7])->tiploc, c_str_n(l[7])->tiploc_instance, c_str_n(l[8:13])->arrival, None->departure, None->pass, c_str_n(l[13:17])->public_arrival, None->public_departure, c_str_n(l[17:20])->platform, None->line, c_str_n(l[20:23])->path, l[23:35]->activity, None->engineering_allowance, None->pathing_allowance, None->performance_allowance
            elif record_type in ["LO", "LI", "LT"]:
                if record_type=="LO":
                    # Clear the midnight comparison values
                    last_time, time_offset = 0,0
                    if transaction_type=="R":
                        location_delete_batch.append((bs_id,))
                        c.execute("UPDATE schedule_validities SET flattened_to=NULL WHERE iid=%s;", [sv_id])

                tiploc, tiploc_instance = c_str(line[0:7]), c_str_n(line[7])
                if record_type=="LO":
                    arrival, departure, pass_time, public_arrival, public_departure, platform, sched_line, path = (
                        None, c_time(line[8:13]), None, None, c_str_n(line[13:17]), c_str_n(line[17:20]),
                        c_str_n(line[20:23]), None)
                    engineering_allowance, pathing_allowance, activity, performance_allowance = (
                        c_str_n(line[23:25]), c_str_n(line[25:27]), line[27:39], c_str_n(line[39:41]))
                    c.execute("UPDATE schedules SET origin_location_iid=%s WHERE iid=%s;", [tl_map[tiploc], bs_id])
                elif record_type=="LI":
                    arrival, departure, pass_time, public_arrival, public_departure, platform, sched_line, path = (
                        c_time(line[8:13]), c_time(line[13:18]), c_time(line[18:23]), c_str_n(line[23:27]),
                        c_str_n(line[27:31]), c_str_n(line[31:34]), c_str_n(line[34:37]), c_str_n(line[37:40]))
                    activity, engineering_allowance, pathing_allowance, performance_allowance = (
                        line[40:52], c_str_n(line[52:54]), c_str_n(line[54:56]), c_str_n(line[56:58]))
                elif record_type=="LT":
                    arrival, departure, pass_time, public_arrival, public_departure, platform, sched_line, path = (
                        c_time(line[8:13]), None, None, c_str_n(line[13:17]), None, c_str_n(line[17:20]), None, c_str_n(line[20:23]))
                    activity, engineering_allowance, pathing_allowance, performance_allowance = (
                        line[23:35], None, None, None)
                    c.execute("UPDATE schedules SET destination_location_iid=%s WHERE iid=%s;", [tl_map[tiploc], bs_id])

                # Ensure that the three time columns are always relative to midnight on the first day of the schedule
                times = []
                for time in [arrival, departure, pass_time]:
                    if time!=None:
                        if time<last_time:
                            time_offset += 1
                        last_time = time
                        times.append(time+time_offset*2880)
                    else:
                        times.append(time)

                arrival, departure, pass_time = times

                if public_arrival == "0000": public_arrival = None
                if public_departure == "0000": public_departure = None

                location_batch.append((
                    bs_id, tl_map[tiploc], tiploc_instance, arrival, departure, pass_time, public_arrival, public_departure,
                    platform, sched_line, path, activity, engineering_allowance, pathing_allowance, performance_allowance
                    ))

            elif record_type == "ZZ":
                duration = int(datetime.datetime.now().timestamp()-start_timestamp)
                print("\r%8s ZZ %ss" % (count, duration))

                # If there's any left, it'd be a good idea to store them!
                psycopg2.extras.execute_batch(c,"EXECUTE location_delete_plan (%s);", location_delete_batch)
                location_delete_batch.clear()
                psycopg2.extras.execute_batch(c,"EXECUTE location_plan (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);", location_batch, page_size=100)
                location_batch.clear()

                if update_indicator=="F":
                    print("Building indexes")
                    # Creating an index is less expensive when the rows are already there
                    c.execute("CREATE INDEX idx_sched_loc_sched_iid ON schedule_locations(schedule_iid);")
                    c.execute("CREATE INDEX idx_sched_loc_iid ON schedule_locations(iid);")
                    c.execute("CREATE INDEX idx_sched_loc_tl_iid ON schedule_locations(location_iid);")
                    c.execute("CREATE INDEX idx_loc_tl_iid ON schedule_locations(location_iid);")
                c.execute("COMMIT;")
                return

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("file")
    parser.add_argument("--no-corpus", "-n", action="store_true")
    args = parser.parse_args()
    if not args.no_corpus:
        print("Using CORPUS for location data... ", end="")
        incorporate_corpus(True)
        print("done")
    with open(args.file) as f:
        parse_cif(f)
