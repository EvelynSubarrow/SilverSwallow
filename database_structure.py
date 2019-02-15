#!/usr/bin/env python3

import json, os, sys, argparse
from collections import Counter, OrderedDict

import psycopg2, psycopg2.extras

from common.database import DatabaseConnection

def initialise(d):
    with d.new_cursor() as c:
        c.execute("BEGIN;")

        c.execute("""CREATE TABLE headers(
        identity            CHAR(20),
        extract_date        DATE,
        extract_time        CHAR(4),
        current_reference   CHAR(7),
        last_reference      CHAR(7),
        update_indicator    CHAR(1),
        version             CHAR(1),
        user_start_date     DATE,
        user_end_date       DATE,
        UNIQUE(identity)
        );
        """)

        c.execute("""CREATE SEQUENCE location_iid_seq;
        CREATE TABLE locations(
            iid    INTEGER UNIQUE NOT NULL DEFAULT nextval('location_iid_seq'),
            nalco  VARCHAR(6) NOT NULL,
            tiploc VARCHAR(7),
            name   VARCHAR(32),
            stanox INTEGER,
            crs    VARCHAR(3),
            PRIMARY KEY(iid)
        );
        ALTER SEQUENCE location_iid_seq OWNED BY locations.iid;
        CREATE INDEX idx_location_iid    ON locations(tiploc);
        CREATE INDEX idx_location_tiploc ON locations(tiploc);
        CREATE INDEX idx_location_stanox ON locations(stanox);
        CREATE INDEX idx_location_crs    ON locations(crs);
        CREATE UNIQUE INDEX idx_location_nalco  ON locations(nalco);
        """)

        c.execute("""CREATE SEQUENCE schedule_validity_iid_seq;
            CREATE TABLE schedule_validities(
                iid                       INTEGER UNIQUE NOT NULL DEFAULT nextval('schedule_validity_iid_seq'),
                uid                       CHAR(6) NOT NULL,
                valid_from                DATE    NOT NULL,
                valid_to                  DATE    NOT NULL,
                weekdays                  VARCHAR(7) NOT NULL,
                bank_holiday_running      VARCHAR(1),
                stp                       VARCHAR(1),
                flattened_to              DATE DEFAULT NULL,
                UNIQUE (uid, valid_from, stp)
            );
            ALTER SEQUENCE schedule_validity_iid_seq OWNED BY schedule_validities.iid;

            CREATE INDEX idx_sched_validities_iid on schedule_validities(iid);
            CREATE INDEX idx_sched_validities_valid_from ON schedule_validities(valid_from);
            CREATE INDEX idx_sched_validities_stp ON schedule_validities(stp);
            """)

        c.execute("CREATE SEQUENCE schedule_iid_seq;")
        c.execute("""CREATE TABLE schedules(
            iid                       INTEGER UNIQUE NOT NULL DEFAULT nextval('schedule_iid_seq'),
            validity_iid              INTEGER UNIQUE NOT NULL REFERENCES schedule_validities(iid) ON DELETE CASCADE,
            segment_instance          SMALLINT NOT NULL,
            status                    VARCHAR(1),
            category                  VARCHAR(2),
            signalling_id             VARCHAR(4),
            headcode                  VARCHAR(4),
            business_sector           VARCHAR(1),
            power_type                VARCHAR(3),
            timing_load               VARCHAR(7),
            speed                     VARCHAR(3),
            operating_characteristics VARCHAR(6),
            seating_class             VARCHAR(1),
            sleepers                  VARCHAR(1),
            reservations              VARCHAR(1),
            catering                  VARCHAR(4),
            branding                  VARCHAR(4),
            traction_class            VARCHAR(4),
            uic_code                  VARCHAR(5),
            atoc_code                 VARCHAR(2),
            applicable_timetable      VARCHAR(1),
            origin_location_iid       INTEGER REFERENCES locations(iid), -- | These can be NULL
            destination_location_iid  INTEGER REFERENCES locations(iid), -- | Cancellations don't have an origin or destination!
            unique (validity_iid, segment_instance),
            PRIMARY KEY (iid)
        );""")
        c.execute("ALTER SEQUENCE schedule_iid_seq OWNED BY schedules.iid;")
        c.execute("CREATE INDEX idx_sched_iid ON schedules(iid);")

        c.execute("""CREATE TABLE associations(
            uid            CHAR(6),
            uid_assoc      CHAR(6),
            valid_from     DATE,
            valid_to       DATE,
            assoc_days     VARCHAR(7),
            category       VARCHAR(2),
            date_indicator VARCHAR(1),
            tiploc         VARCHAR(7),
            suffix         VARCHAR(1),
            suffix_assoc   VARCHAR(1),
            type           VARCHAR(1),
            stp            VARCHAR(1),
            UNIQUE(uid, uid_assoc, valid_from, stp)
        );""")
        c.execute("CREATE INDEX idx_main_uid ON associations(uid);")
        c.execute("CREATE INDEX idx_assoc_uid ON associations(uid_assoc);")

        c.execute("""CREATE SEQUENCE sched_location_iid_seq;
        CREATE TABLE schedule_locations(
            iid                   BIGINT UNIQUE NOT NULL DEFAULT nextval('sched_location_iid_seq'),
            schedule_iid          INTEGER REFERENCES schedules(iid) ON DELETE CASCADE,
            location_iid          INTEGER REFERENCES locations(iid),
            tiploc_instance       VARCHAR(1),
            arrival_time          SMALLINT,
            departure_time        SMALLINT,
            pass_time             SMALLINT,
            arrival_public        VARCHAR(4),
            departure_public      VARCHAR(4),
            platform              VARCHAR(3),
            line                  VARCHAR(3),
            path                  VARCHAR(3),
            activity              VARCHAR(12),
            engineering_allowance VARCHAR(2),
            pathing_allowance     VARCHAR(2),
            performance_allowance VARCHAR(2),
            PRIMARY KEY(iid)
        );
        ALTER SEQUENCE sched_location_iid_seq OWNED BY schedule_locations.iid;

        CREATE INDEX idx_sched_location_iid ON schedule_locations(iid);
        CREATE INDEX idx_sched_location_schedule ON schedule_locations(schedule_iid);
        """)

        c.execute("""CREATE TABLE flat_reconstitution(
            uid CHAR(7) NOT NULL,
            start_date DATE NOT NULL,
            UNIQUE (uid, start_date),
            PRIMARY KEY(uid, start_date)
        );""")

        c.execute("""CREATE SEQUENCE flat_schedule_iid_seq;
        CREATE TABLE flat_schedules(
            iid                   BIGINT UNIQUE NOT NULL DEFAULT nextval('flat_schedule_iid_seq'),
            schedule_validity_iid INTEGER DEFAULT NULL REFERENCES schedule_validities(iid) ON DELETE CASCADE,
            uid                   CHAR(7),
            start_date            DATE     NOT NULL,
            trust_id              CHAR(10) DEFAULT NULL,
            actual_signalling_id  CHAR(4)  DEFAULT NULL,
            actual_service_code   CHAR(8)  DEFAULT NULL,

            activation_datetime   BIGINT   DEFAULT NULL,
            train_call_type       CHAR(1)  DEFAULT NULL, -- A(utomatic)/M(anual)

            cancellation_datetime BIGINT   DEFAULT NULL,
            cancellation_reason   CHAR(2)  DEFAULT NULL,
            cancellation_location INTEGER  DEFAULT NULL REFERENCES locations(iid),

            current_location      INTEGER  DEFAULT NULL REFERENCES locations(iid),
            current_variation     INTEGER  DEFAULT NULL,

            UNIQUE (uid, start_date, trust_id),
            UNIQUE (start_date, trust_id),
            PRIMARY KEY(iid)
        );

        ALTER SEQUENCE flat_schedule_iid_seq OWNED BY flat_schedules.iid;
        CREATE INDEX idx_flat_schedule_sched_validity_iid ON flat_schedules(schedule_validity_iid);
        CREATE INDEX idx_flat_schedule_iid ON flat_schedules(iid);
        CREATE INDEX idx_flat_schedule_uid ON flat_schedules(uid);
        CREATE INDEX idx_flat_schedule_start_date ON flat_schedules(start_date);
        CREATE INDEX idx_flat_schedule_trust_id ON flat_schedules(trust_id);

        CREATE OR REPLACE FUNCTION insert_flat_hole() RETURNS trigger AS $$
            BEGIN
                IF (OLD.uid IS NOT NULL) THEN
                    INSERT INTO flat_reconstitution VALUES (OLD.uid, OLD.start_date) ON CONFLICT DO NOTHING;
                END IF;
                RETURN OLD;
            END;
        $$ LANGUAGE plpgsql;

        CREATE TRIGGER trigger_flat_hole BEFORE DELETE ON flat_schedules FOR EACH ROW
            WHEN (current_setting('application_name') <> 'fs_maintain')
            EXECUTE PROCEDURE insert_flat_hole();
        """)

        c.execute("""CREATE TABLE trust_movements(
            flat_schedule_iid       BIGINT  NOT NULL REFERENCES flat_schedules(iid) ON DELETE CASCADE,
            stanox                  INTEGER NOT NULL,
            datetime_scheduled      BIGINT,
            datetime_actual         BIGINT  NOT NULL,
            movement_type           CHAR(1) NOT NULL,
            actual_platform         VARCHAR(2) DEFAULT NULL,
            actual_route            CHAR(1) DEFAULT NULL,
            actual_line             CHAR(1) DEFAULT NULL,
            actual_variation_status CHAR(1) DEFAULT NULL,
            actual_variation        INTEGER DEFAULT NULL,
            actual_direction        CHAR(1) DEFAULT NULL,
            actual_source           CHAR(1) DEFAULT NULL
        );""")
        c.execute("CREATE INDEX idx_trust_movements_datetime_scheduled ON trust_movements(datetime_scheduled);")
        c.execute("CREATE INDEX idx_trust_movements_datetime_actual ON trust_movements(datetime_actual);")
        c.execute("CREATE INDEX idx_trust_movements_flat_sched_iid ON trust_movements(flat_schedule_iid);")
        c.execute("CREATE INDEX idx_trust_movements_stanox ON trust_movements(stanox);")

        c.execute("""CREATE TABLE flat_timing(
            flat_schedule_iid     BIGINT  NOT NULL REFERENCES flat_schedules(iid) ON DELETE CASCADE,
            schedule_location_iid BIGINT  NOT NULL REFERENCES schedule_locations(iid) ON DELETE CASCADE,
            location_iid          INTEGER NOT NULL REFERENCES locations(iid) ON DELETE CASCADE,
            arrival_scheduled     BIGINT,
            departure_scheduled   BIGINT,
            pass_scheduled        BIGINT
        );

        CREATE INDEX idx_flat_loc_iid ON flat_timing(location_iid);
        CREATE INDEX idx_flat_arrival ON flat_timing(arrival_scheduled);
        CREATE INDEX idx_flat_departure ON flat_timing(departure_scheduled);
        CREATE INDEX idx_flat_pass ON flat_timing(pass_scheduled);
        """)

        c.execute("COMMIT;")

def purge(d):
    with d.new_cursor() as c:
        c.execute("""BEGIN;
            DROP TABLE flat_timing;
            DROP TABLE trust_movements;
            DROP TABLE flat_schedules;
            DROP TABLE schedule_locations;
            DROP TABLE schedules;
            DROP TABLE schedule_validities;

            DROP TABLE flat_reconstitution;
            DROP TABLE associations;
            DROP TABLE headers;
            DROP TABLE locations;
            COMMIT;""")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument('--init', action='store_true', help='Initialise database')
    action.add_argument('--purge', action='store_true', help='Drop all Swallow tables')
    args = parser.parse_args()
    with DatabaseConnection() as d:
        if args.init:
            initialise(d)
            print("Swallow tables initialised")
        elif args.purge:
            purge(d)
            print("All Swallow tables removed")
