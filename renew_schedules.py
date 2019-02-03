#!/usr/bin/env python3

import datetime, gzip, requests, io
from requests.auth import HTTPBasicAuth

import psycopg2, psycopg2.extras

import parser
from common import database, config

WEEKDAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]

if __name__ == "__main__":
    with database.DatabaseConnection() as db_connection, db_connection.new_cursor() as c:
        c.execute("SELECT extract_date FROM headers ORDER BY extract_date DESC LIMIT 1;")
        row = c.fetchone()
        if not row:
            print("No header information in database")
            exit()

        last_updated = row[0]
        today = datetime.datetime.today().date()
        span = (today-last_updated).days

        if span > 7:
            print("Last retrival over 7 days ago, cannot create a non-contiguous schedule")
            exit()
        elif span<=1:
            print("The schedule is already up to date")
            exit()

        for day in [(last_updated+datetime.timedelta(days=a)) for a in range(1,span)]:
            print(day.isoformat())
            request = requests.get('https://datafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate?type=CIF_ALL_UPDATE_DAILY&day=toc-update-{}.CIF.gz'.format(WEEKDAYS[day.weekday()]), auth=HTTPBasicAuth(config.get("nr-username"), config.get("nr-password")))
            parser.parse_cif(io.StringIO(gzip.decompress(request.content).decode("ascii")))
