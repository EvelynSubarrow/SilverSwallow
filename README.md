# SilverSwallow
This is the data collection portion of Swallow, responsible for parsing schedule snapshots and updates, and incorporating
live train movement data from TRUST.

## Dependencies
* [psycopg2](https://pypi.org/project/psycopg2/)
* [stomp.py](https://pypi.org/project/stomp.py/)

## Licence
This project is licenced under the GNU GPL, version 3 (for now)

## Using SilverSwallow for the first time
You'll need an email address and password for a Network Rail open data account. You can sign up
[here](https://datafeeds.networkrail.co.uk/ntrod/login).
It can take several months for your account to become active, and you'll have to
specifically add SCHEDULE and TRUST to your account.

Once you have access to Network Rail data, you should rename `config.json.example` to `config.json`, substituting your
Network Rail feed credentials, as well as those for your database.

Next, you should amend and appropriately rename `cif_pull.sh.example`, then run it in order to retrieve the schedule snapshot for the week.

You must initialise the database (`database_structure.py --init`), then run the parser (`parser.py datasets/sched.cif`), which will populate the
database with schedule records. You should then add update files (`renew_schedules.py`), and finally run the schedule
"flattener" (`flat_maintenance.py`)

Finally, you can run `trust.py`, which inserts movement records from TRUST into the database.

### In short:
```
./cif_pull.sh
./database_structure.py --init
./parser.py datasets/sched.cif
./renew_schedules.py
./flat_maintenance.py
./trust.py &&
```

## Keeping schedules current
A new schedule snapshot is published every day, at approximately 0100. In order to incorporate this, you should run `renew_schedules.py`, then
`flat_maintenance.py`.

### In short:
```
./renew_schedules.py
./flat_maintenance.py
```
