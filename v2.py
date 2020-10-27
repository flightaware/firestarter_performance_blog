#!/usr/bin/env python3
from itertools import islice
import json
import sys

from harness import engine, table, run

from sqlalchemy import event

@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.close()

def write_to_db(message):
    with engine.begin() as conn:
        existing_flight = conn.execute(table.select().where(table.c.id == message["id"])).first()
        if existing_flight:
            conn.execute(table.update().where(table.c.id == message["id"]), message)
        else:
            conn.execute(table.insert(), message)

def main(lines):
    for line in islice(sys.stdin.readlines(), lines):
        write_to_db(json.loads(line))

if __name__ == "__main__":
    run(main)
