#!/usr/bin/env python3
from itertools import islice
import json
import sys
import time

from harness import table, engine, run

def write_to_db(conn, message):
    existing_flight = conn.execute(table.select().where(table.c.id == message["id"])).first()
    if existing_flight:
        conn.execute(table.update().where(table.c.id == message["id"]), message)
    else:
        conn.execute(table.insert(), message)

def main(lines):
    connection = engine.connect()
    transaction = connection.begin()
    start_time = time.time()
    for line in islice(sys.stdin.readlines(), lines):
        write_to_db(connection, json.loads(line))
        if time.time() > start_time + 1:
            transaction.commit()
            transaction = connection.begin()
            start_time = time.time()
    transaction.commit()

if __name__ == "__main__":
    run(main)
