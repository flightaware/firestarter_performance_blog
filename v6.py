#!/usr/bin/env python3
from itertools import islice
import json
import sys
import time

from harness import engine, table, run

from sqlalchemy.sql import bindparam

cache = {}
BASE_MESSAGE = dict.fromkeys(c.name for c in table.c)

def add_to_cache(message):
    cache.setdefault(message["id"], {}).update(message)

def write_to_db(conn):
    existing_flights = conn.execute(table.select().where(table.c.id.in_(cache)))
    updates = [dict(flight, _id=flight.id) | cache.pop(flight.id) for flight in existing_flights]
    if updates:
        # sqlalchemy reserves the "id" bindparam for its own use
        conn.execute(table.update().where(table.c.id == bindparam("_id")), *updates)
    # We popped the updates, so anything left must be an insert.
    # All inserted dicts must have same structure.
    inserts = [BASE_MESSAGE | val for val in cache.values()]
    if inserts:
        conn.execute(table.insert(), *inserts)
    cache.clear()

def main(lines):
    start_time = time.time()
    for line in islice(sys.stdin.readlines(), lines):
        add_to_cache(json.loads(line))
        if time.time() > start_time + 1:
            with engine.begin() as connection:
                write_to_db(connection)
            start_time = time.time()
    with engine.begin() as connection:
        write_to_db(connection)


if __name__ == "__main__":
    run(main)
