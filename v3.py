#!/usr/bin/env python3
from itertools import islice
import json
import sys

from harness import table, engine, run

def write_to_db(conn, message):
    existing_flight = conn.execute(table.select().where(table.c.id == message["id"])).first()
    if existing_flight:
        conn.execute(table.update().where(table.c.id == message["id"]), message)
    else:
        conn.execute(table.insert(), message)

def main(lines):
    with engine.begin() as conn:
        for line in islice(sys.stdin.readlines(), lines):
            write_to_db(conn, json.loads(line))

if __name__ == "__main__":
    run(main)
