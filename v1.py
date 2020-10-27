#!/usr/bin/env python3
import itertools
import json
import sys
import time

from harness import table, engine, run

def write_to_db(message):
    with engine.connect() as conn:
        existing_flight = conn.execute(table.select().where(table.c.id == message["id"])).first()
        if existing_flight:
            conn.execute(table.update().where(table.c.id == message["id"]), message)
        else:
            conn.execute(table.insert(), message)

def main(lines):
    for line in itertools.islice(sys.stdin.readlines(), lines):
        write_to_db(json.loads(line))

if __name__ == "__main__":
    run(main)
