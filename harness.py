import os
import sys
import time

import sqlalchemy as sa

meta = sa.MetaData()
table = sa.Table(
    "flights",
    meta,
    sa.Column("id", sa.String, primary_key=True),
    sa.Column("ident", sa.String),
    sa.Column("reg", sa.String),
    sa.Column("atcident", sa.String),
    sa.Column("hexid", sa.String),
    sa.Column("orig", sa.String),
    sa.Column("dest", sa.String),
    sa.Column("aircrafttype", sa.String),
    sa.Column("gs", sa.String),
    sa.Column("speed", sa.String),
    sa.Column("alt", sa.String),
    sa.Column("trueCancel", sa.String),
    sa.Column("route", sa.String),
    sa.Column("status", sa.String),
    sa.Column("actual_arrival_gate", sa.String),
    sa.Column("estimated_arrival_gate", sa.String),
    sa.Column("actual_departure_gate", sa.String),
    sa.Column("estimated_departure_gate", sa.String),
    sa.Column("actual_arrival_terminal", sa.String),
    sa.Column("scheduled_arrival_terminal", sa.String),
    sa.Column("actual_departure_terminal", sa.String),
    sa.Column("scheduled_departure_terminal", sa.String),
    sa.Column("baggage_claim", sa.String),
    sa.Column("cancelled", sa.String),
    sa.Column("fdt", sa.String),
    sa.Column("actual_out", sa.String),
    sa.Column("adt", sa.String),
    sa.Column("aat", sa.String),
    sa.Column("actual_in", sa.String),
    sa.Column("estimated_out", sa.String),
    sa.Column("edt", sa.String),
    sa.Column("eta", sa.String),
    sa.Column("estimated_in", sa.String),
    sa.Column("scheduled_out", sa.String),
    sa.Column("scheduled_in", sa.String),
    sa.Column("predicted_out", sa.String),
    sa.Column("predicted_off", sa.String),
    sa.Column("predicted_on", sa.String),
    sa.Column("predicted_in", sa.String),
)

try:
    os.remove("flights.db")
except FileNotFoundError:
    pass
engine = sa.create_engine(f"sqlite:///flights.db")

meta.create_all(engine)

def run(fn):
    lines = int(sys.argv[1])
    start = time.time()
    fn(lines)
    total_time = time.time() - start
    print(f"{lines} messages processed in {total_time:.2f} seconds: {lines / total_time:.1f}msg/s")
