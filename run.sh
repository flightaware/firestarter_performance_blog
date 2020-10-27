#!/usr/bin/env bash
set -eu
[[ -d venv ]] || python3 -m venv venv
. ./venv/bin/activate
pip install -r requirements.txt

echo v1
python v1.py 100 < messages.jsonl
echo v2
python v2.py 100 < messages.jsonl
echo v3
python v3.py 100 < messages.jsonl
python v3.py 2000 < messages.jsonl
echo v4
python v4.py 2000 < messages.jsonl
echo v5
python v5.py 2000 < messages.jsonl
python v5.py 20000 < messages.jsonl
echo v6
python v6.py 20000 < messages.jsonl
