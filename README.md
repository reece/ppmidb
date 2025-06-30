# PostgreSQL PPMI importer

Work in progress. Come back later.

This project facilitates creating a PostgreSQL database from PPMI Clinical "data freeze" files. You must have access to those files already.


## Prerequisites

- PostgreSQL "cluster" with a database created (ppmidb below)
- PPMI data freeze file

## Setup Environment and Dependencies

    python3 -m venv .venv
    source .venv/bin/activate
    pip install -U setuptools pip uv
    uv pip install -e .

## Load

    export PGPASSWORD=...
    time ppmidb -v load -t --uri postgresql://localhost/ppmidb -z data/PPMI_20250401.gz
    # -v = verbose
    # -t = infer schema and create tables
    # --uri = where to load (local or remote)
    # -z = zip file (with a .gz suffix :-/)

