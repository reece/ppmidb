#!/usr/bin/env python3

"""infer schema from provided csv file"""

import csv
from io import StringIO
import logging
from pathlib import Path
import re
from typing import Optional

import click
import polars as pl

from .infer_schema import infer_schema, clean_for_sql_name
from .utils import generate_sql_create_table_ddl, schema_as_table

_logger = logging.getLogger()


def read_csv_content(csv_path: str) -> str:
    content = open(csv_path, encoding="cp1252").read()
    content = content.replace('\\"', '""').strip()
    return content


@click.group()
@click.option(
    "--config", "-C",
    type=click.Path(exists=True, dir_okay=False, readable=True),
    help="Path to a global configuration file.",
)
@click.option(
    "--verbose", "-v", is_flag=True, help="Enable verbose output for debugging."
)
def cli(verbose, config):
    """
    A versatile CLI tool with subcommands.

    Global options apply to all subcommands.
    """
    import coloredlogs
    coloredlogs.install(level="INFO")

    # You could store config/verbose in ctx.obj if desired for more complex shared state
    # click.get_current_context().obj = {'verbose': verbose, 'config': config}


@cli.command("generate-schema")
@click.argument("csv_path", type=click.Path(exists=True, dir_okay=False, readable=True))
def generate_schema(csv_path: str):
    csv_content = read_csv_content(csv_path)
    table_name = Path(csv_path).stem
    table_name = clean_for_sql_name(table_name)
    table_name = re.sub(r"_\d{8}$", r"", table_name)

    try:
        df = pl.read_csv(
            StringIO(csv_content),
            has_header=True,
            separator=",",
            infer_schema_length=1000,
        )
    except Exception as e:
        raise RuntimeError(f"Error reading CSV file '{csv_path}': {e}")

    schema = infer_schema(df)
    table = schema_as_table(schema)
    table = re.sub(r"^(?=.)", "-- ", table, flags=re.MULTILINE)

    print(
        f"-- Schema inferred from {csv_path}\n"
        + table
        + "\n"
        + generate_sql_create_table_ddl(
            schema,
            table_name,
        )
    )

@cli.command("generate-copy")
@click.argument("csv_path", type=click.Path(exists=True, dir_okay=False, readable=True))
def generate_copy(csv_path: str):
    csv_content = read_csv_content(csv_path)
    table_name = Path(csv_path).stem
    table_name = clean_for_sql_name(table_name)
    table_name = re.sub(r"_\d{8}$", r"", table_name)

    print(f"COPY {table_name} from STDIN with (format csv, header true);")
    print(csv_content)
    print("\\.")


@cli.command("load")
@click.argument("csv_path", type=click.Path(exists=True, dir_okay=False, readable=True))
@click.option("--uri", type=str)
def load(csv_path: str, uri: str):
    import psycopg

    table_name = Path(csv_path).stem
    table_name = clean_for_sql_name(table_name)
    table_name = re.sub(r"_\d{8}$", r"", table_name)

    csv_content = read_csv_content(csv_path)

    con = psycopg.connect(uri)
    cur = con.cursor()

    csv_rdr = csv.reader(StringIO(csv_content))
    header = next(csv_rdr)
    query = f"COPY {table_name} ({','.join(header)}) from STDIN"
    query += " WITH (FORMAT CSV, HEADER)"

    with cur.copy(query) as copy:
        copy.write(csv_content)
    con.commit()


if __name__ == "__main__":
    cli()
