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
    if "/Primary_Clinical_Diagnosis_" in csv_path:
        # In Primary_Clinical_Diagnosis_20250401.csv at least, the CSV is invalid
        content = content.replace('no tremors today. \\"",,"3",', 'no tremors today.",,"3",')

    header = next(StringIO(content)).strip()
    if len(header) == 1024:
        _logger.warning("The header of this file is 1024 bytes and is likely truncated, resulting in invalid CSV; expect errors")

    return content


@click.group()
@click.option(
    "--config-file", "-C",
    type=click.Path(exists=True, dir_okay=False, readable=True),
    help="Path to a global configuration file.",
)
@click.option(
    "--verbose", "-v", is_flag=True, help="Enable verbose output for debugging."
)
def cli(verbose, config_file):
    """
    A versatile CLI tool with subcommands.

    Global options apply to all subcommands.
    """
    import coloredlogs
    coloredlogs.install(level="INFO")

    click.get_current_context().obj = {'verbose': verbose, 'config_file': config_file}


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
            infer_schema_length=None,
        )
    except Exception as e:
        header = next(StringIO(csv_content)).strip()
        debug_info = f"""
        {csv_path=}
        {len(header)=}"""
        raise RuntimeError(f"Error reading CSV file '{csv_path}': {e}" + "\n" + debug_info)

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
