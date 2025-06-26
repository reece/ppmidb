#!/usr/bin/env python3

"""process PPMI zip file and standalone csv files"""


import csv
from io import StringIO
import logging
from pathlib import Path
import re
from typing import Generator, List, Optional, Tuple
from zipfile import ZipFile

import click
import polars as pl

from .infer_schema import infer_schema, clean_for_sql_name
from .utils import generate_sql_create_table_ddl, schema_as_table

PPMI_CSV_ENCODING = "cp1252"


_logger = logging.getLogger()



def _file_arguments(f):
    """decorator containing common file argument/option decorations"""
    @click.argument('file_paths', nargs=-1, type=str)
    @click.option('--zip-file', "-z", type=click.Path(exists=True), default=None,
                  help='Optional zip file containing additional data.')
    def wrapper(*args, **kwargs):
        return f(*args, **kwargs)
    return wrapper


@click.group()
@click.option(
    "--verbose", "-v", count=True, help="Enable verbose output for debugging."
)
def cli(verbose):
    """
    A versatile CLI tool with subcommands.

    Global options apply to all subcommands.
    """
    import coloredlogs

    log_levels = "WARNING INFO DEBUG".split()
    level = log_levels[verbose] if verbose < len(log_levels) else log_levels[-1]

    coloredlogs.install(level=level)

    click.get_current_context().obj = {'verbose': verbose}


@cli.command("generate-schema")
@_file_arguments
def generate_schema(file_paths: list[str], zip_file: Optional[Path]):
    for csv_path, csv_content in file_generator(file_paths=file_paths, zip_file=zip_file):
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
@_file_arguments
def generate_copy(file_paths: list[str], zip_file: Optional[Path]):
    for csv_path, csv_content in file_generator(file_paths=file_paths, zip_file=zip_file):
        table_name = Path(csv_path).stem
        table_name = clean_for_sql_name(table_name)
        table_name = re.sub(r"_\d{8}$", r"", table_name)
        print(f"COPY {table_name} from STDIN with (format csv, header true);")
        print(csv_content)
        print("\\.")


@cli.command("load-csv")
@click.argument("csv_path", type=click.Path(exists=True, dir_okay=False, readable=True))
@click.option("--uri", type=str, required=True)
def load_csv(uri: str, csv_path: str):
    import psycopg

    con = psycopg.connect(uri)
    cur = con.cursor()

    table_name = Path(csv_path).stem
    table_name = clean_for_sql_name(table_name)
    table_name = re.sub(r"_\d{8}$", r"", table_name)

    csv_content = BROKEN_read_csv_content(csv_path)

    csv_rdr = csv.reader(StringIO(csv_content))
    header = next(csv_rdr)
    query = f"COPY {table_name} ({','.join(header)}) from STDIN"
    query += " WITH (FORMAT CSV, HEADER)"

    try:
        with cur.copy(query) as copy:
            copy.write(csv_content)
        con.commit()
    except Exception as e:
        con.cancel()
        header = next(StringIO(csv_content)).strip()
        debug_info = f"""
        {csv_path=}
        {len(header)=}"""
        if len(header) == 1024:
            debug_info += "\nThe header appears to be truncated and the file is likely corrupt"
        raise RuntimeError(f"Error reading CSV file '{csv_path}': {e}" + "\n" + debug_info)


@cli.command("load-zip")
@click.option("--uri", type=str, required=True)
@click.option("--create-table", "-t", is_flag=True)
def load_zip(uri: str, zipfile_path: str, files: list, create_table: bool):
    """Load zipfile specified by ZIPFILE_PATH into database specified by --uri.  By default, all *.csv files
    in the zipfile are loaded. FILES, if specified,
    is used to filter data to be loaded."""
    import psycopg
    from zipfile import ZipFile

    file_set = set(files)

    con = psycopg.connect(uri)

    errors = []
    zf = ZipFile(zip_path)
    for zi in zf.filelist:
        csv_path = zi.filename
        if file_set and csv_path not in file_set:
            _logger.debug(f"{csv_path}: Not in requested files")
            continue

        table_name = Path(csv_path).stem
        table_name = clean_for_sql_name(table_name)
        table_name = re.sub(r"_\d{8}$", r"", table_name)

        csv_content = fix_csv_content(csv_path, zf.read(zi).decode(PPMI_CSV_ENCODING))

        if create_table:
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
                _logger.warning(f"Error reading CSV file '{csv_path}': {e}" + "\n" + debug_info)
                errors.append(table_name + " (" + str(e) + ")")
                continue
            schema = infer_schema(df)
            schema_ddl = generate_sql_create_table_ddl(schema, table_name)
            with con.cursor() as cur:
                cur.execute(schema_ddl.encode())
                con.commit()

        csv_rdr = csv.reader(StringIO(csv_content))
        header = next(csv_rdr)
        query = f"COPY {table_name} ({','.join(header)}) from STDIN"
        query += " WITH (FORMAT CSV, HEADER)"

        try:
            with con.cursor().copy(query.encode()) as copy:
                copy.write(csv_content)
            con.commit()
            _logger.info(f"{table_name}: Loaded and committed")
        except Exception as e:
            con.cancel()
            header = next(StringIO(csv_content)).strip()
            debug_info = f"""
            {csv_path=}
            {len(header)=}"""
            if len(header) == 1024:
                debug_info += "\nThe header appears to be truncated and the file is likely corrupt"
            _logger.error(f"Error loading '{csv_path}': {e}" + "\n" + debug_info)
            errors.append(table_name + " (" + str(e) + ")")
        
    if errors:
        _logger.critical("\n. ".join([f"{len(errors)} errors:"] + errors))
        return 1



def file_generator(file_paths: List[str], zip_file: Optional[Path]) -> Generator[Tuple[str, str]]:
    if zip_file:
        yield from zip_file_generator(zipfile_path=zip_file, file_paths=file_paths)
    else:
        yield from local_file_generator(file_paths)

def local_file_generator(file_paths: List[str]) -> Generator[Tuple[str, str]]:
    """
    Yields (path, content) tuples for each file in FILE_PATHS.  The content is
    decoded and processed to fix known bugs in the CSV files

    Args:
        file_paths: A list of string paths to the files to read.

    Yields:
        A tuple where the first element is the file path (str) and the second
        element is the content of the file (str).
    """

    for csv_path in file_paths:
        csv_content = open(csv_path, encoding=PPMI_CSV_ENCODING).read()
        csv_content = fix_csv_content(csv_path=csv_path, csv_content=csv_content)
        yield (csv_path, csv_content)

def zip_file_generator(zipfile_path: str, file_paths: List[str]) -> Generator[Tuple[str, str]]:
    """
    Yields (path, content) tuples for each file in the archive ZIPFILE_PATH.
    The content is decoded and processed to fix known bugs in the CSV files.  By default, all files
    are extracted; if FILE_PATHS is not empty, only the named files are extracted.

    Args:
        zipfile_path: The string path to a zipfile.
        file_paths: A list of string paths to the files to read.

    Yields:
        A tuple where the first element is the file path (str) and the second
        element is the content of the file (str).
    """

    file_set = set(file_paths)

    zf = ZipFile(zipfile_path)
    for zi in zf.filelist:
        csv_path = zi.filename
        if file_set and csv_path not in file_set:
            _logger.debug(f"{csv_path}: Not in requested files")
            continue
        csv_content = fix_csv_content(csv_path, zf.read(zi).decode(PPMI_CSV_ENCODING))
        yield (csv_path, csv_content)

def fix_csv_content(csv_path: str, csv_content: str) -> str:
    csv_content = csv_content.replace('\\"', '""').strip()
    if "Primary_Clinical_Diagnosis_" in csv_path:
        # In Primary_Clinical_Diagnosis_20250401.csv at least, the CSV is invalid
        csv_content = csv_content.replace('no tremors today. \\"",,"3",', 'no tremors today.",,"3",')
    return csv_content


if __name__ == "__main__":
    cli()
