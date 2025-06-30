#!/usr/bin/env python3

"""process PPMI zip file and standalone csv files

ppmidb -v load -t --uri postgresql://localhost/ppmidb -z data/PPMI_20250401.gz

Or, load to Cloud SQL:

export PGPASSWORD=PG#4Zz...

"""


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



@click.group()
@click.option(
    "--verbose", "-v", count=True, help="Enable verbose output for debugging."
)
def cli(verbose):
    """Parse PPMI CSV files to generate schema (DDL), database loading (DML), or directly load into PostgreSQL
    """
    import coloredlogs

    log_levels = "WARNING INFO DEBUG".split()
    level = log_levels[verbose] if verbose < len(log_levels) else log_levels[-1]

    coloredlogs.install(level=level)

    click.get_current_context().obj = {'verbose': verbose}


@cli.command("generate-ddl")
@click.argument('file_paths', nargs=-1, type=str)
@click.option('--zip-file', "-z", type=click.Path(exists=True), default=None,
                help='Optional zip file containing additional data.')
def generate_ddl(file_paths: list[str], zip_file: Optional[Path]):
    """Infer schema from CSV data and output DDL (CREATE TABLE and selected indexes)
    
    """
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


@cli.command("generate-dml")
@click.argument('file_paths', nargs=-1, type=str)
@click.option('--zip-file', "-z", type=click.Path(exists=True), default=None,
                help='Optional zip file containing additional data.')
def generate_dml(file_paths: list[str], zip_file: Optional[Path]):
    """Generate DML as COPY with inline data to load database"""
    for csv_path, csv_content in file_generator(file_paths=file_paths, zip_file=zip_file):
        table_name = Path(csv_path).stem
        table_name = clean_for_sql_name(table_name)
        table_name = re.sub(r"_\d{8}$", r"", table_name)

        header = next(csv.reader(StringIO(csv_content)))
        columns = ','.join(map(lambda s: f'"{clean_for_sql_name(s)}"', header))
        print(f"COPY {table_name} ({columns}) from STDIN with (format csv, header true);")
        print(csv_content)
        print("\\.")


@cli.command("load")
@click.option("--uri", type=str, required=True)
@click.option("--create-table", "-t", is_flag=True)
@click.argument('file_paths', nargs=-1, type=str)
@click.option('--zip-file', "-z", type=click.Path(exists=True), default=None,
                help='Optional zip file containing additional data.')
def load(uri: str, zip_file: str, file_paths: list, create_table: bool):
    """Load zipfile specified by ZIPFILE_PATH into database specified by --uri.  By default, all *.csv files
    in the zipfile are loaded. FILES, if specified,
    is used to filter data to be loaded."""
    errors = []

    if uri.startswith("postgresql://"):
        import psycopg
        con = psycopg.connect(uri)

        for csv_path, csv_content in file_generator(file_paths=file_paths, zip_file=zip_file):
            table_name = Path(csv_path).stem
            table_name = clean_for_sql_name(table_name)
            table_name = re.sub(r"_\d{8}$", r"", table_name)

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
            columns = ','.join(map(lambda s: f'"{clean_for_sql_name(s)}"', header))
            query = f"COPY {table_name} ({columns.lower()}) from STDIN"
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
    elif uri.startswith("bigquery://"):
        from google.cloud import bigquery
        # Extract project and dataset from the URI
        # Assuming the format bigquery://project_id/dataset_id
        try:
            # Split off "bigquery://"
            bq_path = uri[len("bigquery://"):]
            project_id, dataset_id = bq_path.split("/")
        except ValueError:
            _logger.critical(f"Invalid BigQuery URI format: {uri}. Expected bigquery://project_id/dataset_id")
            return 1

        client = bigquery.Client(project=project_id)
        
        for csv_path, csv_content in file_generator(file_paths=file_paths, zip_file=zip_file):
            table_name = Path(csv_path).stem
            table_name = clean_for_sql_name(table_name)
            table_name = re.sub(r"_\d{8}$", r"", table_name)

            table_id = f"{project_id}.{dataset_id}.{table_name}"
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1, # Assuming header is always present
                autodetect=True, # Autodetect schema
            )

            try:
                # BigQuery client expects bytes-like object or a file-like object
                # We need to re-read the string content as bytes for the load job
                csv_file_obj = StringIO(csv_content)
                job = client.load_table_from_file(csv_file_obj, table_id, job_config=job_config)
                job.result() # Waits for the job to complete
                _logger.info(f"{table_name}: Loaded into BigQuery table {table_id}")
            except Exception as e:
                _logger.error(f"Error loading '{csv_path}' into BigQuery: {e}")
                errors.append(table_name + " (" + str(e) + ")")

    else:
        _logger.critical(f"Unsupported database URI scheme: {uri}")
        return 1
        
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