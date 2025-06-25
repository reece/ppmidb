import logging
from typing import Optional, List

from .infer_schema import ColumnSchema

_logger = logging.getLogger()

indexed_columns = [
    "patno"
]


def schema_as_table(schema_records: List[ColumnSchema]) -> str:
    """
    Prints a list of ColumnSchema records as a formatted text table.
    Includes CSV Name, SQL Name, Polars Type, Nullable, Value Range, and Optimal SQL Type.
    Args:
        schema_records (List[ColumnSchema]): A list of dataclass instances
                                             representing the inferred schema.
    """
    if not schema_records:
        return "No schema records to display."

    table_str = ""

    headers = [
        "CSV Name",
        "SQL Name",
        "Polars Type",
        "Nullable",
        "Value Range",
        "Optimal SQL Type",
    ]

    # Calculate column widths dynamically
    col_widths = {header: len(header) for header in headers}

    for col_schema in schema_records:
        polars_type_str = str(col_schema.polars_type)
        is_nullable_str = "Yes" if col_schema.is_nullable else "No"
        value_range_str = (
            f"({col_schema.value_range[0]} to {col_schema.value_range[1]})"
            if col_schema.value_range
            else "N/A"
        )

        col_widths["CSV Name"] = max(col_widths["CSV Name"], len(col_schema.csv_name))
        col_widths["SQL Name"] = max(col_widths["SQL Name"], len(col_schema.sql_name))
        col_widths["Polars Type"] = max(col_widths["Polars Type"], len(polars_type_str))
        col_widths["Nullable"] = max(col_widths["Nullable"], len(is_nullable_str))
        col_widths["Value Range"] = max(col_widths["Value Range"], len(value_range_str))
        col_widths["Optimal SQL Type"] = max(
            col_widths["Optimal SQL Type"], len(col_schema.sql_type)
        )

    # Print header
    header_line = (
        f"{headers[0]:<{col_widths['CSV Name']}} | "
        f"{headers[1]:<{col_widths['SQL Name']}} | "
        f"{headers[2]:<{col_widths['Polars Type']}} | "
        f"{headers[3]:<{col_widths['Nullable']}} | "
        f"{headers[4]:<{col_widths['Value Range']}} | "
        f"{headers[5]:<{col_widths['Optimal SQL Type']}}"
    )
    table_str += header_line + "\n"
    table_str += "-" * len(header_line) + "\n"

    # Print data rows
    for col_schema in schema_records:
        polars_type_str = str(col_schema.polars_type)
        is_nullable_str = "Yes" if col_schema.is_nullable else "No"
        value_range_str = (
            f"({col_schema.value_range[0]} to {col_schema.value_range[1]})"
            if col_schema.value_range
            else "N/A"
        )

        row_line = (
            f"{col_schema.csv_name:<{col_widths['CSV Name']}} | "
            f"{col_schema.sql_name:<{col_widths['SQL Name']}} | "
            f"{polars_type_str:<{col_widths['Polars Type']}} | "
            f"{is_nullable_str:<{col_widths['Nullable']}} | "
            f"{value_range_str:<{col_widths['Value Range']}} | "
            f"{col_schema.sql_type:<{col_widths['Optimal SQL Type']}}"
        )
        table_str += row_line + "\n"
    return table_str


def generate_sql_create_table_ddl(
    schema_records: List[ColumnSchema],
    table_name: str,
    primary_key_sql_name: Optional[str] = None,
) -> str:
    """
    Generates a PostgreSQL CREATE TABLE DDL statement from a list of ColumnSchema records.
    Args:
        schema_records (List[ColumnSchema]): A list of dataclass instances representing the inferred schema.
    table_name (str): The desired name for the SQL table.
        primary_key_sql_name (Optional[str]): The `sql_name` of the column to set as PRIMARY KEY.
    If None, no primary key constraint is added.

    Returns:
        str: The complete PostgreSQL CREATE TABLE DDL statement.
    """
    if not schema_records:
        return f"CREATE TABLE {table_name} (); -- No columns inferred from schema."

    column_definitions = []
    existing_sql_names = {col.sql_name for col in schema_records}
    for col_schema in schema_records:
        column_definitions.append(f'    "{col_schema.sql_name}" {col_schema.sql_type}')

    if primary_key_sql_name:
        if primary_key_sql_name in existing_sql_names:
            column_definitions.append(f'    PRIMARY KEY ("{primary_key_sql_name}")')
        else:
            _logger.warning(
                f"Primary key column '{primary_key_sql_name}' not found in inferred SQL names."
            )

    ddl_statement = f'CREATE TABLE "{table_name}" (\n'
    ddl_statement += ",\n".join(column_definitions)
    ddl_statement += "\n);\n\n"

    for col_name in existing_sql_names:
        if col_name in indexed_columns:
            index_name = f"idx_{table_name}_{col_name}"
            ddl_statement += f'CREATE INDEX "{index_name}" ON "{table_name}" ("{col_name}");\n'

    return ddl_statement
