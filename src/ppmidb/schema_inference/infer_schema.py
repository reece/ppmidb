from dataclasses import dataclass, field
import re
from typing import Optional, Tuple, List, Union

import polars as pl


@dataclass
class ColumnSchema:
    """
    Represents the inferred schema for a single column.
    """

    csv_name: str  # Original name from CSV
    sql_name: str  # Legal SQL column name (lowercased, special chars replaced)
    polars_type: pl.DataType
    is_nullable: bool
    value_range: Optional[Tuple[Union[int, float], Union[int, float]]] = (
        None  # (min_value, max_value) for numeric types
    )
    sql_type: str = ""  # Inferred optimal SQL type (will be populated after creation)


def _clean_for_sql_name(csv_name: str) -> str:
    """
    Converts a CSV column name into a legal, lowercase SQL column name.
    - Converts to lowercase.
    - Replaces non-alphanumeric characters (except underscore) with underscores.
    - Handles multiple consecutive underscores.
    - Removes leading/trailing underscores.
    """
    # Convert to lowercase
    sql_name = csv_name.lower()
    # Replace non-alphanumeric (and not underscore) characters with underscore
    sql_name = re.sub(r"[^a-z0-9_]+", "_", sql_name)
    # Replace multiple consecutive underscores with a single underscore
    sql_name = re.sub(r"_+", "_", sql_name)
    # Remove leading/trailing underscores
    sql_name = sql_name.strip("_")
    # Ensure it's not empty after cleaning; if so, provide a default
    if not sql_name:
        sql_name = "column"  # Fallback if name becomes empty
    return sql_name


def get_optimal_sql_type(column_schema: ColumnSchema) -> str:
    """
    Computes the optimal PostgreSQL SQL data type string from a ColumnSchema record.
    Args:
        column_schema (ColumnSchema): A dataclass instance representing the column's inferred schema.
    Returns:
        str: The PostgreSQL SQL data type string (e.g., "INTEGER NOT NULL", "TEXT NULL").
    """
    polars_type = column_schema.polars_type
    is_nullable = column_schema.is_nullable
    value_range = column_schema.value_range
    sql_type_base = "TEXT"  # Default fallback SQL type

    base_polars_type = polars_type.base_type()

    if base_polars_type == pl.Int8:
        sql_type_base = "SMALLINT"
    elif base_polars_type == pl.Int16:
        sql_type_base = "SMALLINT"
    elif base_polars_type == pl.Int32:
        sql_type_base = "INTEGER"
    elif base_polars_type == pl.Int64:
        if value_range:
            min_val, max_val = value_range
            # Ensure values are int/float for comparison, otherwise default to BIGINT
            if isinstance(min_val, (int, float)) and isinstance(max_val, (int, float)):
                # PostgreSQL SMALLINT range: -32768 to +32767
                if min_val >= -32768 and max_val <= 32767:
                    sql_type_base = "SMALLINT"
                # PostgreSQL INTEGER range: -2147483648 to +2147483647
                elif min_val >= -2147483648 and max_val <= 2147483647:
                    sql_type_base = "INTEGER"
                else:
                    sql_type_base = "BIGINT"
            else:
                sql_type_base = "BIGINT"
        else:
            sql_type_base = "BIGINT"
    elif base_polars_type == pl.UInt8:
        sql_type_base = "SMALLINT"
    elif base_polars_type == pl.UInt16:
        sql_type_base = "INTEGER"
    elif base_polars_type == pl.UInt32:
        sql_type_base = "BIGINT"
    elif base_polars_type == pl.UInt64:
        # UInt64 can exceed BIGINT range, so NUMERIC is safer for PostgreSQL
        sql_type_base = "NUMERIC(20, 0)"
    elif base_polars_type == pl.Float32:
        sql_type_base = "REAL"
    elif base_polars_type == pl.Float64:
        sql_type_base = "DOUBLE PRECISION"
    elif base_polars_type == pl.String:
        sql_type_base = "TEXT"
    elif base_polars_type == pl.Boolean:
        sql_type_base = "BOOLEAN"
    elif base_polars_type == pl.Date:
        sql_type_base = "DATE"
    elif base_polars_type == pl.Datetime:
        sql_type_base = "TIMESTAMP WITHOUT TIME ZONE"  # Default for Polars datetime without explicit tz
    elif base_polars_type == pl.Time:
        sql_type_base = "TIME"
    elif base_polars_type == pl.Decimal:
        # If precision/scale are available, use them.
        # Otherwise, a generic NUMERIC.
        if hasattr(polars_type, "precision") and hasattr(polars_type, "scale"):
            precision = (
                polars_type.precision if polars_type.precision is not None else 38
            )
            scale = polars_type.scale if polars_type.scale is not None else 10
            sql_type_base = f"NUMERIC({precision}, {scale})"
        else:
            sql_type_base = "NUMERIC"
    elif base_polars_type == pl.List:
        sql_type_base = "JSONB"  # Flexible for lists of varying types/structures
    elif base_polars_type == pl.Struct:
        sql_type_base = "JSONB"  # Flexible for structured data

    # Add nullability constraint
    null_constraint = "NULL" if is_nullable else "NOT NULL"

    return f"{sql_type_base} {null_constraint}"


def infer_schema(df: pl.DataFrame) -> List[ColumnSchema]:
    """
    Infers schema from polars dataframe
    including CSV name, SQL name, Polars type, nullability, range, and optimal SQL type.
    """

    inferred_schema: List[ColumnSchema] = []

    for csv_col_name, polars_dtype in df.schema.items():
        # Infer nullability
        is_nullable = df[csv_col_name].is_null().any()

        # Determine numeric range
        value_range: Optional[Tuple[Union[int, float], Union[int, float]]] = None
        if polars_dtype.is_numeric():
            min_val = df[csv_col_name].min()
            max_val = df[csv_col_name].max()
            if min_val is not None and max_val is not None:
                value_range = (min_val, max_val)

        # Create a preliminary ColumnSchema object to pass to get_optimal_sql_type
        # We'll populate sql_type in the next step
        temp_col_schema = ColumnSchema(
            csv_name=csv_col_name,
            sql_name=_clean_for_sql_name(csv_col_name),  # Populate sql_name here
            polars_type=polars_dtype,
            is_nullable=is_nullable,
            value_range=value_range,
        )

        # Now, infer the optimal SQL type using the helper function
        temp_col_schema.sql_type = get_optimal_sql_type(temp_col_schema)

        inferred_schema.append(temp_col_schema)

    return inferred_schema
