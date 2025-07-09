CREATE OR REPLACE FUNCTION unique_table_pag_name_pairs_tf()
RETURNS TABLE (table_name text, pag_name text)
LANGUAGE plpgsql AS $$
DECLARE
    -- This variable will hold the aggregated UNION ALL query string.
    v_combined_query text := ''; -- Initialize to empty string
    r record; -- To loop through tables found in information_schema
    first_table boolean := TRUE; -- Flag to manage UNION ALL
BEGIN
    -- 1. Dynamically build the UNION ALL query string within the function.
    -- This is the same logic that was in your _table_pag_name_query_v view.
    FOR r IN
        SELECT
            c.table_schema,
            c.table_name
        FROM
            information_schema.columns c
        JOIN
            information_schema.tables t ON c.table_schema = t.table_schema AND c.table_name = t.table_name
        WHERE
            c.column_name = 'pag_name'
            AND c.table_schema NOT IN ('pg_catalog', 'information_schema')
            AND c.table_name NOT IN ('data_dictionary_annotated', 'data_dictionary_harmonized')
            AND t.table_type = 'BASE TABLE'
    LOOP
        IF NOT first_table THEN
            v_combined_query := v_combined_query || E'\nUNION ALL\n';
        END IF;

        v_combined_query := v_combined_query ||
                            format('SELECT DISTINCT %L AS table_name, pag_name FROM %I.%I',
                                   r.table_name, r.table_schema, r.table_name);

        first_table := FALSE;
    END LOOP;

    -- Optional: Handle the case where no tables with 'pag_name' are found.
    IF v_combined_query = '' THEN
        RAISE NOTICE 'No tables with a "pag_name" column were found. Returning empty set.';
        -- If no query is generated, we can just return an empty set.
        RETURN;
    END IF;

    -- 2. Execute the dynamically generated query and return its results.
    RETURN QUERY EXECUTE v_combined_query;

END;
$$;


create or replace view unique_table_pag_name_pairs_v as select pag_name, table_name from unique_table_pag_name_pairs_tf() order by 1,2;
create or replace view table_pag_names_v as select table_name, count(*) as n, string_agg(pag_name, ', ') as pag_names from unique_table_pag_name_pairs_v group by 1;
create or replace view pag_name_tables_v as select pag_name, count(*) as n, string_agg(table_name, ', ') as table_names from unique_table_pag_name_pairs_v group by 1;

--comment on view table_names_v is 'unique table name, pag_name pairs'


