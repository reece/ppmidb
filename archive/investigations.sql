create or replace view data_dict_shared_descriptions_v as
select
    itm_name,
    lower(dscr) as dscr,
    count(mod_name) as n_mod_names,
    string_agg(mod_name, ', ') as mod_names
from
    data_dictionary_annotated
where
    itm_name ~ '\w'
group by
    1,
    2
order by
    1,
    2 desc;

comment on view data_dict_shared_descriptions_v is 'show items and descriptions shared across all mods (tables)';

create or replace view data_dict_ambiguous_items_v as
select
    itm_name,
    substr(dscr, 0, 50) as "dscr (truncated)",
    n_mod_names,
    mod_names
from
    data_dict_shared_descriptions_v
where
    itm_name in (
        select
            itm_name
        from
            data_dict_shared_descriptions_v
        group by
            1
        having
            count(*) > 1
    );

comment on view data_dict_ambiguous_items_v is 'show all items with ambiguous descriptions';

create or replace view code_list_summary_v as
select
    itm_name,
    mod_name,
    string_agg(
        code || '=' || lower(decode),
        ','
        order by
            code
    ) as code_mapping
from
    code_list_annotated
where
    itm_name not in ('PAG_NAME')
group by
    1,
    2
order by
    itm_name;

comment on view code_list_summary_v is 'code list values, one row per (mod_name,itm_name) pair';

create or replace view code_list_shared_definitions_v as
select
    itm_name,
    count(mod_name) as n_mod_names,
    string_agg(mod_name, ', ') as mod_names,
    code_mapping
from
    code_list_summary_v
group by
    1,
    4
order by
    1,
    2 desc;

comment on view code_list_shared_definitions_v is 'code list items with shared (identical) names and definitions';

create or replace view code_list_variable_definitions_v as
select
    *
from
    code_list_shared_definitions_v
where
    itm_name in (
        select
            itm_name
        from
            code_list_shared_definitions_v
        group by
            1
        having
            count(*) > 1
    );

comment on view code_list_variable_definitions_v is 'code list items with variable definitions in multiple mods (tables)';

create or replace view mod_item_summary_v as
select
    DDA.mod_name,
    DDA.itm_name,
    lower(DDA.dscr) as dscr,
    CLS.code_mapping
from
    data_dictionary_annotated DDA
    left join code_list_summary_v CLS on DDA.mod_name = CLS.mod_name
    and DDA.itm_name = CLS.itm_name
where
    DDA.itm_name ~ '\w'
    and DDA.itm_name not in ('PAG_NAME');

comment on view mod_item_summary_v is 'summary of <mod,item> pairs with descriptions and flattened code mapping';

CREATE OR REPLACE FUNCTION unique_table_pag_name_pairs_tf () RETURNS TABLE (table_name text, pag_name text) LANGUAGE plpgsql AS $$
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

create or replace view unique_table_pag_name_pairs_v as
select
    pag_name,
    table_name
from
    unique_table_pag_name_pairs_tf ()
order by
    1,
    2;

create or replace view table_pag_names_v as
select
    table_name,
    count(*) as n,
    string_agg(pag_name, ', ') as pag_names
from
    unique_table_pag_name_pairs_v
group by
    1;

create or replace view pag_name_tables_v as
select
    pag_name,
    count(*) as n,
    string_agg(table_name, ', ') as table_names
from
    unique_table_pag_name_pairs_v
group by
    1;

--comment on view table_names_v is 'unique table name, pag_name pairs'
