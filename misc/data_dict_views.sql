create or replace view data_dict_shared_descriptions_v as
select
    itm_name,
    lower(dscr) as dscr,
    count(mod_name) as n_mod_names,
    string_agg(mod_name, ', ') as mod_names
from data_dictionary_annotated
where itm_name~'\w'
group by 1,2
order by 1, 2 desc;

comment on view data_dict_shared_descriptions_v is 'show items and descriptions shared across all mods (tables)';


create or replace view data_dict_ambiguous_items_v as
select
    itm_name,
    substr(dscr,0,50) as "dscr (truncated)",
    n_mod_names,
    mod_names
from data_dict_shared_descriptions_v
where itm_name in (select itm_name from data_dict_shared_descriptions_v group by 1 having count(*)>1);

comment on view data_dict_ambiguous_items_v is 'show all items with ambiguous descriptions';


create or replace view code_list_summary_v as
    select itm_name, mod_name, string_agg(code || '=' || lower(decode), ',' order by code) as code_mapping
from code_list_annotated
where itm_name not in ('PAG_NAME')
group by 1,2
order by itm_name;

comment on view code_list_summary_v is 'code list values, one row per (mod_name,itm_name) pair';


create or replace view code_list_shared_definitions_v as
select
    itm_name,
    count(mod_name) as n_mod_names,
    string_agg(mod_name, ', ') as mod_names,
    code_mapping
from code_list_summary_v
group by 1,4
order by 1,2 desc;

comment on view code_list_shared_definitions_v is 'code list items with shared (identical) names and definitions';


create or replace view code_list_variable_definitions_v as
select
    *
from code_list_shared_definitions_v
where itm_name in (select itm_name from code_list_shared_definitions_v group by 1 having count(*) > 1);

comment on view code_list_variable_definitions_v is 'code list items with variable definitions in multiple mods (tables)';


create or replace view mod_item_summary_v as
select
    DDA.mod_name,
    DDA.itm_name,
    lower(DDA.dscr) as dscr,
    CLS.code_mapping
from data_dictionary_annotated DDA
left join code_list_summary_v CLS
    on DDA.mod_name=CLS.mod_name and DDA.itm_name=CLS.itm_name
where DDA.itm_name~'\w' and DDA.itm_name not in ('PAG_NAME');

comment on view mod_item_summary_v is 'summary of <mod,item> pairs with descriptions and flattened code mapping';

