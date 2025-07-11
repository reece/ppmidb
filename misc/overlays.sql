create table if not exists _code_list_overlay (
    mod_name text not null,
    itm_name text not null,
    code text not null,
    decode text not null,
    mapping_notes text
);

create table if not exists _data_dictionary_overlay (
    mod_name text not null,
    itm_name text not null,
    dscr text,
    mapping_notes text
);

create or replace view code_list_v as
select
    CLA.mod_name,
    CLA.itm_name,
    CLA.code,
    coalesce(CLO.decode, CLA.decode) as decode,
    coalesce(CLO.mapping_notes, CLA.mapping_notes) as mapping_notes
from
    code_list_annotated CLA
    left join _code_list_overlay CLO on (CLA.mod_name = CLO.mod_name
    and CLA.itm_name = CLO.itm_name
    and CLA.code = CLO.code);

create or replace view data_dictionary_v as
select
    DDA.mod_name,
    DDA.itm_name,
    coalesce(DDO.dscr, DDA.dscr) as dscr,
    coalesce(DDO.mapping_notes, DDA.mapping_notes) as mapping_notes
from
    data_dictionary_annotated DDA
    left join _data_dictionary_overlay DDO on (DDA.mod_name = DDO.mod_name
    and DDA.itm_name = DDO.itm_name);

CREATE OR REPLACE FUNCTION code_lookup (
    f_mod_name text,
    f_itm_name text,
    f_code text
) RETURNS text LANGUAGE sql IMMUTABLE STRICT AS $function$
select code from code_list_v
where mod_name=f_mod_name and itm_name=f_itm_name and code=f_code;
$function$;
