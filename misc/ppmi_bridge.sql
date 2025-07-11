CREATE OR REPLACE FUNCTION cl_lookup(f_mod_name text, f_itm_name text, f_code text)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE STRICT
AS $function$
select coalesce(CL.decode_bridge, CL.decode) from code_list_bridge CL where CL.mod_name=f_mod_name and CL.itm_name=f_itm_name and CL.code=f_code;
$function$;



CREATE OR REPLACE FUNCTION convert_mm_yyyy_to_yyyy_mm(input_date_str TEXT)
RETURNS TEXT AS $$
BEGIN
    IF input_date_str ~ '^\d{2}/\d{4}$' THEN
        RETURN TO_CHAR(TO_DATE(input_date_str, 'MM/YYYY'), 'YYYY-MM');
    ELSE
        RETURN input_date_str;
    END IF;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION safe_cast_to_boolean(
    p_input_string text
) RETURNS boolean AS $$
DECLARE
    v_result boolean;
BEGIN
    v_result := p_input_string::boolean;
    RETURN v_result;
EXCEPTION
    WHEN invalid_text_representation THEN
        RETURN NULL;
    WHEN others THEN
        RAISE WARNING 'Unexpected error during safe_cast_to_boolean_via_exception for input "%": %', p_input_string, SQLERRM;
        RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


create or replace view demographics as 
select
      rec_id
    , patno -- as-is
    -- , event_id
    -- , pag_name
    , infodt -- as-is
    , safe_cast_to_boolean(cl_lookup('SCREEN', 'AFICBERB', aficberb::text)) as aficberb
    , safe_cast_to_boolean(cl_lookup('SCREEN', 'ASHKJEW', ashkjew::text)) as ashkjew
    , safe_cast_to_boolean(cl_lookup('SCREEN', 'BASQUE', basque::text)) as basque
    , convert_mm_yyyy_to_yyyy_mm(birthdt) as birthdt
    , cl_lookup('SCREEN', 'SEX', sex::text) as sex
    -- , chldbear
    , cl_lookup('SCREEN','HOWLIVE', howlive::text) as gender
    -- , gayles
    -- , hetero
    -- , bisexual
    -- , pansexual
    -- , asexual
    -- , othsexuality
    , cl_lookup('SCREEN','HANDED',handed::text) as handed
    , cl_lookup('SCREEN','HISPLAT',hisplat::text) as ethnicity
    , CASE
        WHEN (raasian + rablack + rahawopi + raindals + ranos + rawhite + raunknown) = 0 THEN 'Decline to Answer'
        WHEN (raasian + rablack + rahawopi + raindals + ranos + rawhite + raunknown) > 1 THEN 'Two or More Races'
        ELSE
            CASE
                WHEN raasian = 1 THEN cl_lookup('SCREEN', 'RAASIAN', 1::text)
                WHEN rablack = 1 THEN cl_lookup('SCREEN', 'RABLACK', 1::text)
                WHEN rahawopi = 1 THEN cl_lookup('SCREEN', 'RAHAWOPI', 1::text)
                WHEN raindals = 1 THEN cl_lookup('SCREEN', 'RAINDALS', 1::text)
                WHEN ranos = 1 THEN cl_lookup('SCREEN', 'RANOS', 1::text)
                WHEN rawhite = 1 THEN cl_lookup('SCREEN', 'RAWHITE', 1::text)
                WHEN raunknown = 1 THEN cl_lookup('SCREEN', 'RAUNKNOWN', 1::text)
                ELSE NULL
            END
    END AS race
    , convert_mm_yyyy_to_yyyy_mm(orig_entry) as orig_entry
    , last_update
    , (select max(educyrs::smallint) from socio_economics SE where SE.patno=D.patno) as educyrs
from ppmi_20250401.demographics D;
